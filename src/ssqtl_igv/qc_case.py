from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from .bundles import StageBundle, logical_bundle_reference, verify_stage_bundle
from .config import WorkflowConfig
from .contracts import PIPELINE_VERSION, validate_case_result_document, validate_task_document
from .evidence_qc import evaluate_evidence
from .qc import inspect_png
from .utils import atomic_write_json, sha256_file, utc_now


STAGE_KEYS = {
    "VALIDATE_CASE_INPUTS": "validate_case_inputs",
    "RUN_IGV": "run_igv",
    "COMPOSE_CASE": "compose_case",
}


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read JSON evidence {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"JSON evidence is not an object: {path}")
    return value


def _artifact(role: str, path: Path, logical_path: str) -> dict[str, Any]:
    return {
        "role": role,
        "relative_path": logical_path,
        "sha256": sha256_file(path),
        "size": path.stat().st_size,
    }


def _deduplicated_warnings(results: list[dict[str, Any]]) -> list[dict[str, str]]:
    observed: set[tuple[str, str]] = set()
    warnings: list[dict[str, str]] = []
    for result in results:
        for warning in result["warnings"]:
            key = (warning["code"], warning["message"])
            if key not in observed:
                observed.add(key)
                warnings.append(dict(warning))
    return warnings


def _causal_failures(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for result in results:
        if result["status"] == "DOMAIN_FAILED":
            return [
                {
                    "stage": result["stage"],
                    "class": failure["class"],
                    "code": failure["code"],
                    "message": failure["message"],
                    "rerun_eligible": failure["rerun_eligible"],
                }
                for failure in result["failures"]
                if not failure["code"].startswith("UPSTREAM_")
            ] or [
                {
                    "stage": result["stage"],
                    "class": failure["class"],
                    "code": failure["code"],
                    "message": failure["message"],
                    "rerun_eligible": failure["rerun_eligible"],
                }
                for failure in result["failures"]
            ]
    return []


def _bundle_references(
    task: dict[str, Any],
    bundle_rows: list[tuple[dict[str, Any], Path]],
) -> dict[str, Any]:
    references: dict[str, Any] = {}
    for result, root in bundle_rows:
        stage = result["stage"]
        references[STAGE_KEYS[stage]] = logical_bundle_reference(
            result,
            root,
            f"stages/{STAGE_KEYS[stage]}/{task['task_id']}/stage_result.json",
        )
    return references


def _case_result(
    task: dict[str, Any],
    *,
    shard_id: str,
    session_id: str,
    attempt: int,
    render_state: str,
    evidence_state: str,
    artifacts: list[dict[str, Any]],
    stage_bundles: dict[str, Any],
    warnings: list[dict[str, str]],
    failures: list[dict[str, Any]],
) -> dict[str, Any]:
    empty_groups = [
        genotype
        for genotype in ("0/0", "0/1", "1/1")
        if task["genotype_groups"][genotype]["empty"]
    ]
    rerun_eligible = any(failure["rerun_eligible"] for failure in failures)
    rerun_reason = (
        ";".join(f"{failure['stage']}:{failure['code']}" for failure in failures)
        if rerun_eligible
        else None
    )
    scientific_state = (
        "INDETERMINATE"
        if evidence_state == "EVIDENCE_INCOMPLETE"
        or (empty_groups and evidence_state != "COMPLETE")
        else "PENDING"
    )
    result = {
        "schema_version": "2.0",
        "pipeline_version": PIPELINE_VERSION,
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "shard_id": shard_id,
        "session_id": session_id,
        "task_id": task["task_id"],
        "manifest_order": task["manifest_order"],
        "attempt": attempt,
        "input_fingerprint": task["input_fingerprint"],
        "render_state": render_state,
        "evidence_state": evidence_state,
        "artifact_review_state": "REVIEW_PENDING",
        "scientific_interpretation": scientific_state,
        "publication_state": "NOT_READY",
        "empty_genotype_groups": empty_groups,
        "artifacts": artifacts,
        "stage_bundles": stage_bundles,
        "warnings": warnings,
        "failures": failures,
        "rerun_eligible": rerun_eligible,
        "rerun_reason": rerun_reason,
        "created_at": utc_now(),
    }
    return result


def qc_case(
    task: dict[str, Any],
    validation_bundle_dir: str | Path,
    render_bundle_dir: str | Path,
    compose_bundle_dir: str | Path,
    output_dir: str | Path,
    *,
    shard_id: str,
    session_id: str,
    attempt: int = 1,
    evidence_evaluator: Callable[..., dict[str, Any]] | None = None,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Reduce declared stage evidence into one immutable, review-pending case result."""

    validate_task_document(task, schema_dir=schema_dir)
    lineage = {
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "shard_id": shard_id,
        "session_id": session_id,
        "attempt": attempt,
    }
    rows: list[tuple[dict[str, Any], dict[str, Path], Path]] = []
    for stage, directory in (
        ("VALIDATE_CASE_INPUTS", validation_bundle_dir),
        ("RUN_IGV", render_bundle_dir),
        ("COMPOSE_CASE", compose_bundle_dir),
    ):
        result, artifacts = verify_stage_bundle(
            directory,
            expected_stage=stage,
            expected_task_id=task["task_id"],
            expected_input_fingerprint=task["input_fingerprint"],
            expected_metadata=lineage,
            schema_dir=schema_dir,
        )
        rows.append((result, artifacts, Path(directory).expanduser().resolve(strict=True)))
    validation_result, validation_artifacts, _validation_root = rows[0]
    render_result, render_artifacts, _render_root = rows[1]
    compose_result, compose_artifacts, compose_root = rows[2]
    del validation_artifacts

    prior_results = [validation_result, render_result, compose_result]
    references = _bundle_references(
        task,
        [(result, root) for result, _artifacts, root in rows],
    )
    warnings = _deduplicated_warnings(prior_results)
    with StageBundle(
        output_dir,
        run_id=task["run_id"],
        generation_id=task["generation_id"],
        shard_id=shard_id,
        session_id=session_id,
        task_id=task["task_id"],
        manifest_order=task["manifest_order"],
        attempt=attempt,
        stage="QC_CASE",
        input_fingerprint=task["input_fingerprint"],
        schema_dir=schema_dir,
    ) as bundle:
        for warning in warnings:
            bundle.add_warning(warning["code"], warning["message"])

        causal = _causal_failures(prior_results)
        if causal:
            for failure in causal:
                bundle.add_domain_failure(
                    failure["code"],
                    failure["message"],
                    rerun_eligible=failure["rerun_eligible"],
                )
            all_empty = not task["tracks"] and all(
                task["genotype_groups"][genotype]["empty"]
                for genotype in ("0/0", "0/1", "1/1")
            )
            result = _case_result(
                task,
                shard_id=shard_id,
                session_id=session_id,
                attempt=attempt,
                render_state=(
                    "SUCCEEDED" if render_result["status"] == "SUCCEEDED" else "FAILED"
                ),
                evidence_state="EVIDENCE_INCOMPLETE" if all_empty else "UNAVAILABLE",
                artifacts=[],
                stage_bundles=references,
                warnings=warnings,
                failures=causal,
            )
            validate_case_result_document(result, schema_dir=schema_dir)
            result_path = bundle.add_artifact("case_result", "case_result.json")
            atomic_write_json(result_path, result)
            return bundle.finish("DOMAIN_FAILED")

        required_render = {
            "raw_qc",
            "igv_batch",
            "capture_metadata",
            "resolved_render_case",
            "resolved_render_config",
        }
        required_compose = {
            "combined_png",
            "composition_layout",
            "sample_table",
            "violin_qc",
        }
        missing_render = sorted(required_render - set(render_artifacts))
        missing_compose = sorted(required_compose - set(compose_artifacts))
        if missing_render or missing_compose:
            raise RuntimeError(
                "successful stage bundle lacks required QC artifacts: "
                f"render={missing_render} compose={missing_compose}"
            )

        raw_qc = _json(render_artifacts["raw_qc"])
        raw_qc_path = bundle.add_artifact("raw_qc", "raw_qc.json")
        atomic_write_json(raw_qc_path, raw_qc)
        render_config = WorkflowConfig.load(render_artifacts["resolved_render_config"])
        final_qc = inspect_png(
            compose_artifacts["combined_png"],
            min_width=int(render_config.get("qc.final_min_width", 2640)),
            min_height=int(render_config.get("qc.final_min_height", 2160)),
            min_stddev=float(render_config.get("qc.min_stddev", 0.5)),
        )
        combined_qc_path = bundle.add_artifact("combined_qc", "combined_qc.json")
        atomic_write_json(combined_qc_path, final_qc)

        resolved_case = _json(render_artifacts["resolved_render_case"])
        capture = _json(render_artifacts["capture_metadata"])
        layout = _json(compose_artifacts["composition_layout"])
        violin_qc = _json(compose_artifacts["violin_qc"])
        batch_text = render_artifacts["igv_batch"].read_text(encoding="utf-8")
        scientific = (evidence_evaluator or evaluate_evidence)(
            resolved_case,
            batch_text=batch_text,
            capture=capture,
            layout=layout,
            violin_qc=violin_qc,
            final_png_qc=final_qc,
            config=render_config,
        )
        scientific_path = bundle.add_artifact("scientific_qc", "scientific_qc.json")
        atomic_write_json(scientific_path, scientific)

        local_failures: list[dict[str, Any]] = []
        if raw_qc.get("status") != "PASS":
            local_failures.append(
                {
                    "stage": "QC_CASE",
                    "class": "DOMAIN",
                    "code": "RAW_CLIENT_QC_FAILED",
                    "message": json.dumps(raw_qc, sort_keys=True),
                    "rerun_eligible": True,
                }
            )
        if final_qc["status"] != "PASS":
            local_failures.append(
                {
                    "stage": "QC_CASE",
                    "class": "DOMAIN",
                    "code": "FINAL_PNG_QC_FAILED",
                    "message": json.dumps(final_qc, sort_keys=True),
                    "rerun_eligible": True,
                }
            )
        if scientific["status"] != "PASS":
            local_failures.append(
                {
                    "stage": "QC_CASE",
                    "class": "DOMAIN",
                    "code": "SCIENTIFIC_EVIDENCE_QC_FAILED",
                    "message": json.dumps(scientific["failed_codes"], sort_keys=True),
                    "rerun_eligible": True,
                }
            )
        if scientific.get("test_double") is True:
            local_failures.append(
                {
                    "stage": "QC_CASE",
                    "class": "DOMAIN",
                    "code": "TEST_DOUBLE_EVIDENCE_NOT_PUBLISHABLE",
                    "message": "synthetic CI evidence is never eligible for review or publication",
                    "rerun_eligible": False,
                }
            )
        for failure in local_failures:
            bundle.add_domain_failure(
                failure["code"],
                failure["message"],
                rerun_eligible=failure["rerun_eligible"],
            )

        logical_base_compose = f"stages/compose_case/{task['task_id']}"
        logical_base_qc = f"stages/qc_case/{task['task_id']}"
        artifacts = [
            _artifact(
                "combined_png",
                compose_artifacts["combined_png"],
                f"{logical_base_compose}/{compose_artifacts['combined_png'].relative_to(compose_root)}",
            ),
            _artifact(
                "sample_table",
                compose_artifacts["sample_table"],
                f"{logical_base_compose}/{compose_artifacts['sample_table'].relative_to(compose_root)}",
            ),
            _artifact(
                "composition_layout",
                compose_artifacts["composition_layout"],
                f"{logical_base_compose}/{compose_artifacts['composition_layout'].relative_to(compose_root)}",
            ),
            _artifact("combined_qc", combined_qc_path, f"{logical_base_qc}/combined_qc.json"),
            _artifact("scientific_qc", scientific_path, f"{logical_base_qc}/scientific_qc.json"),
        ]
        empty_groups = [
            genotype
            for genotype in ("0/0", "0/1", "1/1")
            if task["genotype_groups"][genotype]["empty"]
        ]
        evidence_state = (
            "UNAVAILABLE"
            if local_failures
            else "EVIDENCE_INCOMPLETE"
            if empty_groups
            else "COMPLETE"
        )
        result = _case_result(
            task,
            shard_id=shard_id,
            session_id=session_id,
            attempt=attempt,
            render_state="SUCCEEDED",
            evidence_state=evidence_state,
            artifacts=artifacts,
            stage_bundles=references,
            warnings=warnings,
            failures=local_failures,
        )
        validate_case_result_document(result, schema_dir=schema_dir)
        result_path = bundle.add_artifact("case_result", "case_result.json")
        atomic_write_json(result_path, result)
        return bundle.finish("DOMAIN_FAILED" if local_failures else "SUCCEEDED")
