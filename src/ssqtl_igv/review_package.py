from __future__ import annotations

import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any, Iterable

from .bundles import verify_stage_bundle
from .contracts import validate_case_result_document, validate_task_document, validate_unique_task_set
from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_jsonl,
    sha256_file,
    utc_now,
    write_jsonl,
    write_tsv,
)


REVIEW_ASSERTIONS = (
    "native_igv_complete_and_readable",
    "annotation_track_and_model_visible",
    "strand_and_transcript_reviewed",
    "ag_site_and_reference_ag_context_reviewed",
    "splice_or_junction_presence_absence_judgeable",
    "violin_pair_matches",
)


def _load_tasks(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path).expanduser()
    if source.is_symlink():
        raise ValueError(f"canonical tasks must not be a symlink: {source}")
    tasks = list(read_jsonl(source.resolve(strict=True)))
    for task in tasks:
        validate_task_document(task)
    return validate_unique_task_set(tasks)


def _load_case_results(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path).expanduser()
    if source.is_symlink():
        raise ValueError(f"aggregate case results must not be a symlink: {source}")
    rows = list(read_jsonl(source.resolve(strict=True)))
    for row in rows:
        validate_case_result_document(row)
    task_ids = [row["task_id"] for row in rows]
    orders = [row["manifest_order"] for row in rows]
    if len(task_ids) != len(set(task_ids)) or len(orders) != len(set(orders)):
        raise ValueError("aggregate case results contain duplicate task IDs or orders")
    return sorted(rows, key=lambda row: int(row["manifest_order"]))


def _bundle_map(
    directories: Iterable[str | Path], stage: str
) -> dict[str, tuple[dict[str, Any], dict[str, Path], Path]]:
    result: dict[str, tuple[dict[str, Any], dict[str, Path], Path]] = {}
    for value in directories:
        root = Path(value).expanduser().resolve(strict=True)
        stage_result, artifacts = verify_stage_bundle(root, expected_stage=stage)
        task_id = stage_result["task_id"]
        if task_id in result:
            raise ValueError(f"duplicate {stage} bundle for task: {task_id}")
        result[task_id] = (stage_result, artifacts, root)
    return result


def _artifact_map(case_result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    artifacts = {artifact["role"]: artifact for artifact in case_result["artifacts"]}
    if len(artifacts) != len(case_result["artifacts"]):
        raise ValueError(f"case result contains duplicate artifact roles: {case_result['task_id']}")
    return artifacts


def _safe_chromosome(task: dict[str, Any]) -> str:
    value = str(task["ag"]["chrom"])
    if Path(value).name != value or value in {".", ".."} or any(c.isspace() for c in value):
        raise ValueError(f"unsafe chromosome output component: {value!r}")
    return value


def _write_checksums(root: Path) -> Path:
    files = sorted(
        (
            path
            for path in root.rglob("*")
            if path.is_file() and path.name != "SHA256SUMS"
        ),
        key=lambda path: str(path.relative_to(root)),
    )
    lines = [f"{sha256_file(path)}  {path.relative_to(root)}" for path in files]
    target = root / "SHA256SUMS"
    atomic_write_text(target, "\n".join(lines) + "\n")
    return target


def _assert_review_tree_safe(root: Path, forbidden_values: set[str]) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"review package contains a symlink: {path}")
        if not path.is_file() or path.suffix.lower() == ".png":
            continue
        text = path.read_text(encoding="utf-8")
        lower = text.lower()
        if re.search(r"(?:^|[^a-z0-9_])[a-z0-9._-]+\.ba[mi](?:$|[^a-z0-9_])", lower):
            raise ValueError(f"review package exposes a BAM/BAI token: {path}")
        if re.search(r"(?:^|[\s\"'])/(?:restricted|users|private|tmp|var|home)/", lower):
            raise ValueError(f"review package exposes a private absolute path: {path}")
        for value in forbidden_values:
            if value and value in text:
                raise ValueError(f"review package exposes a private source value: {path}")


def build_review_package(
    canonical_tasks: str | Path,
    aggregate_case_results: str | Path,
    compose_bundle_dirs: Iterable[str | Path],
    qc_bundle_dirs: Iterable[str | Path],
    output_dir: str | Path,
) -> dict[str, Any]:
    """Create a checksum-bound review export without BAM/BAI or work paths."""

    tasks = _load_tasks(canonical_tasks)
    case_results = _load_case_results(aggregate_case_results)
    task_map = {task["task_id"]: task for task in tasks}
    result_map = {result["task_id"]: result for result in case_results}
    if set(task_map) != set(result_map):
        raise ValueError("aggregate case results do not exactly cover canonical tasks")
    for task in tasks:
        result = result_map[task["task_id"]]
        if (
            result["manifest_order"] != task["manifest_order"]
            or result["input_fingerprint"] != task["input_fingerprint"]
        ):
            raise ValueError(f"aggregate result differs from task identity: {task['task_id']}")

    compose_map = _bundle_map(compose_bundle_dirs, "COMPOSE_CASE")
    qc_map = _bundle_map(qc_bundle_dirs, "QC_CASE")
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"review package already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    forbidden_values: set[str] = set()
    contracts: list[dict[str, Any]] = []
    package_rows: list[dict[str, Any]] = []
    excluded_rows: list[dict[str, Any]] = []
    try:
        for task in tasks:
            task_id = task["task_id"]
            result = result_map[task_id]
            for track in task["tracks"]:
                forbidden_values.update({str(track["bam"]), str(track["bai"])})
            eligible = (
                not result["failures"]
                and result["render_state"] == "SUCCEEDED"
                and result["evidence_state"] in {"COMPLETE", "EVIDENCE_INCOMPLETE"}
                and result["artifact_review_state"] == "REVIEW_PENDING"
            )
            if not eligible:
                excluded_rows.append(
                    {
                        "manifest_order": task["manifest_order"],
                        "task_id": task_id,
                        "render_state": result["render_state"],
                        "evidence_state": result["evidence_state"],
                        "failure_codes": ",".join(
                            failure["code"] for failure in result["failures"]
                        ),
                    }
                )
                continue
            if task_id not in compose_map or task_id not in qc_map:
                raise ValueError(f"eligible task lacks compose or QC bundle: {task_id}")
            compose_result, compose_artifacts, compose_root = compose_map[task_id]
            qc_result, qc_artifacts, qc_root = qc_map[task_id]
            forbidden_values.update({str(compose_root), str(qc_root)})
            if compose_result["status"] != "SUCCEEDED" or qc_result["status"] != "SUCCEEDED":
                raise ValueError(f"eligible task has a failed compose/QC bundle: {task_id}")
            required_compose = {"combined_png", "sample_table", "composition_layout"}
            required_qc = {"case_result", "scientific_qc"}
            if required_compose - set(compose_artifacts) or required_qc - set(qc_artifacts):
                raise ValueError(f"eligible task lacks review-bound artifacts: {task_id}")
            observed_case = json.loads(qc_artifacts["case_result"].read_text(encoding="utf-8"))
            if observed_case != result:
                raise ValueError(f"aggregate case result differs from QC artifact: {task_id}")
            recorded = _artifact_map(result)
            expected_artifacts = {
                "combined_png": compose_artifacts["combined_png"],
                "sample_table": compose_artifacts["sample_table"],
                "composition_layout": compose_artifacts["composition_layout"],
                "scientific_qc": qc_artifacts["scientific_qc"],
            }
            for role, source in expected_artifacts.items():
                if role not in recorded or sha256_file(source) != recorded[role]["sha256"]:
                    raise ValueError(f"case artifact binding mismatch for {task_id}:{role}")

            layout = json.loads(compose_artifacts["composition_layout"].read_text(encoding="utf-8"))
            scientific = json.loads(qc_artifacts["scientific_qc"].read_text(encoding="utf-8"))
            left_sha = str(layout.get("evidence", {}).get("final_left_pixel_sha256", ""))
            if not re.fullmatch(r"[a-f0-9]{64}", left_sha):
                raise ValueError(f"layout lacks a valid left-pixel identity: {task_id}")
            chromosome = _safe_chromosome(task)
            png_relative = Path("review_by_chr") / chromosome / f"{task_id}.png"
            table_relative = Path("tables") / chromosome / f"{task_id}.samples.tsv"
            metadata_relative = Path("metadata") / f"{task_id}.json"
            for source, relative in (
                (compose_artifacts["combined_png"], png_relative),
                (compose_artifacts["sample_table"], table_relative),
            ):
                target = staging / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
            metadata = {
                "schema_version": "2.0-review-metadata",
                "run_id": task["run_id"],
                "generation_id": task["generation_id"],
                "task_id": task_id,
                "manifest_order": task["manifest_order"],
                "ag": task["ag"],
                "snp": task["snp"],
                "strand": task["strand"],
                "regions": task["regions"],
                "genotype_groups": task["genotype_groups"],
                "reference": {
                    "id": task["reference"]["id"],
                    "display_name": task["reference"]["display_name"],
                    "annotation_version": task["reference"]["annotation_version"],
                },
                "evidence_state": result["evidence_state"],
                "empty_genotype_groups": result["empty_genotype_groups"],
                "figure_contract_id": task["figure_contract_id"],
                "gui_settle_contract_id": task["gui_settle_contract_id"],
                "left_pixel_sha256": left_sha,
                "combined_sha256": recorded["combined_png"]["sha256"],
                "scientific_qc_sha256": recorded["scientific_qc"]["sha256"],
                "automated_evidence_status": scientific.get("status"),
                "manual_review_required": scientific.get("manual_review_required", []),
            }
            atomic_write_json(staging / metadata_relative, metadata)
            contract = {
                "schema_version": "2.0-review-contract",
                "run_id": task["run_id"],
                "generation_id": task["generation_id"],
                "task_id": task_id,
                "manifest_order": task["manifest_order"],
                "input_fingerprint": task["input_fingerprint"],
                "case_result_sha256": sha256_file(qc_artifacts["case_result"]),
                "combined_sha256": recorded["combined_png"]["sha256"],
                "left_pixel_sha256": left_sha,
                "scientific_qc_sha256": recorded["scientific_qc"]["sha256"],
                "figure_contract_id": task["figure_contract_id"],
                "gui_settle_contract_id": task["gui_settle_contract_id"],
                "evidence_state": result["evidence_state"],
                "combined_relative_path": str(png_relative),
                "sample_table_relative_path": str(table_relative),
                "metadata_relative_path": str(metadata_relative),
            }
            contracts.append(contract)
            package_rows.append(
                {
                    "manifest_order": task["manifest_order"],
                    "task_id": task_id,
                    "chromosome": chromosome,
                    "evidence_state": result["evidence_state"],
                    "combined_relative_path": str(png_relative),
                    "combined_sha256": contract["combined_sha256"],
                    "sample_table_relative_path": str(table_relative),
                    "sample_table_sha256": recorded["sample_table"]["sha256"],
                    "metadata_relative_path": str(metadata_relative),
                }
            )

        write_jsonl(staging / "review_contract.jsonl", contracts)
        manifest_fields = [
            "manifest_order",
            "task_id",
            "chromosome",
            "evidence_state",
            "combined_relative_path",
            "combined_sha256",
            "sample_table_relative_path",
            "sample_table_sha256",
            "metadata_relative_path",
        ]
        write_tsv(staging / "review_manifest.tsv", manifest_fields, package_rows)
        template_fields = [
            "review_record_id",
            "task_id",
            "artifact_review_state",
            "scientific_interpretation",
            "reviewer",
            "reviewed_at",
            "notes",
            *REVIEW_ASSERTIONS,
        ]
        write_tsv(
            staging / "review_template.tsv",
            template_fields,
            (
                {
                    "review_record_id": "",
                    "task_id": row["task_id"],
                    "artifact_review_state": "",
                    "scientific_interpretation": (
                        "INDETERMINATE"
                        if row["evidence_state"] == "EVIDENCE_INCOMPLETE"
                        else ""
                    ),
                    "reviewer": "",
                    "reviewed_at": "",
                    "notes": "",
                    **{assertion: "" for assertion in REVIEW_ASSERTIONS},
                }
                for row in contracts
            ),
        )
        write_tsv(
            staging / "excluded.tsv",
            [
                "manifest_order",
                "task_id",
                "render_state",
                "evidence_state",
                "failure_codes",
            ],
            excluded_rows,
        )
        package = {
            "schema_version": "2.0-review-package",
            "created_at": utc_now(),
            "run_id": tasks[0]["run_id"],
            "generation_id": tasks[0]["generation_id"],
            "canonical_task_count": len(tasks),
            "eligible_review_count": len(contracts),
            "excluded_count": len(excluded_rows),
            "ordering": "manifest_order",
            "contains_bam_or_bai": False,
            "contains_private_work_paths": False,
        }
        atomic_write_json(staging / "package.json", package)
        _assert_review_tree_safe(staging, forbidden_values)
        checksums = _write_checksums(staging)
        _assert_review_tree_safe(staging, forbidden_values)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **package,
        "output_dir": str(destination),
        "checksums_sha256": sha256_file(destination / checksums.name),
    }
