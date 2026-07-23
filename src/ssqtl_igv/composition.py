from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Callable

from .bundles import StageBundle, verify_stage_bundle
from .compose import compose_desktop_case
from .config import WorkflowConfig
from .contracts import validate_task_document
from .qc import inspect_png
from .utils import atomic_write_json, sha256_file, write_tsv
from .violin import render_pdf_page


SAMPLE_TABLE_FIELDS = [
    "task_id",
    "genotype",
    "sample_id",
    "dosage",
    "ratio",
    "selection_label",
]


def _write_review_sample_table(task: dict[str, Any], destination: Path) -> None:
    """Write review metadata without exposing BAM, BAI, or task-local paths."""

    rows = [
        {
            "task_id": task["task_id"],
            "genotype": track["genotype"],
            "sample_id": track["sample_id"],
            "dosage": track["dosage"],
            "ratio": track["ratio"],
            "selection_label": track["selection_label"],
        }
        for track in task["tracks"]
    ]
    write_tsv(destination, SAMPLE_TABLE_FIELDS, rows)


def _propagate_upstream_failure(
    bundle: StageBundle, upstream: dict[str, Any]
) -> dict[str, Any]:
    for warning in upstream["warnings"]:
        bundle.add_warning(warning["code"], warning["message"])
    rerun_eligible = any(failure["rerun_eligible"] for failure in upstream["failures"])
    causal_codes = ",".join(failure["code"] for failure in upstream["failures"])
    bundle.add_domain_failure(
        "UPSTREAM_RUN_IGV_DOMAIN_FAILED",
        f"RUN_IGV did not produce composable evidence: {causal_codes}",
        rerun_eligible=rerun_eligible,
    )
    return bundle.finish("DOMAIN_FAILED")


def compose_case(
    task: dict[str, Any],
    render_bundle_dir: str | Path,
    violin_pdf: str | Path,
    params_path: str | Path,
    output_dir: str | Path,
    *,
    shard_id: str,
    session_id: str,
    attempt: int = 1,
    pdf_renderer: Callable[..., None] | None = None,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Render the exact violin page and append it to untouched native IGV pixels."""

    validate_task_document(task, schema_dir=schema_dir)
    render_result, render_artifacts = verify_stage_bundle(
        render_bundle_dir,
        expected_stage="RUN_IGV",
        expected_task_id=task["task_id"],
        expected_input_fingerprint=task["input_fingerprint"],
        expected_metadata={
            "run_id": task["run_id"],
            "generation_id": task["generation_id"],
            "shard_id": shard_id,
            "session_id": session_id,
            "attempt": attempt,
        },
        schema_dir=schema_dir,
    )
    params = WorkflowConfig.load(params_path)
    with StageBundle(
        output_dir,
        run_id=task["run_id"],
        generation_id=task["generation_id"],
        shard_id=shard_id,
        session_id=session_id,
        task_id=task["task_id"],
        manifest_order=task["manifest_order"],
        attempt=attempt,
        stage="COMPOSE_CASE",
        input_fingerprint=task["input_fingerprint"],
        schema_dir=schema_dir,
    ) as bundle:
        if render_result["status"] == "DOMAIN_FAILED":
            return _propagate_upstream_failure(bundle, render_result)
        missing_render_roles = sorted(
            {"raw_igv_png", "capture_metadata"} - set(render_artifacts)
        )
        if missing_render_roles:
            raise RuntimeError(
                f"successful RUN_IGV bundle lacks required artifacts: {missing_render_roles}"
            )

        plot = task["plot"]
        if plot["state"] != "PRESENT":
            bundle.add_domain_failure(
                "VIOLIN_MAPPING_INVALID",
                json.dumps(plot, sort_keys=True),
                rerun_eligible=False,
            )
            return bundle.finish("DOMAIN_FAILED")
        pdf = Path(violin_pdf).expanduser().resolve(strict=True)
        if not pdf.is_file():
            raise RuntimeError(f"staged violin PDF is not a regular file: {pdf}")
        expected_sha = plot["pdf_identity"].get("sha256")
        if not expected_sha or sha256_file(pdf) != expected_sha:
            raise RuntimeError("staged violin PDF differs from the canonical task identity")

        violin_png = bundle.path("violin/violin.png")
        try:
            (pdf_renderer or render_pdf_page)(
                pdf,
                int(plot["page"]),
                violin_png,
                pdftoppm=params.get("binaries.pdftoppm", "pdftoppm"),
                dpi=int(params.get("render.violin_dpi", 180)),
                timeout=int(params.get("timeouts.pdftoppm_seconds", 300)),
            )
        except subprocess.TimeoutExpired as exc:
            bundle.add_domain_failure(
                "VIOLIN_RENDER_TIMEOUT", str(exc), rerun_eligible=True
            )
            return bundle.finish("DOMAIN_FAILED")
        except RuntimeError as exc:
            bundle.add_domain_failure(
                "VIOLIN_RENDER_FAILED", str(exc), rerun_eligible=True
            )
            return bundle.finish("DOMAIN_FAILED")
        bundle.register_existing_artifact("violin_png", violin_png)

        violin_qc = inspect_png(
            violin_png,
            min_width=300,
            min_height=300,
            min_stddev=float(params.get("qc.min_stddev", 0.5)),
        )
        violin_qc_path = bundle.add_artifact("violin_qc", "violin/violin_qc.json")
        atomic_write_json(violin_qc_path, violin_qc)
        if violin_qc["status"] != "PASS":
            bundle.add_domain_failure(
                "VIOLIN_PNG_QC_FAILED",
                json.dumps(violin_qc, sort_keys=True),
                rerun_eligible=True,
            )
            return bundle.finish("DOMAIN_FAILED")

        capture = json.loads(
            render_artifacts["capture_metadata"].read_text(encoding="utf-8")
        )
        combined = bundle.path(f"combined/{task['task_id']}.png")
        layout_path = bundle.path("combined/layout.json")
        try:
            compose_desktop_case(
                {"case_id": task["task_id"], "violin": plot},
                render_artifacts["raw_igv_png"],
                violin_png,
                combined,
                layout_path,
                capture,
                params,
            )
        except ValueError as exc:
            bundle.add_domain_failure(
                "COMPOSITION_CONTRACT_FAILED", str(exc), rerun_eligible=True
            )
            return bundle.finish("DOMAIN_FAILED")
        bundle.register_existing_artifact("combined_png", combined)
        bundle.register_existing_artifact("composition_layout", layout_path)

        sample_table = bundle.add_artifact(
            "sample_table", f"combined/{task['task_id']}.samples.tsv"
        )
        _write_review_sample_table(task, sample_table)
        return bundle.finish("SUCCEEDED")
