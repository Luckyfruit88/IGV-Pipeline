from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any, Callable

from .bundles import StageBundle
from .case_inputs import expected_stage_inputs
from .config import WorkflowConfig
from .contracts import validate_schema_document, validate_task_document
from .desktop import DesktopFailure, run_desktop_session
from .igv import build_desktop_batch
from .parsing import locus
from .qc import inspect_png
from .utils import atomic_write_json, atomic_write_text, resource_contains_remote_url


def _desktop_log_errors(stdout: Path, stderr: Path, igv_home: Path) -> list[str]:
    pattern = re.compile(
        r"UNKNOWN COMMAND|UNKOWN COMMAND|\bSEVERE\b|Could not load|Error loading|"
        r"FileNotFoundException|SAMException|Connection refused|"
        r"Loading (?:genome|resource):\s*(?:https?|ftp|s3|gs)://|"
        r"genome server.*(?:https?|ftp|s3|gs)://",
        re.IGNORECASE,
    )
    paths = [stdout, stderr]
    if igv_home.is_dir():
        paths.extend(path for path in igv_home.rglob("*.log") if path not in paths)
    errors: list[str] = []
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if pattern.search(line):
                errors.append(f"{path.name}: {line.strip()}")
                if len(errors) >= 20:
                    return errors
    return errors


def _materialize_case(
    task: dict[str, Any],
    input_map: dict[str, str],
    bundle: StageBundle,
    params: WorkflowConfig,
) -> tuple[dict[str, Any], WorkflowConfig]:
    expected = expected_stage_inputs(task)
    if set(input_map) != set(expected):
        raise RuntimeError("RUN_IGV input map differs from the validated task contract")
    staged = {
        name: Path(value).expanduser().resolve(strict=True) for name, value in input_map.items()
    }
    for name, path in staged.items():
        if not path.is_file():
            raise RuntimeError(f"RUN_IGV staged input is unavailable: {name}:{path}")
        if any(character.isspace() for character in str(path)):
            raise RuntimeError(f"RUN_IGV staged input path contains whitespace: {path}")

    definition_resource = task["reference"]["resources"]["definition"]
    source_definition = staged[definition_resource["stage_name"]]
    if resource_contains_remote_url(source_definition):
        raise RuntimeError(f"source genome definition contains a remote URL: {source_definition}")
    try:
        definition = json.loads(source_definition.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot parse staged genome definition: {exc}") from exc
    if not isinstance(definition, dict):
        raise RuntimeError("staged genome definition must contain one JSON object")
    resources = task["reference"]["resources"]
    definition.update(
        {
            "id": task["reference"]["id"],
            "name": task["reference"]["display_name"],
            "fastaURL": str(staged[resources["fasta"]["stage_name"]]),
            "indexURL": str(staged[resources["fai"]["stage_name"]]),
            "cytobandURL": str(staged[resources["cytoband"]["stage_name"]]),
            "tracks": [],
        }
    )
    local_definition = bundle.add_artifact("local_genome_definition", "runtime/genome.json")
    atomic_write_json(local_definition, definition)
    if resource_contains_remote_url(local_definition):
        raise RuntimeError("task-local genome definition contains a remote URL")

    groups = {genotype: [] for genotype in ("0/0", "0/1", "1/1")}
    for track in task["tracks"]:
        groups[track["genotype"]].append(
            {
                "sample_id": track["sample_id"],
                "genotype": track["genotype"],
                "dosage": track["dosage"],
                "ratio": track["ratio"],
                "selection_label": track["selection_label"],
                "bam": str(staged[track["stage_bam"]]),
                "bai": str(staged[track["stage_bai"]]),
            }
        )
    case = {
        "case_id": task["task_id"],
        "figure_contract_id": task["figure_contract_id"],
        "gui_settle_contract_id": task["gui_settle_contract_id"],
        "ag": task["ag"],
        "snp": task["snp"],
        "strand": task["strand"],
        "windows": task["regions"],
        "statistics": task["statistics"],
        "reference_context": task["reference_context"],
        "genotypes": groups,
        "violin": {
            "pdf": str(staged[task["plot"]["stage_pdf"]]),
            "page": task["plot"]["page"],
            "match_key": task["plot"]["match_key"],
        },
        "genome": {
            "id": task["reference"]["id"],
            "display_name": task["reference"]["display_name"],
            "definition": str(local_definition),
            "fasta": str(staged[resources["fasta"]["stage_name"]]),
            "fai": str(staged[resources["fai"]["stage_name"]]),
            "cytoband": str(staged[resources["cytoband"]["stage_name"]]),
            "annotation": str(staged[resources["annotation"]["stage_name"]]),
            "annotation_version": task["reference"]["annotation_version"],
            "annotation_sha256": resources["annotation"]["identity"].get("sha256"),
        },
    }
    resolved_case_path = bundle.add_artifact(
        "resolved_render_case", "runtime/resolved_render_case.json"
    )
    atomic_write_json(resolved_case_path, case)

    derived_data = copy.deepcopy(params.data)
    derived_data.setdefault("genome", {}).update(case["genome"])
    derived_config_path = bundle.add_artifact("resolved_render_config", "runtime/params.json")
    atomic_write_json(derived_config_path, derived_data)
    return case, WorkflowConfig.load(derived_config_path)


def render_case(
    task: dict[str, Any],
    input_map: dict[str, str],
    validation_result: dict[str, Any],
    params_path: str | Path,
    output_dir: str | Path,
    *,
    shard_id: str,
    session_id: str,
    attempt: int = 1,
    desktop_runner: Callable[..., Any] | None = None,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    validate_task_document(task, schema_dir=schema_dir)
    validate_schema_document(validation_result, "stage-result", schema_dir=schema_dir)
    if validation_result["task_id"] != task["task_id"]:
        raise RuntimeError("validation bundle task_id does not match RUN_IGV task")
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
        stage="RUN_IGV",
        input_fingerprint=task["input_fingerprint"],
        schema_dir=schema_dir,
    ) as bundle:
        for warning in validation_result["warnings"]:
            bundle.add_warning(warning["code"], warning["message"])
        if validation_result["status"] == "DOMAIN_FAILED":
            for failure in validation_result["failures"]:
                bundle.add_domain_failure(
                    failure["code"],
                    failure["message"],
                    rerun_eligible=failure["rerun_eligible"],
                )
            return bundle.finish("DOMAIN_FAILED")

        case, render_config = _materialize_case(task, input_map, bundle, params)
        control_root = bundle.path("raw/control")
        batch_text, batch_spec = build_desktop_batch(
            case, control_directory=control_root, config=render_config
        )
        batch_path = bundle.add_artifact("igv_batch", "batch/igv_batch.txt")
        atomic_write_text(batch_path, batch_text)
        client_png = bundle.path("raw/igv_client.png")
        capture_metadata_path = bundle.path("raw/capture_metadata.json")
        igv_home = bundle.path("runtime/igv_home")
        logs = bundle.path("logs")
        try:
            desktop = (desktop_runner or run_desktop_session)(
                render_config,
                batch_path=batch_path,
                ready_marker=batch_spec["ready_marker"],
                expected_locus=locus(case["windows"]["overview"]),
                igv_directory=igv_home,
                log_directory=logs,
                capture_directory=bundle.path("raw/capture"),
                output_png=client_png,
                metadata_path=capture_metadata_path,
            )
        except DesktopFailure as exc:
            bundle.add_domain_failure(exc.code, str(exc), rerun_eligible=True)
            return bundle.finish("DOMAIN_FAILED", exit_code=0)
        except FileNotFoundError as exc:
            raise RuntimeError(f"native IGV runtime executable is unavailable: {exc}") from exc

        bundle.register_existing_artifact("raw_igv_png", client_png)
        bundle.register_existing_artifact("capture_metadata", capture_metadata_path)
        for role, source in (
            ("igv_stdout", desktop.stdout_path),
            ("igv_stderr", desktop.stderr_path),
        ):
            bundle.register_existing_artifact(role, source)
        log_errors = _desktop_log_errors(desktop.stdout_path, desktop.stderr_path, igv_home)
        if log_errors:
            bundle.add_domain_failure("IGV_LOG_ERROR", " | ".join(log_errors), rerun_eligible=True)

        raw_qc = inspect_png(
            client_png,
            min_width=int(render_config.get("desktop.minimum_window_width", 1700)),
            min_height=int(render_config.get("desktop.minimum_window_height", 1800)),
            min_stddev=float(render_config.get("qc.min_stddev", 0.5)),
        )
        raw_qc_path = bundle.add_artifact("raw_qc", "raw/raw_qc.json")
        atomic_write_json(raw_qc_path, raw_qc)
        if raw_qc["status"] != "PASS":
            bundle.add_domain_failure(
                "RAW_CLIENT_QC_FAILED", json.dumps(raw_qc, sort_keys=True), rerun_eligible=True
            )
        runtime = {
            "desktop_started_at_epoch": round(desktop.started_at_epoch, 6),
            "desktop_ended_at_epoch": round(desktop.ended_at_epoch, 6),
            "desktop_wall_time_seconds": round(desktop.wall_time_seconds, 3),
            "desktop_peak_rss_gb": round(desktop.peak_rss_gb, 4),
            "capture_mode": desktop.metadata.get("capture_mode"),
            "geometry_verified": desktop.metadata.get("geometry_verified"),
        }
        runtime_path = bundle.add_artifact("render_runtime", "runtime/runtime.json")
        atomic_write_json(runtime_path, runtime)
        return bundle.finish(
            "DOMAIN_FAILED" if bundle.failures else "SUCCEEDED",
            peak_rss_gb=desktop.peak_rss_gb,
        )
