from __future__ import annotations

import copy
import json
import os
import shutil
import tempfile
import uuid
from collections import Counter
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .contracts import PIPELINE_VERSION, validate_task_document, validate_unique_task_set
from .identity import canonical_fingerprint, file_identity, staged_name, task_set_fingerprint
from .prepare import prepare_run
from .r_prepare import run_r_prepare
from .utils import atomic_write_json, read_jsonl, sha256_file, sha256_json, write_jsonl, write_tsv


GENOTYPES = ("0/0", "0/1", "1/1")
REFERENCE_STAGE_NAMES = {
    "definition": "reference_genome.json",
    "fasta": "reference_genome.fa",
    "fai": "reference_genome.fa.fai",
    "cytoband": "reference_cytoband.txt.gz",
    "annotation": "reference_MANE.gff.gz",
}


def _resolved_file(value: str | Path) -> Path:
    path = Path(value).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ValueError(f"required input is not a regular file: {path}")
    return path


def _issue_rows(values: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for value in list(values or []):
        code = str(value.get("code", "UNKNOWN_ERROR")).strip().upper()
        message = str(value.get("message", ""))
        rows.append({"code": code, "message": message})
    return rows


def _reference_resource(
    legacy: dict[str, Any], key: str, configured_sha256: str | None
) -> dict[str, Any]:
    source = _resolved_file(legacy[key])
    identity = file_identity(source, sha256=configured_sha256 or None)
    return {
        "source_path": str(source),
        "stage_name": REFERENCE_STAGE_NAMES[key],
        "identity": identity,
    }


def _convert_task(
    legacy: dict[str, Any],
    *,
    run_id: str,
    generation_id: str,
    manifest_order: int,
    config: WorkflowConfig,
    estimated_runtime_seconds: float,
) -> dict[str, Any]:
    if not legacy.get("ag", {}).get("chrom") or not legacy.get("snp", {}).get("chrom"):
        raise ValueError(
            f"cannot create schema-v2 task from malformed coordinates: {legacy.get('case_id')}"
        )

    warnings = _issue_rows(legacy.get("preflight_warnings"))
    errors = _issue_rows(legacy.get("preflight_errors"))
    tracks: list[dict[str, Any]] = []
    missing_track_count = 0
    for genotype in GENOTYPES:
        for sample in list(legacy.get("genotypes", {}).get(genotype, [])):
            bam_value = str(sample.get("bam", "")).strip()
            bai_value = str(sample.get("bai", "")).strip()
            bam_identity = sample.get("bam_identity")
            bai_identity = sample.get("bai_identity")
            if not bam_value or not bai_value or not bam_identity or not bai_identity:
                missing_track_count += 1
                continue
            bam = Path(bam_value).expanduser().resolve(strict=False)
            bai = Path(bai_value).expanduser().resolve(strict=False)
            if not bam.is_file() or not bai.is_file():
                missing_track_count += 1
                continue
            sample_id = str(sample["sample_id"])
            stage_bam = staged_name(bam, role="bam", discriminator=sample_id)
            tracks.append(
                {
                    "sample_id": sample_id,
                    "genotype": genotype,
                    "dosage": int(sample["dosage"]),
                    "ratio": float(sample["ratio"]),
                    "selection_label": str(sample["selection_label"]),
                    "bam": str(bam),
                    "bai": str(bai),
                    "stage_bam": stage_bam,
                    "stage_bai": f"{stage_bam}.bai",
                    "bam_identity": {
                        "size": int(bam_identity["size"]),
                        "mtime_ns": int(bam_identity["mtime_ns"]),
                    },
                    "bai_identity": {
                        "size": int(bai_identity["size"]),
                        "mtime_ns": int(bai_identity["mtime_ns"]),
                    },
                    "bai_fresh": sample.get("bai_fresh"),
                }
            )

    if missing_track_count and not any(issue["code"] in {"BAM_MISSING", "BAI_MISSING"} for issue in errors):
        errors.append(
            {
                "code": "TRACK_INPUT_UNAVAILABLE",
                "message": f"{missing_track_count} selected BAM/BAI track inputs are unavailable",
            }
        )

    group_counts = Counter(track["genotype"] for track in tracks)
    genotype_groups = {
        genotype: {
            "selected_count": group_counts[genotype],
            "empty": group_counts[genotype] == 0,
        }
        for genotype in GENOTYPES
    }

    plot_legacy = dict(legacy.get("violin", {}))
    plot_path_value = plot_legacy.get("pdf")
    plot_identity = plot_legacy.get("pdf_identity")
    if plot_path_value and isinstance(plot_legacy.get("page"), int) and plot_identity:
        plot_path = Path(plot_path_value).expanduser().resolve(strict=False)
        if plot_path.is_file():
            plot = {
                "state": "PRESENT",
                "pdf": str(plot_path),
                "stage_pdf": staged_name(plot_path, role="violin"),
                "page": int(plot_legacy["page"]),
                "match_key": dict(plot_legacy["match_key"]),
                "pdf_identity": {
                    "size": int(plot_identity["size"]),
                    "mtime_ns": int(plot_identity["mtime_ns"]),
                    "sha256": str(plot_identity["sha256"]),
                },
            }
        else:
            plot = {
                "state": "UNAVAILABLE",
                "pdf": str(plot_path),
                "stage_pdf": None,
                "page": None,
                "match_key": dict(plot_legacy.get("match_key", {})),
                "pdf_identity": None,
            }
    else:
        plot = {
            "state": "UNAVAILABLE",
            "pdf": str(Path(plot_path_value).expanduser().resolve(strict=False)) if plot_path_value else None,
            "stage_pdf": None,
            "page": None,
            "match_key": dict(plot_legacy.get("match_key", {})),
            "pdf_identity": None,
        }

    genome = dict(legacy["genome"])
    resources = {
        "definition": _reference_resource(
            genome, "definition", str(genome.get("definition_sha256") or "")
        ),
        "fasta": _reference_resource(genome, "fasta", None),
        "fai": _reference_resource(genome, "fai", None),
        "cytoband": _reference_resource(
            genome, "cytoband", str(genome.get("cytoband_sha256") or "")
        ),
        "annotation": _reference_resource(
            genome, "annotation", str(genome.get("annotation_sha256") or "")
        ),
    }
    reference_fingerprint = sha256_json(resources)

    render_policy = {
        "screen_width": int(config.get("desktop.screen_width", 1920)),
        "screen_height": int(config.get("desktop.screen_height", 2160)),
        "screen_depth": int(config.get("desktop.screen_depth", 24)),
        "violin_panel_width": int(config.get("compose.violin_panel_width", 720)),
        "igv_version": str(config.get("runtime.igv_version", "2.16.2")),
        "overview_padding": int(config.get("render.overview_padding", 55)),
        "detail_padding": int(config.get("render.detail_padding", 12)),
    }
    render_policy["policy_fingerprint"] = sha256_json(render_policy)

    task: dict[str, Any] = {
        "schema_version": "2.0",
        "pipeline_version": PIPELINE_VERSION,
        "run_id": run_id,
        "generation_id": generation_id,
        "task_id": str(legacy["case_id"]),
        "manifest_order": manifest_order,
        "association": {
            "row": int(legacy["association_row"]),
            "input_sha256": str(legacy["associations_sha256"]),
        },
        "figure_contract_id": str(legacy.get("figure_contract_id") or config.get("workflow.figure_contract_id")),
        "gui_settle_contract_id": str(legacy.get("gui_settle_contract_id") or config.get("workflow.gui_settle_contract_id")),
        "ag": dict(legacy["ag"]),
        "snp": dict(legacy["snp"]),
        "strand": str(legacy["strand"]),
        "regions": dict(legacy["windows"]),
        "statistics": dict(legacy["statistics"]),
        "genotype_groups": genotype_groups,
        "tracks": tracks,
        "plot": plot,
        "reference": {
            "id": str(genome["id"]),
            "display_name": str(genome["display_name"]),
            "annotation_version": str(genome["annotation_version"]),
            "resource_fingerprint": reference_fingerprint,
            "resources": resources,
        },
        "reference_context": dict(legacy["reference_context"]),
        "render_contract": render_policy,
        "preflight_state": "CASE_INPUT_INVALID" if errors else "READY",
        "preflight_warnings": warnings,
        "preflight_errors": errors,
        "shard_hint": str(legacy["shard"]),
        "estimated_runtime_seconds": float(estimated_runtime_seconds),
    }
    task["input_fingerprint"] = canonical_fingerprint(task)
    validate_task_document(task)
    return task


def normalize_manifest(
    params_path: str | Path,
    output_dir: str | Path,
    *,
    run_id: str,
    generation_id: str,
    associations: str | Path | None = None,
    prepared_cases: str | Path | None = None,
    prepared_samples: str | Path | None = None,
    rds_dir: str | Path | None = None,
    bam_lookup: str | Path | None = None,
    violin_dir: str | Path | None = None,
    genome_definition: str | Path | None = None,
    fasta: str | Path | None = None,
    fai: str | Path | None = None,
    cytoband: str | Path | None = None,
    annotation: str | Path | None = None,
    r_wrapper: str | Path | None = None,
    r_implementation: str | Path | None = None,
    expected_case_count: int | None = None,
    estimated_runtime_seconds: float = 90.0,
) -> dict[str, Any]:
    """Create an immutable schema-v2 planning bundle.

    The legacy preparer is confined to a temporary process-local run root. The
    returned destination contains only declared schema-v2 outputs.
    """

    params = WorkflowConfig.load(params_path)
    association_input = Path(
        associations if associations is not None else params.path_value("paths.associations")
    ).expanduser().resolve(strict=True)
    resolved_overrides = {
        "rds_dir": Path(
            rds_dir if rds_dir is not None else params.path_value("paths.rds_dir")
        ).expanduser().resolve(strict=True),
        "bam_lookup": Path(
            bam_lookup if bam_lookup is not None else params.path_value("paths.bam_lookup")
        ).expanduser().resolve(strict=True),
        "violin_dir": Path(
            violin_dir if violin_dir is not None else params.path_value("paths.violin_dir")
        ).expanduser().resolve(strict=True),
        "definition": Path(
            genome_definition
            if genome_definition is not None
            else params.path_value("genome.definition")
        ).expanduser().resolve(strict=True),
        "fasta": Path(
            fasta if fasta is not None else params.path_value("genome.fasta")
        ).expanduser().resolve(strict=True),
        "fai": Path(
            fai if fai is not None else params.path_value("genome.fai")
        ).expanduser().resolve(strict=True),
        "cytoband": Path(
            cytoband if cytoband is not None else params.path_value("genome.cytoband")
        ).expanduser().resolve(strict=True),
        "annotation": Path(
            annotation if annotation is not None else params.path_value("genome.annotation")
        ).expanduser().resolve(strict=True),
    }
    for key, path in resolved_overrides.items():
        if key in {"rds_dir", "violin_dir"}:
            if not path.is_dir():
                raise ValueError(f"normalization directory input is unavailable: {key}:{path}")
        elif not path.is_file():
            raise ValueError(f"normalization file input is unavailable: {key}:{path}")
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"normalization output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        with tempfile.TemporaryDirectory(prefix="igv-normalize-", dir=destination.parent) as temp_name:
            temporary = Path(temp_name)
            legacy_output = temporary / "legacy-runs"
            legacy_publish = temporary / "legacy-publish"
            legacy_output.mkdir()
            legacy_publish.mkdir()
            derived_data = copy.deepcopy(params.data)
            derived_paths = derived_data.setdefault("paths", {})
            derived_paths["output_root"] = str(legacy_output)
            derived_paths["publish_root"] = str(legacy_publish)
            derived_paths["associations"] = str(association_input)
            derived_paths["rds_dir"] = str(resolved_overrides["rds_dir"])
            derived_paths["bam_lookup"] = str(resolved_overrides["bam_lookup"])
            derived_paths["violin_dir"] = str(resolved_overrides["violin_dir"])
            derived_genome = derived_data.setdefault("genome", {})
            for key in ("definition", "fasta", "fai", "cytoband", "annotation"):
                derived_genome[key] = str(resolved_overrides[key])
            derived_data.setdefault("execution", {})["mode"] = "local"
            derived_config_path = temporary / "params.json"
            derived_config_path.write_text(
                json.dumps(derived_data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            derived_config = WorkflowConfig.load(derived_config_path)
            if prepared_cases is None and prepared_samples is None:
                if r_wrapper is None or r_implementation is None:
                    raise ValueError(
                        "raw normalization requires explicit r_wrapper and r_implementation inputs"
                    )
                r_bundle = run_r_prepare(
                    derived_config_path,
                    association_input,
                    resolved_overrides["rds_dir"],
                    resolved_overrides["bam_lookup"],
                    temporary / "r_prepare",
                    r_wrapper=r_wrapper,
                    r_implementation=r_implementation,
                )
                prepared_cases_value: str | Path = r_bundle["prepared_cases"]
                prepared_samples_value: str | Path = r_bundle["prepared_samples"]
                r_prepare_report = json.loads(
                    Path(r_bundle["report"]).read_text(encoding="utf-8")
                )
                r_prepare_report["mode"] = "EXECUTED"
            elif prepared_cases is not None and prepared_samples is not None:
                prepared_cases_value = _resolved_file(prepared_cases)
                prepared_samples_value = _resolved_file(prepared_samples)
                r_prepare_report = {
                    "schema_version": "2.0-r-prepare",
                    "status": "PASS",
                    "mode": "PROVIDED_FIXTURE",
                    "prepared_cases_sha256": sha256_file(prepared_cases_value),
                    "prepared_samples_sha256": sha256_file(prepared_samples_value),
                }
            else:
                raise ValueError(
                    "prepared_cases and prepared_samples must be supplied together"
                )
            legacy_run = legacy_output / "normalize"
            report = prepare_run(
                derived_config,
                legacy_run,
                associations=association_input,
                prepared_cases=prepared_cases_value,
                prepared_samples=prepared_samples_value,
            )
            legacy_tasks = list(read_jsonl(report["manifest"]))

            required_count = (
                int(expected_case_count)
                if expected_case_count is not None
                else int(params.get("inputs.expected_case_count"))
                if params.get("inputs.expected_case_count") not in (None, "")
                else None
            )
            if required_count is not None and len(legacy_tasks) != required_count:
                raise ValueError(
                    f"canonical case count mismatch: expected {required_count}, observed {len(legacy_tasks)}"
                )

            tasks = [
                _convert_task(
                    legacy,
                    run_id=run_id,
                    generation_id=generation_id,
                    manifest_order=index,
                    config=params,
                    estimated_runtime_seconds=estimated_runtime_seconds,
                )
                for index, legacy in enumerate(legacy_tasks, 1)
            ]
            tasks = validate_unique_task_set(tasks)
            task_set_sha = task_set_fingerprint(tasks)

            tasks_path = staging / "tasks.jsonl"
            write_jsonl(tasks_path, tasks)
            write_tsv(
                staging / "normalized_manifest.tsv",
                [
                    "manifest_order",
                    "task_id",
                    "association_row",
                    "chrom",
                    "strand",
                    "track_count",
                    "empty_genotype_groups",
                    "preflight_state",
                    "estimated_runtime_seconds",
                    "input_fingerprint",
                ],
                (
                    {
                        "manifest_order": task["manifest_order"],
                        "task_id": task["task_id"],
                        "association_row": task["association"]["row"],
                        "chrom": task["ag"]["chrom"],
                        "strand": task["strand"],
                        "track_count": len(task["tracks"]),
                        "empty_genotype_groups": ",".join(
                            genotype
                            for genotype in GENOTYPES
                            if task["genotype_groups"][genotype]["empty"]
                        ),
                        "preflight_state": task["preflight_state"],
                        "estimated_runtime_seconds": task["estimated_runtime_seconds"],
                        "input_fingerprint": task["input_fingerprint"],
                    }
                    for task in tasks
                ),
            )
            parameters = {
                "run_id": run_id,
                "generation_id": generation_id,
                "source_params": str(Path(params_path).expanduser().resolve(strict=True)),
                "source_params_sha256": sha256_file(params_path),
                "estimated_runtime_seconds": estimated_runtime_seconds,
                "expected_case_count": required_count,
            }
            atomic_write_json(staging / "parameters.json", parameters)
            atomic_write_json(staging / "r_prepare.json", r_prepare_report)
            invalid_count = sum(task["preflight_state"] != "READY" for task in tasks)
            incomplete_count = sum(
                any(task["genotype_groups"][genotype]["empty"] for genotype in GENOTYPES)
                for task in tasks
            )
            validation = {
                "schema_version": "2.0",
                "pipeline_version": PIPELINE_VERSION,
                "status": "PASS_WITH_CASE_INPUT_ERRORS" if invalid_count else "PASS",
                "run_id": run_id,
                "generation_id": generation_id,
                "task_count": len(tasks),
                "ready_task_count": len(tasks) - invalid_count,
                "case_input_invalid_count": invalid_count,
                "evidence_incomplete_candidate_count": incomplete_count,
                "tasks_sha256": sha256_file(tasks_path),
                "task_set_sha256": task_set_sha,
                "source_associations_sha256": report["associations_sha256"],
                "source_params_sha256": parameters["source_params_sha256"],
                "r_prepare_mode": r_prepare_report["mode"],
            }
            atomic_write_json(staging / "validation.json", validation)
        os.replace(staging, destination)
        return {
            **validation,
            "output_dir": str(destination),
            "tasks": str(destination / "tasks.jsonl"),
            "normalized_manifest": str(destination / "normalized_manifest.tsv"),
            "validation": str(destination / "validation.json"),
            "parameters": str(destination / "parameters.json"),
            "r_prepare": str(destination / "r_prepare.json"),
        }
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
