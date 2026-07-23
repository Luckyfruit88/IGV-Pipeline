from __future__ import annotations

import copy
import argparse
import csv
import io
import json
import os
import shutil
import tempfile
import uuid
from importlib.resources import files
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

import yaml

from .contracts import (
    FIGURE_CONTRACT_ID,
    GUI_SETTLE_CONTRACT_ID,
    V3_CANONICAL_SCHEMA_VERSION,
    V3_PIPELINE_VERSION,
    validate_unique_task_set,
)
from .identity import file_identity, task_set_fingerprint
from .normalize import normalize_manifest
from .r_prepare import (
    DEFAULT_BAM_ID_COLUMNS,
    DEFAULT_BAM_PATH_COLUMNS,
    DEFAULT_BAM_SUFFIXES,
    DEFAULT_LOCUS_SAMPLE_COLUMNS,
    run_r_prepare,
)
from .utils import (
    atomic_write_json,
    read_jsonl,
    reject_symlink_path_components,
    sha256_file,
    sha256_json,
    write_jsonl,
    write_tsv,
)
from .v3_manifest import (
    _load_reference,
    _relative_path,
    _resolve_relative_file,
    native_ssqtl_task_from_prepared,
)


SSQTL_ADAPTER_SCHEMA_VERSION = "3.0-ssqtl"
_GENOTYPES = ("0/0", "0/1", "1/1")
_ALLOWED_OPTIONS = {
    "schema_version",
    "association_columns",
    "rds_filename_template",
    "locus_sample_columns",
    "ratio_column",
    "bam_lookup_id_columns",
    "bam_lookup_path_columns",
    "bam_suffixes",
    "violin_pdf_template",
    "expected_case_count",
    "stale_bai_policy",
    "overview_padding",
    "detail_padding",
    "estimated_runtime_seconds",
}
_DEFAULT_OPTIONS: dict[str, Any] = {
    "schema_version": SSQTL_ADAPTER_SCHEMA_VERSION,
    "association_columns": {"ag_site": "AG_site", "snp": "SNP", "strand": "strand"},
    "rds_filename_template": "AGratio_SNPgeno_{strand_token}_{chrom}_list.rds",
    "locus_sample_columns": list(DEFAULT_LOCUS_SAMPLE_COLUMNS),
    "ratio_column": "ratio",
    "bam_lookup_id_columns": list(DEFAULT_BAM_ID_COLUMNS),
    "bam_lookup_path_columns": list(DEFAULT_BAM_PATH_COLUMNS),
    "bam_suffixes": list(DEFAULT_BAM_SUFFIXES),
    "violin_pdf_template": "violin_plots_{strand_token}_{chrom}.pdf",
    "expected_case_count": None,
    "stale_bai_policy": "warn",
    "overview_padding": 55,
    "detail_padding": 12,
    "estimated_runtime_seconds": 90.0,
}


def _relative_directory(root: Path, value: str, *, label: str) -> tuple[str, Path]:
    relative = _relative_path(value, label=label)
    candidate = root.joinpath(*relative.parts)
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} path escapes its mounted root: {relative}") from exc
    if not resolved.is_dir():
        raise ValueError(f"{label} target is not a directory: {relative}")
    return relative.as_posix(), resolved


def _nonempty_token(value: Any, *, label: str) -> str:
    token = str(value).strip()
    if not token or any(character in token for character in "\r\n\t,"):
        raise ValueError(f"{label} must be a non-empty token without controls or commas")
    return token


def _token_list(value: Any, *, label: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} must be a non-empty list")
    result = [_nonempty_token(item, label=label) for item in value]
    if len(result) != len(set(result)):
        raise ValueError(f"{label} contains duplicate values")
    return result


def _filename_template(value: Any, *, label: str) -> str:
    template = _nonempty_token(value, label=label)
    if "/" in template or "\\" in template or ".." in template:
        raise ValueError(f"{label} must produce a file directly under its declared directory")
    if "{chrom}" not in template or "{strand_token}" not in template:
        raise ValueError(f"{label} must contain {{chrom}} and {{strand_token}}")
    unknown = template.replace("{chrom}", "").replace("{strand_token}", "").replace("{strand}", "")
    if "{" in unknown or "}" in unknown:
        raise ValueError(f"{label} contains an unsupported placeholder")
    return template


def _load_options(config_path: Path | None) -> tuple[dict[str, Any], str, str | None]:
    options = copy.deepcopy(_DEFAULT_OPTIONS)
    source_sha: str | None = None
    if config_path is not None:
        try:
            raw = config_path.read_bytes()
            value = yaml.safe_load(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
            raise ValueError(f"cannot read ssQTL adapter config: {exc}") from exc
        if not isinstance(value, Mapping):
            raise ValueError("ssQTL adapter config must contain one mapping")
        unknown = set(value) - _ALLOWED_OPTIONS
        if unknown:
            raise ValueError("unknown ssQTL adapter config fields: " + ", ".join(sorted(unknown)))
        if value.get("schema_version") != SSQTL_ADAPTER_SCHEMA_VERSION:
            raise ValueError("ssQTL adapter config schema_version must be 3.0-ssqtl")
        options.update(copy.deepcopy(dict(value)))
        source_sha = sha256_file(config_path)

    columns = options.get("association_columns")
    if not isinstance(columns, Mapping) or set(columns) != {"ag_site", "snp", "strand"}:
        raise ValueError("association_columns must define exactly ag_site, snp, and strand")
    options["association_columns"] = {
        key: _nonempty_token(columns[key], label=f"association_columns.{key}")
        for key in ("ag_site", "snp", "strand")
    }
    options["rds_filename_template"] = _filename_template(
        options["rds_filename_template"], label="rds_filename_template"
    )
    options["violin_pdf_template"] = _filename_template(
        options["violin_pdf_template"], label="violin_pdf_template"
    )
    for key in (
        "locus_sample_columns",
        "bam_lookup_id_columns",
        "bam_lookup_path_columns",
        "bam_suffixes",
    ):
        options[key] = _token_list(options[key], label=key)
    options["ratio_column"] = _nonempty_token(options["ratio_column"], label="ratio_column")
    if any("/" in suffix or "\\" in suffix or not suffix.lower().endswith(".bam") for suffix in options["bam_suffixes"]):
        raise ValueError("bam_suffixes must be path-free BAM filename suffixes")
    expected = options.get("expected_case_count")
    if expected is not None and (not isinstance(expected, int) or isinstance(expected, bool) or expected < 1):
        raise ValueError("expected_case_count must be null or a positive integer")
    if options.get("stale_bai_policy") not in {"warn", "fail"}:
        raise ValueError("stale_bai_policy must be warn or fail")
    for key in ("overview_padding", "detail_padding"):
        if not isinstance(options.get(key), int) or isinstance(options[key], bool) or options[key] < 0:
            raise ValueError(f"{key} must be a non-negative integer")
    runtime = options.get("estimated_runtime_seconds")
    if not isinstance(runtime, (int, float)) or isinstance(runtime, bool) or runtime <= 0:
        raise ValueError("estimated_runtime_seconds must be positive")
    options["estimated_runtime_seconds"] = float(runtime)
    return options, sha256_json(options), source_sha


def _resolved_bam_lookup_rows(
    source: Path,
    input_root: Path,
    *,
    id_candidates: list[str],
    path_candidates: list[str],
) -> tuple[list[str], str, str, list[tuple[dict[str, str], Path]]]:
    """Parse lookup rows once so normalization and resume binding share path semantics."""

    try:
        text = source.read_bytes().decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"BAM lookup must be UTF-8: {exc}") from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    fields = list(reader.fieldnames or [])
    id_column = next((value for value in id_candidates if value in fields), None)
    path_column = next((value for value in path_candidates if value in fields), None)
    if id_column is None or path_column is None:
        raise ValueError("BAM lookup lacks a configured ID or path column")
    rows: list[tuple[dict[str, str], Path]] = []
    for line_number, raw in enumerate(reader, 2):
        if None in raw:
            raise ValueError(f"BAM lookup line {line_number} has unexpected extra columns")
        row = {field: str(raw.get(field) or "").strip() for field in fields}
        sample_id = row[id_column]
        if not sample_id:
            raise ValueError(f"BAM lookup line {line_number} has an empty sample ID")
        relative = _relative_path(row[path_column], label=f"BAM lookup line {line_number}")
        candidate = input_root.joinpath(*relative.parts)
        resolved = candidate.resolve(strict=True)
        try:
            resolved.relative_to(input_root)
        except ValueError as exc:
            raise ValueError(f"BAM lookup line {line_number} escapes /input") from exc
        if not resolved.is_dir() and not resolved.is_file():
            raise ValueError(f"BAM lookup line {line_number} is neither a BAM nor a directory")
        if resolved.is_file() and resolved.suffix.lower() != ".bam":
            raise ValueError(f"BAM lookup line {line_number} file must end in .bam")
        rows.append((row, resolved))
    if not rows:
        raise ValueError("BAM lookup contains no data rows")
    return fields, id_column, path_column, rows


def _normalize_bam_lookup(
    source: Path,
    input_root: Path,
    output: Path,
    *,
    id_candidates: list[str],
    path_candidates: list[str],
) -> dict[str, Any]:
    fields, id_column, path_column, resolved_rows = _resolved_bam_lookup_rows(
        source,
        input_root,
        id_candidates=id_candidates,
        path_candidates=path_candidates,
    )
    rows: list[dict[str, str]] = []
    for raw_row, resolved in resolved_rows:
        row = dict(raw_row)
        row[path_column] = str(resolved)
        rows.append(row)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return {
        "id_column": id_column,
        "path_column": path_column,
        "row_count": len(rows),
    }


def _runtime_config(
    *,
    temporary: Path,
    associations: Path,
    rds_dir: Path,
    bam_lookup: Path,
    violin_dir: Path,
    reference: Mapping[str, Any],
    options: Mapping[str, Any],
) -> dict[str, Any]:
    resources = reference["resources"]
    return {
        "paths": {
            "associations": str(associations),
            "associations_sha256": sha256_file(associations),
            "rds_dir": str(rds_dir),
            "bam_lookup": str(bam_lookup),
            "violin_dir": str(violin_dir),
            "violin_pdf_template": options["violin_pdf_template"],
            "output_root": str(temporary / "science-runs"),
            "publish_root": str(temporary / "science-publish"),
        },
        "workflow": {
            "figure_contract_id": FIGURE_CONTRACT_ID,
            "gui_settle_contract_id": GUI_SETTLE_CONTRACT_ID,
        },
        "genome": {
            "id": reference["id"],
            "display_name": reference["display_name"],
            "annotation_version": reference["version"],
            **{
                role: resource["source_path"] for role, resource in resources.items()
            },
            **{
                f"{role}_sha256": resource["identity"]["sha256"]
                for role, resource in resources.items()
            },
        },
        "binaries": {"rscript": "Rscript", "igv": "/opt/igv/bin/igv"},
        "execution": {"mode": "local"},
        "scheduler": {
            "max_parallel": 1,
            "max_tasks_per_array": 1,
            "cases_per_task": 1,
            "memory_gb": 8,
            "total_parallel_memory_gb": 8,
        },
        "inputs": {
            "expected_case_count": options["expected_case_count"],
            "association_columns": options["association_columns"],
            "rds_filename_template": options["rds_filename_template"],
            "locus_sample_columns": options["locus_sample_columns"],
            "ratio_column": options["ratio_column"],
            "bam_lookup_id_columns": options["bam_lookup_id_columns"],
            "bam_lookup_path_columns": options["bam_lookup_path_columns"],
            "bam_suffixes": options["bam_suffixes"],
            "stale_bai_policy": options["stale_bai_policy"],
        },
        "render": {
            "overview_padding": options["overview_padding"],
            "detail_padding": options["detail_padding"],
        },
        "desktop": {"screen_width": 1920, "screen_height": 2160, "screen_depth": 24},
        "compose": {"violin_panel_width": 720},
        "publication": {"chromosomes": [], "generate_svg": False},
        "timeouts": {"r_prepare_seconds": 129600, "pdftotext_seconds": 900},
    }


def _rds_resource(
    task: Mapping[str, Any],
    *,
    input_root: Path,
    rds_declared: str,
    template: str,
) -> dict[str, Any]:
    strand = str(task["strand"])
    name = (
        template.replace("{strand_token}", "pos" if strand == "+" else "neg")
        .replace("{strand}", strand)
        .replace("{chrom}", str(task["ag"]["chrom"]))
    )
    declared = PurePosixPath(rds_declared, name).as_posix()
    relative, source = _resolve_relative_file(input_root, declared, label="ssQTL RDS")
    return {
        "declared_path": relative,
        "source_path": str(source),
        "identity": file_identity(source, sha256=sha256_file(source)),
    }


def _copy_preparation_artifact(
    source: str | Path, destination: Path, *, relative_path: str
) -> dict[str, Any]:
    source_value = Path(source)
    if source_value.is_symlink():
        raise ValueError(f"R preparation artifact must not be a symlink: {source_value}")
    source_path = source_value.resolve(strict=True)
    if not source_path.is_file():
        raise ValueError(f"R preparation artifact must be a regular file: {source_path}")
    target = destination / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, target)
    if target.is_symlink() or not target.is_file():
        raise RuntimeError(f"failed to freeze R preparation artifact: {target}")
    return {
        "relative_path": relative_path,
        "sha256": sha256_file(target),
        "size": target.stat().st_size,
    }


def _preparation_member(output: Path, value: str | Path, *, name: str) -> Path:
    source = Path(value)
    if source.is_symlink():
        raise ValueError(f"R preparation member must not be a symlink: {source}")
    resolved = source.resolve(strict=True)
    expected = (output / name).resolve(strict=True)
    if resolved != expected or not resolved.is_file():
        raise ValueError(f"R preparation member is outside its declared bundle: {name}")
    return resolved


def normalize_ssqtl_inputs(
    *,
    associations: str,
    rds_dir: str,
    bam_lookup: str,
    violin_dir: str,
    input_root: str | Path,
    reference_path: str | Path,
    output_dir: str | Path,
    run_id: str,
    generation_id: str,
    config: str | None = None,
) -> dict[str, Any]:
    """Normalize raw ssQTL sources directly into native schema-v3 tasks."""

    root = Path(input_root).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"input_root is not a directory: {root}")
    association_declared, association_path = _resolve_relative_file(
        root, associations, label="ssQTL associations"
    )
    bam_lookup_declared, bam_lookup_path = _resolve_relative_file(
        root, bam_lookup, label="ssQTL BAM lookup"
    )
    rds_declared, rds_root = _relative_directory(root, rds_dir, label="ssQTL RDS directory")
    violin_declared, violin_root = _relative_directory(
        root, violin_dir, label="ssQTL violin directory"
    )
    config_path: Path | None = None
    config_declared: str | None = None
    if config:
        config_declared, config_path = _resolve_relative_file(
            root, config, label="ssQTL adapter config"
        )
    options, options_sha, config_source_sha = _load_options(config_path)
    reference, reference_source = _load_reference(reference_path)

    destination = reject_symlink_path_components(
        output_dir, label="ssQTL normalization output"
    ).resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"normalization output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        with tempfile.TemporaryDirectory(prefix="igv-ssqtl-v3-", dir=destination.parent) as name:
            temporary = Path(name)
            normalized_lookup = temporary / "bam_lookup.normalized.csv"
            lookup_contract = _normalize_bam_lookup(
                bam_lookup_path,
                root,
                normalized_lookup,
                id_candidates=options["bam_lookup_id_columns"],
                path_candidates=options["bam_lookup_path_columns"],
            )
            config_value = _runtime_config(
                temporary=temporary,
                associations=association_path,
                rds_dir=rds_root,
                bam_lookup=normalized_lookup,
                violin_dir=violin_root,
                reference=reference,
                options=options,
            )
            params_path = temporary / "ssqtl-runtime-config.json"
            params_path.write_text(
                json.dumps(config_value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            resources = files("ssqtl_igv.resources")
            wrapper = Path(str(resources.joinpath("prepare_cases_wrapper.R")))
            implementation = Path(str(resources.joinpath("prepare_cases.R")))
            r_bundle = run_r_prepare(
                params_path,
                association_path,
                rds_root,
                normalized_lookup,
                temporary / "r_prepare_native",
                r_wrapper=wrapper,
                r_implementation=implementation,
            )
            r_output = Path(str(r_bundle["output_dir"])).resolve(strict=True)
            if not r_output.is_dir():
                raise ValueError("R preparation output is not a directory")
            prepared_cases_path = _preparation_member(
                r_output, r_bundle["prepared_cases"], name="prepared_cases.tsv"
            )
            prepared_samples_path = _preparation_member(
                r_output, r_bundle["prepared_samples"], name="prepared_samples.tsv"
            )
            r_report_path = _preparation_member(
                r_output, r_bundle["report"], name="r_prepare.json"
            )
            r_report = json.loads(r_report_path.read_text(encoding="utf-8"))
            if r_report.get("schema_version") != "2.0-r-prepare":
                raise ValueError("private R preparation report has an unexpected schema")
            if r_report.get("status") != "PASS":
                raise ValueError("ssQTL R preparation did not produce a passing report")
            expected_report_identity = {
                "association_sha256": sha256_file(association_path),
                "bam_lookup_sha256": sha256_file(normalized_lookup),
                "r_wrapper_sha256": sha256_file(wrapper),
                "r_implementation_sha256": sha256_file(implementation),
            }
            for key, expected in expected_report_identity.items():
                if r_report.get(key) != expected:
                    raise RuntimeError(
                        f"R preparation report identity differs from declared input: {key}"
                    )
            private = normalize_manifest(
                params_path,
                temporary / "prepared",
                run_id=run_id,
                generation_id=generation_id,
                associations=association_path,
                prepared_cases=prepared_cases_path,
                prepared_samples=prepared_samples_path,
                rds_dir=rds_root,
                bam_lookup=normalized_lookup,
                violin_dir=violin_root,
                genome_definition=reference["resources"]["definition"]["source_path"],
                fasta=reference["resources"]["fasta"]["source_path"],
                fai=reference["resources"]["fai"]["source_path"],
                cytoband=reference["resources"]["cytoband"]["source_path"],
                annotation=reference["resources"]["annotation"]["source_path"],
                expected_case_count=options["expected_case_count"],
                estimated_runtime_seconds=options["estimated_runtime_seconds"],
            )
            prepared_tasks = list(read_jsonl(private["tasks"]))
            if not prepared_tasks:
                raise ValueError("ssQTL normalization produced no cases")
            base_evidence = {
                "association_declared_path": association_declared,
                "association_sha256": sha256_file(association_path),
                "bam_lookup_declared_path": bam_lookup_declared,
                "bam_lookup_sha256": sha256_file(bam_lookup_path),
                "adapter_config_sha256": options_sha,
                "r_wrapper_sha256": sha256_file(wrapper),
                "r_implementation_sha256": sha256_file(implementation),
                "prepared_cases_sha256": str(r_report["prepared_cases_sha256"]),
                "prepared_samples_sha256": str(r_report["prepared_samples_sha256"]),
            }
            tasks: list[dict[str, Any]] = []
            rds_inventory: dict[str, dict[str, Any]] = {}
            for prepared in prepared_tasks:
                rds = _rds_resource(
                    prepared,
                    input_root=root,
                    rds_declared=rds_declared,
                    template=options["rds_filename_template"],
                )
                rds_inventory[rds["declared_path"]] = rds
                evidence = {**base_evidence, "rds": rds}
                tasks.append(
                    native_ssqtl_task_from_prepared(
                        prepared,
                        input_root=root,
                        reference=reference,
                        preparation_evidence=evidence,
                    )
                )
            tasks = validate_unique_task_set(tasks)

            tasks_path = staging / "tasks.jsonl"
            write_jsonl(tasks_path, tasks)
            write_tsv(
                staging / "normalized_manifest.tsv",
                [
                    "manifest_order",
                    "task_id",
                    "association_row",
                    "ag_site",
                    "snp",
                    "strand",
                    "track_count",
                    "empty_genotype_groups",
                    "violin_state",
                    "preflight_state",
                    "input_fingerprint",
                ],
                (
                    {
                        "manifest_order": task["manifest_order"],
                        "task_id": task["task_id"],
                        "association_row": task["adapter_data"]["association"]["row"],
                        "ag_site": task["adapter_data"]["ag"]["raw"],
                        "snp": task["adapter_data"]["snp"]["raw"],
                        "strand": task["core"]["strand"],
                        "track_count": len(task["core"]["tracks"]),
                        "empty_genotype_groups": ",".join(
                            genotype
                            for genotype in _GENOTYPES
                            if task["adapter_data"]["genotype_groups"][genotype]["empty"]
                        ),
                        "violin_state": task["adapter_data"]["violin"]["state"],
                        "preflight_state": task["core"]["preflight"]["state"],
                        "input_fingerprint": task["input_fingerprint"],
                    }
                    for task in tasks
                ),
            )
            preparation_artifacts = {
                "prepared_cases": _copy_preparation_artifact(
                    prepared_cases_path, staging, relative_path="prepared_cases.tsv"
                ),
                "prepared_samples": _copy_preparation_artifact(
                    prepared_samples_path, staging, relative_path="prepared_samples.tsv"
                ),
                "r_stdout": _copy_preparation_artifact(
                    r_output / "r_prepare.stdout.log",
                    staging,
                    relative_path="r_prepare.stdout.log",
                ),
                "r_stderr": _copy_preparation_artifact(
                    r_output / "r_prepare.stderr.log",
                    staging,
                    relative_path="r_prepare.stderr.log",
                ),
            }
            if (
                preparation_artifacts["prepared_cases"]["sha256"]
                != r_report["prepared_cases_sha256"]
                or preparation_artifacts["prepared_samples"]["sha256"]
                != r_report["prepared_samples_sha256"]
                or preparation_artifacts["r_stdout"]["sha256"] != r_report["stdout_sha256"]
                or preparation_artifacts["r_stderr"]["sha256"] != r_report["stderr_sha256"]
            ):
                raise RuntimeError("frozen R preparation artifact differs from its execution report")
            native_r_report = {
                "schema_version": "3.0-ssqtl-r-prepare",
                "pipeline_version": V3_PIPELINE_VERSION,
                "status": "PASS",
                "started_at": r_report["started_at"],
                "finished_at": r_report["finished_at"],
                "wall_time_seconds": r_report["wall_time_seconds"],
                "association_sha256": r_report["association_sha256"],
                "bam_lookup_sha256": base_evidence["bam_lookup_sha256"],
                "normalized_bam_lookup_sha256": r_report["bam_lookup_sha256"],
                "r_wrapper_sha256": r_report["r_wrapper_sha256"],
                "r_implementation_sha256": r_report["r_implementation_sha256"],
                "case_count": r_report["case_count"],
                "sample_count": r_report["sample_count"],
                "prepared_cases_sha256": r_report["prepared_cases_sha256"],
                "prepared_samples_sha256": r_report["prepared_samples_sha256"],
                "stdout_sha256": r_report["stdout_sha256"],
                "stderr_sha256": r_report["stderr_sha256"],
            }
            atomic_write_json(staging / "r_prepare.json", native_r_report)
            preparation_artifacts["r_report"] = {
                "relative_path": "r_prepare.json",
                "sha256": sha256_file(staging / "r_prepare.json"),
                "size": (staging / "r_prepare.json").stat().st_size,
            }
            preparation_artifact_set_sha256 = sha256_json(preparation_artifacts)
            preparation_receipt = {
                "schema_version": "3.0-ssqtl-preparation",
                "status": "PASS",
                **base_evidence,
                "association_row_count": int(r_report["case_count"]),
                "selected_sample_count": int(r_report["sample_count"]),
                "r_stdout_sha256": str(r_report["stdout_sha256"]),
                "r_stderr_sha256": str(r_report["stderr_sha256"]),
                "bam_lookup_contract": lookup_contract,
                "artifacts": preparation_artifacts,
                "artifact_set_sha256": preparation_artifact_set_sha256,
                "rds_resources": [
                    rds_inventory[key] for key in sorted(rds_inventory)
                ],
            }
            atomic_write_json(staging / "ssqtl_preparation.json", preparation_receipt)
            parameters = {
                "schema_version": V3_CANONICAL_SCHEMA_VERSION,
                "pipeline_version": V3_PIPELINE_VERSION,
                "adapter_id": "ssqtl",
                "adapter_schema_version": SSQTL_ADAPTER_SCHEMA_VERSION,
                "run_id": run_id,
                "generation_id": generation_id,
                "input_root": str(root),
                "associations": association_declared,
                "rds_dir": rds_declared,
                "bam_lookup": bam_lookup_declared,
                "violin_dir": violin_declared,
                "adapter_config": config_declared,
                "adapter_config_source_sha256": config_source_sha,
                "adapter_config_sha256": options_sha,
                "source_reference": str(reference_source),
                "source_reference_sha256": sha256_file(reference_source),
            }
            atomic_write_json(staging / "parameters.json", parameters)
            invalid_count = sum(
                task["core"]["preflight"]["state"] != "READY" for task in tasks
            )
            validation = {
                "schema_version": V3_CANONICAL_SCHEMA_VERSION,
                "pipeline_version": V3_PIPELINE_VERSION,
                "status": "PASS_WITH_CASE_INPUT_ERRORS" if invalid_count else "PASS",
                "adapter_id": "ssqtl",
                "adapter_schema_version": SSQTL_ADAPTER_SCHEMA_VERSION,
                "run_id": run_id,
                "generation_id": generation_id,
                "task_count": len(tasks),
                "ready_task_count": len(tasks) - invalid_count,
                "case_input_invalid_count": invalid_count,
                "task_set_sha256": task_set_fingerprint(tasks),
                "tasks_sha256": sha256_file(tasks_path),
                "reference_resource_fingerprint": reference["resource_fingerprint"],
                "source_associations_sha256": sha256_file(association_path),
                "source_bam_lookup_sha256": sha256_file(bam_lookup_path),
                "adapter_config_sha256": options_sha,
                "normalized_manifest_sha256": sha256_file(
                    staging / "normalized_manifest.tsv"
                ),
                "parameters_sha256": sha256_file(staging / "parameters.json"),
                "preparation_receipt_sha256": sha256_file(
                    staging / "ssqtl_preparation.json"
                ),
                "preparation_artifact_set_sha256": preparation_artifact_set_sha256,
            }
            atomic_write_json(staging / "validation.json", validation)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **validation,
        "output_dir": str(destination),
        "tasks": str(destination / "tasks.jsonl"),
        "normalized_manifest": str(destination / "normalized_manifest.tsv"),
        "validation": str(destination / "validation.json"),
        "parameters": str(destination / "parameters.json"),
        "preparation": str(destination / "ssqtl_preparation.json"),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ssqtl_igv.ssqtl_adapter_v3",
        description="Normalize raw ssQTL inputs into native schema-v3 tasks",
    )
    parser.add_argument("--associations", required=True)
    parser.add_argument("--rds-dir", required=True)
    parser.add_argument("--bam-lookup", required=True)
    parser.add_argument("--violin-dir", required=True)
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--reference", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--generation-id", required=True)
    parser.add_argument("--config")
    args = parser.parse_args(argv)
    result = normalize_ssqtl_inputs(
        associations=args.associations,
        rds_dir=args.rds_dir,
        bam_lookup=args.bam_lookup,
        violin_dir=args.violin_dir,
        input_root=args.input_root,
        reference_path=args.reference,
        output_dir=args.output_dir,
        run_id=args.run_id,
        generation_id=args.generation_id,
        config=args.config,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
