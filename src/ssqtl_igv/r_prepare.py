from __future__ import annotations

import csv
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .utils import atomic_write_json, atomic_write_text, command_prefix, sha256_file, utc_now


DEFAULT_LOCUS_SAMPLE_COLUMNS = ("sample_id",)
DEFAULT_BAM_ID_COLUMNS = ("sample_id",)
DEFAULT_BAM_PATH_COLUMNS = ("directory", "bam", "bam_path", "path")
DEFAULT_BAM_SUFFIXES = (
    ".accepted_hits.merged.markeddups.recal.bam",
    ".accepted_hits.merged.markdups.recal.bam",
    ".accepted_hits.merged.nodups.recal.bam",
    ".accepted_hits.merged.dedups.recal.bam",
    ".accepted_hits.bam",
    "_Aligned.sortedByCoord.out.bam",
    ".Aligned.sortedByCoord.out.bam",
    ".bam",
)


def _regular_file(value: str | Path, label: str) -> Path:
    path = Path(value).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ValueError(f"{label} is not a regular file: {path}")
    return path


def _directory(value: str | Path, label: str) -> Path:
    path = Path(value).expanduser().resolve(strict=True)
    if not path.is_dir():
        raise ValueError(f"{label} is not a directory: {path}")
    return path


def _row_count(path: Path) -> int:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not reader.fieldnames:
            raise ValueError(f"R preparation output lacks a TSV header: {path}")
        return sum(1 for _row in reader)


def run_r_prepare(
    params_path: str | Path,
    associations: str | Path,
    rds_dir: str | Path,
    bam_lookup: str | Path,
    output_dir: str | Path,
    *,
    r_wrapper: str | Path,
    r_implementation: str | Path,
) -> dict[str, Any]:
    """Run the cohort R preparation as declared inputs to declared outputs."""

    params = WorkflowConfig.load(params_path)
    association_path = _regular_file(associations, "association table")
    rds_root = _directory(rds_dir, "RDS root")
    bam_lookup_path = _regular_file(bam_lookup, "BAM lookup")
    wrapper = _regular_file(r_wrapper, "R preparation wrapper")
    implementation = _regular_file(r_implementation, "R preparation implementation")
    if wrapper == implementation:
        raise ValueError("R preparation wrapper and implementation must be distinct files")
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"R preparation output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        runtime = staging / ".r_runtime"
        runtime.mkdir(mode=0o700)
        staged_wrapper = runtime / "prepare_cases_wrapper.R"
        staged_implementation = runtime / "prepare_cases_implementation.R"
        shutil.copy2(wrapper, staged_wrapper)
        shutil.copy2(implementation, staged_implementation)
        wrapper_sha256 = sha256_file(wrapper)
        implementation_sha256 = sha256_file(implementation)
        if sha256_file(staged_wrapper) != wrapper_sha256:
            raise RuntimeError("staged R preparation wrapper identity mismatch")
        if sha256_file(staged_implementation) != implementation_sha256:
            raise RuntimeError("staged R preparation implementation identity mismatch")
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    cases = staging / "prepared_cases.tsv"
    samples = staging / "prepared_samples.tsv"
    stdout_path = staging / "r_prepare.stdout.log"
    stderr_path = staging / "r_prepare.stderr.log"
    association_columns = params.get("inputs.association_columns", {})
    if not isinstance(association_columns, dict):
        raise ValueError("inputs.association_columns must be a mapping")
    command = [
        *command_prefix(params.get("binaries.rscript"), default="Rscript"),
        str(staged_wrapper),
        f"--associations={association_path}",
        f"--rds_dir={rds_root}",
        f"--bam_lookup={bam_lookup_path}",
        f"--cases_out={cases}",
        f"--samples_out={samples}",
        f"--ag_column={association_columns.get('ag_site', 'AG_site')}",
        f"--snp_column={association_columns.get('snp', 'SNP')}",
        f"--strand_column={association_columns.get('strand', 'strand')}",
        f"--rds_filename_template={params.get('inputs.rds_filename_template', 'AGratio_SNPgeno_{strand_token}_{chrom}_list.rds')}",
        f"--locus_sample_columns={','.join(params.get('inputs.locus_sample_columns', DEFAULT_LOCUS_SAMPLE_COLUMNS))}",
        f"--ratio_column={params.get('inputs.ratio_column', 'ratio')}",
        f"--bam_lookup_id_columns={','.join(params.get('inputs.bam_lookup_id_columns', DEFAULT_BAM_ID_COLUMNS))}",
        f"--bam_lookup_path_columns={','.join(params.get('inputs.bam_lookup_path_columns', DEFAULT_BAM_PATH_COLUMNS))}",
        f"--bam_suffixes={','.join(params.get('inputs.bam_suffixes', DEFAULT_BAM_SUFFIXES))}",
    ]
    started_at = utc_now()
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=int(params.get("timeouts.r_prepare_seconds", 129600)),
        )
        atomic_write_text(stdout_path, completed.stdout)
        atomic_write_text(stderr_path, completed.stderr)
        if completed.returncode != 0:
            raise RuntimeError(
                f"R preparation failed ({completed.returncode}): {completed.stderr.strip()}"
            )
        if not cases.is_file() or not samples.is_file():
            raise RuntimeError("R preparation did not emit both declared TSV outputs")
        shutil.rmtree(runtime)
        report = {
            "schema_version": "2.0-r-prepare",
            "status": "PASS",
            "started_at": started_at,
            "finished_at": utc_now(),
            "wall_time_seconds": round(time.monotonic() - started, 3),
            "association_sha256": sha256_file(association_path),
            "bam_lookup_sha256": sha256_file(bam_lookup_path),
            "r_wrapper_sha256": wrapper_sha256,
            "r_implementation_sha256": implementation_sha256,
            "case_count": _row_count(cases),
            "sample_count": _row_count(samples),
            "prepared_cases_sha256": sha256_file(cases),
            "prepared_samples_sha256": sha256_file(samples),
            "stdout_sha256": sha256_file(stdout_path),
            "stderr_sha256": sha256_file(stderr_path),
        }
        atomic_write_json(staging / "r_prepare.json", report)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **report,
        "output_dir": str(destination),
        "prepared_cases": str(destination / cases.name),
        "prepared_samples": str(destination / samples.name),
        "report": str(destination / "r_prepare.json"),
    }
