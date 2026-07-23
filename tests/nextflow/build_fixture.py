#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import time
from pathlib import Path
from typing import Any


TASK_ID = "AG_chrA_2_3__SNP_chrA_4_T_C"


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_tsv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def build_fixture(root: Path, project_root: Path) -> dict[str, str]:
    fixture = root / "fixture"
    if fixture.exists() or fixture.is_symlink():
        raise FileExistsError(f"fixture destination already exists: {fixture}")
    inputs = fixture / "inputs"
    rds_dir = inputs / "rds"
    violin_dir = inputs / "violin"
    rds_dir.mkdir(parents=True)
    violin_dir.mkdir(parents=True)

    associations = inputs / "associations.csv"
    bam_lookup = inputs / "bam_lookup.csv"
    genome_definition = inputs / "genome.json"
    fasta = inputs / "genome.fa"
    fai = inputs / "genome.fa.fai"
    cytoband = inputs / "cytoband.txt.gz"
    annotation = inputs / "annotation.gff.gz"
    violin_pdf = violin_dir / "violin_plots_pos_chrA.pdf"
    bam = inputs / "sample-1.bam"
    bai = inputs / "sample-1.bam.bai"

    _write(
        associations,
        "AG_site,SNP,strand,n_total,n_0,n_1,n_2,Beta,abs_Tvalue\n"
        "chrA:2-3,chrA.4_T.C,+,1,1,0,0,0.5,2.0\n",
    )
    _write(bam_lookup, f"sample_id,bam\nsample-1,{bam}\n")
    _write(genome_definition, "{}\n")
    _write(fasta, ">chrA\nAAGTC\n")
    _write(fai, "chrA\t5\t6\t5\t6\n")
    _write(cytoband, "cytoband")
    _write(annotation, "annotation")
    _write(violin_pdf, "fixture PDF payload")
    _write(bam, "bam")
    _write(bai, "bai")
    timestamp = time.time_ns()
    os.utime(bam, ns=(timestamp, timestamp))
    os.utime(bai, ns=(timestamp + 1_000_000_000, timestamp + 1_000_000_000))

    prepared_cases = fixture / "prepared_cases.tsv"
    prepared_samples = fixture / "prepared_samples.tsv"
    _write_tsv(
        prepared_cases,
        [
            "association_row",
            "case_id",
            "ag_site",
            "snp",
            "strand",
            "n_total",
            "n_0",
            "n_1",
            "n_2",
            "eligible_n_0",
            "eligible_n_1",
            "eligible_n_2",
            "beta",
            "abs_tvalue",
            "error_code",
            "error_message",
        ],
        [
            {
                "association_row": 1,
                "case_id": TASK_ID,
                "ag_site": "chrA:2-3",
                "snp": "chrA.4_T.C",
                "strand": "+",
                "n_total": 1,
                "n_0": 1,
                "n_1": 0,
                "n_2": 0,
                "eligible_n_0": 1,
                "eligible_n_1": 0,
                "eligible_n_2": 0,
                "beta": 0.5,
                "abs_tvalue": 2.0,
                "error_code": "",
                "error_message": "",
            }
        ],
    )
    _write_tsv(
        prepared_samples,
        [
            "case_id",
            "genotype",
            "sample_id",
            "dosage",
            "ratio",
            "selection_label",
            "bam",
            "bai",
            "bai_fresh",
        ],
        [
            {
                "case_id": TASK_ID,
                "genotype": "0/0",
                "sample_id": "sample-1",
                "dosage": 0,
                "ratio": 0.25,
                "selection_label": "all",
                "bam": str(bam),
                "bai": str(bai),
                "bai_fresh": "true",
            }
        ],
    )

    params_file = fixture / "params.json"
    params = {
        "paths": {
            "associations": str(associations),
            "associations_sha256": _sha256(associations),
            "rds_dir": str(rds_dir),
            "bam_lookup": str(bam_lookup),
            "violin_dir": str(violin_dir),
            "violin_pdf_template": "violin_plots_{strand_token}_{chrom}.pdf",
            "output_root": str(fixture / "unused-runs"),
            "publish_root": str(fixture / "unused-publish"),
        },
        "workflow": {
            "figure_contract_id": "v031_native_igv_pixel_exact",
            "gui_settle_contract_id": "v031_toolbar_locus_settle_v1",
        },
        "genome": {
            "id": "hg38_MANEv1.5",
            "display_name": "Fixture genome",
            "definition": str(genome_definition),
            "definition_sha256": _sha256(genome_definition),
            "fasta": str(fasta),
            "fasta_sha256": _sha256(fasta),
            "fai": str(fai),
            "fai_sha256": _sha256(fai),
            "cytoband": str(cytoband),
            "cytoband_sha256": _sha256(cytoband),
            "annotation": str(annotation),
            "annotation_version": "MANE v1.5",
            "annotation_sha256": _sha256(annotation),
        },
        "binaries": {
            "rscript": "Rscript",
            "igv": "igv",
            "pdftotext": str(project_root / "tests/fixtures/bin/pdftotext"),
            "pdftoppm": "pdftoppm",
            "qsub": "qsub",
            "qacct": "qacct",
        },
        "execution": {"mode": "local"},
        "scheduler": {
            "max_parallel": 1,
            "max_tasks_per_array": 1,
            "cases_per_task": 1,
            "memory_gb": 8,
            "total_parallel_memory_gb": 8,
        },
        "inputs": {"expected_case_count": 1, "stale_bai_policy": "warn"},
        "render": {"overview_padding": 55, "detail_padding": 12},
        "desktop": {
            "screen_width": 1920,
            "screen_height": 2160,
            "screen_depth": 24,
            "toolbar_locus_roi": {"x": 0, "y": 0, "width": 1920, "height": 120},
            "locus_field_roi": {"x": 467, "y": 27, "width": 226, "height": 24},
        },
        "compose": {"violin_panel_width": 720},
        "publication": {"chromosomes": [], "generate_svg": False},
        "storage": {
            "provider": "filesystem",
            "minimum_free_gb": 0,
            "minimum_free_inodes": 0,
            "gate_max_age_seconds": 1800,
            "remaining_case_buffer_factor": 1,
            "work_gb_per_case": 0,
            "publish_gb_per_case": 0,
            "scratch_gb_per_parallel_task": 0,
            "reserve_gb": 0,
            "work_inodes_per_case": 0,
            "publish_inodes_per_case": 0,
            "reserve_inodes": 0,
        },
    }
    params_file.write_text(json.dumps(params, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    reviews = fixture / "reviews.jsonl"
    reviews.touch()
    r_implementation = fixture / "r_prepare_implementation.R"
    shutil.copy2(
        project_root / "src/ssqtl_igv/resources/prepare_cases.R",
        r_implementation,
    )
    return {
        "fixture": str(fixture),
        "params_file": str(params_file),
        "prepared_cases": str(prepared_cases),
        "prepared_samples": str(prepared_samples),
        "reviews": str(reviews),
        "r_implementation": str(r_implementation),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the one-case Nextflow test fixture")
    parser.add_argument("root", type=Path)
    parser.add_argument("--project-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.expanduser().resolve(strict=True)
    project_root = args.project_root.expanduser().resolve(strict=True)
    print(json.dumps(build_fixture(root, project_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
