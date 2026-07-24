#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import yaml

from ssqtl_igv.utils import sha256_file
from ssqtl_igv.v3_manifest import GENERIC_MANIFEST_FIELDS


def build_fixture(root: Path, *, cases: int = 2) -> dict[str, str]:
    project = root / "project"
    inputs = project / "input"
    reference_root = project / "reference"
    inputs.mkdir(parents=True)
    reference_root.mkdir()

    resources = {
        "definition": reference_root / "genome.json",
        "fasta": reference_root / "genome.fa",
        "fai": reference_root / "genome.fa.fai",
        "cytoband": reference_root / "cytoband.txt",
        "annotation": reference_root / "annotation.gff",
    }
    resources["definition"].write_text("{}\n", encoding="utf-8")
    resources["fasta"].write_text(">chr1\nACGTACGTACGT\n", encoding="utf-8")
    resources["fai"].write_text("chr1\t12\t6\t12\t13\n", encoding="utf-8")
    resources["cytoband"].write_text("chr1\t0\t12\tp1\tgneg\n", encoding="utf-8")
    resources["annotation"].write_text("##gff-version 3\n", encoding="utf-8")

    reference = reference_root / "reference.yaml"
    reference.write_text(
        yaml.safe_dump(
            {
                "schema_version": "3.0",
                "id": "fixture",
                "display_name": "Native PROJECT_RUN fixture",
                "version": "fixture-v1",
                "resources": {
                    role: {"path": path.name, "sha256": sha256_file(path)}
                    for role, path in resources.items()
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    manifest = project / "cases.tsv"
    with manifest.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=GENERIC_MANIFEST_FIELDS, delimiter="\t"
        )
        writer.writeheader()
        for index in range(1, cases + 1):
            bam = inputs / f"case-{index}.bam"
            bai = inputs / f"case-{index}.bam.bai"
            bam.write_bytes(f"bam-{index}".encode("ascii"))
            bai.write_bytes(f"bai-{index}".encode("ascii"))
            writer.writerow(
                {
                    "schema_version": "3.0",
                    "case_id": f"case_{index}",
                    "locus": f"chr1:{index}-{index + 1}",
                    "strand": "+",
                    "bam": f"input/{bam.name}",
                    "bai": f"input/{bai.name}",
                    "track_label": f"track {index}",
                    "group": "fixture",
                    "aux_path": "",
                    "aux_page": "",
                }
            )

    project_yaml = project / "project.yaml"
    project_yaml.write_text(
        'schema_version: "3.0"\n'
        "adapter: generic\n"
        "inputs:\n"
        "  cases: cases.tsv\n"
        "reference: reference/reference.yaml\n",
        encoding="utf-8",
    )
    result = {"project": str(project_yaml), "project_root": str(project)}
    print(json.dumps(result, sort_keys=True))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a native PROJECT_RUN fixture")
    parser.add_argument("root", type=Path)
    parser.add_argument("--cases", type=int, default=2)
    args = parser.parse_args()
    if args.cases < 1:
        parser.error("--cases must be positive")
    root = args.root.expanduser().resolve(strict=True)
    build_fixture(root, cases=args.cases)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
