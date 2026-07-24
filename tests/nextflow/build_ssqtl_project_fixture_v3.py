#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_fixture(root: Path) -> dict[str, str]:
    project = root / "project"
    if project.exists() or project.is_symlink():
        raise FileExistsError(f"fixture project already exists: {project}")
    for directory in ("rds", "tracks", "violin", "reference"):
        (project / directory).mkdir(parents=True)

    _write(
        project / "associations.csv",
        "AG_site,SNP,strand,n_total,n_0,n_1,n_2,Beta,abs_Tvalue\n"
        "chrA:2-3,chrA.4_T.C,+,1,1,0,0,0.5,2.0\n",
    )
    _write(
        project / "bam_lookup.csv",
        "sample_id,bam\n"
        "sample-1,tracks/sample-1.bam\n",
    )
    bam = project / "tracks" / "sample-1.bam"
    bai = project / "tracks" / "sample-1.bam.bai"
    bam.write_bytes(b"synthetic-bam")
    bai.write_bytes(b"synthetic-bai")
    timestamp = time.time_ns()
    os.utime(bam, ns=(timestamp, timestamp))
    os.utime(bai, ns=(timestamp + 1_000_000_000, timestamp + 1_000_000_000))

    reference_root = project / "reference"
    resources = {
        "definition": reference_root / "genome.json",
        "fasta": reference_root / "genome.fa",
        "fai": reference_root / "genome.fa.fai",
        "cytoband": reference_root / "cytoband.txt",
        "annotation": reference_root / "annotation.gff",
    }
    _write(resources["definition"], "{}\n")
    _write(resources["fasta"], ">chrA\nAAGTC\n")
    _write(resources["fai"], "chrA\t5\t6\t5\t6\n")
    _write(resources["cytoband"], "chrA\t0\t5\tp1\tgneg\n")
    _write(resources["annotation"], "##gff-version 3\n")
    resource_lines = "\n".join(
        f"  {role}:\n    path: {path.name}\n    sha256: {_sha256(path)}"
        for role, path in resources.items()
    )
    _write(
        reference_root / "reference.yaml",
        'schema_version: "3.0"\n'
        "id: fixture-ssqtl\n"
        "display_name: Native ssQTL PROJECT_RUN fixture\n"
        "version: fixture-v1\n"
        "resources:\n"
        f"{resource_lines}\n",
    )
    _write(
        project / "ssqtl.yaml",
        "schema_version: 3.0-ssqtl\n"
        "expected_case_count: 1\n"
        "stale_bai_policy: fail\n",
    )
    _write(
        project / "project.yaml",
        'schema_version: "3.0"\n'
        "adapter: ssqtl\n"
        "inputs:\n"
        "  associations: associations.csv\n"
        "  rds_dir: rds\n"
        "  bam_lookup: bam_lookup.csv\n"
        "  violin_dir: violin\n"
        "  config: ssqtl.yaml\n"
        "reference: reference/reference.yaml\n",
    )
    result = {
        "project": str(project / "project.yaml"),
        "project_root": str(project),
    }
    print(json.dumps(result, sort_keys=True))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build a raw ssQTL PROJECT_RUN fixture"
    )
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.expanduser().resolve(strict=True)
    build_fixture(root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
