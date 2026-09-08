"""Optional, offline, real-render installation test; never a run prerequisite."""
from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path
from typing import Any

from .utils import reject_symlink_path_components


def create_smoke_project(root: Path, *, samtools: str = "samtools") -> Path:
    """Create two synthetic loci with real indexed BAM data in a new directory."""
    root = reject_symlink_path_components(root, label="smoke project").resolve(strict=False)
    root.mkdir(parents=True, exist_ok=False)
    reference = root / "reference"
    reference.mkdir()
    sequence = "ACGT" * 1000
    fasta = reference / "genome.fa"
    fasta.write_text(">chr1\n" + "\n".join(sequence[i:i + 80] for i in range(0, len(sequence), 80)) + "\n", encoding="ascii")
    subprocess.run([samtools, "faidx", str(fasta)], check=True, capture_output=True, text=True, timeout=60)
    (reference / "genome.json").write_text("{}\n", encoding="ascii")
    (reference / "cytoband.txt").write_text("chr1\t0\t4000\tp1\tgneg\n", encoding="ascii")
    (reference / "annotation.gff").write_text(
        "##gff-version 3\n"
        "chr1\tsmoke\tgene\t301\t700\t.\t+\t.\tID=gene1;Name=smoke_gene_1\n"
        "chr1\tsmoke\tmRNA\t301\t700\t.\t+\t.\tID=tx1;Parent=gene1\n"
        "chr1\tsmoke\texon\t301\t700\t.\t+\t.\tID=exon1;Parent=tx1\n"
        "chr1\tsmoke\tgene\t1201\t1600\t.\t-\t.\tID=gene2;Name=smoke_gene_2\n"
        "chr1\tsmoke\tmRNA\t1201\t1600\t.\t-\t.\tID=tx2;Parent=gene2\n"
        "chr1\tsmoke\texon\t1201\t1600\t.\t-\t.\tID=exon2;Parent=tx2\n",
        encoding="ascii",
    )
    sam = root / "reads.sam"
    with sam.open("w", encoding="ascii") as handle:
        handle.write("@HD\tVN:1.6\tSO:coordinate\n@SQ\tSN:chr1\tLN:4000\n")
        for region, start in enumerate((350, 1250), 1):
            for index in range(40):
                position = start + index * 3
                read = sequence[position - 1:position - 1 + 100]
                handle.write(f"read{region}_{index}\t0\tchr1\t{position}\t60\t100M\t*\t0\t0\t{read}\t{'I' * 100}\n")
    bam = root / "reads.bam"
    subprocess.run([samtools, "view", "-b", "-o", str(bam), str(sam)], check=True, capture_output=True, text=True, timeout=60)
    subprocess.run([samtools, "index", str(bam)], check=True, capture_output=True, text=True, timeout=60)
    (reference / "reference.yaml").write_text(
        'schema_version: "3.0"\nid: smoke-local\ndisplay_name: IGV smoke test\nversion: synthetic-v1\nresources:\n'
        + "".join(f"  {role}:\n    path: {name}\n" for role, name in (
            ("definition", "genome.json"), ("fasta", "genome.fa"), ("fai", "genome.fa.fai"),
            ("cytoband", "cytoband.txt"), ("annotation", "annotation.gff"))), encoding="ascii")
    with (root / "cases.tsv").open("w", encoding="utf-8", newline="") as handle:
        from .v3_manifest import GENERIC_MANIFEST_FIELDS
        writer = csv.DictWriter(handle, fieldnames=GENERIC_MANIFEST_FIELDS, delimiter="\t")
        writer.writeheader()
        for index, (locus, strand) in enumerate((("chr1:301-700", "+"), ("chr1:1201-1600", "-")), 1):
            writer.writerow(dict(schema_version="3.0", case_id=f"smoke_{index}", locus=locus, strand=strand,
                                 bam="reads.bam", bai="reads.bam.bai", track_label="Synthetic reads", group="smoke"))
    project = root / "project.yaml"
    project.write_text('schema_version: "3.0"\nadapter: generic\ninputs:\n  cases: cases.tsv\nreference: reference/reference.yaml\n', encoding="ascii")
    return project


def run_smoke_test(output: str | Path) -> dict[str, Any]:
    """Run the ordinary production DAG; reject stale output and stub evidence."""
    from .project_launcher import run_project_workflow
    from .v3_cli import _embedded_runtime_manifest

    root = reject_symlink_path_components(output, label="smoke output").resolve(strict=False)
    root.mkdir(parents=True, exist_ok=False)
    project = create_smoke_project(root / "project")
    results = root / "results"
    summary, code = run_project_workflow(
        project=project, batch_request=None, output=results, work=results / ".work",
        resume=False, max_parallel=1, max_cases_per_shard=256,
        runtime_manifest=_embedded_runtime_manifest(), igv_memory="4GiB",
    )
    if code:
        return {"status": "SMOKE_TEST_FAILED", "exit_code": code, "output": str(root), "run": summary}
    with (results / "snapshots.tsv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    if {row["task_id"] for row in rows} != {"smoke_1", "smoke_2"} or len(rows) != 2:
        raise RuntimeError("real smoke test did not account for both expected tasks")
    from .qc import inspect_png
    from .utils import sha256_file
    for row in rows:
        relative = Path(row["relative_path"])
        image = (results / relative).resolve(strict=True)
        if relative.is_absolute() or ".." in relative.parts or not image.is_relative_to(results):
            raise RuntimeError("smoke test image escaped its output directory")
        if row["status"] != "SNAPSHOT_READY" or not image.is_file() or sha256_file(image) != row["sha256"]:
            raise RuntimeError("smoke test returned missing or inconsistent image evidence")
        if inspect_png(image, min_width=100, min_height=100, min_stddev=0.5)["status"] != "PASS":
            raise RuntimeError("smoke test image failed independent PNG checks")
    return {"status": "SMOKE_TEST_PASSED", "exit_code": 0, "case_count": 2, "output": str(root), "run": summary}
