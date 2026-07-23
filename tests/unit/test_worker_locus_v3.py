from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from ssqtl_igv.v3_worker import _samtools_validate


def _track() -> dict:
    return {
        "track_label": "fixture",
        "bam": {"stage_name": "sample.bam"},
        "bai": {"stage_name": "sample.bam.bai"},
    }


def test_worker_requires_locus_in_reference_and_bam_contig_sets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bam = tmp_path / "sample.bam"
    bai = tmp_path / "sample.bam.bai"
    fai = tmp_path / "genome.fa.fai"
    bam.write_bytes(b"bam")
    bai.write_bytes(b"bai")
    fai.write_text("chr1\t100\t0\t0\t0\n", encoding="utf-8")

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        output = "" if command[1] == "quickcheck" else "chr2\t100\t1\t0\n"
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr("ssqtl_igv.v3_worker.subprocess.run", fake_run)
    with pytest.raises(ValueError, match="absent from BAM"):
        _samtools_validate(
            [_track()],
            {"sample.bam": bam, "sample.bam.bai": bai},
            "samtools",
            locus={"contig": "chr1", "start": 1, "end": 10},
            fai=fai,
        )


def test_worker_rejects_reference_bam_length_drift_and_out_of_range_locus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bam = tmp_path / "sample.bam"
    bai = tmp_path / "sample.bam.bai"
    fai = tmp_path / "genome.fa.fai"
    bam.write_bytes(b"bam")
    bai.write_bytes(b"bai")
    fai.write_text("chr1\t100\t0\t0\t0\n", encoding="utf-8")

    def fake_run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        output = "" if command[1] == "quickcheck" else "chr1\t99\t1\t0\n"
        return subprocess.CompletedProcess(command, 0, output, "")

    monkeypatch.setattr("ssqtl_igv.v3_worker.subprocess.run", fake_run)
    with pytest.raises(ValueError, match="BAM/reference contig length differs"):
        _samtools_validate(
            [_track()],
            {"sample.bam": bam, "sample.bam.bai": bai},
            "samtools",
            locus={"contig": "chr1", "start": 1, "end": 10},
            fai=fai,
        )

    with pytest.raises(ValueError, match="exceeds reference"):
        _samtools_validate(
            [],
            {},
            "samtools",
            locus={"contig": "chr1", "start": 90, "end": 101},
            fai=fai,
        )
