from __future__ import annotations

import shutil
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from ssqtl_igv.contracts import ContractValidationError, validate_v3_task_document
from ssqtl_igv.utils import read_jsonl, sha256_file
from ssqtl_igv.v3_manifest import (
    GENERIC_MANIFEST_FIELDS,
    init_templates,
    normalize_generic_manifest,
)


def _write_reference(root: Path, *, remote_definition: bool = False) -> Path:
    root.mkdir()
    resources = {
        "definition": root / "genome.json",
        "fasta": root / "genome.fa",
        "fai": root / "genome.fa.fai",
        "cytoband": root / "cytoband.txt.gz",
        "annotation": root / "annotation.gff.gz",
    }
    resources["definition"].write_text(
        '{"fastaURL":"https://example.invalid/genome.fa"}\n'
        if remote_definition
        else "{}\n",
        encoding="utf-8",
    )
    resources["fasta"].write_text(">chr1\nACGT\n", encoding="utf-8")
    resources["fai"].write_text("chr1\t4\t6\t4\t5\n", encoding="utf-8")
    resources["cytoband"].write_bytes(b"cytoband")
    resources["annotation"].write_bytes(b"annotation")
    reference = {
        "schema_version": "3.0",
        "id": "fixture-hg38",
        "display_name": "Fixture GRCh38",
        "version": "GRCh38-test",
        "resources": {
            role: {
                "path": path.name,
                "sha256": sha256_file(path) if role != "fasta" else None,
            }
            for role, path in resources.items()
        },
    }
    path = root / "reference.yaml"
    path.write_text(yaml.safe_dump(reference, sort_keys=False), encoding="utf-8")
    return path


def _write_manifest(path: Path, rows: list[list[str]]) -> Path:
    text = "\t".join(GENERIC_MANIFEST_FIELDS) + "\n"
    text += "".join("\t".join(row) + "\n" for row in rows)
    path.write_text(text, encoding="utf-8")
    return path


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    input_root = tmp_path / "input"
    input_root.mkdir()
    (input_root / "a.bam").write_bytes(b"bam-a")
    (input_root / "a.bam.bai").write_bytes(b"bai-a")
    (input_root / "b.bam").write_bytes(b"bam-b")
    (input_root / "b.bam.bai").write_bytes(b"bai-b")
    (input_root / "panel.png").write_bytes(b"png fixture")
    reference = _write_reference(tmp_path / "reference")
    manifest = _write_manifest(
        tmp_path / "cases.tsv",
        [
            ["3.0", "case_b", "chr2:20-30", "-", "a.bam", "", "A", "tumor", "", ""],
            ["3.0", "case_a", "chr1:10-12", "+", "b.bam", "", "B", "", "panel.png", ""],
            ["3.0", "case_b", "chr2:20-30", "-", "b.bam", "b.bam.bai", "C", "normal", "", ""],
        ],
    )
    return manifest, input_root, reference


def test_init_templates_is_atomic_and_declares_v3_headers(tmp_path: Path) -> None:
    output = tmp_path / "starter"
    result = init_templates(output)
    assert result["status"] == "INITIALIZED"
    assert result["adapter"] == "generic"
    assert yaml.safe_load((output / "project.yaml").read_text()) == {
        "schema_version": "3.0",
        "adapter": "generic",
        "inputs": {"cases": "cases.tsv"},
        "reference": "reference.yaml",
    }
    assert (output / "cases.tsv").read_text().strip().split("\t") == list(
        GENERIC_MANIFEST_FIELDS
    )
    assert yaml.safe_load((output / "reference.yaml").read_text())["schema_version"] == "3.0"
    with pytest.raises(FileExistsError):
        init_templates(output)


def test_init_ssqtl_templates_declares_complete_project_layout(tmp_path: Path) -> None:
    output = tmp_path / "starter-ssqtl"
    result = init_templates(output, adapter="ssqtl")
    assert result["adapter"] == "ssqtl"
    assert result["manifest"] is None
    project = yaml.safe_load((output / "project.yaml").read_text())
    assert project["adapter"] == "ssqtl"
    assert project["inputs"] == {
        "associations": "associations.csv",
        "rds_dir": "rds",
        "bam_lookup": "bam_lookup.csv",
        "violin_dir": "violin",
        "config": "ssqtl.yaml",
    }
    assert (output / "rds").is_dir()
    assert (output / "violin").is_dir()
    assert (output / "ssqtl.yaml").is_file()



def test_generic_normalization_preserves_case_and_track_order(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    output = tmp_path / "normalized"
    result = normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        output,
        "run_001",
        "gen_001",
    )
    assert result["status"] == "PASS"
    assert result["task_count"] == 2
    assert sorted(path.name for path in output.iterdir()) == [
        "normalized_manifest.tsv",
        "parameters.json",
        "reference.json",
        "tasks.jsonl",
        "validation.json",
    ]
    tasks = list(read_jsonl(result["tasks"]))
    assert [task["task_id"] for task in tasks] == ["case_b", "case_a"]
    assert [track["track_label"] for track in tasks[0]["core"]["tracks"]] == ["A", "C"]
    assert tasks[0]["core"]["tracks"][0]["bai"]["declared_path"] == "a.bam.bai"
    assert tasks[1]["core"]["auxiliary"]["kind"] == "PNG"
    assert tasks[1]["adapter_data"]["scientific_interpretation"] == "NOT_APPLICABLE"
    for task in tasks:
        validate_v3_task_document(task)
        assert all(
            Path(track["bam"]["source_path"]).is_absolute()
            for track in task["core"]["tracks"]
        )


def test_generic_fingerprint_does_not_depend_on_input_mount_location(tmp_path: Path) -> None:
    manifest, first_root, reference = _fixture(tmp_path)
    second_root = tmp_path / "input-copy"
    shutil.copytree(first_root, second_root, copy_function=shutil.copy2)
    first = normalize_generic_manifest(
        manifest,
        first_root,
        reference,
        tmp_path / "first",
        "run_001",
        "gen_001",
    )
    second = normalize_generic_manifest(
        manifest,
        second_root,
        reference,
        tmp_path / "second",
        "run_001",
        "gen_001",
    )
    first_tasks = list(read_jsonl(first["tasks"]))
    second_tasks = list(read_jsonl(second["tasks"]))
    assert [task["input_fingerprint"] for task in first_tasks] == [
        task["input_fingerprint"] for task in second_tasks
    ]
    assert first_tasks[0]["core"]["tracks"][0]["bam"]["source_path"] != second_tasks[0][
        "core"
    ]["tracks"][0]["bam"]["source_path"]


@pytest.mark.parametrize(
    "unsafe_path",
    ["/absolute/a.bam", "../outside.bam", "https://example.test/a.bam", "*.bam", "dir\\a.bam"],
)
def test_generic_manifest_rejects_nonportable_paths(
    tmp_path: Path, unsafe_path: str
) -> None:
    _manifest, input_root, reference = _fixture(tmp_path)
    manifest = _write_manifest(
        tmp_path / "unsafe.tsv",
        [["3.0", "case_1", "chr1:1-2", "+", unsafe_path, "a.bam.bai", "A", "", "", ""]],
    )
    with pytest.raises((ValueError, FileNotFoundError), match="path|BAM"):
        normalize_generic_manifest(
            manifest, input_root, reference, tmp_path / "out", "run_001", "gen_001"
        )
    assert not (tmp_path / "out").exists()


def test_generic_manifest_rejects_symlink_escape(tmp_path: Path) -> None:
    _manifest, input_root, reference = _fixture(tmp_path)
    outside = tmp_path / "outside.bam"
    outside.write_bytes(b"outside")
    (input_root / "escape.bam").symlink_to(outside)
    manifest = _write_manifest(
        tmp_path / "escape.tsv",
        [["3.0", "case_1", "chr1:1-2", "+", "escape.bam", "a.bam.bai", "A", "", "", ""]],
    )
    with pytest.raises(ValueError, match="escapes"):
        normalize_generic_manifest(
            manifest, input_root, reference, tmp_path / "out", "run_001", "gen_001"
        )


def test_bai_inference_fails_when_both_conventions_exist(tmp_path: Path) -> None:
    _manifest, input_root, reference = _fixture(tmp_path)
    (input_root / "a.bai").write_bytes(b"second index")
    manifest = _write_manifest(
        tmp_path / "ambiguous.tsv",
        [["3.0", "case_1", "chr1:1-2", "+", "a.bam", "", "A", "", "", ""]],
    )
    with pytest.raises(ValueError, match="infer BAI unambiguously"):
        normalize_generic_manifest(
            manifest, input_root, reference, tmp_path / "out", "run_001", "gen_001"
        )


def test_multi_page_pdf_requires_explicit_page(tmp_path: Path) -> None:
    _manifest, input_root, reference = _fixture(tmp_path)
    (input_root / "panel.pdf").write_bytes(b"%PDF fixture")
    without_page = _write_manifest(
        tmp_path / "pdf.tsv",
        [["3.0", "case_1", "chr1:1-2", "+", "a.bam", "", "A", "", "panel.pdf", ""]],
    )
    with patch("ssqtl_igv.v3_manifest._pdf_page_count", return_value=2):
        with pytest.raises(ValueError, match="multi-page PDF requires aux_page"):
            normalize_generic_manifest(
                without_page,
                input_root,
                reference,
                tmp_path / "out-fail",
                "run_001",
                "gen_001",
            )

    with_page = _write_manifest(
        tmp_path / "pdf-page.tsv",
        [["3.0", "case_1", "chr1:1-2", "+", "a.bam", "", "A", "", "panel.pdf", "2"]],
    )
    with patch("ssqtl_igv.v3_manifest._pdf_page_count", return_value=2):
        result = normalize_generic_manifest(
            with_page,
            input_root,
            reference,
            tmp_path / "out-pass",
            "run_001",
            "gen_001",
        )
    task = next(read_jsonl(result["tasks"]))
    assert task["core"]["auxiliary"]["page"] == 2


def test_reference_bundle_rejects_remote_genome_definition(tmp_path: Path) -> None:
    manifest, input_root, _reference = _fixture(tmp_path)
    remote = _write_reference(tmp_path / "remote-reference", remote_definition=True)
    with pytest.raises(ValueError, match="remote URL"):
        normalize_generic_manifest(
            manifest, input_root, remote, tmp_path / "out", "run_001", "gen_001"
        )


def test_reference_bundle_verifies_configured_checksum(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    document = yaml.safe_load(reference.read_text())
    document["resources"]["annotation"]["sha256"] = "f" * 64
    reference.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        normalize_generic_manifest(
            manifest, input_root, reference, tmp_path / "out", "run_001", "gen_001"
        )


def test_v3_validator_detects_fingerprint_tampering(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    result = normalize_generic_manifest(
        manifest, input_root, reference, tmp_path / "out", "run_001", "gen_001"
    )
    task = next(read_jsonl(result["tasks"]))
    task["core"]["tracks"][0]["track_label"] = "tampered"
    with pytest.raises(ContractValidationError, match="input_fingerprint"):
        validate_v3_task_document(task)
