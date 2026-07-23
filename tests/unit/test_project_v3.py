from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from ssqtl_igv.project_v3 import build_project_source_binding, load_project_config
from ssqtl_igv.utils import sha256_file


def _write_reference(path: Path) -> None:
    path.write_text('schema_version: "3.0"\n', encoding="utf-8")


def test_load_generic_project_binds_relative_inputs(tmp_path: Path) -> None:
    (tmp_path / "cases.tsv").write_text("header\n", encoding="utf-8")
    _write_reference(tmp_path / "reference.yaml")
    project = tmp_path / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: generic\n"
        "inputs:\n"
        "  cases: cases.tsv\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )

    loaded = load_project_config(project)

    assert loaded["adapter"] == "generic"
    assert loaded["project_root"] == str(tmp_path.resolve())
    assert loaded["inputs"]["cases"] == {
        "declared_path": "cases.tsv",
        "source_path": str((tmp_path / "cases.tsv").resolve()),
    }
    assert len(loaded["project_sha256"]) == 64


def test_load_ssqtl_project_accepts_optional_config_omission(tmp_path: Path) -> None:
    for name in ("associations.csv", "bam_lookup.csv", "reference.yaml"):
        (tmp_path / name).write_text("fixture\n", encoding="utf-8")
    (tmp_path / "rds").mkdir()
    (tmp_path / "violin").mkdir()
    project = tmp_path / "project.yaml"
    project.write_text(
        yaml.safe_dump(
            {
                "schema_version": "3.0",
                "adapter": "ssqtl",
                "inputs": {
                    "associations": "associations.csv",
                    "rds_dir": "rds",
                    "bam_lookup": "bam_lookup.csv",
                    "violin_dir": "violin",
                },
                "reference": "reference.yaml",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    loaded = load_project_config(project)

    assert loaded["adapter"] == "ssqtl"
    assert set(loaded["inputs"]) == {
        "associations",
        "rds_dir",
        "bam_lookup",
        "violin_dir",
    }
    assert Path(loaded["inputs"]["rds_dir"]["source_path"]).is_dir()


@pytest.mark.parametrize(
    "value",
    ["/tmp/cases.tsv", "../cases.tsv", "https://example.test/cases.tsv", "*.tsv", "dir\\cases.tsv"],
)
def test_project_rejects_unsafe_input_paths(tmp_path: Path, value: str) -> None:
    (tmp_path / "cases.tsv").write_text("header\n", encoding="utf-8")
    _write_reference(tmp_path / "reference.yaml")
    project = tmp_path / "project.yaml"
    project.write_text(
        yaml.safe_dump(
            {
                "schema_version": "3.0",
                "adapter": "generic",
                "inputs": {"cases": value},
                "reference": "reference.yaml",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="path|URI|relative|glob"):
        load_project_config(project)


def test_project_rejects_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path / "outside.tsv"
    outside.write_text("header\n", encoding="utf-8")
    project_root = tmp_path / "project"
    project_root.mkdir()
    (project_root / "cases.tsv").symlink_to(outside)
    _write_reference(project_root / "reference.yaml")
    project = project_root / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: generic\n"
        "inputs: {cases: cases.tsv}\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="escapes"):
        load_project_config(project)


def test_project_rejects_mixed_adapter_fields(tmp_path: Path) -> None:
    (tmp_path / "cases.tsv").write_text("header\n", encoding="utf-8")
    (tmp_path / "associations.csv").write_text("header\n", encoding="utf-8")
    _write_reference(tmp_path / "reference.yaml")
    project = tmp_path / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: generic\n"
        "inputs:\n"
        "  cases: cases.tsv\n"
        "  associations: associations.csv\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unexpected=associations"):
        load_project_config(project)


def test_project_schema_version_must_be_quoted_string(tmp_path: Path) -> None:
    (tmp_path / "cases.tsv").write_text("header\n", encoding="utf-8")
    _write_reference(tmp_path / "reference.yaml")
    project = tmp_path / "project.yaml"
    project.write_text(
        "schema_version: 3.0\n"
        "adapter: generic\n"
        "inputs: {cases: cases.tsv}\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="string"):
        load_project_config(project)


def test_generic_source_binding_changes_with_manifest_metadata(tmp_path: Path) -> None:
    cases = tmp_path / "cases.tsv"
    cases.write_text("header\n", encoding="utf-8")
    _write_reference(tmp_path / "reference.yaml")
    project = tmp_path / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: generic\n"
        "inputs: {cases: cases.tsv}\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )
    first = build_project_source_binding(load_project_config(project))

    cases.write_text("header\nchanged\n", encoding="utf-8")
    second = build_project_source_binding(load_project_config(project))

    assert first["binding_sha256"] != second["binding_sha256"]
    assert first["inputs"]["cases"]["sha256"] != second["inputs"]["cases"]["sha256"]


def test_ssqtl_source_binding_tracks_directory_file_metadata(tmp_path: Path) -> None:
    for name in ("associations.csv", "reference.yaml"):
        (tmp_path / name).write_text("fixture\n", encoding="utf-8")
    (tmp_path / "rds").mkdir()
    (tmp_path / "violin").mkdir()
    (tmp_path / "tracks").mkdir()
    (tmp_path / "tracks" / "sample-1.bam").write_bytes(b"bam")
    (tmp_path / "tracks" / "sample-1.bam.bai").write_bytes(b"bai")
    (tmp_path / "bam_lookup.csv").write_text(
        "sample_id,directory\nsample-1,tracks\n", encoding="utf-8"
    )
    rds = tmp_path / "rds" / "chr1.rds"
    rds.write_bytes(b"first")
    (tmp_path / "violin" / "case.pdf").write_bytes(b"pdf")
    project = tmp_path / "project.yaml"
    project.write_text(
        yaml.safe_dump(
            {
                "schema_version": "3.0",
                "adapter": "ssqtl",
                "inputs": {
                    "associations": "associations.csv",
                    "rds_dir": "rds",
                    "bam_lookup": "bam_lookup.csv",
                    "violin_dir": "violin",
                },
                "reference": "reference.yaml",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    first = build_project_source_binding(load_project_config(project))

    rds.write_bytes(b"second")
    second = build_project_source_binding(load_project_config(project))

    assert first["binding_sha256"] != second["binding_sha256"]
    assert first["inputs"]["rds_dir"]["inventory_sha256"] != second["inputs"][
        "rds_dir"
    ]["inventory_sha256"]

    (tmp_path / "tracks" / "unrelated.bam").write_bytes(b"unrelated")
    unrelated = build_project_source_binding(load_project_config(project))
    assert second["inputs"]["bam_lookup"]["eligible_resources"] == unrelated[
        "inputs"
    ]["bam_lookup"]["eligible_resources"]

    (tmp_path / "tracks" / "sample-1.bam.bai").write_bytes(b"changed-bai")
    index_changed = build_project_source_binding(load_project_config(project))
    assert unrelated["inputs"]["bam_lookup"]["eligible_resources"][
        "inventory_sha256"
    ] != index_changed["inputs"]["bam_lookup"]["eligible_resources"][
        "inventory_sha256"
    ]

    (tmp_path / "tracks" / "sample-1.secondary.bam").write_bytes(b"eligible")
    direct_still_selected = build_project_source_binding(load_project_config(project))
    assert index_changed["inputs"]["bam_lookup"]["eligible_resources"] == (
        direct_still_selected["inputs"]["bam_lookup"]["eligible_resources"]
    )

    # R list.files() excludes dotfiles unless all.files=TRUE.  The Python
    # source binding must freeze the same visible fallback that R selects.
    (tmp_path / "tracks" / ".sample-1.hidden.bam").write_bytes(b"hidden")
    (tmp_path / "tracks" / "sample-1.bam").unlink()
    fallback_selected = build_project_source_binding(load_project_config(project))
    assert direct_still_selected["inputs"]["bam_lookup"]["eligible_resources"][
        "inventory_sha256"
    ] != fallback_selected["inputs"]["bam_lookup"]["eligible_resources"][
        "inventory_sha256"
    ]
    assert [
        item["path"]
        for item in fallback_selected["inputs"]["bam_lookup"]["eligible_resources"][
            "files"
        ]
    ] == ["tracks/sample-1.secondary.bam"]


def test_ssqtl_source_binding_does_not_hash_large_scientific_inputs(
    tmp_path: Path,
) -> None:
    for name in ("associations.csv", "reference.yaml"):
        (tmp_path / name).write_text("fixture\n", encoding="utf-8")
    (tmp_path / "rds").mkdir()
    (tmp_path / "violin").mkdir()
    (tmp_path / "tracks").mkdir()
    (tmp_path / "rds" / "chr1.rds").write_bytes(b"rds")
    (tmp_path / "violin" / "case.pdf").write_bytes(b"pdf")
    (tmp_path / "tracks" / "sample-1.bam").write_bytes(b"bam")
    (tmp_path / "tracks" / "sample-1.bam.bai").write_bytes(b"bai")
    (tmp_path / "bam_lookup.csv").write_text(
        "sample_id,directory\nsample-1,tracks\n", encoding="utf-8"
    )
    project = tmp_path / "project.yaml"
    project.write_text(
        yaml.safe_dump(
            {
                "schema_version": "3.0",
                "adapter": "ssqtl",
                "inputs": {
                    "associations": "associations.csv",
                    "rds_dir": "rds",
                    "bam_lookup": "bam_lookup.csv",
                    "violin_dir": "violin",
                },
                "reference": "reference.yaml",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with patch("ssqtl_igv.project_v3.sha256_file", wraps=sha256_file) as hash_file:
        binding = build_project_source_binding(load_project_config(project))

    assert binding["inputs"]["bam_lookup"]["eligible_resources"]["file_count"] == 2
    hashed = {Path(call.args[0]).name for call in hash_file.call_args_list}
    assert hashed == {
        "associations.csv",
        "bam_lookup.csv",
        "project.yaml",
        "reference.yaml",
    }


def test_ssqtl_source_binding_rejects_nested_symlink_escape(tmp_path: Path) -> None:
    outside = tmp_path / "outside.rds"
    outside.write_bytes(b"outside")
    project_root = tmp_path / "project"
    project_root.mkdir()
    for name in ("associations.csv", "reference.yaml"):
        (project_root / name).write_text("fixture\n", encoding="utf-8")
    (project_root / "rds").mkdir()
    (project_root / "violin").mkdir()
    (project_root / "tracks").mkdir()
    (project_root / "tracks" / "sample-1.bam").write_bytes(b"bam")
    (project_root / "bam_lookup.csv").write_text(
        "sample_id,bam\nsample-1,tracks/sample-1.bam\n", encoding="utf-8"
    )
    (project_root / "rds" / "escape.rds").symlink_to(outside)
    project = project_root / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: ssqtl\n"
        "inputs:\n"
        "  associations: associations.csv\n"
        "  rds_dir: rds\n"
        "  bam_lookup: bam_lookup.csv\n"
        "  violin_dir: violin\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="symlink escape"):
        build_project_source_binding(load_project_config(project))


def test_ssqtl_bam_lookup_binding_rejects_matching_symlink_escape(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside.bam"
    outside.write_bytes(b"outside")
    project_root = tmp_path / "project"
    project_root.mkdir()
    for name in ("associations.csv", "reference.yaml"):
        (project_root / name).write_text("fixture\n", encoding="utf-8")
    (project_root / "rds").mkdir()
    (project_root / "violin").mkdir()
    (project_root / "tracks").mkdir()
    (project_root / "tracks" / "sample-1.bam").symlink_to(outside)
    (project_root / "bam_lookup.csv").write_text(
        "sample_id,directory\nsample-1,tracks\n", encoding="utf-8"
    )
    project = project_root / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: ssqtl\n"
        "inputs:\n"
        "  associations: associations.csv\n"
        "  rds_dir: rds\n"
        "  bam_lookup: bam_lookup.csv\n"
        "  violin_dir: violin\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="BAM symlink escapes"):
        build_project_source_binding(load_project_config(project))
