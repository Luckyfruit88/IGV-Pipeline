from __future__ import annotations

from pathlib import Path

import pytest

from ssqtl_igv.utils import reject_symlink_path_components, resource_contains_remote_url
from ssqtl_igv.v3_manifest import init_templates
from ssqtl_igv.probes_v3 import _writable_directory_probe
from ssqtl_igv.v3_cli import main as v3_main


def test_writable_v3_targets_reject_parent_symlink(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)

    with pytest.raises(ValueError, match="traverses a symlink"):
        init_templates(alias / "project")
    assert not (real / "project").exists()

    project = tmp_path / "project.yaml"
    cases = tmp_path / "cases.tsv"
    reference = tmp_path / "reference.yaml"
    cases.write_text("header\n", encoding="utf-8")
    reference.write_text("fixture\n", encoding="utf-8")
    project.write_text(
        'schema_version: "3.0"\nadapter: generic\n'
        "inputs: {cases: cases.tsv}\nreference: reference.yaml\n",
        encoding="utf-8",
    )
    assert v3_main(
        ["run", "--project", str(project), "--output", str(alias / "run")]
    ) == 1
    assert not (real / "run").exists()


def test_writable_v3_targets_reject_parent_traversal(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must not contain"):
        reject_symlink_path_components(
            tmp_path / "child" / ".." / "output", label="fixture output"
        )


def test_doctor_write_probe_uses_actual_directory_and_leaves_no_file(tmp_path: Path) -> None:
    result = _writable_directory_probe("run_directory_write", tmp_path)
    assert result["status"] == "PASS"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("uri", ["HTTPS://example.invalid/genome.fa", "ftp://host/a.fa"])
def test_local_genome_definition_rejects_case_insensitive_remote_schemes(
    tmp_path: Path, uri: str
) -> None:
    definition = tmp_path / "genome.json"
    definition.write_text('{"fastaURL": "' + uri + '"}\n', encoding="utf-8")
    assert resource_contains_remote_url(definition) is True
