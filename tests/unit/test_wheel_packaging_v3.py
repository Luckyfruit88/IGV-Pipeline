from __future__ import annotations

import glob
import sys
import tomllib
from pathlib import Path

from ssqtl_igv import orchestrator_v3, runtime_identity


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_DATA_ROOT = Path("share/igv-snapshot-workflow/pipeline")


def _declared_pipeline_files() -> set[Path]:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    declarations = project["tool"]["setuptools"]["data-files"]
    installed: set[Path] = set()
    for destination, patterns in declarations.items():
        destination_path = Path(destination)
        if destination_path != PIPELINE_DATA_ROOT and PIPELINE_DATA_ROOT not in destination_path.parents:
            continue
        for pattern in patterns:
            matches = [Path(path) for path in glob.glob(str(PROJECT_ROOT / pattern))]
            assert matches, f"wheel data-file pattern matches nothing: {pattern}"
            for source in matches:
                assert source.is_file(), source
                installed.add(destination_path.relative_to(PIPELINE_DATA_ROOT) / source.name)
    return installed


def _required_pipeline_files() -> set[Path]:
    required = {
        Path("LICENSE"),
        Path("MANIFEST.in"),
        Path("README.md"),
        Path("README.zh-CN.md"),
        Path("main.nf"),
        Path("nextflow.config"),
        Path("nextflow_schema.json"),
        Path("pyproject.toml"),
        Path("requirements-build.lock"),
        Path("requirements.lock"),
        Path("uv.lock"),
    }
    selections = {
        "bin": {".py", ".R"},
        "conf": {".config"},
        "config": {".yaml", ".json"},
        "containers": {".def", ".dockerignore", ".json", ".lock", ".sh", ".Dockerfile"},
        "containers/bin": {""},
        "docs": {".md"},
        "docs/architecture": {".md"},
        "docs/contracts": {".md"},
        "docs/runtime": {".md"},
        "modules/local": {".nf"},
        "schema": {".json"},
        "scripts": {".py", ".sh"},
        "src/ssqtl_igv": {".py"},
        "src/ssqtl_igv/resources": {".py", ".R", ".yaml"},
        "workflows": {".nf"},
    }
    for relative_directory, suffixes in selections.items():
        directory = PROJECT_ROOT / relative_directory
        for source in directory.iterdir():
            if source.is_file() and source.suffix in suffixes:
                required.add(Path(relative_directory) / source.name)
    return required


def test_active_dockerignore_is_not_required_as_wheel_data() -> None:
    project = tomllib.loads((PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pipeline_files = project["tool"]["setuptools"]["data-files"][str(PIPELINE_DATA_ROOT)]

    # Docker uses this file to filter the build context and does not make it
    # available to COPY. Requiring it as wheel data makes the production image
    # unbuildable even though a complete source checkout can build the wheel.
    assert ".dockerignore" not in pipeline_files


def test_wheel_declares_complete_v3_pipeline_and_runtime_contract_tree() -> None:
    declared = _declared_pipeline_files()
    required = _required_pipeline_files()
    assert required <= declared, sorted(str(path) for path in required - declared)


def test_installed_prefix_is_a_project_root_for_both_runtime_consumers(
    tmp_path: Path, monkeypatch,
) -> None:
    prefix = tmp_path / "isolated-prefix"
    pipeline = prefix / PIPELINE_DATA_ROOT
    (pipeline / "containers").mkdir(parents=True)
    (pipeline / "src/ssqtl_igv/resources").mkdir(parents=True)
    (pipeline / "main.nf").write_text("nextflow.enable.dsl = 2\n", encoding="utf-8")
    (pipeline / "nextflow.config").write_text("nextflow.enable.dsl = 2\n", encoding="utf-8")
    for name in (
        "runtime-manifest.schema.json",
        "runtime-materials.lock.json",
    ):
        (pipeline / "containers" / name).write_text("{}\n", encoding="utf-8")
    (pipeline / "src/ssqtl_igv/resources/v3-runtime.yaml").write_text(
        'schema_version: "3.0"\n', encoding="utf-8"
    )
    outside = tmp_path / "outside-source-tree"
    outside.mkdir()
    fake_package = tmp_path / "site-packages/ssqtl_igv"
    fake_package.mkdir(parents=True)

    monkeypatch.delenv("IGV_SNAPSHOT_PIPELINE_DIR", raising=False)
    monkeypatch.chdir(outside)
    monkeypatch.setattr(sys, "prefix", str(prefix))
    monkeypatch.setattr(orchestrator_v3, "__file__", str(fake_package / "orchestrator_v3.py"))
    monkeypatch.setattr(runtime_identity, "__file__", str(fake_package / "runtime_identity.py"))

    assert orchestrator_v3._project_root() == pipeline
    assert runtime_identity._project_root() == pipeline
