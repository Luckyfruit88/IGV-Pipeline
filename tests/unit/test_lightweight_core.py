from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_normal_cli_import_does_not_load_research_layers():
    code = '''import sys
from ssqtl_igv import v3_cli
from ssqtl_igv import project_admission_v3
assert 'ssqtl_igv.campaign_v3' not in sys.modules
assert 'ssqtl_igv.review_server' not in sys.modules
assert 'ssqtl_igv.review_package_v3' not in sys.modules
print('PASS')
'''
    result = subprocess.run([sys.executable, "-c", code], env={**os.environ, "PYTHONPATH": str(ROOT / "src")}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"


def test_campaign_is_not_a_product_command():
    from ssqtl_igv import v3_cli
    parser = v3_cli._parser()
    choices = next(a.choices for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    assert "campaign" not in choices
    assert {"run", "rerun-failed", "reconcile", "smoke-test"} <= set(choices)
    with pytest.raises(SystemExit):
        parser.parse_args(["campaign", "prepare"])
    # Keep compatibility without advertising an internal grouping parameter.
    assert "--max-cases-per-shard" not in choices["run"].format_help()
    assert parser.parse_args(["run", "--max-cases-per-shard", "32"]).max_cases_per_shard == 32


def test_archived_campaign_is_still_explicitly_available():
    from ssqtl_igv import benchmark_cli
    args = benchmark_cli._parser().parse_args(["campaign", "status", "--campaign-dir", "/unused"])
    assert args.campaign_command == "status"
    assert not (ROOT / ".github/workflows/pilot-candidate.yml").exists()
    assert (ROOT / "benchmarks/legacy/pilot-candidate.yml").is_file()


def _release_module():
    pytest.importorskip("tomllib", reason="release metadata tooling uses Python 3.11+")
    spec = importlib.util.spec_from_file_location("release_version", ROOT / "scripts/release-version.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("version", ["3.0.0", "3.0.1", "4.12.9"])
def test_release_version_is_not_hardcoded(tmp_path, version):
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nversion = "' + version + '"\n')
    assert _release_module().release_metadata("v" + version, project)["version"] == version


@pytest.mark.parametrize("tag", ["v3.0.0-rc1", "3.0.0", "v03.0.0", "v3.0", "v3.0.1", "v3.0.0\n"])
def test_release_rejects_invalid_or_mismatched_tags(tmp_path, tag):
    project = tmp_path / "pyproject.toml"
    project.write_text('[project]\nversion = "3.0.0"\n')
    with pytest.raises(ValueError):
        _release_module().release_metadata(tag, project)


def test_smoke_fixture_uses_real_samtools_commands(tmp_path, monkeypatch):
    from ssqtl_igv import smoke_test
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        if command[1] == "faidx":
            Path(command[2] + ".fai").write_text("chr1\t4000\t6\t80\t81\n")
        elif command[1] == "view":
            Path(command[command.index("-o") + 1]).write_bytes(b"test-only mock")
        elif command[1] == "index":
            Path(command[2] + ".bai").write_bytes(b"test-only mock")
    monkeypatch.setattr(smoke_test.subprocess, "run", run)
    project = smoke_test.create_smoke_project(tmp_path / "project")
    assert project.is_file()
    assert [c[1] for c in calls] == ["faidx", "view", "index"]
    assert len((project.parent / "reads.sam").read_text().splitlines()) == 82
    with pytest.raises(FileExistsError):
        smoke_test.create_smoke_project(project.parent)


def test_smoke_refuses_existing_output(tmp_path):
    from ssqtl_igv.smoke_test import run_smoke_test
    (tmp_path / "keep.txt").write_text("preserve")
    with pytest.raises(FileExistsError):
        run_smoke_test(tmp_path)
    assert (tmp_path / "keep.txt").read_text() == "preserve"


def test_smoke_propagates_failure_without_passing(tmp_path, monkeypatch):
    from ssqtl_igv import smoke_test, project_launcher
    monkeypatch.setattr(smoke_test, "create_smoke_project", lambda path: path / "project.yaml")
    observed = {}
    def run(**kwargs):
        observed.update(kwargs)
        return {"status": "CASE_FAILURES"}, 2
    monkeypatch.setattr(project_launcher, "run_project_workflow", run)
    result = smoke_test.run_smoke_test(tmp_path / "smoke")
    assert result["exit_code"] == 2
    assert result["status"] == "SMOKE_TEST_FAILED"
    assert observed["batch_request"] is None
    assert observed["resume"] is False
    assert observed["max_parallel"] == 1
    assert not observed.get("fake_runtime")
