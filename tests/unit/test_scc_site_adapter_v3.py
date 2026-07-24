from __future__ import annotations

import json
import subprocess
from pathlib import Path

import jsonschema
import pytest

from ssqtl_igv import orchestrator_v3
from ssqtl_igv.orchestrator_v3 import (
    _freeze_scc_site_adapter,
    _run_nextflow_shard,
    _validate_scc_site_options,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ("project", "qname"),
    [
        (None, None),
        ("", None),
        (" fixture-project", None),
        ("fixture-project ", None),
        ("fixture-project;id", None),
        ("mtdna/alcohol", None),
        ("fixture-project", "batch queue"),
        ("fixture-project", "batch;id"),
        ("fixture-project", "batch@node*"),
    ],
)
def test_scc_site_tokens_reject_missing_ambiguous_or_shell_active_values(
    project: str | None, qname: str | None
) -> None:
    with pytest.raises(ValueError, match="SCC (project|qname)"):
        _validate_scc_site_options(project, qname)


def test_scc_site_tokens_preserve_exact_safe_values() -> None:
    assert _validate_scc_site_options("fixture-project", None) == (
        "fixture-project",
        None,
    )
    assert _validate_scc_site_options("project_01", "fixture.q") == (
        "project_01",
        "fixture.q",
    )


def test_scc_site_adapter_is_immutable_across_resume_attempts(tmp_path: Path) -> None:
    run = tmp_path / "run"
    (run / "contract").mkdir(parents=True)
    expected = _freeze_scc_site_adapter(
        run, project="fixture-project", qname="fixture.q"
    )
    assert expected["cluster_options"] == ["-P", "fixture-project", "-q", "fixture.q"]
    assert expected["scheduling_role"] == "LEGACY_SGE_SHARD_LIMIT"
    assert expected["portable_render_max_forks"] == 8
    assert expected["max_cases_per_shard_limit"] == 256
    assert _freeze_scc_site_adapter(
        run, project="fixture-project", qname="fixture.q"
    ) == expected

    with pytest.raises(ValueError, match="immutable run contract"):
        _freeze_scc_site_adapter(run, project="different-project", qname="fixture.q")
    with pytest.raises(ValueError, match="immutable run contract"):
        _freeze_scc_site_adapter(run, project="fixture-project", qname="other.q")


def test_scc_nextflow_command_freezes_exact_project_and_qname(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[list[str]] = []

    def completed(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.append(command)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(orchestrator_v3.subprocess, "run", completed)
    run = tmp_path / "run"
    run.mkdir()
    result = _run_nextflow_shard(
        shard={"shard_id": "shard-0001", "path": str(tmp_path / "tasks.jsonl")},
        run_dir=run,
        profile="scc",
        runtime_identity=tmp_path / "runtime-manifest.json",
        runtime_identity_value={
            "runtime_manifest_sha256": "a" * 64,
            "runtime_fingerprint_sha256": "b" * 64,
        },
        nextflow="nextflow",
        work_dir=tmp_path / "work",
        resume=False,
        fake_runtime=False,
        runtime_image=None,
        runtime_sif=str(tmp_path / "runtime.sif"),
        runtime_sif_sha256="c" * 64,
        run_id="run-001",
        generation_id="generation-001",
        scc_project="fixture-project",
        scc_qname="fixture.q",
    )

    assert observed == [result["command"]]
    command = result["command"]
    project_index = command.index("--scc_project")
    qname_index = command.index("--scc_qname")
    assert command[project_index + 1] == "fixture-project"
    assert command[qname_index + 1] == "fixture.q"
    assert "-P" not in command and "-q" not in command


def test_standalone_nextflow_command_carries_manifest_fingerprint_and_parallelism(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        orchestrator_v3.subprocess,
        "run",
        lambda command, **_kwargs: subprocess.CompletedProcess(command, 0),
    )
    run = tmp_path / "run"
    run.mkdir()
    result = _run_nextflow_shard(
        shard={"shard_id": "shard-0001", "path": str(tmp_path / "tasks.jsonl")},
        run_dir=run,
        profile="standalone",
        runtime_identity=tmp_path / "runtime-manifest.json",
        runtime_identity_value={
            "runtime_manifest_sha256": "a" * 64,
            "runtime_fingerprint_sha256": "b" * 64,
        },
        nextflow="nextflow",
        work_dir=tmp_path / "work",
        resume=False,
        fake_runtime=False,
        runtime_image=None,
        runtime_sif=None,
        runtime_sif_sha256=None,
        run_id="run-001",
        generation_id="generation-001",
        max_parallel=4,
    )
    command = result["command"]
    assert command[command.index("--runtime_manifest_sha256") + 1] == "a" * 64
    assert command[command.index("--runtime_fingerprint_sha256") + 1] == "b" * 64
    assert command[command.index("--max_parallel") + 1] == "4"
    assert "--runtime_image" not in command


def test_scc_schema_and_config_share_the_strict_site_contract() -> None:
    schema = json.loads((PROJECT_ROOT / "nextflow_schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    validator = jsonschema.Draft202012Validator(schema)
    validator.validate(
        {
            "runtime_execution_mode": "scc",
            "scc_project": "fixture-project",
            "scc_qname": "fixture.q",
        }
    )
    with pytest.raises(jsonschema.ValidationError):
        validator.validate({"runtime_execution_mode": "scc"})
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(
            {"runtime_execution_mode": "scc", "scc_project": "fixture-project;id"}
        )

    base = (PROJECT_ROOT / "conf/base.config").read_text(encoding="utf-8")
    scc = (PROJECT_ROOT / "conf/scc.config").read_text(encoding="utf-8")
    sharding = (PROJECT_ROOT / "src/ssqtl_igv/sharding_v3.py").read_text(encoding="utf-8")
    orchestrator = (PROJECT_ROOT / "src/ssqtl_igv/orchestrator_v3.py").read_text(
        encoding="utf-8"
    )
    assert "scc_project = null" in base
    assert "scc_qname = null" in base
    assert "/restricted/" not in scc
    assert "clusterOptions = '-P" not in scc
    for site_path in (
        "software_root",
        "container_root",
        "work_root",
        "session_root",
        "results_parent",
    ):
        assert f"{site_path} = null" in scc
    assert "withLabel: portable_runtime" in scc
    assert "params.scc_project" in scc and "params.scc_qname" in scc
    assert 'qname ? "-P ${project} -q ${qname}" : "-P ${project}"' in scc
    assert "maxForks = params.max_parallel.toString() == 'auto' ? 8" in scc
    assert '"scheduling_role": "LOGICAL_ONLY"' in sharding
    assert 'expected_project=site_adapter["project"]' in orchestrator
    assert 'expected_qname=site_adapter["qname"]' in orchestrator


def test_standalone_schema_and_execution_do_not_require_an_oci_reference() -> None:
    schema = json.loads((PROJECT_ROOT / "nextflow_schema.json").read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(schema)
    validator.validate({"runtime_execution_mode": "standalone"})

    orchestrator = (PROJECT_ROOT / "src/ssqtl_igv/orchestrator_v3.py").read_text(
        encoding="utf-8"
    )
    workflow = (PROJECT_ROOT / "workflows/portable_run.nf").read_text(encoding="utf-8")
    cli = (PROJECT_ROOT / "src/ssqtl_igv/v3_cli.py").read_text(encoding="utf-8")
    assert "runtime_image != reference" not in orchestrator
    assert "requires runtime_image" not in workflow
    assert "--runtime-image" not in cli
    assert "--runtime-identity" not in cli
