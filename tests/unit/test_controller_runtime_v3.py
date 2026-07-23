from __future__ import annotations

from pathlib import Path
import json

import pytest

from ssqtl_igv.controller_runtime_v3 import (
    collect_controller_runtime_identity,
    freeze_controller_runtime_identity,
    validate_controller_source_identity,
)
import ssqtl_igv.controller_runtime_v3 as controller_runtime_v3
from ssqtl_igv.orchestrator_v3 import (
    _expected_ssqtl_normalization_trace_bindings,
    _identity_contract,
    _validate_normalization_controller_contract,
)
from ssqtl_igv.utils import sha256_file


def _executable(path: Path, output: str) -> Path:
    path.write_text(
        "#!/bin/sh\nprintf '%s\\n' '" + output.replace("'", "'\\''") + "'\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def test_controller_runtime_requires_exact_nextflow_and_java(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    java = _executable(tmp_path / "java", 'openjdk version "21.0.8"')
    nextflow = _executable(
        tmp_path / "nextflow",
        "Version: 25.04.7 build 6000\nRuntime: Groovy on OpenJDK VM 21.0.8",
    )
    monkeypatch.setenv("JAVA_CMD", str(java))

    identity = collect_controller_runtime_identity(nextflow)

    assert identity["schema_version"] == "3.0-controller-runtime"
    assert identity["nextflow"]["required_version"] == "25.04.7"
    assert identity["java"]["required_major"] == 21
    assert len(identity["identity_sha256"]) == 64


@pytest.mark.parametrize(
    ("java_version", "nextflow_version", "message"),
    [
        (
            'openjdk version "11.0.20"',
            "Version: 25.04.7\nRuntime: Groovy on OpenJDK VM 21.0.8",
            "Java must be major 21",
        ),
        (
            'openjdk version "21.0.8"',
            "Version: 25.04.6\nRuntime: Groovy on OpenJDK VM 21.0.8",
            "exactly 25.04.7",
        ),
        (
            'openjdk version "21.0.8"',
            "Version: 25.04.7\nRuntime: Groovy on OpenJDK VM 17.0.12",
            "did not report the admitted Java major 21",
        ),
    ],
)
def test_controller_runtime_rejects_version_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    java_version: str,
    nextflow_version: str,
    message: str,
) -> None:
    java = _executable(tmp_path / "java", java_version)
    nextflow = _executable(tmp_path / "nextflow", nextflow_version)
    monkeypatch.setenv("JAVA_CMD", str(java))

    with pytest.raises(ValueError, match=message):
        collect_controller_runtime_identity(nextflow)


def test_controller_runtime_contract_rejects_binary_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = tmp_path / "run"
    (run / "contract").mkdir(parents=True)
    java = _executable(tmp_path / "java", 'openjdk version "21.0.8"')
    nextflow = _executable(
        tmp_path / "nextflow",
        "Version: 25.04.7\nRuntime: Groovy on OpenJDK VM 21.0.8",
    )
    monkeypatch.setenv("JAVA_CMD", str(java))
    frozen = freeze_controller_runtime_identity(run, nextflow)
    assert frozen["java"]["selector"] == "JAVA_CMD"

    nextflow.write_text(
        "#!/bin/sh\nprintf '%s\\n' 'Version: 25.04.7 build changed' "
        "'Runtime: Groovy on OpenJDK VM 21.0.8'\n",
        encoding="utf-8",
    )
    nextflow.chmod(0o755)
    with pytest.raises(ValueError, match="differs from the immutable run"):
        freeze_controller_runtime_identity(run, nextflow)


def test_controller_runtime_preserves_no_whitespace_java_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = _executable(tmp_path / "java target", 'openjdk version "21.0.8"')
    selector = tmp_path / "java-link"
    selector.symlink_to(target)
    nextflow = _executable(
        tmp_path / "nextflow",
        "Version: 25.04.7\nRuntime: Groovy on OpenJDK VM 21.0.8",
    )
    monkeypatch.setenv("JAVA_CMD", str(selector))

    identity = collect_controller_runtime_identity(nextflow)

    assert identity["java"]["executable"] == str(selector)
    assert identity["java"]["resolved_executable"] == str(target)


def test_controller_runtime_rejects_whitespace_java_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    java = _executable(tmp_path / "java selector", 'openjdk version "21.0.8"')
    nextflow = _executable(
        tmp_path / "nextflow",
        "Version: 25.04.7\nRuntime: Groovy on OpenJDK VM 21.0.8",
    )
    monkeypatch.setenv("JAVA_CMD", str(java))

    with pytest.raises(ValueError, match="selector cannot contain whitespace"):
        collect_controller_runtime_identity(nextflow)


def test_controller_runtime_rejects_root_controller(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nextflow = _executable(
        tmp_path / "nextflow",
        "Version: 25.04.7\nRuntime: Groovy on OpenJDK VM 21.0.8",
    )
    monkeypatch.setattr(controller_runtime_v3.os, "getuid", lambda: 0)

    with pytest.raises(ValueError, match="non-root"):
        collect_controller_runtime_identity(nextflow)


def test_standalone_installed_tree_uses_embedded_manifest_without_sidecar(
    tmp_path: Path,
) -> None:
    project = tmp_path / "installed" / "pipeline"
    project.mkdir(parents=True)
    source = {"commit": "a" * 40, "tree": "b" * 40}

    result = validate_controller_source_identity("standalone", source, project)

    assert result["status"] == "PASS"
    assert result["mode"] == "embedded_runtime_manifest"
    assert result["commit"] == source["commit"]
    assert "attestation" not in result
    assert not (project.parent / "source-identity.json").exists()


def test_host_profile_without_git_checkout_is_rejected(tmp_path: Path) -> None:
    project = tmp_path / "pipeline"
    project.mkdir()
    source = {"commit": "a" * 40, "tree": "b" * 40}

    with pytest.raises(ValueError, match="clean Git checkout"):
        validate_controller_source_identity("scc", source, project)


def test_ssqtl_run_identity_binds_normalization_controller_and_rejects_render_drift(
    tmp_path: Path,
) -> None:
    run = tmp_path / "run"
    contract = run / "contract"
    shards = run / "shards"
    contract.mkdir(parents=True)
    shards.mkdir()
    tasks = contract / "tasks.jsonl"
    plan = shards / "shard_plan.json"
    runtime_path = tmp_path / "runtime.json"
    runtime_snapshot = contract / "runtime_manifest.snapshot.json"
    for path, value in (
        (tasks, "{}\n"),
        (plan, "{}\n"),
        (runtime_path, "{}\n"),
        (runtime_snapshot, "{}\n"),
    ):
        path.write_text(value, encoding="utf-8")
    controller = {
        "schema_version": "3.0-controller-runtime",
        "identity_sha256": "c" * 64,
    }
    controller_path = contract / "controller_runtime.json"
    controller_path.write_text(json.dumps(controller) + "\n", encoding="utf-8")
    execution = {
        "schema_version": "3.0-ssqtl-normalization-execution",
        "status": "SUCCEEDED",
        "controller_runtime_identity_sha256": controller["identity_sha256"],
        "controller_runtime_sha256": sha256_file(controller_path),
    }
    execution_path = contract / "ssqtl_normalization_execution.json"
    execution_path.write_text(json.dumps(execution) + "\n", encoding="utf-8")
    runtime = {
        "runtime_manifest_sha256": "a" * 64,
        "runtime_fingerprint_sha256": "b" * 64,
    }

    identity = _identity_contract(
        run_id="run_001",
        generation_id="generation_001",
        profile="test",
        adapter="ssqtl",
        tasks_path=tasks,
        shard_plan_path=plan,
        runtime_path=runtime_path,
        runtime_identity=runtime,
        runtime_snapshot_path=runtime_snapshot,
    )
    (contract / "run_identity.json").write_text(
        json.dumps(identity) + "\n", encoding="utf-8"
    )
    assert identity["normalization_controller_runtime_identity_sha256"] == "c" * 64
    assert identity["normalization_controller_runtime_contract_sha256"] == sha256_file(
        controller_path
    )
    assert identity["normalization_execution_receipt_sha256"] == sha256_file(
        execution_path
    )
    _validate_normalization_controller_contract(
        run, observed_controller=controller
    )

    drifted = {**controller, "identity_sha256": "d" * 64}
    with pytest.raises(ValueError, match="render controller differs"):
        _validate_normalization_controller_contract(
            run, observed_controller=drifted
        )


def test_rerun_generation_does_not_reaccount_source_normalization_trace(
    tmp_path: Path,
) -> None:
    run = tmp_path / "rerun"
    (run / "contract").mkdir(parents=True)
    (run / "contract" / "run_identity.json").write_text("{}\n", encoding="utf-8")

    _validate_normalization_controller_contract(run, observed_controller={})
    assert _expected_ssqtl_normalization_trace_bindings(
        run,
        runtime_identity={},
        run_id="run_001",
        generation_id="generation_002",
        profile="test",
    ) == ([], [])
