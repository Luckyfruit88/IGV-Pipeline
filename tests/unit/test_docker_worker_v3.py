from __future__ import annotations

import json
import subprocess
from pathlib import Path

import jsonschema
import pytest

from ssqtl_igv import orchestrator_v3, probes_v3
from ssqtl_igv.docker_worker_v3 import docker_worker_identity
from ssqtl_igv.orchestrator_v3 import (
    _identity_contract,
    _run_nextflow_shard,
    execute_portable_run,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
OCI_REFERENCE = "ghcr.io/luckyfruit88/igv-pipeline@sha256:" + "b" * 64


def test_docker_worker_identity_is_non_root_canonical_and_profile_scoped() -> None:
    identity = docker_worker_identity("docker", "502", "20")
    assert identity == {
        "schema_version": "3.0-docker-worker-identity",
        "uid": 502,
        "gid": 20,
        "home": "/run/home",
        "home_tmpfs": "/run/home:rw,noexec,nosuid,nodev,size=64m,uid=502,gid=20,mode=0700",
        "tmp_tmpfs": "/tmp:rw,noexec,nosuid,nodev,size=256m",
        "root_user_prohibited": True,
    }
    assert docker_worker_identity("standalone", None, None) is None

    for uid, gid in (
        (None, None),
        ("502", None),
        (None, "20"),
        ("0", "20"),
        ("502", "0"),
        ("0502", "20"),
        ("-1", "20"),
        ("502.0", "20"),
        (str(2_147_483_648), "20"),
    ):
        with pytest.raises(ValueError, match="Docker worker"):
            docker_worker_identity("docker", uid, gid)

    with pytest.raises(ValueError, match="only with the docker profile"):
        docker_worker_identity("scc", "502", "20")


def test_immutable_run_identity_binds_docker_worker_mapping(tmp_path: Path) -> None:
    tasks = tmp_path / "tasks.jsonl"
    plan = tmp_path / "shard_plan.json"
    snapshot = tmp_path / "runtime_manifest.snapshot.json"
    runtime_identity = tmp_path / "runtime-manifest.json"
    tasks.write_text("{}\n", encoding="utf-8")
    plan.write_text("{}\n", encoding="utf-8")
    snapshot.write_text("{}\n", encoding="utf-8")
    runtime_identity.write_text("{}\n", encoding="utf-8")
    worker = docker_worker_identity("docker", 502, 20)

    identity = _identity_contract(
        run_id="run-001",
        generation_id="generation-001",
        profile="docker",
        adapter="generic",
        tasks_path=tasks,
        shard_plan_path=plan,
        runtime_path=runtime_identity,
        runtime_identity={
            "runtime_manifest_sha256": "a" * 64,
            "runtime_fingerprint_sha256": "b" * 64,
        },
        runtime_snapshot_path=snapshot,
        docker_worker=worker,
    )

    assert identity["docker_worker_identity"] == worker
    with pytest.raises(ValueError, match="differs from the immutable run identity"):
        execute_portable_run(
            {"identity": identity},
            profile="docker",
            runtime_identity_path=runtime_identity,
            docker_worker_uid=503,
            docker_worker_gid=20,
        )


def test_docker_nextflow_command_passes_exact_worker_ids(
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
        profile="docker",
        runtime_identity=tmp_path / "runtime-manifest.json",
        runtime_identity_value={
            "runtime_manifest_sha256": "a" * 64,
            "runtime_fingerprint_sha256": "b" * 64,
        },
        nextflow="nextflow",
        work_dir=tmp_path / "work",
        resume=False,
        fake_runtime=False,
        runtime_image=OCI_REFERENCE,
        runtime_sif=None,
        runtime_sif_sha256=None,
        run_id="run-001",
        generation_id="generation-001",
        docker_worker_uid=502,
        docker_worker_gid=20,
    )

    assert observed == [result["command"]]
    command = result["command"]
    uid_index = command.index("--host_uid")
    gid_index = command.index("--host_gid")
    assert command[uid_index + 1] == "502"
    assert command[gid_index + 1] == "20"

    with pytest.raises(ValueError, match="missing or invalid"):
        _run_nextflow_shard(
            shard={"shard_id": "shard-0002", "path": str(tmp_path / "tasks.jsonl")},
            run_dir=run,
            profile="docker",
            runtime_identity=tmp_path / "runtime-manifest.json",
            runtime_identity_value={
                "runtime_manifest_sha256": "a" * 64,
                "runtime_fingerprint_sha256": "b" * 64,
            },
            nextflow="nextflow",
            work_dir=tmp_path / "work",
            resume=False,
            fake_runtime=False,
            runtime_image=OCI_REFERENCE,
            runtime_sif=None,
            runtime_sif_sha256=None,
            run_id="run-001",
            generation_id="generation-001",
        )


def test_docker_profile_schema_workflow_and_run_options_close_the_same_contract() -> None:
    schema = json.loads((PROJECT_ROOT / "nextflow_schema.json").read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    validator = jsonschema.Draft202012Validator(schema)
    validator.validate(
        {
            "runtime_execution_mode": "docker",
            "runtime_image": OCI_REFERENCE,
            "host_uid": 502,
            "host_gid": 20,
        }
    )
    for invalid in (
        {"runtime_execution_mode": "docker", "runtime_image": OCI_REFERENCE},
        {
            "runtime_execution_mode": "docker",
            "runtime_image": OCI_REFERENCE,
            "host_uid": 0,
            "host_gid": 20,
        },
        {"runtime_execution_mode": "standalone", "host_uid": 502, "host_gid": 20},
    ):
        with pytest.raises(jsonschema.ValidationError):
            validator.validate(invalid)

    dockerfile = (PROJECT_ROOT / "containers/runtime.Dockerfile").read_text(encoding="utf-8")
    config = (PROJECT_ROOT / "conf/docker.config").read_text(encoding="utf-8")
    workflow = (PROJECT_ROOT / "workflows/portable_run.nf").read_text(encoding="utf-8")
    assert "USER 65532:65532" in dockerfile
    assert "--platform linux/amd64" in config
    assert "--user ${params.host_uid}:${params.host_gid}" in config
    assert "--read-only" in config
    assert "--cap-drop=ALL" in config
    assert "--security-opt=no-new-privileges" in config
    assert "--network=none" in config
    assert "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=256m" in config
    assert "--tmpfs=/run/home:rw,noexec,nosuid,nodev,size=64m,uid=${params.host_uid},gid=${params.host_gid},mode=0700" in config
    assert "--env HOME=/run/home" in config
    assert "docker profile requires canonical non-root host_uid and host_gid" in workflow
    assert "hostUid.toLong() > 2147483647L" in workflow


def test_docker_doctor_reuses_worker_user_and_tmpfs_constraints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[tuple[str, list[str]]] = []

    def validate(*_args: object, **_kwargs: object) -> dict[str, object]:
        return {
            "runtime_manifest_path": "/opt/igv-pipeline/runtime-manifest.json",
            "runtime_manifest_sha256": "a" * 64,
            "runtime_fingerprint_sha256": "b" * 64,
            "observed_provenance": {"oci": None, "sif_sha256": None},
            "source": {"commit": "c" * 40, "tree": "d" * 40},
        }

    def passing(name: str, command: list[str], **_kwargs: object) -> dict[str, object]:
        commands.append((name, command))
        return {"name": name, "required": True, "status": "PASS", "detail": "PASS"}

    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", validate)
    monkeypatch.setattr(
        probes_v3,
        "_controller_source_probe",
        lambda *_args, **_kwargs: {
            "name": "controller_source_identity",
            "required": True,
            "status": "PASS",
            "detail": "fixture PASS",
        },
    )
    monkeypatch.setattr(probes_v3, "_probe_command", passing)
    report = probes_v3.collect_doctor_report(
        "docker",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
        runtime_image=OCI_REFERENCE,
        host_uid=502,
        host_gid=20,
    )
    assert report["status"] == "PASS"
    assert report["docker_worker_identity"]["uid"] == 502
    for name, command in commands:
        if name in {"portable_runtime_self_test", "igv_runtime_self_test"}:
            assert command[command.index("--user") + 1] == "502:20"
            assert "--tmpfs=/run/home:rw,noexec,nosuid,nodev,size=64m,uid=502,gid=20,mode=0700" in command
            assert command[command.index("--env") + 1] == "HOME=/run/home"

    commands.clear()
    rejected = probes_v3.collect_doctor_report(
        "docker",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
        runtime_image=OCI_REFERENCE,
        host_uid=0,
        host_gid=20,
    )
    assert rejected["status"] == "FAIL"
    assert "portable_runtime:docker_worker_identity" in rejected["failed_checks"]
    assert all(
        name not in {"portable_runtime_self_test", "igv_runtime_self_test"}
        for name, _ in commands
    )
