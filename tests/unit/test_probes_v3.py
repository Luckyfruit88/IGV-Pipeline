from __future__ import annotations

from pathlib import Path

import pytest

from ssqtl_igv import probes_v3
from ssqtl_igv.utils import sha256_file


OCI_REFERENCE = "ghcr.io/luckyfruit88/igv-pipeline@sha256:" + "a" * 64


@pytest.fixture(autouse=True)
def _controller_source_is_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        probes_v3,
        "_controller_source_probe",
        lambda *_args, **_kwargs: {
            "name": "controller_source_identity",
            "required": True,
            "status": "PASS",
            "detail": "fixture source identity",
        },
    )


def _executable(path: Path, output: str, exit_code: int = 0) -> Path:
    path.write_text(
        "#!/bin/sh\nprintf '%s\\n' '" + output.replace("'", "'\\''") + "'\nexit " + str(exit_code) + "\n",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _validation(*, observed_sif_sha256: str | None = None) -> dict:
    return {
        "runtime_manifest_path": "/opt/igv-pipeline/runtime-manifest.json",
        "runtime_manifest_sha256": "b" * 64,
        "runtime_fingerprint_sha256": "c" * 64,
        "observed_provenance": {
            "oci": {"reference": OCI_REFERENCE, "digest": "sha256:" + "a" * 64},
            "sif_sha256": observed_sif_sha256,
        },
        "source": {"commit": "d" * 40, "tree": "e" * 40},
    }


def _passing_probe(name: str, command: list[str], **_: object) -> dict:
    return {
        "name": name,
        "required": True,
        "status": "PASS",
        "command": command,
        "detail": "fixture PASS",
    }


def test_nextflow_probe_rejects_wrong_version_even_when_command_exits_zero(
    tmp_path: Path,
) -> None:
    nextflow = _executable(tmp_path / "nextflow", "version 0.0.1")

    result = probes_v3._nextflow_probe(str(nextflow))

    assert result["status"] == "FAIL"
    assert "Nextflow 25.04.7" in result["detail"]


def test_controller_java_matches_official_nextflow_launcher_precedence(
    monkeypatch,
) -> None:
    monkeypatch.setenv("NEXTFLOW_JAVA_CMD", "/site/java-21/bin/java")
    monkeypatch.setenv("NXF_JAVA_HOME", "/opt/java-21")
    assert probes_v3._controller_java_executable() == "/opt/java-21/bin/java"

    monkeypatch.setenv("NXF_JAVA_HOME", "")
    monkeypatch.setenv("JAVA_CMD", "/fallback/java-command")
    monkeypatch.setenv("JAVA_HOME", "/fallback/java-home")
    assert probes_v3._controller_java_executable() == "/fallback/java-command"

    monkeypatch.delenv("JAVA_CMD")
    assert probes_v3._controller_java_executable() == "/fallback/java-home/bin/java"

    monkeypatch.setenv("JAVA_HOME", "")
    assert probes_v3._controller_java_executable() == "java"


def test_standalone_doctor_probes_container_java21_from_nxf_java_home(
    monkeypatch,
) -> None:
    commands: list[tuple[str, list[str]]] = []

    def record(name: str, command: list[str], **kwargs: object) -> dict:
        commands.append((name, command))
        return _passing_probe(name, command, **kwargs)

    monkeypatch.delenv("NEXTFLOW_JAVA_CMD", raising=False)
    monkeypatch.setenv("NXF_JAVA_HOME", "/opt/java-21")
    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", lambda *a, **k: _validation())
    monkeypatch.setattr(
        probes_v3,
        "normalized_nextflow_environment",
        lambda *_args, **_kwargs: {},
    )
    monkeypatch.setattr(probes_v3, "_probe_command", record)

    report = probes_v3.collect_doctor_report(
        "standalone",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
    )

    assert report["status"] == "PASS"
    assert dict(commands)["nextflow_java_21"] == ["/opt/java-21/bin/java", "-version"]


def test_doctor_never_executes_runtime_when_manifest_validation_fails(
    monkeypatch,
) -> None:
    commands: list[tuple[str, list[str]]] = []

    def fail_manifest(*_: object, **__: object) -> dict:
        raise ValueError("not valid JSON")

    def record(name: str, command: list[str], **kwargs: object) -> dict:
        commands.append((name, command))
        return _passing_probe(name, command, **kwargs)

    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", fail_manifest)
    monkeypatch.setattr(probes_v3, "_probe_command", record)

    report = probes_v3.collect_doctor_report(
        "docker",
        runtime_manifest="/identity/bad.json",
        runtime_image=OCI_REFERENCE,
    )

    assert report["status"] == "FAIL"
    assert "portable_runtime:runtime_manifest" in report["failed_checks"]
    assert all(name not in {"portable_runtime_self_test", "igv_runtime_self_test"} for name, _ in commands)


def test_docker_doctor_runs_the_user_selected_image_without_certification(monkeypatch) -> None:
    commands: list[tuple[str, list[str]]] = []

    def record(name: str, command: list[str], **kwargs: object) -> dict:
        commands.append((name, command))
        return _passing_probe(name, command, **kwargs)

    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", lambda *a, **k: _validation())
    monkeypatch.setattr(probes_v3, "_probe_command", record)

    report = probes_v3.collect_doctor_report(
        "docker",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
        runtime_image=OCI_REFERENCE,
    )

    assert report["status"] == "PASS"
    assert report["selected_runtime"] == OCI_REFERENCE
    assert report["runtime_manifest_sha256"] == "b" * 64
    assert report["runtime_fingerprint_sha256"] == "c" * 64
    assert report["snapshot_runtime_ready"] is True
    assert "runtime_certification" not in report
    assert "publication_eligible" not in report
    runtime_commands = {
        name: command
        for name, command in commands
        if name in {"portable_runtime_self_test", "igv_runtime_self_test"}
    }
    assert set(runtime_commands) == {"portable_runtime_self_test", "igv_runtime_self_test"}
    for command in runtime_commands.values():
        assert command[:2] == ["docker", "run"]
        assert "--platform" in command and "linux/amd64" in command
        assert "--read-only" in command
        assert "--network=none" in command
        assert OCI_REFERENCE in command
    assert not any(command and command[0] in {"samtools", "igv", "Xvfb"} for _, command in commands)


def test_scc_doctor_binds_actual_sif_sha_and_executes_inside_it(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    sif = tmp_path / "runtime.sif"
    sif.write_bytes(b"fixture-sif")
    sif.chmod(0o444)
    observed_kwargs: dict[str, object] = {}
    commands: list[tuple[str, list[str]]] = []

    def validate(*_: object, **kwargs: object) -> dict:
        observed_kwargs.update(kwargs)
        return _validation(observed_sif_sha256=sha256_file(sif))

    def record(name: str, command: list[str], **kwargs: object) -> dict:
        commands.append((name, command))
        return _passing_probe(name, command, **kwargs)

    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", validate)
    monkeypatch.setattr(probes_v3, "_probe_command", record)

    report = probes_v3.collect_doctor_report(
        "scc",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
        runtime_sif=sif,
        run_dir=run_dir,
    )

    assert report["status"] == "PASS"
    assert observed_kwargs["observed_sif_sha256"] == sha256_file(sif)
    for name, command in commands:
        if name in {"portable_runtime_self_test", "igv_runtime_self_test"}:
            assert command[:8] == [
                "apptainer",
                "exec",
                "--cleanenv",
                "--containall",
                "--no-home",
                "--env",
                f"NXF_HOME={probes_v3.SIF_NXF_HOME}",
                str(sif),
            ]


def test_scc_doctor_accepts_regular_unsigned_sif(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    sif = tmp_path / "runtime.sif"
    sif.write_bytes(b"fixture-sif")

    monkeypatch.setattr(
        probes_v3, "validate_runtime_manifest", lambda *a, **k: _validation()
    )
    monkeypatch.setattr(probes_v3, "_probe_command", _passing_probe)

    report = probes_v3.collect_doctor_report(
        "scc",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
        runtime_sif=sif,
        run_dir=run_dir,
    )

    assert report["status"] == "PASS"


def test_docker_doctor_accepts_release_tag_and_rejects_missing_image(monkeypatch) -> None:
    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", lambda *a, **k: _validation())
    monkeypatch.setattr(probes_v3, "_probe_command", _passing_probe)

    report = probes_v3.collect_doctor_report(
        "docker",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
        runtime_image="ghcr.io/luckyfruit88/igv-pipeline:3.0.0",
    )

    assert report["status"] == "PASS"
    assert report["selected_runtime"] == "ghcr.io/luckyfruit88/igv-pipeline:3.0.0"

    missing = probes_v3.collect_doctor_report(
        "docker", runtime_manifest="/opt/igv-pipeline/runtime-manifest.json"
    )
    assert missing["status"] == "FAIL"
    assert "portable_runtime:runtime_manifest" in missing["failed_checks"]


def test_standalone_doctor_uses_embedded_manifest_without_image_argument(monkeypatch) -> None:
    commands: list[tuple[str, list[str]]] = []

    def record(name: str, command: list[str], **kwargs: object) -> dict:
        commands.append((name, command))
        return _passing_probe(name, command, **kwargs)

    monkeypatch.setattr(probes_v3, "validate_runtime_manifest", lambda *a, **k: _validation())
    monkeypatch.setattr(probes_v3, "_probe_command", record)

    report = probes_v3.collect_doctor_report(
        "standalone",
        runtime_manifest="/opt/igv-pipeline/runtime-manifest.json",
    )
    assert report["status"] == "PASS"
    assert report["selected_runtime"] is None
    runtime = {
        name: command
        for name, command in commands
        if name in {"portable_runtime_self_test", "igv_runtime_self_test"}
    }
    assert runtime["portable_runtime_self_test"] == ["/usr/local/bin/runtime-self-test"]
    assert runtime["igv_runtime_self_test"] == [
        "/opt/igv/bin/igv",
        "--runtime-self-test",
    ]
