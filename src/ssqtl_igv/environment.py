from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any, Iterable

from .utils import atomic_write_json, sha256_file, utc_now


def _command_identity(command: str) -> dict[str, Any]:
    executable = shutil.which(command)
    if executable is None:
        return {"command": command, "available": False, "path": None}
    path = Path(executable).resolve(strict=True)
    identity: dict[str, Any] = {
        "command": command,
        "available": True,
        "path": str(path),
        "size": path.stat().st_size,
        "mtime_ns": path.stat().st_mtime_ns,
    }
    if path.is_file() and path.stat().st_size <= 64 * 1024 * 1024:
        identity["sha256"] = sha256_file(path)
    return identity


def _java_identity() -> dict[str, Any] | None:
    java = shutil.which("java")
    if java is None:
        return None
    completed = subprocess.run(
        [java, "-version"],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
    )
    return {
        "path": str(Path(java).resolve(strict=True)),
        "exit_code": completed.returncode,
        "version_output": completed.stdout.strip().splitlines()[:4],
    }


def validate_environment(
    output_dir: str | Path,
    *,
    phase: str,
    pipeline_commit: str,
    nextflow_version: str,
    required_commands: Iterable[str] = (),
    helper_sif: str | Path | None = None,
    helper_sif_sha256: str | None = None,
    test_mode: bool = False,
) -> dict[str, Any]:
    """Freeze a read-only execution fingerprint and fail on missing declared tools."""

    if len(pipeline_commit) != 40 or any(
        character not in "0123456789abcdef" for character in pipeline_commit
    ):
        raise ValueError("pipeline_commit must be a full lowercase Git commit")
    if not test_mode and pipeline_commit == "0" * 40:
        raise ValueError("pipeline_commit must identify a real commit outside test mode")
    commands = sorted({str(command).strip() for command in required_commands if str(command).strip()})
    command_rows = [_command_identity(command) for command in commands]
    missing = [row["command"] for row in command_rows if not row["available"]]
    if missing and not test_mode:
        raise RuntimeError(f"required runtime commands are unavailable: {', '.join(missing)}")

    sif_record: dict[str, Any] | None = None
    if helper_sif is not None:
        path = Path(helper_sif).expanduser()
        if path.is_symlink():
            raise ValueError(f"helper SIF must not be a symlink: {path}")
        path = path.resolve(strict=True)
        if not path.is_file():
            raise ValueError(f"helper SIF is not a regular file: {path}")
        observed = sha256_file(path)
        if helper_sif_sha256 and observed != helper_sif_sha256:
            raise ValueError("helper SIF checksum differs from the declared identity")
        sif_record = {
            "path": str(path),
            "sha256": observed,
            "size": path.stat().st_size,
        }
    elif helper_sif_sha256:
        raise ValueError("helper_sif_sha256 requires helper_sif")

    report = {
        "schema_version": "2.0-environment",
        "created_at": utc_now(),
        "status": "PASS_WITH_TEST_RELAXATIONS" if missing else "PASS",
        "phase": phase,
        "test_mode": bool(test_mode),
        "pipeline_commit": pipeline_commit,
        "nextflow_version": nextflow_version,
        "host": platform.node(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": {
            "executable": str(Path(sys.executable).resolve(strict=True)),
            "version": platform.python_version(),
        },
        "java": _java_identity(),
        "loaded_modules": [
            value for value in os.environ.get("LOADEDMODULES", "").split(":") if value
        ],
        "commands": command_rows,
        "missing_commands": missing,
        "helper_sif": sif_record,
    }
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"environment output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        atomic_write_json(staging / "environment.json", report)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**report, "output_dir": str(destination)}
