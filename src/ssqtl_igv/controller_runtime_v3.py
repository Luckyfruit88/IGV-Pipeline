from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from .utils import atomic_write_json, sha256_file, sha256_json


NEXTFLOW_VERSION = "25.04.7"
CONTROLLER_JAVA_MAJOR = 21

_NEXTFLOW_VERSION_PATTERN = re.compile(
    rf"(?im)\bversion\s*:?[ \t]*{re.escape(NEXTFLOW_VERSION)}(?:\s|$)"
)
_JAVA_VERSION_PATTERN = re.compile(
    rf'(?im)(?:version\s+"|openjdk\s+){CONTROLLER_JAVA_MAJOR}(?:\.|\s|\")'
)
_NEXTFLOW_RUNTIME_JAVA_PATTERN = re.compile(
    rf"(?im)^\s*Runtime:\s+.*\b{CONTROLLER_JAVA_MAJOR}(?:\.|\+|\s|$)"
)
_GIT_OBJECT_PATTERN = re.compile(r"^[a-f0-9]{40}$")


def controller_java_command() -> tuple[str, str]:
    """Return the Java selector used by the standard Nextflow launcher."""

    for name in ("NXF_JAVA_HOME",):
        value = os.environ.get(name, "").strip()
        if value:
            return str(Path(value).expanduser() / "bin" / "java"), name
    for name in ("JAVA_CMD",):
        value = os.environ.get(name, "").strip()
        if value:
            return value, name
    for name in ("JAVA_HOME",):
        value = os.environ.get(name, "").strip()
        if value:
            return str(Path(value).expanduser() / "bin" / "java"), name
    return "java", "PATH"


def normalized_nextflow_environment(
    java_executable: str | Path,
    *,
    base: dict[str, str] | None = None,
) -> dict[str, str]:
    """Force probes and executions through the exact Java binary we admitted."""

    environment = dict(os.environ if base is None else base)
    declared = Path(java_executable).expanduser()
    observed = (
        declared
        if declared.is_absolute() or "/" in str(java_executable)
        else Path(shutil.which(str(java_executable)) or str(java_executable))
    )
    java_path = Path(os.path.abspath(observed))
    if any(character.isspace() for character in str(java_path)):
        raise ValueError(
            "controller Java selector cannot contain whitespace; create a stable "
            f"no-whitespace symlink to the Java executable: {java_path}"
        )
    try:
        resolved = java_path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"controller Java is unavailable: {java_path}: {exc}") from exc
    if not resolved.is_file() or not os.access(java_path, os.X_OK):
        raise ValueError(f"controller Java is not executable: {java_path}")
    # NXF_JAVA_HOME overrides JAVA_CMD in the official launcher. Remove it so
    # both the probe and every real launch use this exact, hashed executable.
    environment.pop("NXF_JAVA_HOME", None)
    environment.pop("NEXTFLOW_JAVA_CMD", None)
    environment["JAVA_CMD"] = str(java_path)
    environment["JAVA_HOME"] = str(resolved.parent.parent)
    return environment


def _resolve_executable(
    value: str,
    label: str,
    *,
    preserve_selector: bool = False,
) -> Path:
    declared = Path(value).expanduser()
    observed = str(declared) if declared.is_absolute() or "/" in value else shutil.which(value)
    if not observed:
        raise FileNotFoundError(f"{label} executable is unavailable: {value}")
    selector = Path(os.path.abspath(Path(observed).expanduser()))
    try:
        resolved = selector.resolve(strict=True)
    except OSError as exc:
        raise FileNotFoundError(f"{label} executable is unavailable: {value}: {exc}") from exc
    if not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise ValueError(f"{label} executable is not an executable regular file: {resolved}")
    if preserve_selector:
        if any(character.isspace() for character in str(selector)):
            raise ValueError(
                f"{label} selector cannot contain whitespace; create a stable "
                f"no-whitespace symlink: {selector}"
            )
        return selector
    return resolved


def _version_output(
    command: list[str],
    label: str,
    *,
    environment: dict[str, str] | None = None,
) -> str:
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            env={
                **(os.environ if environment is None else environment),
                "LC_ALL": "C.UTF-8",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"cannot execute {label} version probe: {exc}") from exc
    output = "\n".join(
        part.strip() for part in (completed.stdout, completed.stderr) if part.strip()
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{label} version probe failed with exit {completed.returncode}: "
            + (output or "<empty>")
        )
    return output


def collect_controller_runtime_identity(nextflow: str | Path) -> dict[str, Any]:
    """Fail closed unless the actual controller is Nextflow 25.04.7 on Java 21."""

    uid = os.getuid() if hasattr(os, "getuid") else None
    gid = os.getgid() if hasattr(os, "getgid") else None
    if uid == 0:
        raise ValueError("Nextflow controller must run as a non-root user")
    nextflow_path = _resolve_executable(str(nextflow), "Nextflow")
    java_value, java_source = controller_java_command()
    java_path = _resolve_executable(
        java_value,
        "controller Java",
        preserve_selector=True,
    )
    java_resolved_path = java_path.resolve(strict=True)
    java_output = _version_output([str(java_path), "-version"], "controller Java")
    if _JAVA_VERSION_PATTERN.search(java_output) is None:
        raise ValueError(
            f"controller Java must be major {CONTROLLER_JAVA_MAJOR}; observed: {java_output}"
        )
    nextflow_environment = normalized_nextflow_environment(java_path)
    nextflow_output = _version_output(
        [str(nextflow_path), "info"],
        "Nextflow",
        environment=nextflow_environment,
    )
    if _NEXTFLOW_VERSION_PATTERN.search(nextflow_output) is None:
        raise ValueError(
            f"Nextflow must be exactly {NEXTFLOW_VERSION}; observed: {nextflow_output}"
        )
    if _NEXTFLOW_RUNTIME_JAVA_PATTERN.search(nextflow_output) is None:
        raise ValueError(
            "Nextflow runtime did not report the admitted Java major "
            f"{CONTROLLER_JAVA_MAJOR}: {nextflow_output}"
        )
    identity: dict[str, Any] = {
        "schema_version": "3.0-controller-runtime",
        "nextflow": {
            "required_version": NEXTFLOW_VERSION,
            "executable": str(nextflow_path),
            "sha256": sha256_file(nextflow_path),
            "version_output_sha256": sha256_json(nextflow_output),
        },
        "java": {
            "required_major": CONTROLLER_JAVA_MAJOR,
            "nextflow_reported_major": CONTROLLER_JAVA_MAJOR,
            "selector": java_source,
            "executable": str(java_path),
            "resolved_executable": str(java_resolved_path),
            "sha256": sha256_file(java_resolved_path),
            "version_output_sha256": sha256_json(java_output),
        },
        "process_identity": {
            "uid": uid,
            "gid": gid,
            "non_root": uid is None or uid != 0,
        },
    }
    identity["identity_sha256"] = sha256_json(identity)
    return identity


def freeze_controller_runtime_identity(
    run_dir: str | Path,
    nextflow: str | Path,
) -> dict[str, Any]:
    """Freeze the observed controller identity; reject drift before any resume."""

    root = Path(run_dir).resolve(strict=True)
    contract = root / "contract" / "controller_runtime.json"
    if contract.is_symlink():
        raise ValueError("controller runtime contract cannot be a symlink")
    observed = collect_controller_runtime_identity(nextflow)
    if contract.exists():
        if not contract.is_file():
            raise ValueError("controller runtime contract is not a regular file")
        try:
            frozen = json.loads(contract.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"cannot read controller runtime contract: {exc}") from exc
        if frozen != observed:
            raise ValueError("controller Nextflow/Java identity differs from the immutable run")
    else:
        atomic_write_json(contract, observed)
    return observed


def validate_controller_source_identity(
    profile: str,
    source: dict[str, Any],
    project_root: str | Path,
    *,
    allow_test_runtime: bool = False,
) -> dict[str, Any]:
    """Bind controller source to the source recorded by the runtime manifest."""

    project = Path(project_root).resolve(strict=True)
    if allow_test_runtime:
        return {"status": "TEST_ONLY", "source": source, "project_root": str(project)}
    if set(source) != {"commit", "tree"} or any(
        _GIT_OBJECT_PATTERN.fullmatch(str(source.get(field, ""))) is None
        for field in ("commit", "tree")
    ):
        raise ValueError("runtime manifest lacks exact source commit/tree provenance")
    if (project / ".git").exists():
        commit = subprocess.run(
            ["git", "-C", str(project), "rev-parse", "HEAD"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        tree = subprocess.run(
            ["git", "-C", str(project), "rev-parse", "HEAD^{tree}"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(project), "status", "--porcelain"],
            check=True,
            text=True,
            stdout=subprocess.PIPE,
        ).stdout.strip()
        if commit != source["commit"] or tree != source["tree"] or dirty:
            raise ValueError(
                "controller source commit/tree differs from the runtime manifest"
            )
        return {
            "status": "PASS",
            "mode": "clean_git_checkout",
            "commit": commit,
            "tree": tree,
            "project_root": str(project),
        }
    if profile != "standalone":
        raise ValueError(
            "host controller requires a clean Git checkout matching the runtime "
            "manifest source"
        )
    # The pull-and-run image installs an immutable source tree without .git.
    # Its already-validated embedded manifest is the source observation; no
    # external source-identity sidecar or user-supplied assertion is required.
    return {
        "status": "PASS",
        "mode": "embedded_runtime_manifest",
        "commit": source["commit"],
        "tree": source["tree"],
        "project_root": str(project),
    }
