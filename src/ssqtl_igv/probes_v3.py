from __future__ import annotations

import fcntl
import os
import platform
import re
import secrets
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .docker_worker_v3 import (
    DOCKER_WORKER_HOME,
    DOCKER_WORKER_TMP_TMPFS,
    docker_worker_identity,
)
from .controller_runtime_v3 import (
    controller_java_command,
    normalized_nextflow_environment,
    validate_controller_source_identity,
)
from .runtime_identity import _project_root, validate_runtime_manifest
from .utils import reject_symlink_path_components, sha256_file, utc_now


SIF_NXF_HOME = "/tmp/.nextflow"


def _probe_command(
    name: str,
    command: list[str],
    *,
    required: bool = True,
    timeout: float = 20.0,
    expected_pattern: str | None = None,
    expected_description: str | None = None,
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Run one read-only probe and optionally require an exact output contract."""

    executable = shutil.which(command[0]) if not Path(command[0]).is_absolute() else command[0]
    result: dict[str, Any] = {
        "name": name,
        "required": required,
        "command": command,
        "executable": executable,
    }
    if not executable or not Path(executable).is_file():
        result.update(status="FAIL" if required else "NOT_AVAILABLE", detail="executable not found")
        return result
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            env={
                **(os.environ if environment is None else environment),
                "LC_ALL": "C.UTF-8",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        result.update(status="FAIL" if required else "NOT_AVAILABLE", detail=str(exc))
        return result
    combined = "\n".join(
        part.strip() for part in (completed.stdout, completed.stderr) if part.strip()
    )
    detail = combined.splitlines()
    passed = completed.returncode == 0
    if passed and expected_pattern is not None:
        passed = re.search(expected_pattern, combined) is not None
    if completed.returncode != 0:
        observed = "\n".join(detail[:12]) or "command returned no diagnostic output"
    elif not passed:
        expected = expected_description or expected_pattern or "required output"
        observed = f"expected {expected}; observed: " + ("\n".join(detail[:12]) or "<empty>")
    else:
        observed = "\n".join(detail[:12])
    result.update(
        status="PASS" if passed else ("FAIL" if required else "NOT_AVAILABLE"),
        exit_code=completed.returncode,
        detail=observed,
    )
    if expected_description is not None:
        result["expected"] = expected_description
    return result


def _java_probe(name: str, executable: str, major: int, *, required: bool = True) -> dict[str, Any]:
    return _probe_command(
        name,
        [executable, "-version"],
        required=required,
        expected_pattern=rf'(?im)(?:version\s+"|openjdk\s+){major}(?:\.|\s|\")',
        expected_description=f"Java major version {major}",
    )


def _nextflow_probe(executable: str) -> dict[str, Any]:
    java_value, _ = controller_java_command()
    java_executable = (
        str(Path(java_value).expanduser())
        if Path(java_value).is_absolute() or "/" in java_value
        else shutil.which(java_value) or java_value
    )
    try:
        environment = normalized_nextflow_environment(java_executable)
    except (OSError, ValueError) as exc:
        return _failed_probe("nextflow_25_04_7", f"cannot bind controller Java: {exc}")
    return _probe_command(
        "nextflow_25_04_7",
        [executable, "info"],
        expected_pattern=(
            r"(?ims)(?=.*\bversion\s*:?[ \t]*25\.04\.7(?:\s|$))"
            r"(?=.*^\s*Runtime:\s+.*\b21(?:\.|\+|\s|$))"
        ),
        expected_description="Nextflow 25.04.7 running on Java 21",
        environment=environment,
    )


def _controller_java_executable() -> str:
    return controller_java_command()[0]


def _failed_probe(name: str, detail: str) -> dict[str, Any]:
    return {"name": name, "required": True, "status": "FAIL", "detail": detail}


def _controller_user_probe() -> dict[str, Any]:
    uid = os.getuid() if hasattr(os, "getuid") else None
    gid = os.getgid() if hasattr(os, "getgid") else None
    return {
        "name": "controller_non_root",
        "required": True,
        "status": "FAIL" if uid == 0 else "PASS",
        "detail": f"uid={uid if uid is not None else 'unavailable'} gid={gid if gid is not None else 'unavailable'}",
    }


def _writable_directory_probe(name: str, value: str | Path) -> dict[str, Any]:
    probe: Path | None = None
    descriptor: int | None = None
    failure: OSError | ValueError | None = None
    try:
        declared = reject_symlink_path_components(value, label=name)
        root = declared.resolve(strict=True)
        if not root.is_dir():
            raise ValueError(f"not a directory: {root}")
        probe = root / f".igv-snapshot-doctor-{secrets.token_hex(12)}"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(probe, flags, 0o600)
        os.write(descriptor, b"IGV_SNAPSHOT_WRITE_PROBE\n")
        os.fsync(descriptor)
    except (OSError, ValueError) as exc:
        failure = exc
    finally:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError as exc:
                failure = failure or exc
        if probe is not None:
            try:
                probe.unlink(missing_ok=True)
            except OSError as exc:
                failure = failure or exc
    if failure is not None:
        return _failed_probe(name, f"write probe failed: {failure}")
    return {
        "name": name,
        "required": True,
        "status": "PASS",
        "detail": f"create, fsync, and unlink passed: {root}",
    }


def _shared_filesystem_flock_probe(value: str | Path | None) -> dict[str, Any]:
    """Prove that an independent process cannot steal a held campaign lock."""

    if value is None:
        return _failed_probe(
            "campaign_shared_filesystem_lock",
            "SCC doctor requires --run-dir for the campaign lock probe",
        )
    lock_path: Path | None = None
    descriptor: int | None = None
    try:
        root = reject_symlink_path_components(value, label="campaign lock probe root").resolve(
            strict=True
        )
        if not root.is_dir():
            raise ValueError(f"not a directory: {root}")
        lock_path = root / f".igv-snapshot-flock-{secrets.token_hex(12)}"
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(lock_path, flags, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        child = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "import fcntl,os,sys; "
                    "fd=os.open(sys.argv[1],os.O_RDWR); "
                    "blocked=False; "
                    "\ntry:\n fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)"
                    "\nexcept BlockingIOError:\n blocked=True"
                    "\nfinally:\n os.close(fd)"
                    "\nsys.exit(0 if blocked else 9)"
                ),
                str(lock_path),
            ],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=10.0,
        )
        if child.returncode != 0:
            raise RuntimeError(
                "independent process acquired the held flock"
                + (f": {child.stderr.strip()}" if child.stderr.strip() else "")
            )
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
        descriptor = None
        with lock_path.open("r+", encoding="utf-8") as released:
            fcntl.flock(released.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(released.fileno(), fcntl.LOCK_UN)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        return _failed_probe("campaign_shared_filesystem_lock", str(exc))
    finally:
        if descriptor is not None:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            except OSError:
                pass
            os.close(descriptor)
        if lock_path is not None:
            try:
                lock_path.unlink(missing_ok=True)
            except OSError:
                pass
    return {
        "name": "campaign_shared_filesystem_lock",
        "required": True,
        "status": "PASS",
        "detail": "cross-process exclusive flock and release passed",
    }


def _controller_source_probe(profile: str, validation: dict[str, Any]) -> dict[str, Any]:
    try:
        result = validate_controller_source_identity(
            profile,
            dict(validation.get("source") or {}),
            _project_root(),
        )
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return _failed_probe("controller_source_identity", str(exc))
    return {
        "name": "controller_source_identity",
        "required": True,
        "status": "PASS",
        "detail": f"{result['mode']}:{result['commit']}:{result['tree']}",
    }


def _manifest_probe(
    profile: str,
    *,
    runtime_manifest: str | Path | None,
    runtime_manifest_sha256: str | None,
    runtime_image: str | None,
    runtime_sif: str | Path | None,
) -> tuple[dict[str, Any], dict[str, Any] | None, str | Path | None]:
    if runtime_manifest is None:
        return _failed_probe("runtime_manifest", "embedded runtime manifest is unavailable"), None, None

    observed_sif_sha256: str | None = None
    selected_sif: Path | None = None
    if profile == "scc":
        if runtime_sif is None:
            return _failed_probe("runtime_manifest", "SCC doctor requires a runtime SIF"), None, None
        source = Path(runtime_sif).expanduser()
        if source.is_symlink():
            return _failed_probe("runtime_manifest", f"runtime SIF must not be a symlink: {source}"), None, None
        try:
            selected_sif = source.resolve(strict=True)
        except OSError as exc:
            return _failed_probe("runtime_manifest", f"runtime SIF is unavailable: {exc}"), None, None
        if not selected_sif.is_file():
            return _failed_probe("runtime_manifest", f"runtime SIF is not a regular file: {selected_sif}"), None, None
        observed_sif_sha256 = sha256_file(selected_sif)

    observed_oci_digest: str | None = None
    if runtime_image:
        match = re.search(r"@(?P<digest>sha256:[a-f0-9]{64})$", runtime_image)
        if match:
            observed_oci_digest = match.group("digest")

    try:
        validation = validate_runtime_manifest(
            runtime_manifest,
            expected_manifest_sha256=runtime_manifest_sha256,
            observed_oci_digest=observed_oci_digest,
            observed_sif_sha256=observed_sif_sha256,
        )
    except (OSError, ValueError) as exc:
        return _failed_probe("runtime_manifest", f"validation failed: {exc}"), None, None

    selected: str | Path | None = None
    if profile == "standalone":
        selected = runtime_image
    elif profile == "docker":
        if not runtime_image:
            return (
                _failed_probe(
                    "runtime_manifest",
                    "docker doctor requires the worker image reference",
                ),
                None,
                None,
            )
        selected = runtime_image
    elif profile == "scc":
        selected = selected_sif

    check = {
        "name": "runtime_manifest",
        "required": True,
        "status": "PASS",
        "detail": str(validation["runtime_manifest_path"]),
        "runtime_manifest_sha256": validation["runtime_manifest_sha256"],
        "runtime_fingerprint_sha256": validation["runtime_fingerprint_sha256"],
        "observed_provenance": validation["observed_provenance"],
    }
    return check, validation, selected


def _docker_command(
    image: str,
    worker_identity: dict[str, Any],
    entrypoint: str,
    *arguments: str,
) -> list[str]:
    return [
        "docker",
        "run",
        "--rm",
        "--platform",
        "linux/amd64",
        "--user",
        f"{worker_identity['uid']}:{worker_identity['gid']}",
        "--read-only",
        "--cap-drop=ALL",
        "--security-opt=no-new-privileges",
        "--network=none",
        f"--tmpfs={DOCKER_WORKER_TMP_TMPFS}",
        f"--tmpfs={worker_identity['home_tmpfs']}",
        "--env",
        f"HOME={DOCKER_WORKER_HOME}",
        "--entrypoint",
        entrypoint,
        image,
        *arguments,
    ]


def _runtime_commands(
    profile: str,
    selected: str | Path | None,
    worker_identity: dict[str, Any] | None = None,
) -> tuple[list[str], list[str]]:
    self_test = "/usr/local/bin/runtime-self-test"
    igv = "/opt/igv/bin/igv"
    if profile == "standalone":
        return [self_test], [igv, "--runtime-self-test"]
    if profile == "docker" and isinstance(selected, str) and worker_identity is not None:
        return _docker_command(selected, worker_identity, self_test), _docker_command(
            selected, worker_identity, igv, "--runtime-self-test"
        )
    if profile == "scc" and isinstance(selected, Path):
        runner = shutil.which("apptainer") or shutil.which("singularity") or "apptainer"
        prefix = [
            runner,
            "exec",
            "--cleanenv",
            "--containall",
            "--no-home",
            "--env",
            f"NXF_HOME={SIF_NXF_HOME}",
            str(selected),
        ]
        return [*prefix, self_test], [*prefix, igv, "--runtime-self-test"]
    raise ValueError("validated runtime selection is unavailable")


def collect_doctor_report(
    profile: str,
    *,
    runtime_manifest: str | Path | None = None,
    runtime_manifest_sha256: str | None = None,
    runtime_image: str | None = None,
    runtime_sif: str | Path | None = None,
    host_uid: object | None = None,
    host_gid: object | None = None,
    run_dir: str | Path | None = None,
    work_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Observe controller, exact portable/render runtime, and site accounting layers."""

    normalized = profile.strip().lower()
    if normalized not in {"standalone", "docker", "scc"}:
        raise ValueError("profile must be standalone, docker, or scc")

    worker_contract: dict[str, Any] | None = None
    worker_check: dict[str, Any] | None = None
    worker_error: str | None = None
    try:
        worker_contract = docker_worker_identity(
            normalized, host_uid, host_gid, discover=True
        )
        if worker_contract is not None:
            worker_check = {
                "name": "docker_worker_identity",
                "required": True,
                "status": "PASS",
                "detail": (
                    f"non-root worker {worker_contract['uid']}:{worker_contract['gid']} "
                    f"with isolated HOME {worker_contract['home']}"
                ),
                "uid": worker_contract["uid"],
                "gid": worker_contract["gid"],
                "home": worker_contract["home"],
                "home_tmpfs": worker_contract["home_tmpfs"],
            }
    except ValueError as exc:
        worker_error = str(exc)
        worker_check = _failed_probe("docker_worker_identity", worker_error)

    controller_java = _controller_java_executable()
    nextflow = os.environ.get("NEXTFLOW", "nextflow")
    controller = [
        _controller_user_probe(),
        {
            "name": "python",
            "required": True,
            "status": "PASS" if sys.version_info >= (3, 10) else "FAIL",
            "detail": platform.python_version(),
            "expected": "Python >=3.10",
        },
        _java_probe("nextflow_java_21", controller_java, 21),
        _nextflow_probe(nextflow),
    ]

    manifest_check, validation, selected = _manifest_probe(
        normalized,
        runtime_manifest=runtime_manifest,
        runtime_manifest_sha256=runtime_manifest_sha256,
        runtime_image=runtime_image,
        runtime_sif=runtime_sif,
    )
    portable = [manifest_check]
    if validation is not None:
        controller.append(_controller_source_probe(normalized, validation))
    if worker_check is not None:
        portable.append(worker_check)
    if run_dir is not None:
        portable.append(_writable_directory_probe("run_directory_write", run_dir))
    if work_dir is not None:
        portable.append(_writable_directory_probe("work_directory_write", work_dir))
    if validation is None or worker_error is not None:
        reason = (
            "runtime manifest did not validate"
            if validation is None
            else "Docker worker identity did not validate"
        )
        portable.append(
            _failed_probe("portable_runtime_self_test", f"not executed because {reason}")
        )
        render = [
            _failed_probe("igv_runtime_self_test", f"not executed because {reason}")
        ]
    else:
        portable_command, render_command = _runtime_commands(
            normalized, selected, worker_contract
        )
        portable.append(
            _probe_command(
                "portable_runtime_self_test",
                portable_command,
                timeout=60.0,
                expected_pattern=r"(?m)^PORTABLE_RUNTIME_SELF_TEST=PASS$",
                expected_description="the complete pinned portable runtime self-test marker",
            )
        )
        render = [
            _probe_command(
                "igv_runtime_self_test",
                render_command,
                timeout=60.0,
                expected_pattern=r"(?m)^IGV_RUNTIME_OK version=2\.16\.2 java=11 heap=6g$",
                expected_description="IGV 2.16.2 on JRE 11 with a 6g heap",
            )
        ]

    if normalized == "standalone":
        site = [
            {
                "name": "nextflow_trace",
                "required": True,
                "status": "PASS",
                "detail": "local accounting provider; each run later reconciles exact trace lineage",
            }
        ]
    elif normalized == "docker":
        site = [
            _probe_command("docker", ["docker", "version", "--format", "{{.Server.Version}}"]),
            {
                "name": "nextflow_trace",
                "required": True,
                "status": "PASS",
                "detail": "host-controller accounting provider; each run later reconciles exact trace lineage",
            },
        ]
    else:
        sif_runner = shutil.which("apptainer") or shutil.which("singularity") or "apptainer"
        site = [
            _probe_command("apptainer_or_singularity", [sif_runner, "--version"]),
            _probe_command("qsub", ["qsub", "-help"]),
            _probe_command("qacct", ["qacct", "-help"], required=False),
            _shared_filesystem_flock_probe(run_dir),
        ]

    classes = {
        "controller": controller,
        "portable_runtime": portable,
        "render_runtime": render,
        "site_accounting": site,
    }
    failed = [
        f"{class_name}:{check['name']}"
        for class_name, checks in classes.items()
        for check in checks
        if check.get("required") and check.get("status") != "PASS"
    ]
    return {
        "schema_version": "3.0",
        "profile": normalized,
        "observed_at": utc_now(),
        "status": "PASS" if not failed else "FAIL",
        "failed_checks": failed,
        "selected_runtime": str(selected) if selected is not None else None,
        "docker_worker_identity": worker_contract,
        "runtime_manifest_sha256": manifest_check.get("runtime_manifest_sha256"),
        "runtime_fingerprint_sha256": manifest_check.get(
            "runtime_fingerprint_sha256"
        ),
        "snapshot_runtime_ready": not failed,
        "probe_classes": classes,
    }
