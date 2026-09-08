from __future__ import annotations

import csv
import fcntl
import hashlib
import json
import os
import re
import subprocess
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping

from .contracts import validate_v3_terminal_bundle_document
from .controller_runtime_v3 import normalized_nextflow_environment
from .orchestrator_v3 import _nextflow_executable, _project_root
from .utils import (
    atomic_write_json,
    read_jsonl,
    reject_symlink_path_components,
    sha256_file,
    utc_now,
)


_CASE_PROCESS = re.compile(r"(?:^|:)RUN_PORTABLE_CASE\s*\(([^()]*)\)$")
_FINAL_TRACE_STATES = {"COMPLETED", "CACHED"}
_RETRY_TRACE_EXITS = {"75", "137", "143"}


@contextmanager
def exclusive_run_output(output: Path):
    """One controller/reconciler owns a run; a second caller never waits blindly."""
    reports = reject_symlink_path_components(output / "reports", label="reports")
    reports.mkdir(parents=True, exist_ok=True)
    lock_path = reports / "controller.lock"
    if lock_path.is_symlink():
        raise ValueError("controller lock must not be a symlink")
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another controller owns this run output") from exc
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


@contextmanager
def observed_run_output(output: Path):
    """Observe a completed producer without writing to its retained workspace."""
    lock_path = reject_symlink_path_components(output / "reports/controller.lock", label="producer lock")
    if not lock_path.exists():
        yield  # Legacy producers have no lease; their retained evidence is checked.
        return
    with lock_path.open("r") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("batch producer is still active") from exc
        try:
            yield
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _summary_projection(root: Path, derived: dict[str, Any], *, view: Path | None = None, write: bool = True) -> dict[str, Any]:
    """Retain old observations, then rebuild the UX projection from evidence."""
    path = (view or root) / "run_summary.json"
    if path.is_symlink():
        raise ValueError("run summary must not be a symlink")
    previous: dict[str, Any] = {}
    raw = path.read_bytes() if path.is_file() else None
    if raw is not None:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                previous = parsed
        except (ValueError, UnicodeDecodeError):
            pass
    projection = {**previous, **derived}
    if isinstance(previous.get("source_digests"), dict):
        projection["source_digests"] = {**previous["source_digests"], **derived.get("source_digests", {})}
    projection.pop("reason", None)
    projection.pop("nextflow_exit_code", None)
    if not write or (view is not None and view != root):
        return projection
    if raw is not None and previous != projection:
        history = root / "reports" / "projection-history"
        history.mkdir(parents=True, exist_ok=True)
        archived = history / (hashlib.sha256(raw).hexdigest() + ".json")
        if not archived.exists():
            with archived.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
    if raw is None or previous != projection:
        atomic_write_json(path, projection)
    return projection


def _regular_input(path: str | Path, *, label: str) -> Path:
    value = Path(path).expanduser()
    if value.is_symlink():
        raise ValueError(f"{label} must not be a symlink: {value}")
    resolved = value.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"{label} must be a regular file: {resolved}")
    return resolved


def _output_directory(path: str | Path) -> Path:
    output = reject_symlink_path_components(path, label="output directory").resolve(
        strict=False
    )
    if output.exists() and not output.is_dir():
        raise ValueError(f"output path is not a directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    return output


def build_project_run_command(
    *,
    project: str | Path | None,
    batch_request: str | Path | None,
    output: str | Path,
    work: str | Path | None,
    resume: bool,
    max_parallel: str | int,
    max_cases_per_shard: int,
    runtime_manifest: str | Path,
    rerun_source_run: str | Path | None = None,
    rerun_receipt: str | Path | None = None,
    run_id: str | None = None,
    generation_id: str | None = None,
    igv_cpus: int = 1,
    igv_memory: str = "8GiB",
    igv_timeout: str = "30m",
    normalization_cpus: int = 1,
    normalization_memory: str = "12GiB",
    normalization_timeout: str = "36h",
    nextflow: str | None = None,
    profile: str = "standalone",
) -> tuple[list[str], Path, Path]:
    if (rerun_source_run is None) != (rerun_receipt is None):
        raise ValueError(
            "rerun_source_run and rerun_receipt must be supplied together"
        )
    entry_count = sum(
        (
            project is not None,
            batch_request is not None,
            rerun_source_run is not None,
        )
    )
    if entry_count != 1:
        raise ValueError(
            "exactly one of project, batch_request, or rerun source is required"
        )
    if not 1 <= int(max_cases_per_shard) <= 256:
        raise ValueError("max-cases-per-shard must be between 1 and 256")
    if not isinstance(igv_cpus, int) or isinstance(igv_cpus, bool) or igv_cpus < 1:
        raise ValueError("igv-cpus must be a positive integer")
    if (
        not isinstance(normalization_cpus, int)
        or isinstance(normalization_cpus, bool)
        or normalization_cpus < 1
    ):
        raise ValueError("normalization-cpus must be a positive integer")

    source_args: list[str]
    if rerun_source_run is not None and rerun_receipt is not None:
        source_value = Path(rerun_source_run).expanduser()
        if source_value.is_symlink() or not source_value.resolve(strict=True).is_dir():
            raise ValueError("rerun source run must be a regular directory")
        if not run_id or not generation_id:
            raise ValueError("rerun launch requires run_id and generation_id")
        source_args = [
            "--rerun_source_run",
            str(source_value.resolve(strict=True)),
            "--rerun_receipt",
            str(_regular_input(rerun_receipt, label="rerun receipt")),
            "--run_id",
            str(run_id),
            "--generation_id",
            str(generation_id),
        ]
    elif project is not None:
        source_args = ["--project", str(_regular_input(project, label="project.yaml"))]
    else:
        source_args = [
            "--batch_request",
            str(_regular_input(batch_request, label="batch-request")),
        ]

    output_path = _output_directory(output)
    work_value = work if work is not None else output_path / ".work"
    work_path = reject_symlink_path_components(
        work_value, label="work directory"
    ).resolve(strict=False)
    if work_path.exists() and not work_path.is_dir():
        raise ValueError(f"work path is not a directory: {work_path}")
    work_path.mkdir(parents=True, exist_ok=True)
    runtime_path = _regular_input(runtime_manifest, label="embedded runtime manifest")

    command = [
        _nextflow_executable(nextflow),
        "run",
        str(_project_root()),
        "-profile",
        profile,
        "-work-dir",
        str(work_path),
        *source_args,
        "--output",
        str(output_path),
        "--session_output",
        str(output_path / "reports"),
        "--max_parallel",
        str(max_parallel),
        "--max_cases_per_shard",
        str(max_cases_per_shard),
        "--igv_cpus",
        str(igv_cpus),
        "--igv_memory",
        str(igv_memory),
        "--igv_timeout",
        str(igv_timeout),
        "--normalization_cpus",
        str(normalization_cpus),
        "--normalization_memory",
        str(normalization_memory),
        "--normalization_timeout",
        str(normalization_timeout),
        "--runtime_manifest",
        str(runtime_path),
        "--enable_reports",
        "true",
    ]
    if resume:
        command.append("-resume")
    return command, output_path, work_path


def _read_json_object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def _case_trace_lineage(
    trace_path: Path, expected_task_ids: set[str]
) -> dict[str, list[dict[str, str]]]:
    try:
        handle = trace_path.open(encoding="utf-8", newline="")
    except OSError as exc:
        raise ValueError(f"cannot read complete Nextflow trace {trace_path}: {exc}") from exc
    with handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    if not rows:
        raise ValueError("Nextflow trace contains no task rows")
    lineage: dict[str, list[dict[str, str]]] = {
        task_id: [] for task_id in expected_task_ids
    }
    for row in rows:
        # ``name`` contains the task tag while ``process`` is only the process
        # identifier on current Nextflow releases.
        process = str(row.get("name") or row.get("process") or "")
        match = _CASE_PROCESS.search(process)
        if not match:
            continue
        task_id = match.group(1)
        if task_id not in expected_task_ids:
            raise ValueError(f"Nextflow trace contains an unknown case task: {task_id}")
        lineage[task_id].append({str(key): str(value or "") for key, value in row.items()})
    for task_id, attempts in lineage.items():
        if not attempts:
            raise ValueError(f"Nextflow trace has no render lineage for case {task_id}")
        if len(attempts) > 3:
            raise ValueError(f"case {task_id} exceeds the fixed three-attempt ladder")
        terminal = [
            row for row in attempts if str(row.get("status", "")).upper() in _FINAL_TRACE_STATES
        ]
        if len(terminal) != 1:
            raise ValueError(
                f"case {task_id} must have exactly one completed/cached trace row; "
                f"observed {len(terminal)}"
            )
        if attempts[-1] is not terminal[0]:
            raise ValueError(f"case {task_id} terminal trace row is not the final attempt")
        for row in attempts[:-1]:
            if (
                str(row.get("status", "")).upper() != "FAILED"
                or str(row.get("exit", "")).strip() not in _RETRY_TRACE_EXITS
            ):
                raise ValueError(
                    f"case {task_id} contains a non-resource retry attempt"
                )
        if str(terminal[0].get("exit", "")).strip() not in {"", "0"}:
            raise ValueError(f"case {task_id} final trace row has a nonzero exit")
        observed_attempts = [str(row.get("attempt", "")).strip() for row in attempts]
        if len(attempts) > 1 and observed_attempts != [
            str(index) for index in range(1, len(attempts) + 1)
        ]:
            raise ValueError(f"case {task_id} trace attempt numbers are not contiguous")
    return lineage


def validate_project_postflight(output: str | Path, *, repair_projection: bool = True) -> dict[str, Any]:
    from .snapshot_store import snapshot_view
    root = Path(output).expanduser()
    if root.is_symlink() or not root.resolve(strict=True).is_dir():
        raise ValueError(f"completed output must be a regular non-symlink directory: {root}")
    root = root.resolve(strict=True)
    view = snapshot_view(root)
    contract_root = root / "contract"
    case_root_parent = root / "results" / "cases"
    direct_product = False
    if not contract_root.is_dir():
        contract_root = root / ".igv-pipeline" / "contract"
        case_root_parent = root / ".igv-pipeline" / "cases"
        direct_product = True
    if view != root and (view / ".igv-pipeline/cases").is_dir():
        case_root_parent = view / ".igv-pipeline/cases"
    tasks_path = contract_root / "tasks.jsonl"
    if tasks_path.is_symlink() or not tasks_path.is_file():
        raise ValueError("completed run is missing its canonical task set")
    tasks = list(read_jsonl(tasks_path))
    task_ids = [str(task.get("task_id", "")) for task in tasks]
    if not task_ids or any(
        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,250}", task_id) is None
        for task_id in task_ids
    ):
        raise ValueError("canonical task set is empty or contains a missing task_id")
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("canonical task set contains duplicate task IDs")

    failed: list[str] = []
    bundle_digests: list[dict[str, str]] = []
    for task, task_id in zip(tasks, task_ids):
        case_root = case_root_parent / task_id
        case_path = case_root / "case_result.json"
        bundle_path = case_root / "terminal_bundle.json"
        case_document = _read_json_object(case_path, label=f"case result {task_id}")
        bundle_document = _read_json_object(
            bundle_path, label=f"terminal bundle {task_id}"
        )
        validate_v3_terminal_bundle_document(bundle_document, case_document)
        if str(case_document.get("task_id")) != task_id:
            raise ValueError(f"case result task_id differs from canonical task {task_id}")
        for field in ("input_fingerprint", "run_id", "generation_id", "manifest_order"):
            if field in task and case_document.get(field) != task[field]:
                raise ValueError(f"case result {field} differs from canonical task {task_id}")
        if not bool(case_document.get("eligible")):
            failed.append(task_id)
        bundle_digests.append(
            {"task_id": task_id, "sha256": sha256_file(bundle_path)}
        )

    if direct_product:
        with (view / "snapshots.tsv").open(encoding="utf-8", newline="") as handle:
            snapshot_rows = list(csv.DictReader(handle, delimiter="\t"))
        snapshot_fields = list(snapshot_rows[0].keys()) if snapshot_rows else []
        if snapshot_fields != [
            "manifest_order",
            "task_id",
            "chromosome",
            "relative_path",
            "sha256",
            "status",
        ]:
            raise ValueError("snapshots.tsv uses an unexpected field contract")
        if [row["task_id"] for row in snapshot_rows] != task_ids:
            raise ValueError("snapshots.tsv task order differs from canonical tasks")
        for row in snapshot_rows:
            task_id = row["task_id"]
            result = _read_json_object(
                case_root_parent / task_id / "case_result.json",
                label=f"case result {task_id}",
            )
            if bool(result.get("eligible")):
                if row["status"] != "SNAPSHOT_READY":
                    raise ValueError(f"eligible case lacks SNAPSHOT_READY: {task_id}")
                relative = Path(row["relative_path"])
                if relative.is_absolute() or ".." in relative.parts:
                    raise ValueError(f"snapshot path is unsafe: {task_id}")
                snapshot = view / relative
                if snapshot.is_symlink() or not snapshot.is_file():
                    raise ValueError(f"snapshot is unavailable: {task_id}")
                expected_sha = str(
                    result.get("artifacts", {})
                    .get("review_image", {})
                    .get("sha256", "")
                )
                if row["sha256"] != expected_sha or sha256_file(snapshot) != expected_sha:
                    raise ValueError(f"snapshot checksum differs from terminal evidence: {task_id}")
            elif row["status"] != "CASE_FAILED" or row["relative_path"] or row["sha256"]:
                raise ValueError(f"failed case exposes a snapshot: {task_id}")

    trace_path = root / "reports" / "trace.txt"
    lineage = _case_trace_lineage(trace_path, set(task_ids))
    expected_status = "CASE_FAILURES" if failed else "SNAPSHOTS_READY"
    expected_exit = 2 if failed else 0

    postflight = {
        "schema_version": "3.0-project-postflight",
        "authoritative": False,
        "status": "PASS",
        "product_status": expected_status,
        "product_exit_code": expected_exit,
        "task_count": len(task_ids),
        "failed_task_ids": failed,
        "canonical_tasks_sha256": sha256_file(tasks_path),
        "trace_sha256": sha256_file(trace_path),
        "terminal_bundles": bundle_digests,
        "trace_attempt_counts": {
            task_id: len(attempts) for task_id, attempts in sorted(lineage.items())
        },
    }
    reports = root / "reports"
    if view != root:
        postflight["publication_generation"] = view.name
    if repair_projection:
        reports.mkdir(parents=True, exist_ok=True)
        atomic_write_json(reports / "postflight.json", postflight)
    digests = {"canonical_tasks": sha256_file(tasks_path)}
    for name in ("snapshots", "failed_cases"):
        path = view / (name + ".tsv")
        if path.is_file():
            digests[name] = sha256_file(path)
    summary = _summary_projection(root, {
        "schema_version": "3.0", "pipeline_version": "3.0.0",
        "authoritative": False, "projection_kind": "UX_ONLY",
        "status": expected_status, "exit_code": expected_exit,
        "expected_case_count": len(task_ids), "observed_case_count": len(task_ids),
        "failed_case_count": len(failed), "rerun_required": bool(failed), "source_digests": digests,
    }, view=view, write=repair_projection)
    return {**summary, "postflight": postflight}


def reconcile_project_output(output: str | Path) -> dict[str, Any]:
    """Reconcile retained terminal evidence without invoking Nextflow or IGV."""
    root = reject_symlink_path_components(output, label="run output").resolve(strict=True)
    with exclusive_run_output(root):
        return validate_project_postflight(root)


def run_project_workflow(
    *,
    project: str | Path | None,
    batch_request: str | Path | None,
    output: str | Path,
    work: str | Path | None,
    resume: bool,
    max_parallel: str | int,
    max_cases_per_shard: int,
    runtime_manifest: str | Path,
    rerun_source_run: str | Path | None = None,
    rerun_receipt: str | Path | None = None,
    run_id: str | None = None,
    generation_id: str | None = None,
    igv_cpus: int = 1,
    igv_memory: str = "8GiB",
    igv_timeout: str = "30m",
    normalization_cpus: int = 1,
    normalization_memory: str = "12GiB",
    normalization_timeout: str = "36h",
    nextflow: str | None = None,
    profile: str = "standalone",
    environment: Mapping[str, str] | None = None,
    persist_fatal_summary: bool = True,
) -> tuple[dict[str, Any], int]:
    command, output_path, _work_path = build_project_run_command(
        project=project,
        batch_request=batch_request,
        output=output,
        work=work,
        resume=resume,
        max_parallel=max_parallel,
        max_cases_per_shard=max_cases_per_shard,
        runtime_manifest=runtime_manifest,
        rerun_source_run=rerun_source_run,
        rerun_receipt=rerun_receipt,
        run_id=run_id,
        generation_id=generation_id,
        igv_cpus=igv_cpus,
        igv_memory=igv_memory,
        igv_timeout=igv_timeout,
        normalization_cpus=normalization_cpus,
        normalization_memory=normalization_memory,
        normalization_timeout=normalization_timeout,
        nextflow=nextflow,
        profile=profile,
    )
    launch_environment = {
        **os.environ,
        **dict(environment or {}),
        "NXF_ANSI_LOG": "false",
    }
    java_home = launch_environment.get("NXF_JAVA_HOME")
    if java_home:
        java = Path(java_home) / "bin" / "java"
        if java.is_file():
            launch_environment = normalized_nextflow_environment(
                java, base=launch_environment
            )
    # ``persist_fatal_summary`` is retained for API compatibility. Failures are
    # always attempt observations now; they must never overwrite product state.
    with exclusive_run_output(output_path):
        attempt = output_path / "reports" / "attempts" / uuid.uuid4().hex
        attempt.mkdir(parents=True)
        atomic_write_json(attempt / "start.json", {
            "schema_version": "3.0-controller-attempt", "started_at": utc_now(),
            "resume": resume, "command": command,
        })
        nextflow_exit = None
        try:
            completed = subprocess.run(
                command, check=False, text=True, env=launch_environment, cwd=output_path,
            )
            nextflow_exit = completed.returncode
            if nextflow_exit:
                raise RuntimeError(f"Nextflow exited with status {nextflow_exit}")
            result = validate_project_postflight(output_path)
        except (OSError, ValueError, RuntimeError) as exc:
            result = {
                "schema_version": "3.0", "pipeline_version": "3.0.0",
                "authoritative": False, "status": "INFRASTRUCTURE_FATAL", "exit_code": 1,
                "nextflow_exit_code": nextflow_exit,
                "reason": f"{type(exc).__name__}: {exc}",
            }
        receipt = {"schema_version": "3.0-controller-attempt", "ended_at": utc_now(),
                   "nextflow_exit_code": nextflow_exit, "product_status": result["status"],
                   "product_exit_code": int(result["exit_code"])}
        if "reason" in result:
            receipt["reason"] = result["reason"]
        atomic_write_json(attempt / "terminal.json", receipt)
        return result, int(result["exit_code"])
