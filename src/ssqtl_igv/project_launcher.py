from __future__ import annotations

import csv
import json
import os
import re
import subprocess
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
)


_CASE_PROCESS = re.compile(r"(?:^|:)RUN_PORTABLE_CASE\s*\(([^()]*)\)$")
_FINAL_TRACE_STATES = {"COMPLETED", "CACHED"}
_RETRY_TRACE_EXITS = {"75", "137", "143"}


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
    igv_cpus: int = 1,
    igv_memory: str = "8GiB",
    igv_timeout: str = "30m",
    normalization_cpus: int = 1,
    normalization_memory: str = "12GiB",
    normalization_timeout: str = "36h",
    nextflow: str | None = None,
    profile: str = "standalone",
) -> tuple[list[str], Path, Path]:
    if (project is None) == (batch_request is None):
        raise ValueError("exactly one of project or batch_request is required")
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

    source_flag: str
    source_path: Path
    if project is not None:
        source_flag = "--project"
        source_path = _regular_input(project, label="project.yaml")
    else:
        source_flag = "--batch_request"
        source_path = _regular_input(batch_request, label="batch-request")

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
        source_flag,
        str(source_path),
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


def validate_project_postflight(output: str | Path) -> dict[str, Any]:
    root = Path(output).expanduser()
    if root.is_symlink() or not root.resolve(strict=True).is_dir():
        raise ValueError(f"completed output must be a regular non-symlink directory: {root}")
    root = root.resolve(strict=True)
    tasks_path = root / "contract" / "tasks.jsonl"
    if tasks_path.is_symlink() or not tasks_path.is_file():
        raise ValueError("completed run is missing its canonical task set")
    tasks = list(read_jsonl(tasks_path))
    task_ids = [str(task.get("task_id", "")) for task in tasks]
    if not task_ids or any(not task_id for task_id in task_ids):
        raise ValueError("canonical task set is empty or contains a missing task_id")
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("canonical task set contains duplicate task IDs")

    failed: list[str] = []
    bundle_digests: list[dict[str, str]] = []
    for task_id in task_ids:
        case_root = root / "results" / "cases" / task_id
        case_path = case_root / "case_result.json"
        bundle_path = case_root / "terminal_bundle.json"
        case_document = _read_json_object(case_path, label=f"case result {task_id}")
        bundle_document = _read_json_object(
            bundle_path, label=f"terminal bundle {task_id}"
        )
        validate_v3_terminal_bundle_document(bundle_document, case_document)
        if str(case_document.get("task_id")) != task_id:
            raise ValueError(f"case result task_id differs from canonical task {task_id}")
        if not bool(case_document.get("eligible")):
            failed.append(task_id)
        bundle_digests.append(
            {"task_id": task_id, "sha256": sha256_file(bundle_path)}
        )

    trace_path = root / "reports" / "trace.txt"
    lineage = _case_trace_lineage(trace_path, set(task_ids))
    summary_path = root / "run_summary.json"
    summary = _read_json_object(summary_path, label="run summary")
    if summary.get("authoritative") is not False:
        raise ValueError("run_summary.json must be marked authoritative:false")
    expected_status = "CASE_FAILURES" if failed else "SNAPSHOTS_READY"
    expected_exit = 2 if failed else 0
    if summary.get("status") != expected_status:
        raise ValueError(
            f"run summary status {summary.get('status')!r} differs from {expected_status}"
        )
    if int(summary.get("exit_code", -1)) != expected_exit:
        raise ValueError("run summary product exit code differs from terminal case evidence")
    if int(summary.get("expected_case_count", -1)) != len(task_ids):
        raise ValueError("run summary expected case count differs from canonical tasks")
    if int(summary.get("observed_case_count", -1)) != len(task_ids):
        raise ValueError("run summary observed case count differs from terminal bundles")

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
    reports.mkdir(parents=True, exist_ok=True)
    atomic_write_json(reports / "postflight.json", postflight)
    return {**summary, "postflight": postflight}


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
    igv_cpus: int = 1,
    igv_memory: str = "8GiB",
    igv_timeout: str = "30m",
    normalization_cpus: int = 1,
    normalization_memory: str = "12GiB",
    normalization_timeout: str = "36h",
    nextflow: str | None = None,
    profile: str = "standalone",
    environment: Mapping[str, str] | None = None,
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
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        env=launch_environment,
        cwd=output_path,
    )
    if completed.returncode != 0:
        summary = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "authoritative": False,
            "status": "INFRASTRUCTURE_FATAL",
            "exit_code": 1,
            "nextflow_exit_code": completed.returncode,
        }
        try:
            atomic_write_json(output_path / "run_summary.json", summary)
        except OSError:
            pass
        return summary, 1
    try:
        result = validate_project_postflight(output_path)
    except (OSError, ValueError, RuntimeError) as exc:
        summary = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "authoritative": False,
            "status": "INFRASTRUCTURE_FATAL",
            "exit_code": 1,
            "nextflow_exit_code": 0,
            "reason": f"postflight validation failed: {type(exc).__name__}: {exc}",
        }
        atomic_write_json(output_path / "run_summary.json", summary)
        return summary, 1
    return result, int(result["exit_code"])
