from __future__ import annotations

import csv
import fcntl
import json
import shlex
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .contracts import DEFAULT_MAX_TASKS_PER_ARRAY, SCHEDULER_THROTTLE_CONTRACT_ID
from .preflight import run_preflight
from .runner import assert_manifest_config, load_cases, run_shard
from .state import FAILED, PENDING, PREPARED, RERUN, REVIEW_PENDING, PUBLISHED, CaseState
from .utils import (
    atomic_write_json,
    atomic_write_text,
    command_prefix,
    optional_text,
    safe_name,
    sha256_file,
    sha256_json,
    utc_now,
    write_tsv,
)
from .validation_lineage import bounded_contiguous_task_ranges, parse_task_range



def _q(value: str | Path) -> str:
    return shlex.quote(str(value))


def _module_lines(config: WorkflowConfig) -> list[str]:
    lines = ["module purge"] if bool(config.get("environment.module_purge", True)) else []
    if config.modules:
        lines.append("module load " + " ".join(_q(module) for module in config.modules))
    venv = config.get("environment.venv")
    if venv:
        lines.append(f"source {_q(Path(venv) / 'bin' / 'activate')}")
    return lines


@contextmanager
def _submission_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another submission process holds {path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _submission_capacity(config: WorkflowConfig, **_: Any) -> dict[str, Any]:
    max_parallel = int(config.get("scheduler.max_parallel", 1))
    return {
        "cases_per_task": int(config.get("scheduler.cases_per_task", 1)),
        "max_parallel": max_parallel,
        "requested_memory_gb": int(config.get("scheduler.memory_gb", 1)),
        "total_parallel_memory_gb": int(config.get("scheduler.total_parallel_memory_gb", max_parallel)),
    }


def _write_chunk_map(
    scheduler_root: Path,
    cases: list[dict[str, Any]],
    cases_per_task: int,
) -> tuple[Path, list[dict[str, Any]]]:
    if cases_per_task < 1:
        raise ValueError("cases_per_task must be positive")
    by_shard: dict[str, list[dict[str, Any]]] = {}
    for case in sorted(cases, key=lambda item: (str(item["shard"]), int(item["association_row"]), item["case_id"])):
        by_shard.setdefault(str(case["shard"]), []).append(case)
    lists_root = scheduler_root / "case_lists"
    lists_root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    task_id = 1
    for shard in sorted(by_shard):
        shard_cases = by_shard[shard]
        for offset in range(0, len(shard_cases), cases_per_task):
            chunk_cases = shard_cases[offset : offset + cases_per_task]
            chunk_index = offset // cases_per_task + 1
            chunk = f"{shard}__chunk{chunk_index:04d}"
            case_list = lists_root / f"{chunk}.tsv"
            write_tsv(
                case_list,
                ["case_id"],
                ({"case_id": case["case_id"]} for case in chunk_cases),
            )
            rows.append(
                {
                    "task_id": task_id,
                    "chunk": chunk,
                    "report_id": f"{scheduler_root.name}__{chunk}",
                    "shard": shard,
                    "case_count": len(chunk_cases),
                    "case_list": str(case_list),
                }
            )
            task_id += 1
    chunk_map = scheduler_root / "chunks.tsv"
    write_tsv(
        chunk_map,
        ["task_id", "chunk", "report_id", "shard", "case_count", "case_list"],
        rows,
    )
    return chunk_map, rows


def _directives(
    config: WorkflowConfig,
    name: str,
    logs: Path,
    *,
    memory_gb: int | None = None,
) -> list[str]:
    values = ["#!/bin/bash -l", f"#$ -N {name}", f"#$ -o {logs}", f"#$ -e {logs}"]
    project = optional_text(config.get("scheduler.project"))
    if project:
        values.append(f"#$ -P {project}")
    runtime = config.get("scheduler.runtime", "12:00:00")
    memory = f"{memory_gb}G" if memory_gb is not None else config.get("scheduler.memory_per_core", "8G")
    values.extend([f"#$ -l h_rt={runtime}", f"#$ -l mem_per_core={memory}"])
    return values


def _remaining_case_count(run_root: Path, cases: list[dict[str, Any]]) -> int:
    completed = 0
    for case in cases:
        state_path = run_root / ".work" / "state" / f"{safe_name(case['case_id'])}.json"
        if state_path.is_file():
            state = CaseState.load(state_path)
            if state.status in {REVIEW_PENDING, PUBLISHED}:
                completed += 1
    return len(cases) - completed


def _incomplete_cases(run_root: Path, cases: list[dict[str, Any]]) -> list[dict[str, Any]]:
    incomplete = []
    for case in cases:
        state_path = run_root / ".work" / "state" / f"{safe_name(case['case_id'])}.json"
        if state_path.is_file() and CaseState.load(state_path).status in {REVIEW_PENDING, PUBLISHED}:
            continue
        incomplete.append(case)
    return incomplete


def _require_preflight(
    config: WorkflowConfig,
    run_root: Path,
    manifest: Path,
    *,
    remaining_cases: int,
) -> dict[str, Any]:
    result = run_preflight(config, run_root=run_root, manifest=manifest)
    if result["status"] not in {"PASS", "PASS_WITH_CASE_FAILURES"}:
        raise ValueError("preflight did not pass")
    if int(result.get("remaining_cases", -1)) != int(remaining_cases):
        raise ValueError("preflight remaining-case identity changed before submission")
    return result


def _hard_throttle_array_ranges(
    task_count: int,
    max_tasks_per_array: int = DEFAULT_MAX_TASKS_PER_ARRAY,
) -> list[str]:
    if int(task_count) < 1 or int(max_tasks_per_array) < 1:
        raise ValueError("scheduler task count and array limit must be positive")
    ranges = bounded_contiguous_task_ranges(
        list(range(1, int(task_count) + 1)),
        int(max_tasks_per_array),
    )
    covered = [task_id for value in ranges for task_id in parse_task_range(value)]
    if covered != list(range(1, int(task_count) + 1)):
        raise ValueError("hard-throttle array ranges do not exactly cover scheduler tasks")
    if any(len(parse_task_range(value)) > int(max_tasks_per_array) for value in ranges):
        raise ValueError("hard-throttle array range exceeds the configured task limit")
    return ranges


def _validate_submitted_array_chain(
    plan: dict[str, Any],
    array_jobs: list[dict[str, Any]],
    *,
    require_complete: bool,
) -> None:
    expected_ranges = list(plan.get("array_ranges", []))
    if len(array_jobs) > len(expected_ranges):
        raise ValueError("submitted array chain is longer than the hard-throttle plan")
    job_ids: set[str] = set()
    previous_job_id = ""
    for index, row in enumerate(array_jobs):
        job_id = str(row.get("job_id", ""))
        array_range = str(row.get("array_range", ""))
        task_ids = parse_task_range(array_range)
        if (
            not job_id.isdigit()
            or job_id in job_ids
            or int(row.get("sequence", 0)) != index + 1
            or array_range != expected_ranges[index]
            or row.get("task_ids") != task_ids
            or int(row.get("task_count", 0)) != len(task_ids)
            or len(task_ids) > int(plan.get("hard_max_tasks_per_array", 0))
            or str(row.get("hold_jid", "")) != previous_job_id
        ):
            raise ValueError("submitted scheduler array chain differs from the hard-throttle plan")
        job_ids.add(job_id)
        previous_job_id = job_id
    if require_complete and len(array_jobs) != len(expected_ranges):
        raise ValueError("submitted scheduler array chain is incomplete")


def _scheduler_plan_identity(plan: dict[str, Any]) -> str:
    identity_keys = (
        "schema_version",
        "run_root",
        "manifest_sha256",
        "config_fingerprint",
        "associations_sha256",
        "submission_generation",
        "scheduler_task_count",
        "array_range",
        "array_range_role",
        "array_ranges",
        "array_job_count",
        "max_batch_task_count",
        "throttle_contract_id",
        "hard_max_tasks_per_array",
        "scheduler_tc_requested",
        "scheduler_tc_role",
        "serial_hold_chain_required",
        "max_parallel",
        "requested_memory_gb",
        "expected_owner",
        "expected_job_name",
        "expected_project",
        "chunk_map",
        "chunk_map_sha256",
        "shard_map",
        "shard_map_sha256",
        "rerun_manifest",
        "rerun_manifest_sha256",
        "runner_script",
        "runner_script_sha256",
        "summary_script",
        "summary_script_sha256",
    )
    return sha256_json({key: plan.get(key) for key in identity_keys})


def _submit_plan(
    config: WorkflowConfig,
    scheduler_root: Path,
    plan: dict[str, Any],
    runner_script: Path,
    summary_script: Path,
    *,
    execute: bool,
) -> dict[str, Any]:
    jobs_path = scheduler_root / "jobs.json"
    planned_ranges = list(plan.get("array_ranges", []))
    planned_counts = [len(parse_task_range(value)) for value in planned_ranges]
    planned_tasks = [task_id for value in planned_ranges for task_id in parse_task_range(value)]
    scheduler_task_count = int(plan.get("scheduler_task_count", 0))
    if (
        plan.get("schema_version") != "portable-grid-engine-scheduler-plan-v1"
        or plan.get("throttle_contract_id") != SCHEDULER_THROTTLE_CONTRACT_ID
        or int(plan.get("hard_max_tasks_per_array", 0)) < 1
        or plan.get("scheduler_tc_role") != "defense_in_depth_only"
        or plan.get("serial_hold_chain_required") is not True
        or int(plan.get("scheduler_tc_requested", 0)) != int(plan.get("max_parallel", 0))
        or int(plan.get("array_job_count", 0)) != len(planned_ranges)
        or int(plan.get("max_batch_task_count", 0)) != max(planned_counts, default=0)
        or not planned_counts
        or any(count < 1 or count > int(plan.get("hard_max_tasks_per_array", 0)) for count in planned_counts)
        or scheduler_task_count < 1
        or planned_tasks != list(range(1, scheduler_task_count + 1))
        or parse_task_range(str(plan.get("array_range", ""))) != planned_tasks
        or plan.get("array_range_role") != "logical_task_coverage_only"
        or plan.get("runner_script") != str(runner_script)
        or plan.get("summary_script") != str(summary_script)
        or not runner_script.is_file()
        or not summary_script.is_file()
        or plan.get("runner_script_sha256") != sha256_file(runner_script)
        or plan.get("summary_script_sha256") != sha256_file(summary_script)
        or plan.get("plan_identity_sha256") != _scheduler_plan_identity(plan)
    ):
        raise ValueError("scheduler plan lacks a valid configured throttle contract")

    def reconcile_existing() -> dict[str, Any] | None:
        if not jobs_path.is_file():
            return None
        existing = json.loads(jobs_path.read_text(encoding="utf-8"))
        identity_keys = (
            "schema_version",
            "plan_identity_sha256",
            "run_root",
            "manifest_sha256",
            "config_fingerprint",
            "associations_sha256",
            "submission_generation",
            "scheduler_task_count",
            "array_range",
            "array_range_role",
            "array_ranges",
            "array_job_count",
            "max_batch_task_count",
            "throttle_contract_id",
            "hard_max_tasks_per_array",
            "scheduler_tc_requested",
            "scheduler_tc_role",
            "serial_hold_chain_required",
            "max_parallel",
            "requested_memory_gb",
            "expected_owner",
            "expected_job_name",
            "expected_project",
            "runner_script",
            "runner_script_sha256",
            "summary_script",
            "summary_script_sha256",
        )
        mismatched_identity = [
            key for key in identity_keys if existing.get(key) != plan.get(key)
        ]
        if mismatched_identity:
            raise ValueError("existing scheduler record belongs to different inputs")
        array_jobs = list(existing.get("array_jobs", []))
        if existing.get("array_job_id") and not array_jobs:
            raise RuntimeError(
                "legacy single-array submission cannot satisfy the configured bounded-array contract"
            )
        if array_jobs:
            _validate_submitted_array_chain(plan, array_jobs, require_complete=False)
        complete_chain = (
            bool(array_jobs)
            and len(array_jobs) == len(plan["array_ranges"])
            and existing.get("summary_job_id")
        )
        if complete_chain:
            _validate_submitted_array_chain(plan, array_jobs, require_complete=True)
            if (
                existing.get("first_array_job_id") != array_jobs[0]["job_id"]
                or existing.get("last_array_job_id") != array_jobs[-1]["job_id"]
                or existing.get("array_job_id") != array_jobs[-1]["job_id"]
                or existing.get("array_job_ids") != [row["job_id"] for row in array_jobs]
            ):
                raise ValueError("terminal scheduler record has inconsistent array identities")
            return {**existing, "action": "SKIP_ALREADY_SUBMITTED"}
        if existing.get("summary_job_id"):
            raise ValueError("summary job is bound to an incomplete scheduler array chain")
        if existing.get("status") in {"ARRAY_SUBMITTING", "SUMMARY_SUBMITTING"}:
            raise RuntimeError(
                "previous qsub outcome is ambiguous; inspect qstat/qacct and jobs.json before retrying"
            )
        mutable_fields = {
            "status",
            "execute",
            "array_jobs",
            "array_job_ids",
            "first_array_job_id",
            "last_array_job_id",
            "array_job_id",
            "summary_job_id",
            "pending_array_range",
            "pending_hold_jid",
            "array_submit_error",
            "array_submit_unparsed_output",
            "summary_submit_error",
        }
        for key in mutable_fields:
            if key in existing:
                plan[key] = existing[key]
        return None

    if not execute:
        already_submitted = reconcile_existing()
        if already_submitted:
            return already_submitted
        if plan.get("array_jobs") or plan.get("array_job_id"):
            return {**plan, "action": "EXISTING_SUBMISSION"}
        atomic_write_json(jobs_path, plan)
        return plan

    timeout = int(config.get("scheduler.qsub_timeout_seconds", 60))
    with _submission_lock(scheduler_root / "submit.lock"):
        already_submitted = reconcile_existing()
        if already_submitted:
            return already_submitted
        prefix = command_prefix(config.get("binaries.qsub"), default="qsub")
        expected_ranges = list(plan.get("array_ranges", []))
        if not expected_ranges:
            raise ValueError("scheduler plan lacks hard-throttled array ranges")
        if any(
            len(parse_task_range(value)) > int(plan.get("hard_max_tasks_per_array", 0))
            for value in expected_ranges
        ):
            raise ValueError(
                "scheduler plan contains an array job larger than the configured "
                f"limit ({plan.get('hard_max_tasks_per_array')})"
            )
        array_jobs = list(plan.get("array_jobs", []))
        _validate_submitted_array_chain(plan, array_jobs, require_complete=False)

        while len(array_jobs) < len(expected_ranges):
            array_range = expected_ranges[len(array_jobs)]
            previous_job_id = str(array_jobs[-1]["job_id"]) if array_jobs else ""
            plan["status"] = "ARRAY_SUBMITTING"
            plan["execute"] = True
            plan["pending_array_range"] = array_range
            plan["pending_hold_jid"] = previous_job_id
            atomic_write_json(jobs_path, plan)
            array_argv = prefix + ["-terse", "-t", array_range]
            if plan.get("max_parallel"):
                array_argv.extend(["-tc", str(plan["max_parallel"])])
            if previous_job_id:
                array_argv.extend(["-hold_jid", previous_job_id])
            array_argv.append(str(runner_script))
            array = subprocess.run(
                array_argv,
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
            if array.returncode != 0:
                plan["status"] = "ARRAY_SUBMIT_FAILED"
                plan["array_submit_error"] = array.stderr.strip()
                atomic_write_json(jobs_path, plan)
                raise RuntimeError(f"array qsub failed: {array.stderr.strip()}")
            array_job = array.stdout.strip().split(".", 1)[0]
            if not array_job.isdigit():
                plan["array_submit_unparsed_output"] = array.stdout.strip()
                atomic_write_json(jobs_path, plan)
                raise RuntimeError(f"cannot parse array job ID: {array.stdout.strip()}")
            array_jobs.append(
                {
                    "sequence": len(array_jobs) + 1,
                    "job_id": array_job,
                    "array_range": array_range,
                    "task_ids": parse_task_range(array_range),
                    "task_count": len(parse_task_range(array_range)),
                    "hold_jid": previous_job_id,
                }
            )
            plan["array_jobs"] = array_jobs
            plan["array_job_ids"] = [row["job_id"] for row in array_jobs]
            plan["first_array_job_id"] = array_jobs[0]["job_id"]
            plan["last_array_job_id"] = array_jobs[-1]["job_id"]
            # Keep the legacy singular key aligned with the job that gates the
            # summary; it is the last job in the strict hold chain.
            plan["array_job_id"] = array_jobs[-1]["job_id"]
            plan.pop("pending_array_range", None)
            plan.pop("pending_hold_jid", None)
            plan["status"] = (
                "ARRAYS_SUBMITTED"
                if len(array_jobs) == len(expected_ranges)
                else "ARRAY_SUBMITTED_PARTIAL"
            )
            atomic_write_json(jobs_path, plan)
        array_job = str(array_jobs[-1]["job_id"])
        _validate_submitted_array_chain(plan, array_jobs, require_complete=True)
        plan["status"] = "SUMMARY_SUBMITTING"
        atomic_write_json(jobs_path, plan)
        summary = subprocess.run(
            prefix + ["-terse", "-hold_jid", array_job, str(summary_script)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
        )
        if summary.returncode != 0:
            plan["status"] = "SUMMARY_SUBMIT_FAILED"
            plan["summary_submit_error"] = summary.stderr.strip()
            atomic_write_json(jobs_path, plan)
            raise RuntimeError(f"summary qsub failed: {summary.stderr.strip()}")
        summary_job_id = summary.stdout.strip().split(".", 1)[0]
        if not summary_job_id.isdigit():
            raise RuntimeError(f"cannot parse summary job ID: {summary.stdout.strip()}")
        plan["summary_job_id"] = summary_job_id
        plan["status"] = "ARRAY_AND_SUMMARY_SUBMITTED"
        atomic_write_json(jobs_path, plan)
    return plan


def create_submission(
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    execute: bool = False,
    generation: int = 1,
) -> dict[str, Any]:
    if optional_text(config.get("execution.mode", "local")).lower() != "grid_engine":
        raise ValueError("scheduler submission requires execution.mode=grid_engine")
    if generation < 1:
        raise ValueError("submission generation must be a positive integer")
    root = config.validate_run_root(run_root, must_exist=True)
    cases = load_cases(root)
    assert_manifest_config(cases, config, root)
    manifest = root / ".work" / "manifests" / "case_manifest.jsonl"
    manifest_sha = sha256_file(manifest)
    remaining_cases = _remaining_case_count(root, cases)
    refreshed_preflight = _require_preflight(
        config, root, manifest, remaining_cases=remaining_cases
    )
    capacity = _submission_capacity(
        config,
        run_root=root,
        run_id=root.name,
        manifest_sha256=manifest_sha,
    )
    incomplete_cases = _incomplete_cases(root, cases)
    if not incomplete_cases:
        return {
            "created_at": utc_now(),
            "case_count": len(cases),
            "submitted_case_count": 0,
            "action": "NOOP_ALL_CASES_COMPLETE",
            "exit_code": 0,
        }
    scheduler_root = root / ".work" / "scheduler" / f"full_g{generation:03d}"
    logs = scheduler_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    chunk_map, chunks = _write_chunk_map(
        scheduler_root,
        incomplete_cases,
        int(capacity["cases_per_task"]),
    )
    if not chunks:
        raise ValueError("chunk map is empty")
    max_tasks_per_array = int(
        config.get("scheduler.max_tasks_per_array", DEFAULT_MAX_TASKS_PER_ARRAY)
    )
    array_ranges = _hard_throttle_array_ranges(len(chunks), max_tasks_per_array)
    config_path = config.path.resolve(strict=False)
    runner_lines = _directives(
        config,
        f"igv_{root.name}",
        logs,
        memory_gb=int(capacity["requested_memory_gb"]),
    ) + [
        "set -euo pipefail",
        *_module_lines(config),
        (
            f"ssqtl-igv run-shard --config {_q(config_path)} --run-root {_q(root)} "
            f'--shard-index "${{SGE_TASK_ID}}" --shard-map {_q(chunk_map)}'
        ),
        "exit $?",
    ]
    summary_lines = _directives(config, f"igvsum_{root.name}", logs) + [
        "set -euo pipefail",
        *_module_lines(config),
        f"ssqtl-igv summarize --config {_q(config_path)} --run-root {_q(root)}",
        "exit $?",
    ]
    runner_script = scheduler_root / "run_shards.qsub"
    summary_script = scheduler_root / "summarize.qsub"
    atomic_write_text(runner_script, "\n".join(runner_lines) + "\n")
    atomic_write_text(summary_script, "\n".join(summary_lines) + "\n")
    plan = {
        "schema_version": "portable-grid-engine-scheduler-plan-v1",
        "created_at": utc_now(),
        "run_root": str(root),
        "case_count": len(cases),
        "submitted_case_count": len(incomplete_cases),
        "completed_case_count": len(cases) - len(incomplete_cases),
        "logical_shard_count": len({case["shard"] for case in cases}),
        "chunk_count": len(chunks),
        "scheduler_task_count": len(chunks),
        "array_range": f"1-{len(chunks)}",
        "array_range_role": "logical_task_coverage_only",
        "array_ranges": array_ranges,
        "array_job_count": len(array_ranges),
        "max_batch_task_count": max(len(parse_task_range(value)) for value in array_ranges),
        "throttle_contract_id": SCHEDULER_THROTTLE_CONTRACT_ID,
        "hard_max_tasks_per_array": max_tasks_per_array,
        "scheduler_tc_requested": int(capacity["max_parallel"]),
        "scheduler_tc_role": "defense_in_depth_only",
        "serial_hold_chain_required": True,
        "chunk_map": str(chunk_map),
        "chunk_map_sha256": sha256_file(chunk_map),
        "runner_script": str(runner_script),
        "runner_script_sha256": sha256_file(runner_script),
        "summary_script": str(summary_script),
        "summary_script_sha256": sha256_file(summary_script),
        "execute": execute,
        "manifest_sha256": manifest_sha,
        "config_fingerprint": sha256_json(config.data),
        "associations_sha256": str(cases[0].get("associations_sha256", "")),
        "submission_generation": generation,
        "scheduler_capacity": capacity,
        "max_parallel": int(capacity["max_parallel"]),
        "requested_memory_gb": int(capacity["requested_memory_gb"]),
        "expected_owner": optional_text(config.get("scheduler.owner")),
        "expected_job_name": f"igv_{root.name}",
        "expected_project": optional_text(config.get("scheduler.project")),
        "remaining_cases_at_submit": remaining_cases,
        "storage_evidence_at_submit": refreshed_preflight.get("storage"),
    }
    plan["plan_identity_sha256"] = _scheduler_plan_identity(plan)
    return _submit_plan(config, scheduler_root, plan, runner_script, summary_script, execute=execute)


def _validated_rerun(
    config: WorkflowConfig, root: Path, rerun_manifest: str | Path
) -> tuple[list[dict[str, Any]], set[str], list[str], list[dict[str, str]], str, str]:
    with Path(rerun_manifest).open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    manifest_sha = sha256_file(root / ".work" / "manifests" / "case_manifest.jsonl")
    if rows:
        mismatched = [
            row
            for row in rows
            if row.get("run_id") != root.name or row.get("manifest_sha256") != manifest_sha
        ]
        if mismatched:
            raise ValueError("rerun manifest is not bound to this run ID and manifest SHA-256")
    case_ids = {row["case_id"] for row in rows if row.get("case_id")}
    if len(case_ids) != len(rows):
        raise ValueError("rerun manifest contains blank or duplicate case IDs")
    cases = load_cases(root)
    assert_manifest_config(cases, config, root)
    by_id = {case["case_id"]: case for case in cases}
    known = set(by_id)
    unknown = sorted(case_ids - known)
    if unknown:
        raise ValueError("rerun manifest contains unknown case IDs: " + ", ".join(unknown[:10]))
    mismatched_shards = [
        row["case_id"]
        for row in rows
        if row.get("shard") and row["case_id"] in by_id and row["shard"] != by_id[row["case_id"]]["shard"]
    ]
    if mismatched_shards:
        raise ValueError(
            "rerun manifest shard differs from authoritative manifest: " + ", ".join(mismatched_shards[:10])
        )
    ineligible = []
    state_versions: dict[str, Any] = {}
    for case_id in sorted(case_ids):
        state_path = root / ".work" / "state" / f"{safe_name(case_id)}.json"
        if state_path.is_file():
            try:
                state = CaseState.load(state_path)
                status = state.status
                state_versions[case_id] = {
                    "status": status,
                    "history_length": len(state.history),
                    "sha256": sha256_file(state_path),
                }
            except Exception:
                status = FAILED
                state_versions[case_id] = {"status": "CORRUPT", "sha256": sha256_file(state_path)}
        else:
            status = PENDING
            state_versions[case_id] = {"status": PENDING, "sha256": None}
        if status not in {FAILED, RERUN, PENDING}:
            ineligible.append(f"{case_id}:{status}")
    if ineligible:
        raise ValueError("rerun manifest contains immutable/non-failed cases: " + ", ".join(ineligible[:10]))
    shards = sorted({case["shard"] for case in cases if case["case_id"] in case_ids})
    return cases, case_ids, shards, rows, manifest_sha, sha256_json(state_versions)


def resume_cases(
    config: WorkflowConfig,
    run_root: str | Path,
    rerun_manifest: str | Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    root = config.validate_run_root(run_root, must_exist=True)
    _cases, case_ids, shards, _rows, manifest_sha, state_version = _validated_rerun(
        config, root, rerun_manifest
    )
    if not case_ids:
        return {"created_at": utc_now(), "case_count": 0, "failed": 0, "exit_code": 0, "action": "NOOP"}
    resume_scope = sha256_json(
        {
            "manifest_sha256": manifest_sha,
            "case_ids": sorted(case_ids),
            "state_version": state_version,
        }
    )[:16]
    reports = [
        run_shard(
            config,
            root,
            shard=shard,
            case_ids=case_ids,
            report_id=f"resume_local_{resume_scope}__{shard}",
            force=force,
        )
        for shard in shards
    ]
    failed = sum(
        int(report["failed"]) + len(report.get("unstarted_case_ids", []))
        for report in reports
    )
    return {
        "created_at": utc_now(),
        "case_count": len(case_ids),
        "shard_count": len(shards),
        "failed": failed,
        "reports": reports,
        "exit_code": 2 if failed else 0,
    }


def create_resume_submission(
    config: WorkflowConfig,
    run_root: str | Path,
    rerun_manifest: str | Path,
    *,
    execute: bool = False,
    generation: int = 1,
) -> dict[str, Any]:
    if optional_text(config.get("execution.mode", "local")).lower() != "grid_engine":
        raise ValueError("scheduler resume requires execution.mode=grid_engine")
    if generation < 1:
        raise ValueError("submission generation must be a positive integer")
    root = config.validate_run_root(run_root, must_exist=True)
    cases, case_ids, shards, rows, manifest_sha, state_version = _validated_rerun(
        config, root, rerun_manifest
    )
    if not case_ids:
        return {"created_at": utc_now(), "case_count": 0, "exit_code": 0, "action": "NOOP"}
    remaining_cases = _remaining_case_count(root, cases)
    refreshed_preflight = _require_preflight(
        config,
        root,
        root / ".work" / "manifests" / "case_manifest.jsonl",
        remaining_cases=remaining_cases,
    )
    capacity = _submission_capacity(
        config,
        run_root=root,
        run_id=root.name,
        manifest_sha256=manifest_sha,
    )
    resume_key = sha256_json(
        {
            "manifest": manifest_sha,
            "case_ids": sorted(case_ids),
            "state_version": state_version,
            "generation": generation,
        }
    )[:12]
    scheduler_root = root / ".work" / "scheduler" / f"resume_{resume_key}"
    logs = scheduler_root / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    copied_manifest = scheduler_root / "rerun_manifest.tsv"
    fieldnames = list(rows[0].keys())
    write_tsv(copied_manifest, fieldnames, rows)
    shard_map = scheduler_root / "shards.tsv"
    case_counts = {
        shard: sum(1 for case in cases if case["case_id"] in case_ids and case["shard"] == shard)
        for shard in shards
    }
    write_tsv(
        shard_map,
        ["task_id", "chunk", "report_id", "shard", "case_count"],
        (
            {
                "task_id": index,
                "chunk": f"resume_{index:04d}_{shard}",
                "report_id": f"resume_{resume_key}__task_{index:04d}__{shard}",
                "shard": shard,
                "case_count": case_counts[shard],
            }
            for index, shard in enumerate(shards, 1)
        ),
    )
    config_path = config.path.resolve(strict=False)
    runner_lines = _directives(
        config,
        f"igvr_{root.name}",
        logs,
        memory_gb=int(capacity["requested_memory_gb"]),
    ) + [
        "set -euo pipefail",
        *_module_lines(config),
        (
            f"ssqtl-igv run-shard --config {_q(config_path)} --run-root {_q(root)} "
            '--shard-index "${SGE_TASK_ID}" '
            f"--shard-map {_q(shard_map)} --case-list {_q(copied_manifest)}"
        ),
        "exit $?",
    ]
    summary_lines = _directives(config, f"igvrsum_{root.name}", logs) + [
        "set -euo pipefail",
        *_module_lines(config),
        f"ssqtl-igv summarize --config {_q(config_path)} --run-root {_q(root)}",
        "exit $?",
    ]
    runner_script = scheduler_root / "run_rerun_shards.qsub"
    summary_script = scheduler_root / "summarize_rerun.qsub"
    atomic_write_text(runner_script, "\n".join(runner_lines) + "\n")
    atomic_write_text(summary_script, "\n".join(summary_lines) + "\n")
    max_tasks_per_array = int(
        config.get("scheduler.max_tasks_per_array", DEFAULT_MAX_TASKS_PER_ARRAY)
    )
    array_ranges = _hard_throttle_array_ranges(len(shards), max_tasks_per_array)
    plan = {
        "schema_version": "portable-grid-engine-scheduler-plan-v1",
        "created_at": utc_now(),
        "run_root": str(root),
        "case_count": len(case_ids),
        "shard_count": len(shards),
        "scheduler_task_count": len(shards),
        "array_range": f"1-{len(shards)}",
        "array_range_role": "logical_task_coverage_only",
        "array_ranges": array_ranges,
        "array_job_count": len(array_ranges),
        "max_batch_task_count": max(len(parse_task_range(value)) for value in array_ranges),
        "throttle_contract_id": SCHEDULER_THROTTLE_CONTRACT_ID,
        "hard_max_tasks_per_array": max_tasks_per_array,
        "scheduler_tc_requested": int(capacity["max_parallel"]),
        "scheduler_tc_role": "defense_in_depth_only",
        "serial_hold_chain_required": True,
        "runner_script": str(runner_script),
        "runner_script_sha256": sha256_file(runner_script),
        "summary_script": str(summary_script),
        "summary_script_sha256": sha256_file(summary_script),
        "rerun_manifest": str(copied_manifest),
        "rerun_manifest_sha256": sha256_file(copied_manifest),
        "shard_map": str(shard_map),
        "shard_map_sha256": sha256_file(shard_map),
        "execute": execute,
        "manifest_sha256": manifest_sha,
        "config_fingerprint": sha256_json(config.data),
        "associations_sha256": str(cases[0].get("associations_sha256", "")),
        "state_version": state_version,
        "submission_generation": generation,
        "scheduler_capacity": capacity,
        "max_parallel": int(capacity["max_parallel"]),
        "requested_memory_gb": int(capacity["requested_memory_gb"]),
        "expected_owner": optional_text(config.get("scheduler.owner")),
        "expected_job_name": f"igvr_{root.name}",
        "expected_project": optional_text(config.get("scheduler.project")),
        "remaining_cases_at_submit": remaining_cases,
        "storage_evidence_at_submit": refreshed_preflight.get("storage"),
    }
    plan["plan_identity_sha256"] = _scheduler_plan_identity(plan)
    return _submit_plan(config, scheduler_root, plan, runner_script, summary_script, execute=execute)
