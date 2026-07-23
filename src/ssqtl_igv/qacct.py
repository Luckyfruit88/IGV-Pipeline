from __future__ import annotations

import json
import getpass
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable

from .contracts import SCHEDULER_THROTTLE_CONTRACT_ID
from .scheduler import _scheduler_plan_identity, _validate_submitted_array_chain
from .utils import (
    atomic_write_json,
    atomic_write_text,
    command_prefix,
    optional_text,
    sha256_file,
    utc_now,
)
from .validation_lineage import _validated_qacct_rows, observed_peak_concurrency, parse_task_range


QACCT_EVIDENCE_SCHEMA = "grid-engine-qacct-evidence-v1"
_DIRECTIVE = re.compile(r"^#\$\s+-(N|P)\s+(.+?)\s*$")


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def qacct_paths(jobs_path: str | Path) -> dict[str, Path]:
    jobs = _resolved(jobs_path)
    return {
        "evidence": jobs.with_name("qacct_evidence.json"),
        "raw_root": jobs.with_name("qacct_raw"),
    }


def _load_jobs(jobs_path: str | Path, *, run_root: str | Path) -> dict[str, Any]:
    path = _resolved(jobs_path)
    root = _resolved(run_root)
    expected_parent = root / ".work" / "scheduler"
    try:
        path.relative_to(expected_parent)
    except ValueError as exc:
        raise ValueError("scheduler jobs.json is outside the run scheduler root") from exc
    if path.is_symlink() or not path.is_file() or path.name != "jobs.json":
        raise ValueError(f"missing or symlinked scheduler jobs record: {path}")
    record = json.loads(path.read_text(encoding="utf-8"))
    if (
        record.get("schema_version") != "portable-grid-engine-scheduler-plan-v1"
        or record.get("status") != "ARRAY_AND_SUMMARY_SUBMITTED"
        or _resolved(record.get("run_root", "")) != root
        or record.get("plan_identity_sha256") != _scheduler_plan_identity(record)
        or record.get("throttle_contract_id") != SCHEDULER_THROTTLE_CONTRACT_ID
        or int(record.get("hard_max_tasks_per_array", 0)) < 1
        or int(record.get("scheduler_tc_requested", 0)) < 1
        or int(record.get("scheduler_tc_requested", 0))
        != int(record.get("max_parallel", 0))
        or record.get("scheduler_tc_role") != "defense_in_depth_only"
        or record.get("serial_hold_chain_required") is not True
    ):
        raise ValueError("scheduler jobs record lacks a complete submitted identity")
    runner_script = _resolved(record.get("runner_script", ""))
    if (
        runner_script.is_symlink()
        or not runner_script.is_file()
        or sha256_file(runner_script) != record.get("runner_script_sha256")
    ):
        raise ValueError("scheduler runner script differs from jobs.json")
    directives: dict[str, str] = {}
    for line in runner_script.read_text(encoding="utf-8").splitlines():
        match = _DIRECTIVE.match(line)
        if match:
            directives[match.group(1)] = match.group(2).strip()
    job_name = optional_text(record.get("expected_job_name")) or directives.get("N", "")
    project = optional_text(record.get("expected_project")) or directives.get("P", "")
    if not job_name or not project:
        raise ValueError("scheduler job name/project cannot be derived from the runner")
    if directives.get("N") != job_name or directives.get("P") != project:
        raise ValueError("scheduler identity differs from the plan-bound runner")
    array_jobs = list(record.get("array_jobs", []))
    _validate_submitted_array_chain(record, array_jobs, require_complete=True)
    expected_tasks = [
        task_id
        for array_range in record.get("array_ranges", [])
        for task_id in parse_task_range(str(array_range))
    ]
    if expected_tasks != list(range(1, int(record.get("scheduler_task_count", 0)) + 1)):
        raise ValueError("scheduler jobs record does not exactly cover scheduler tasks")
    return {
        **record,
        "expected_owner": optional_text(record.get("expected_owner")),
        "expected_job_name": job_name,
        "expected_project": project,
        "path": str(path),
        "sha256": sha256_file(path),
    }


def _validate_array_evidence(
    record: dict[str, Any], raw_root: Path, array_jobs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    observed_files = {path.resolve(strict=False) for path in raw_root.iterdir()}
    expected_files: set[Path] = set()
    all_tasks: list[dict[str, Any]] = []
    previous_job_id = ""
    previous_job_end = 0.0
    for submitted, evidence in zip(record["array_jobs"], array_jobs, strict=True):
        identity = ("job_id", "array_range", "hold_jid")
        if any(str(evidence.get(key, "")) != str(submitted.get(key, "")) for key in identity):
            raise ValueError("Grid Engine qacct array identity differs from jobs.json")
        if str(evidence.get("hold_jid", "")) != previous_job_id:
            raise ValueError("Grid Engine qacct evidence breaks the hold_jid chain")
        raw_path = _resolved(evidence.get("raw_path", ""))
        try:
            raw_path.relative_to(raw_root.resolve(strict=False))
        except ValueError as exc:
            raise ValueError("Grid Engine qacct raw evidence escapes its directory") from exc
        if raw_path.is_symlink() or not raw_path.is_file() or raw_path.stat().st_mode & 0o777 != 0o444:
            raise ValueError(f"Grid Engine qacct raw evidence is missing or mutable: {raw_path}")
        if sha256_file(raw_path) != evidence.get("raw_sha256"):
            raise ValueError(f"Grid Engine qacct raw evidence checksum mismatch: {raw_path}")
        expected_files.add(raw_path)
        rows = _validated_qacct_rows(
            raw_path.read_text(encoding="utf-8", errors="strict"),
            job_id=str(submitted["job_id"]),
            array_range=str(submitted["array_range"]),
            owner=str(record["expected_owner"]),
            job_name=str(record["expected_job_name"]),
            project=str(record["expected_project"]),
        )
        if rows != evidence.get("tasks"):
            raise ValueError("Grid Engine qacct parsed tasks differ from immutable raw evidence")
        if rows and previous_job_end and min(float(row["start_epoch"]) for row in rows) < previous_job_end:
            raise ValueError("scheduler arrays overlap despite the strict hold_jid chain")
        if rows:
            previous_job_end = max(float(row["reported_end_epoch"]) for row in rows)
        previous_job_id = str(submitted["job_id"])
        all_tasks.extend(rows)
    if len(array_jobs) != len(record["array_jobs"]) or observed_files != expected_files:
        raise ValueError("Grid Engine qacct raw evidence coverage mismatch")
    return sorted(all_tasks, key=lambda row: row["task_id"])


def validate_qacct_evidence(
    jobs_path: str | Path, *, run_root: str | Path
) -> dict[str, Any]:
    record = _load_jobs(jobs_path, run_root=run_root)
    paths = qacct_paths(record["path"])
    evidence_path, raw_root = paths["evidence"], paths["raw_root"]
    if (
        evidence_path.is_symlink()
        or not evidence_path.is_file()
        or evidence_path.stat().st_mode & 0o777 != 0o444
        or raw_root.is_symlink()
        or not raw_root.is_dir()
        or raw_root.stat().st_mode & 0o777 != 0o555
    ):
        raise ValueError("Grid Engine qacct evidence is missing, symlinked, or mutable")
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    required = {
        "schema_version": QACCT_EVIDENCE_SCHEMA,
        "run_root": str(_resolved(run_root)),
        "jobs_record": record["path"],
        "jobs_record_sha256": record["sha256"],
        "plan_identity_sha256": record["plan_identity_sha256"],
        "manifest_sha256": record["manifest_sha256"],
        "submission_generation": record["submission_generation"],
        "expected_job_name": record["expected_job_name"],
        "expected_project": record["expected_project"],
        "hard_max_tasks_per_array": int(record["hard_max_tasks_per_array"]),
        "scheduler_tc_requested": int(record["scheduler_tc_requested"]),
        "effective_max_parallel": min(
            int(record["hard_max_tasks_per_array"]),
            int(record["scheduler_tc_requested"]),
        ),
        "serial_hold_chain_required": True,
        "inter_array_overlap": False,
        "hard_limit_pass": True,
    }
    mismatched = [key for key, value in required.items() if evidence.get(key) != value]
    if record["expected_owner"] and evidence.get("expected_owner") != record["expected_owner"]:
        mismatched.append("expected_owner")
    if not str(evidence.get("expected_owner", "")).strip():
        mismatched.append("expected_owner")
    if mismatched:
        raise ValueError("Grid Engine qacct evidence identity mismatch: " + ", ".join(mismatched))
    evidence_record = {**record, "expected_owner": str(evidence["expected_owner"])}
    rows = _validate_array_evidence(evidence_record, raw_root, list(evidence.get("array_jobs", [])))
    expected_task_ids = list(range(1, int(record["scheduler_task_count"]) + 1))
    if [row["task_id"] for row in rows] != expected_task_ids or evidence.get("tasks") != rows:
        raise ValueError("Grid Engine qacct evidence does not exactly cover scheduler tasks")
    peak = observed_peak_concurrency(rows)
    effective_limit = min(
        int(record["hard_max_tasks_per_array"]),
        int(record["scheduler_tc_requested"]),
    )
    if peak > effective_limit or evidence.get("observed_peak_concurrency") != peak:
        raise ValueError("Grid Engine qacct evidence violates the configured concurrency limit")
    return {**evidence, "path": str(evidence_path), "sha256": sha256_file(evidence_path)}


def collect_qacct_evidence(
    jobs_path: str | Path,
    *,
    run_root: str | Path,
    qacct_command: Any,
    runner: Callable[[list[str]], str] | None = None,
) -> dict[str, Any]:
    prefix = command_prefix(qacct_command)
    record = _load_jobs(jobs_path, run_root=run_root)
    if not record["expected_owner"]:
        record["expected_owner"] = getpass.getuser()
    paths = qacct_paths(record["path"])
    if paths["evidence"].exists() or paths["raw_root"].exists():
        return validate_qacct_evidence(record["path"], run_root=run_root)

    def default_runner(command: list[str]) -> str:
        completed = subprocess.run(command, check=False, text=True, capture_output=True)
        if completed.returncode != 0:
            raise ValueError(
                f"qacct failed ({completed.returncode}) for {' '.join(command)}: {completed.stderr.strip()}"
            )
        return completed.stdout

    invoke = runner or default_runner
    staging = Path(tempfile.mkdtemp(prefix=".qacct_raw.", dir=paths["raw_root"].parent))
    array_evidence: list[dict[str, Any]] = []
    all_tasks: list[dict[str, Any]] = []
    previous_job_end = 0.0
    try:
        for submitted in record["array_jobs"]:
            job_id = str(submitted["job_id"])
            array_range = str(submitted["array_range"])
            text = invoke([*prefix, "-j", job_id, "-t", array_range])
            rows = _validated_qacct_rows(
                text,
                job_id=job_id,
                array_range=array_range,
                owner=str(record["expected_owner"]),
                job_name=str(record["expected_job_name"]),
                project=str(record["expected_project"]),
            )
            if rows and previous_job_end and min(float(row["start_epoch"]) for row in rows) < previous_job_end:
                raise ValueError("scheduler arrays overlap despite the strict hold_jid chain")
            if rows:
                previous_job_end = max(float(row["reported_end_epoch"]) for row in rows)
            raw_path = paths["raw_root"] / f"job_{job_id}_tasks_{array_range.replace(':', '_')}.txt"
            staged_raw = staging / raw_path.name
            atomic_write_text(staged_raw, text)
            array_evidence.append(
                {
                    "job_id": job_id,
                    "array_range": array_range,
                    "hold_jid": str(submitted.get("hold_jid", "")),
                    "raw_path": str(raw_path),
                    "raw_sha256": sha256_file(staged_raw),
                    "tasks": rows,
                }
            )
            all_tasks.extend(rows)
        all_tasks.sort(key=lambda row: row["task_id"])
        peak = observed_peak_concurrency(all_tasks)
        effective_limit = min(
            int(record["hard_max_tasks_per_array"]),
            int(record["scheduler_tc_requested"]),
        )
        if peak > effective_limit:
            raise ValueError(
                "Grid Engine qacct evidence violates the configured concurrency limit"
            )
        evidence = {
            "schema_version": QACCT_EVIDENCE_SCHEMA,
            "created_at": utc_now(),
            "run_root": str(_resolved(run_root)),
            "jobs_record": record["path"],
            "jobs_record_sha256": record["sha256"],
            "plan_identity_sha256": record["plan_identity_sha256"],
            "manifest_sha256": record["manifest_sha256"],
            "submission_generation": record["submission_generation"],
            "expected_owner": record["expected_owner"],
            "expected_job_name": record["expected_job_name"],
            "expected_project": record["expected_project"],
            "hard_max_tasks_per_array": int(record["hard_max_tasks_per_array"]),
            "scheduler_tc_requested": int(record["scheduler_tc_requested"]),
            "effective_max_parallel": effective_limit,
            "serial_hold_chain_required": True,
            "inter_array_overlap": False,
            "observed_peak_concurrency": peak,
            "hard_limit_pass": True,
            "array_jobs": array_evidence,
            "tasks": all_tasks,
            "failed_task_count": sum(
                row["failed"] != 0 or row["exit_status"] != 0 for row in all_tasks
            ),
        }
        os.replace(staging, paths["raw_root"])
        atomic_write_json(paths["evidence"], evidence)
        for path in paths["raw_root"].iterdir():
            path.chmod(0o444)
        paths["raw_root"].chmod(0o555)
        paths["evidence"].chmod(0o444)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return validate_qacct_evidence(record["path"], run_root=run_root)


def submitted_job_records(run_root: str | Path) -> list[Path]:
    scheduler_root = _resolved(run_root) / ".work" / "scheduler"
    records: list[Path] = []
    for path in sorted(scheduler_root.glob("*/jobs.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if record.get("status") == "ARRAY_AND_SUMMARY_SUBMITTED":
            records.append(path)
    return records
