from __future__ import annotations

import re
from datetime import datetime
from typing import Any

_RANGE = re.compile(r"([1-9][0-9]*)(?:-([1-9][0-9]*)(?::([1-9][0-9]*))?)?")
_QACCT_TIME_FORMAT = "%a %b %d %H:%M:%S %Y"


def parse_task_range(value: str) -> list[int]:
    match = _RANGE.fullmatch(str(value).strip())
    if not match:
        raise ValueError(f"invalid Grid Engine task range: {value!r}")
    start = int(match.group(1))
    end = int(match.group(2) or match.group(1))
    step = int(match.group(3) or 1)
    if end < start:
        raise ValueError(f"descending Grid Engine task range: {value!r}")
    return list(range(start, end + 1, step))


def contiguous_task_ranges(task_ids: list[int]) -> list[str]:
    ordered = sorted(set(task_ids))
    if not ordered or any(task_id < 1 for task_id in ordered):
        raise ValueError("task IDs must be positive")
    groups: list[list[int]] = [[ordered[0]]]
    for task_id in ordered[1:]:
        if task_id == groups[-1][-1] + 1:
            groups[-1].append(task_id)
        else:
            groups.append([task_id])
    return [
        f"{group[0]}-{group[-1]}" if len(group) > 1 else str(group[0])
        for group in groups
    ]


def bounded_contiguous_task_ranges(task_ids: list[int], limit: int = 8) -> list[str]:
    if limit < 1 or sorted(set(task_ids)) != list(range(1, len(task_ids) + 1)):
        raise ValueError("task IDs must be unique contiguous positive integers")
    return [
        f"{chunk[0]}-{chunk[-1]}" if len(chunk) > 1 else str(chunk[0])
        for offset in range(0, len(task_ids), limit)
        for chunk in [task_ids[offset : offset + limit]]
    ]


def parse_qacct_output(text: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if set(stripped) == {"="}:
            if current:
                records.append(current)
                current = {}
            continue
        parts = stripped.split(None, 1)
        if len(parts) == 2:
            current[parts[0]] = parts[1].strip()
    if current:
        records.append(current)
    return records


def _accounting_code(value: str, field: str) -> int:
    token = str(value).strip().split(None, 1)[0] if str(value).strip() else ""
    try:
        parsed = int(token)
    except ValueError as exc:
        raise ValueError(f"qacct {field} is not an integer: {value!r}") from exc
    if parsed < 0:
        raise ValueError(f"qacct {field} is negative: {value!r}")
    return parsed


def _parse_qacct_timestamp(value: str) -> float:
    try:
        parsed = datetime.strptime(value.strip(), _QACCT_TIME_FORMAT)
    except ValueError as exc:
        raise ValueError(f"invalid qacct timestamp: {value!r}") from exc
    return parsed.astimezone().timestamp()


def _validated_qacct_rows(
    text: str,
    *,
    job_id: str,
    array_range: str,
    owner: str,
    job_name: str,
    project: str,
) -> list[dict[str, Any]]:
    expected_tasks = parse_task_range(array_range)
    parsed = parse_qacct_output(text)
    if len(parsed) != len(expected_tasks):
        raise ValueError(
            f"qacct task count mismatch for job {job_id}: "
            f"expected={len(expected_tasks)}, observed={len(parsed)}"
        )
    rows: list[dict[str, Any]] = []
    observed_tasks: list[int] = []
    for raw in parsed:
        task_text = str(raw.get("taskid", ""))
        if not task_text.isdigit():
            raise ValueError("qacct taskid is missing or nonnumeric")
        task_id = int(task_text)
        observed_tasks.append(task_id)
        required = {
            "jobnumber": job_id,
            "owner": owner,
            "jobname": job_name,
            "project": project,
        }
        mismatched = [
            key for key, expected in required.items() if expected and raw.get(key) != expected
        ]
        if mismatched:
            raise ValueError("qacct scheduler identity mismatch: " + ", ".join(mismatched))
        failed = _accounting_code(raw.get("failed", ""), "failed")
        exit_status = _accounting_code(raw.get("exit_status", ""), "exit_status")
        start_epoch = _parse_qacct_timestamp(raw.get("start_time", ""))
        reported_end_epoch = _parse_qacct_timestamp(raw.get("end_time", ""))
        try:
            wallclock = float(raw.get("ru_wallclock", ""))
        except ValueError as exc:
            raise ValueError("qacct ru_wallclock is not numeric") from exc
        if wallclock < 0 or reported_end_epoch < start_epoch:
            raise ValueError("qacct accounting interval is invalid")
        if reported_end_epoch == start_epoch:
            if failed == 0 and exit_status == 0:
                raise ValueError("successful qacct task has no positive accounting interval")
            end_epoch = start_epoch + max(wallclock, 0.001)
        else:
            end_epoch = reported_end_epoch
        if failed == 0 and exit_status == 0 and wallclock <= 0:
            raise ValueError("successful qacct ru_wallclock must be positive")
        rows.append(
            {
                "job_id": job_id,
                "task_id": task_id,
                "owner": raw.get("owner"),
                "job_name": raw.get("jobname"),
                "project": raw.get("project"),
                "qname": raw.get("qname"),
                "hostname": raw.get("hostname"),
                "qsub_time": raw.get("qsub_time"),
                "start_time": raw.get("start_time"),
                "end_time": raw.get("end_time"),
                "start_epoch": start_epoch,
                "end_epoch": end_epoch,
                "reported_end_epoch": reported_end_epoch,
                "ru_wallclock_seconds": wallclock,
                "ru_maxrss": raw.get("ru_maxrss"),
                "maxvmem": raw.get("maxvmem"),
                "failed": failed,
                "exit_status": exit_status,
            }
        )
    if sorted(observed_tasks) != expected_tasks or len(set(observed_tasks)) != len(observed_tasks):
        raise ValueError("qacct output does not exactly cover its submitted array range")
    return sorted(rows, key=lambda row: row["task_id"])


def observed_peak_concurrency(rows: list[dict[str, Any]]) -> int:
    """Return half-open interval concurrency, with ends ordered before starts."""

    events: list[tuple[float, int]] = []
    for row in rows:
        events.append((float(row["start_epoch"]), 1))
        events.append((float(row.get("end_epoch", row["reported_end_epoch"])), -1))
    active = 0
    peak = 0
    for _epoch, delta in sorted(events, key=lambda item: (item[0], item[1])):
        active += delta
        if active < 0:
            raise ValueError("qacct concurrency intervals are internally inconsistent")
        peak = max(peak, active)
    if active != 0:
        raise ValueError("qacct concurrency intervals do not close")
    return peak
