#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from ssqtl_igv.project_launcher import validate_project_postflight


def _object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"expected one JSON object: {path}")
    return value


def verify(
    output: Path,
    *,
    expected_cases: int,
    require_cached: bool,
    expected_adapter: str | None,
) -> dict[str, Any]:
    root = output.expanduser().resolve(strict=True)
    tasks = [
        json.loads(line)
        for line in (root / "contract" / "tasks.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    task_ids = [str(task["task_id"]) for task in tasks]
    assert len(task_ids) == expected_cases
    assert len(task_ids) == len(set(task_ids))
    if expected_adapter is not None:
        assert {str(task["adapter_id"]) for task in tasks} == {expected_adapter}

    shard_plan = _object(root / "shards" / "shard_plan.json")
    assert shard_plan["scheduling_role"] == "LOGICAL_ONLY"
    assert sum(int(row["case_count"]) for row in shard_plan["shards"]) == expected_cases

    for task_id in task_ids:
        case_root = root / "results" / "cases" / task_id
        for relative in (
            "terminal_bundle.json",
            "case_result.json",
            "raw/igv.png",
            "review.png",
        ):
            assert (case_root / relative).is_file(), f"missing {task_id}/{relative}"

    assert not (root / "reports" / "case_outputs").exists()
    postflight = validate_project_postflight(root)
    assert postflight["exit_code"] == 2
    assert postflight["postflight"]["task_count"] == expected_cases

    with (root / "reports" / "trace.txt").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    render_rows = [
        row for row in rows if "RUN_PORTABLE_CASE (" in str(row.get("name", ""))
    ]
    assert len(render_rows) == expected_cases
    if require_cached:
        assert {str(row["status"]).upper() for row in render_rows} == {"CACHED"}

    return {
        "status": "PASS",
        "task_count": expected_cases,
        "render_trace_statuses": sorted(
            {str(row["status"]).upper() for row in render_rows}
        ),
        "product_exit_code": postflight["exit_code"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify native PROJECT_RUN output")
    parser.add_argument("output", type=Path)
    parser.add_argument("--expected-cases", type=int, required=True)
    parser.add_argument("--expected-adapter", choices=("generic", "ssqtl"))
    parser.add_argument("--require-cached", action="store_true")
    args = parser.parse_args()
    if args.expected_cases < 1:
        parser.error("--expected-cases must be positive")
    print(
        json.dumps(
            verify(
                args.output,
                expected_cases=args.expected_cases,
                require_cached=args.require_cached,
                expected_adapter=args.expected_adapter,
            ),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
