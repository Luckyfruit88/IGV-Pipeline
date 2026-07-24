from __future__ import annotations

from pathlib import Path
from typing import Any

from .utils import atomic_write_json, read_jsonl, sha256_file, write_jsonl


def create_bounded_shards(
    tasks_path: str | Path,
    output_dir: str | Path,
    *,
    max_cases_per_shard: int = 256,
    relative_paths: bool = False,
) -> dict[str, Any]:
    """Create deterministic logical groups without controlling task scheduling."""

    if not 1 <= int(max_cases_per_shard) <= 256:
        raise ValueError("max_cases_per_shard must be between 1 and 256")
    source = Path(tasks_path).expanduser().resolve(strict=True)
    tasks = list(read_jsonl(source))
    if not tasks:
        raise ValueError("canonical task set is empty")
    orders = [int(task["manifest_order"]) for task in tasks]
    if orders != list(range(1, len(tasks) + 1)):
        raise ValueError("canonical tasks must have contiguous one-based manifest_order")
    ids = [str(task["task_id"]) for task in tasks]
    if len(ids) != len(set(ids)):
        raise ValueError("canonical task_id values are not unique")

    root = Path(output_dir).expanduser().resolve(strict=False)
    root.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    for offset in range(0, len(tasks), int(max_cases_per_shard)):
        subset = tasks[offset : offset + int(max_cases_per_shard)]
        index = len(rows) + 1
        shard_id = f"shard-{index:04d}"
        path = root / f"{shard_id}.jsonl"
        write_jsonl(path, subset)
        rows.append(
            {
                "shard_id": shard_id,
                "shard_order": index - 1,
                "first_manifest_order": int(subset[0]["manifest_order"]),
                "last_manifest_order": int(subset[-1]["manifest_order"]),
                "case_count": len(subset),
                "task_ids": [str(task["task_id"]) for task in subset],
                "path": f"shards/{path.name}" if relative_paths else str(path),
                "sha256": sha256_file(path),
            }
        )
    plan = {
        "schema_version": "3.0",
        "source_tasks": "contract/tasks.jsonl" if relative_paths else str(source),
        "source_sha256": sha256_file(source),
        "case_count": len(tasks),
        "max_cases_per_shard": int(max_cases_per_shard),
        "scheduling_role": "LOGICAL_ONLY",
        "shard_count": len(rows),
        "shards": rows,
    }
    atomic_write_json(root / "shard_plan.json", plan)
    return plan
