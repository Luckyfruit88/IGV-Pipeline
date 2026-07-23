from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from .contracts import validate_task_document, validate_unique_task_set
from .identity import task_set_fingerprint
from .utils import atomic_write_json, read_jsonl, sha256_file, write_jsonl, write_tsv


SHARD_ALGORITHM = "manifest_order_greedy_score_v1"


def plan_shards(
    tasks: list[dict[str, Any]],
    *,
    max_cases_per_shard: int = 256,
    score_budget_seconds: float = 23040.0,
) -> list[list[dict[str, Any]]]:
    if max_cases_per_shard <= 0:
        raise ValueError("max_cases_per_shard must be positive")
    if score_budget_seconds <= 0:
        raise ValueError("score_budget_seconds must be positive")
    ordered = validate_unique_task_set(tasks)
    if not ordered:
        raise ValueError("cannot shard an empty task set")

    shards: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_score = 0.0
    for task in ordered:
        score = float(task["estimated_runtime_seconds"])
        if score <= 0:
            raise ValueError(f"task score must be positive: {task['task_id']}")
        if score > score_budget_seconds:
            raise ValueError(
                f"task score exceeds shard budget: {task['task_id']}:{score}>{score_budget_seconds}"
            )
        exceeds_count = len(current) >= max_cases_per_shard
        exceeds_score = bool(current) and current_score + score > score_budget_seconds
        if exceeds_count or exceeds_score:
            shards.append(current)
            current = []
            current_score = 0.0
        current.append(task)
        current_score += score
    if current:
        shards.append(current)
    return shards


def create_shards(
    tasks_path: str | Path,
    output_dir: str | Path,
    *,
    max_cases_per_shard: int = 256,
    score_budget_seconds: float = 23040.0,
) -> dict[str, Any]:
    source_input = Path(tasks_path).expanduser()
    if source_input.is_symlink():
        raise ValueError(f"tasks input must not be a symlink: {source_input}")
    source = source_input.resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"tasks input must be a regular non-symlink file: {source}")
    tasks = list(read_jsonl(source))
    for task in tasks:
        validate_task_document(task)
    ordered = validate_unique_task_set(tasks)
    run_ids = {task["run_id"] for task in ordered}
    generation_ids = {task["generation_id"] for task in ordered}
    if len(run_ids) != 1 or len(generation_ids) != 1:
        raise ValueError("task set contains mixed run_id or generation_id values")

    planned = plan_shards(
        ordered,
        max_cases_per_shard=max_cases_per_shard,
        score_budget_seconds=score_budget_seconds,
    )
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"shard output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    shard_directory = staging / "shards"
    shard_directory.mkdir(parents=True, mode=0o700)
    try:
        plan_rows: list[dict[str, Any]] = []
        seen_task_ids: set[str] = set()
        for index, shard_tasks in enumerate(planned, 1):
            shard_id = f"shard_{index:03d}"
            relative_path = Path("shards") / f"{shard_id}.jsonl"
            shard_path = staging / relative_path
            write_jsonl(shard_path, shard_tasks)
            shard_task_ids = {task["task_id"] for task in shard_tasks}
            overlap = seen_task_ids.intersection(shard_task_ids)
            if overlap:
                raise ValueError(f"task appears in multiple shards: {sorted(overlap)[:10]}")
            seen_task_ids.update(shard_task_ids)
            plan_rows.append(
                {
                    "shard_id": shard_id,
                    "relative_path": str(relative_path),
                    "task_count": len(shard_tasks),
                    "estimated_runtime_seconds": sum(
                        float(task["estimated_runtime_seconds"]) for task in shard_tasks
                    ),
                    "first_manifest_order": shard_tasks[0]["manifest_order"],
                    "last_manifest_order": shard_tasks[-1]["manifest_order"],
                    "task_set_sha256": task_set_fingerprint(shard_tasks),
                    "manifest_sha256": sha256_file(shard_path),
                }
            )

        expected_ids = {task["task_id"] for task in ordered}
        if seen_task_ids != expected_ids:
            missing = sorted(expected_ids - seen_task_ids)
            unexpected = sorted(seen_task_ids - expected_ids)
            raise ValueError(
                f"shard task-set mismatch; missing={missing[:10]} unexpected={unexpected[:10]}"
            )

        plan = {
            "schema_version": "2.0",
            "algorithm": SHARD_ALGORITHM,
            "run_id": next(iter(run_ids)),
            "generation_id": next(iter(generation_ids)),
            "source_tasks": str(source),
            "source_tasks_sha256": sha256_file(source),
            "task_set_sha256": task_set_fingerprint(ordered),
            "task_count": len(ordered),
            "shard_count": len(plan_rows),
            "max_cases_per_shard": max_cases_per_shard,
            "score_budget_seconds": score_budget_seconds,
            "shards": plan_rows,
        }
        atomic_write_json(staging / "shard_plan.json", plan)
        write_tsv(
            staging / "shard_plan.tsv",
            [
                "shard_id",
                "relative_path",
                "task_count",
                "estimated_runtime_seconds",
                "first_manifest_order",
                "last_manifest_order",
                "task_set_sha256",
                "manifest_sha256",
            ],
            plan_rows,
        )
        os.replace(staging, destination)
        return {
            **plan,
            "output_dir": str(destination),
            "shard_plan": str(destination / "shard_plan.json"),
            "shard_table": str(destination / "shard_plan.tsv"),
        }
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
