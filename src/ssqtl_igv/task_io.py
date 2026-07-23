from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .case_inputs import expected_stage_inputs
from .contracts import validate_task_document
from .utils import read_jsonl


def task_from_manifest(path: str | Path, task_id: str) -> dict[str, Any]:
    matches = [task for task in read_jsonl(path) if task.get("task_id") == task_id]
    if len(matches) != 1:
        raise ValueError(
            f"task_id must occur exactly once in the shard manifest: {task_id}:{len(matches)}"
        )
    validate_task_document(matches[0])
    return matches[0]


def staged_input_map(
    task: dict[str, Any], staged_paths: Iterable[str | Path]
) -> dict[str, str]:
    """Bind ordered Nextflow path inputs to their canonical collision-free names."""

    if task["preflight_state"] == "CASE_INPUT_INVALID":
        return {}
    names = list(expected_stage_inputs(task))
    paths = [str(Path(path).expanduser().resolve(strict=True)) for path in staged_paths]
    if len(names) != len(paths):
        raise RuntimeError(
            f"staged input cardinality differs from task contract: {len(paths)} != {len(names)}"
        )
    return dict(zip(names, paths, strict=True))
