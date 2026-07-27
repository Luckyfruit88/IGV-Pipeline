from __future__ import annotations

import csv
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .utils import sha256_file


def contract_root(run_root: Path) -> Path:
    """Return the immutable contract root for legacy or pull-and-run products."""

    legacy = run_root / "contract"
    hidden = run_root / ".igv-pipeline" / "contract"
    return legacy if legacy.is_dir() and not legacy.is_symlink() else hidden


def cases_root(run_root: Path) -> Path:
    """Return the internal case-result root without exposing it publicly."""

    legacy = run_root / "results" / "cases"
    hidden = run_root / ".igv-pipeline" / "cases"
    return legacy if legacy.is_dir() and not legacy.is_symlink() else hidden


def snapshot_record(run_root: Path, task_id: str) -> dict[str, str]:
    manifest = run_root / "snapshots.tsv"
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError(f"snapshot manifest is unavailable: {manifest}")
    with manifest.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        expected = (
            "manifest_order",
            "task_id",
            "chromosome",
            "relative_path",
            "sha256",
            "status",
        )
        if tuple(reader.fieldnames or ()) != expected:
            raise ValueError("snapshots.tsv field contract differs")
        matches = [
            {key: str(value or "") for key, value in row.items()}
            for row in reader
            if str(row.get("task_id", "")) == task_id
        ]
    if len(matches) != 1:
        raise ValueError(f"snapshots.tsv does not uniquely identify task: {task_id}")
    return matches[0]


def resolve_case_artifact(
    run_root: Path,
    case_result: Mapping[str, Any],
    role: str,
    record: Mapping[str, Any],
) -> Path:
    """Resolve an artifact across legacy results and the compact public product.

    Pull-and-run keeps non-image evidence under ``.igv-pipeline/cases`` and
    publishes the combined review image only as the task-addressed snapshot.
    The checksum-bound case result remains authoritative for both layouts.
    """

    root = run_root.resolve(strict=True)
    relative = Path(str(record.get("relative_path", record.get("path", ""))))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"unsafe case artifact path: {relative}")
    candidate = root / relative
    if not candidate.is_file() and role in {"review_image", "combined_png"}:
        task_id = str(case_result.get("task_id", ""))
        snapshot = snapshot_record(root, task_id)
        if snapshot["status"] != "SNAPSHOT_READY":
            raise ValueError(f"eligible review snapshot is not ready: {task_id}")
        if snapshot["sha256"] != str(record.get("sha256", "")):
            raise ValueError(f"snapshot differs from case-result binding: {task_id}")
        candidate = root / snapshot["relative_path"]
    elif not candidate.is_file():
        task_id = str(case_result.get("task_id", ""))
        prefix = ("results", "cases", task_id)
        if relative.parts[:3] != prefix:
            raise ValueError(f"case artifact is unavailable: {relative}")
        candidate = cases_root(root) / task_id / Path(*relative.parts[3:])
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"case artifact is unavailable or symlinked: {candidate}")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"case artifact escapes the run root: {candidate}") from exc
    digest = str(record.get("sha256", "")).strip().lower()
    if sha256_file(resolved) != digest:
        raise ValueError(f"case artifact checksum drift: {candidate}")
    return resolved
