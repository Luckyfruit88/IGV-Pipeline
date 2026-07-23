from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def assert_not_debug_metadata(value: Any, *, label: str) -> None:
    """Reject any structured marker that identifies diagnostic-only material."""

    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).lower()
            if key == "artifact_class" and str(child).upper() == "DEBUG_ONLY":
                raise ValueError(f"{label} is DEBUG_ONLY and cannot enter production")
            if key == "image_role" and str(child).lower() == "runtime-debug":
                raise ValueError(f"{label} comes from runtime-debug and cannot enter production")
            if key == "debug_only" and child is True:
                raise ValueError(f"{label} is debug_only and cannot enter production")
            assert_not_debug_metadata(child, label=label)
    elif isinstance(value, (list, tuple)):
        for child in value:
            assert_not_debug_metadata(child, label=label)


def assert_production_artifact(path: Path, *, label: str) -> None:
    """Reject one symlinked or debug-marked candidate production artifact."""

    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    if "DEBUG_ONLY" in path.name.upper():
        raise ValueError(f"{label} has a DEBUG_ONLY filename: {path}")
    if path.suffix.lower() == ".json":
        if path.stat().st_size > 16 * 1024 * 1024:
            raise ValueError(f"{label} JSON is too large for debug-marker admission: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"{label} JSON cannot be inspected: {path}") from exc
        assert_not_debug_metadata(payload, label=label)
    elif path.suffix.lower() == ".png" and b"DEBUG_ONLY" in path.read_bytes():
        raise ValueError(f"{label} contains DEBUG_ONLY PNG metadata: {path}")


def assert_production_artifact_tree(root: Path, *, label: str) -> None:
    """Reject a tree containing symlinks or diagnostic-only artifacts."""

    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"{label} must be a regular non-symlink directory: {root}")
    for directory, directory_names, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        for name in directory_names:
            candidate = directory_path / name
            if candidate.is_symlink():
                raise ValueError(f"{label} contains a directory symlink: {candidate}")
        for name in filenames:
            assert_production_artifact(directory_path / name, label=label)
