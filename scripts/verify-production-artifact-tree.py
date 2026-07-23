#!/usr/bin/env python3
"""Reject DEBUG_ONLY material before a tree is used for review or publication."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


class ArtifactGateError(ValueError):
    """A production artifact admission failure."""


def _debug_marker(value: Any) -> bool:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized_key = str(key).lower()
            if normalized_key == "artifact_class" and str(child).upper() == "DEBUG_ONLY":
                return True
            if normalized_key == "image_role" and str(child).lower() == "runtime-debug":
                return True
            if normalized_key == "debug_only" and child is True:
                return True
            if _debug_marker(child):
                return True
        return False
    if isinstance(value, list):
        return any(_debug_marker(child) for child in value)
    return False


def verify(tree: Path) -> None:
    if not tree.is_absolute() or tree.is_symlink() or not tree.is_dir():
        raise ArtifactGateError("tree must be an absolute, non-symlink directory")
    for directory, directory_names, filenames in os.walk(tree, followlinks=False):
        directory_path = Path(directory)
        for name in directory_names:
            if (directory_path / name).is_symlink():
                raise ArtifactGateError(f"artifact tree contains a directory symlink: {name}")
        for name in filenames:
            path = directory_path / name
            if path.is_symlink() or not path.is_file():
                raise ArtifactGateError(f"artifact tree contains a non-regular file: {path}")
            if "DEBUG_ONLY" in name.upper():
                raise ArtifactGateError(f"artifact tree contains a DEBUG_ONLY filename: {path}")
            if path.suffix.lower() == ".json":
                if path.stat().st_size > 16 * 1024 * 1024:
                    raise ArtifactGateError(
                        f"JSON artifact is too large for admission inspection: {path}"
                    )
                try:
                    payload = json.loads(path.read_text(encoding="utf-8"))
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise ArtifactGateError(f"JSON artifact cannot be inspected: {path}") from exc
                if _debug_marker(payload):
                    raise ArtifactGateError(f"artifact tree contains DEBUG_ONLY metadata: {path}")
            elif path.suffix.lower() == ".png":
                # The supported debug capture writes an uncompressed PNG comment
                # in addition to its filename and JSON sidecar. This byte check is
                # defense in depth; absence is never used to promote a debug image.
                if b"DEBUG_ONLY" in path.read_bytes():
                    raise ArtifactGateError(
                        f"artifact tree contains DEBUG_ONLY PNG metadata: {path}"
                    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tree", required=True, type=Path)
    args = parser.parse_args()
    try:
        verify(args.tree)
    except (ArtifactGateError, OSError) as exc:
        print(f"PRODUCTION_ARTIFACT_GATE=BLOCKED: {exc}", file=sys.stderr)
        return 2
    print("PRODUCTION_ARTIFACT_GATE=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
