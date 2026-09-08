#!/usr/bin/env python3
"""Validate a stable version tag against project metadata without a site gate."""
from __future__ import annotations
import argparse
import json
import re
import tomllib
from pathlib import Path


def release_metadata(tag: str, project: Path) -> dict[str, str]:
    if re.fullmatch(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", tag) is None:
        raise ValueError("release tag must be a stable vMAJOR.MINOR.PATCH version")
    version = tag[1:]
    configured = tomllib.loads(project.read_text(encoding="utf-8"))["project"]["version"]
    if configured != version:
        raise ValueError(f"tag version {version} differs from package version {configured}")
    return {"tag": tag, "version": version}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--project", type=Path, default=Path("pyproject.toml"))
    args = parser.parse_args()
    try:
        print(json.dumps(release_metadata(args.tag, args.project), sort_keys=True))
    except (OSError, ValueError, KeyError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
