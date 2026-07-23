#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def _object(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"gate document must be a JSON object: {path}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Assert a completed Nextflow control gate")
    parser.add_argument("--kind", required=True, choices=("environment", "accounting"))
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--phase")
    parser.add_argument("--allow-test", action="store_true")
    args = parser.parse_args()

    root = Path(args.bundle).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"gate bundle is not a directory: {root}")
    filename = "environment.json" if args.kind == "environment" else "accounting.json"
    document = _object(root / filename)
    if args.kind == "environment":
        allowed = {"PASS"}
        if args.allow_test:
            allowed.add("PASS_WITH_TEST_RELAXATIONS")
        if document.get("status") not in allowed:
            raise ValueError(f"environment gate did not pass: {document.get('status')}")
        if args.phase and document.get("phase") != args.phase:
            raise ValueError("environment gate phase differs from the consuming workflow")
    else:
        allowed = {"PASS"}
        if args.allow_test:
            allowed.add("SKIPPED_TEST_MODE")
        if document.get("status") not in allowed:
            raise ValueError(f"accounting gate did not pass: {document.get('status')}")
    print(json.dumps({"kind": args.kind, "status": "PASS"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
