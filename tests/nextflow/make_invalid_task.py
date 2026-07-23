#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ssqtl_igv.contracts import validate_task_document
from ssqtl_igv.identity import canonical_fingerprint
from ssqtl_igv.utils import read_jsonl, write_jsonl


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Derive a deterministic preflight-domain-failure shard fixture"
    )
    parser.add_argument("canonical_tasks", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    tasks = list(read_jsonl(args.canonical_tasks))
    if len(tasks) != 1:
        raise ValueError(f"expected one canonical task, observed {len(tasks)}")
    task = tasks[0]
    task["preflight_state"] = "CASE_INPUT_INVALID"
    task["preflight_errors"] = [
        {
            "code": "TEST_DECLARED_INPUT_INVALID",
            "message": "synthetic domain failure for Nextflow integration",
        }
    ]
    task["input_fingerprint"] = canonical_fingerprint(task)
    validate_task_document(task)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    write_jsonl(args.output, [task])
    print(json.dumps({"status": "PASS", "output": str(args.output)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
