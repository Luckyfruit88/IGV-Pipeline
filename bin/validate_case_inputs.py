#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ssqtl_igv.case_inputs import validate_case_inputs
from ssqtl_igv.task_io import staged_input_map, task_from_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate one schema-v2 case's staged inputs")
    task_group = parser.add_mutually_exclusive_group(required=True)
    task_group.add_argument("--task")
    task_group.add_argument("--task-manifest")
    parser.add_argument("--task-id")
    parser.add_argument("--input-map")
    parser.add_argument("--staged-input", action="append", default=[])
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--shard-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--samtools", default="samtools")
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--schema-dir")
    args = parser.parse_args()
    if args.task_manifest:
        if not args.task_id:
            parser.error("--task-id is required with --task-manifest")
        task = task_from_manifest(args.task_manifest, args.task_id)
    else:
        task = json.loads(Path(args.task).read_text(encoding="utf-8"))
    if args.input_map:
        input_map = json.loads(Path(args.input_map).read_text(encoding="utf-8"))
    else:
        input_map = staged_input_map(task, args.staged_input)
    samtools_checker = None
    if args.test_mode:
        from ssqtl_igv.test_doubles import fake_samtools_check, require_test_task

        require_test_task(task)
        samtools_checker = fake_samtools_check
    result = validate_case_inputs(
        task,
        input_map,
        args.output_dir,
        shard_id=args.shard_id,
        session_id=args.session_id,
        attempt=args.attempt,
        samtools=args.samtools,
        **({"samtools_checker": samtools_checker} if samtools_checker else {}),
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
