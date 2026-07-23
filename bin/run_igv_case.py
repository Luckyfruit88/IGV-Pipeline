#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ssqtl_igv.render_case import render_case
from ssqtl_igv.bundles import verify_stage_bundle
from ssqtl_igv.task_io import staged_input_map, task_from_manifest


def _json(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Render one validated case with native IGV")
    task_group = parser.add_mutually_exclusive_group(required=True)
    task_group.add_argument("--task")
    task_group.add_argument("--task-manifest")
    parser.add_argument("--task-id")
    parser.add_argument("--input-map")
    parser.add_argument("--staged-input", action="append", default=[])
    validation_group = parser.add_mutually_exclusive_group(required=True)
    validation_group.add_argument("--validation-result")
    validation_group.add_argument("--validation-bundle")
    parser.add_argument("--params", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--shard-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--attempt", type=int, default=1)
    parser.add_argument("--test-mode", action="store_true")
    parser.add_argument("--schema-dir")
    args = parser.parse_args()
    if args.task_manifest:
        if not args.task_id:
            parser.error("--task-id is required with --task-manifest")
        task = task_from_manifest(args.task_manifest, args.task_id)
    else:
        task = _json(args.task)
    input_map = _json(args.input_map) if args.input_map else staged_input_map(task, args.staged_input)
    if args.validation_bundle:
        validation, _artifacts = verify_stage_bundle(
            args.validation_bundle,
            expected_stage="VALIDATE_CASE_INPUTS",
            expected_task_id=task["task_id"],
            expected_input_fingerprint=task["input_fingerprint"],
            schema_dir=args.schema_dir,
        )
    else:
        validation = _json(args.validation_result)
    desktop_runner = None
    if args.test_mode:
        from ssqtl_igv.test_doubles import fake_desktop_session, require_test_task

        require_test_task(task)
        desktop_runner = fake_desktop_session
    result = render_case(
        task,
        input_map,
        validation,
        args.params,
        args.output_dir,
        shard_id=args.shard_id,
        session_id=args.session_id,
        attempt=args.attempt,
        **({"desktop_runner": desktop_runner} if desktop_runner else {}),
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
