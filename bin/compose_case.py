#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ssqtl_igv.composition import compose_case
from ssqtl_igv.task_io import task_from_manifest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compose one native IGV capture with its exact violin page"
    )
    task_group = parser.add_mutually_exclusive_group(required=True)
    task_group.add_argument("--task")
    task_group.add_argument("--task-manifest")
    parser.add_argument("--task-id")
    parser.add_argument("--render-bundle", required=True)
    parser.add_argument("--violin-pdf", required=True)
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
        task = json.loads(Path(args.task).read_text(encoding="utf-8"))
    pdf_renderer = None
    if args.test_mode:
        from ssqtl_igv.test_doubles import fake_pdf_page, require_test_task

        require_test_task(task)
        pdf_renderer = fake_pdf_page
    result = compose_case(
        task,
        args.render_bundle,
        args.violin_pdf,
        args.params,
        args.output_dir,
        shard_id=args.shard_id,
        session_id=args.session_id,
        attempt=args.attempt,
        **({"pdf_renderer": pdf_renderer} if pdf_renderer else {}),
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
