#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

from ssqtl_igv.project_admission_v3 import (
    admit_project_tasks,
    finalize_cases,
    resolve_project_entry,
)


def _decoded(value: str | None) -> str | None:
    if value is None:
        return None
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding).decode("utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    resolve = commands.add_parser("resolve")
    resolve.add_argument("--project-b64")
    resolve.add_argument("--batch-request-b64")
    resolve.add_argument("--runtime-manifest", required=True)
    resolve.add_argument("--output-dir", required=True)
    resolve.add_argument("--run-id")
    resolve.add_argument("--generation-id")
    resolve.add_argument("--profile", required=True)

    admit = commands.add_parser("admit")
    admit.add_argument("--entry-source", required=True)
    admit.add_argument("--normalization-bundle", required=True)
    admit.add_argument("--execution-policy", required=True)
    admit.add_argument("--output-dir", required=True)
    admit.add_argument("--max-cases-per-shard", type=int, required=True)
    admit.add_argument("--allow-staged-symlink", action="store_true")

    finalize = commands.add_parser("finalize")
    finalize.add_argument("--admission-bundle", required=True)
    finalize.add_argument("--case-bundle-root", required=True)
    finalize.add_argument("--output-dir", required=True)
    finalize.add_argument("--allow-debug-only", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "resolve":
        result = resolve_project_entry(
            args.output_dir,
            runtime_manifest=args.runtime_manifest,
            project=_decoded(args.project_b64),
            batch_request=_decoded(args.batch_request_b64),
            run_id=args.run_id,
            generation_id=args.generation_id,
            profile=args.profile,
        )
    elif args.command == "admit":
        result = admit_project_tasks(
            args.entry_source,
            args.normalization_bundle,
            args.execution_policy,
            args.output_dir,
            max_cases_per_shard=args.max_cases_per_shard,
            allow_staged_symlink=args.allow_staged_symlink,
        )
    else:
        result = finalize_cases(
            args.admission_bundle,
            args.case_bundle_root,
            args.output_dir,
            allow_debug_only=args.allow_debug_only,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
