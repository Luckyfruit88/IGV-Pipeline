#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json

from ssqtl_igv.execution_policy import resolve_execution_policy, validate_execution_policy, write_execution_policy
from ssqtl_igv.utils import atomic_write_json


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Resolve one deterministic IGV Pipeline execution policy"
    )
    parser.add_argument("--output")
    parser.add_argument("--stdout", action="store_true")
    parser.add_argument("--from-json-b64")
    parser.add_argument(
        "--execution-mode",
        choices=("standalone", "docker", "scc", "test"),
        default="standalone",
    )
    parser.add_argument("--max-parallel", default="auto")
    parser.add_argument("--igv-cpus", default="1")
    parser.add_argument("--igv-memory", default="8GiB")
    parser.add_argument("--igv-timeout", default="30m")
    parser.add_argument("--normalization-cpus", default="1")
    parser.add_argument("--normalization-memory", default="12GiB")
    parser.add_argument("--normalization-timeout", default="36h")
    args = parser.parse_args(argv)
    if not args.output and not args.stdout:
        parser.error("--output or --stdout is required")
    if args.from_json_b64:
        policy = validate_execution_policy(json.loads(base64.urlsafe_b64decode(args.from_json_b64)))
        if args.output:
            atomic_write_json(args.output, policy)
        print(json.dumps(policy, sort_keys=True))
        return 0
    options = dict(
        execution_mode=args.execution_mode,
        max_parallel=args.max_parallel,
        igv_cpus=args.igv_cpus,
        igv_memory=args.igv_memory,
        igv_timeout=args.igv_timeout,
        normalization_cpus=args.normalization_cpus,
        normalization_memory=args.normalization_memory,
        normalization_timeout=args.normalization_timeout,
    )
    policy = write_execution_policy(args.output, **options) if args.output else resolve_execution_policy(**options)
    print(json.dumps(policy, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
