#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.execution_policy import write_execution_policy


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Resolve one deterministic IGV Pipeline execution policy"
    )
    parser.add_argument("--output", required=True)
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
    policy = write_execution_policy(
        args.output,
        execution_mode=args.execution_mode,
        max_parallel=args.max_parallel,
        igv_cpus=args.igv_cpus,
        igv_memory=args.igv_memory,
        igv_timeout=args.igv_timeout,
        normalization_cpus=args.normalization_cpus,
        normalization_memory=args.normalization_memory,
        normalization_timeout=args.normalization_timeout,
    )
    print(json.dumps(policy, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
