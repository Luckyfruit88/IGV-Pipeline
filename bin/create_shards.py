#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.sharding import create_shards


def main() -> int:
    parser = argparse.ArgumentParser(description="Create deterministic bounded shard manifests")
    parser.add_argument("--tasks", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--max-cases-per-shard", type=int, default=256)
    parser.add_argument("--score-budget-seconds", type=float, default=23040.0)
    args = parser.parse_args()
    report = create_shards(
        args.tasks,
        args.output_dir,
        max_cases_per_shard=args.max_cases_per_shard,
        score_budget_seconds=args.score_budget_seconds,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
