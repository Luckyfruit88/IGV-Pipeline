#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.reconcile import aggregate_run


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile every immutable shard summary")
    parser.add_argument("--canonical-tasks", required=True)
    parser.add_argument("--shard-plan", required=True)
    parser.add_argument("--shard-summary", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--schema-dir")
    args = parser.parse_args()
    result = aggregate_run(
        args.canonical_tasks,
        args.shard_plan,
        args.shard_summary,
        args.output_dir,
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
