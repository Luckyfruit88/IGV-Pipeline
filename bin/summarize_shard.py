#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.reconcile import summarize_shard


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile one shard's terminal QC bundles")
    parser.add_argument("--shard-manifest", required=True)
    parser.add_argument("--qc-bundle", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--shard-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--pipeline-commit", required=True)
    parser.add_argument("--controller-job-id")
    parser.add_argument("--ledger-sequence", type=int, default=1)
    parser.add_argument("--schema-dir")
    args = parser.parse_args()
    result = summarize_shard(
        args.shard_manifest,
        args.qc_bundle,
        args.output_dir,
        shard_id=args.shard_id,
        session_id=args.session_id,
        pipeline_commit=args.pipeline_commit,
        controller_job_id=args.controller_job_id,
        ledger_sequence=args.ledger_sequence,
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
