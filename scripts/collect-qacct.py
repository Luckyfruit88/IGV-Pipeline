#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ssqtl_igv.config import WorkflowConfig
from ssqtl_igv.qacct import (
    collect_qacct_evidence,
    submitted_job_records,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect immutable qacct evidence for submitted scheduler arrays."
    )
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument(
        "--jobs",
        action="append",
        type=Path,
        help="specific jobs.json; default collects every submitted full-run/resume record",
    )
    args = parser.parse_args()
    config = WorkflowConfig.load(args.config)
    run_root = config.validate_run_root(args.run_root, must_exist=True)
    records = args.jobs or submitted_job_records(run_root)
    if not records:
        raise SystemExit("no submitted scheduler jobs.json records found")
    results = [
        collect_qacct_evidence(
            path,
            run_root=run_root,
            qacct_command=config.get("binaries.qacct"),
        )
        for path in records
    ]
    print(json.dumps({"count": len(results), "evidence": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
