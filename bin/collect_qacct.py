#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.accounting import collect_nextflow_accounting


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze qacct evidence for Nextflow tasks")
    parser.add_argument("--trace", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--qacct", default="qacct")
    parser.add_argument("--skip-qacct", action="store_true")
    parser.add_argument("--test-mode", action="store_true")
    args = parser.parse_args()
    result = collect_nextflow_accounting(
        args.trace,
        args.output_dir,
        qacct_command=args.qacct,
        skip_qacct=args.skip_qacct,
        test_mode=args.test_mode,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
