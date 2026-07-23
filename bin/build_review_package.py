#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.review_package import build_review_package


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a path-redacted human review package")
    parser.add_argument("--canonical-tasks", required=True)
    parser.add_argument("--case-results", required=True)
    parser.add_argument("--compose-bundle", action="append", required=True)
    parser.add_argument("--qc-bundle", action="append", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    result = build_review_package(
        args.canonical_tasks,
        args.case_results,
        args.compose_bundle,
        args.qc_bundle,
        args.output_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
