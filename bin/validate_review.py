#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.review_records import validate_reviews


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate append-only human review records")
    parser.add_argument("--review-contract", required=True)
    parser.add_argument("--reviews", required=True)
    parser.add_argument("--case-results", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--schema-dir")
    args = parser.parse_args()
    result = validate_reviews(
        args.review_contract,
        args.reviews,
        args.case_results,
        args.output_dir,
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
