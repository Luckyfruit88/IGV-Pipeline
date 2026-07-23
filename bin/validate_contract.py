#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from ssqtl_igv.contracts import (
    validate_case_result_document,
    validate_review_document,
    validate_schema_document,
    validate_task_document,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate one IGV workflow JSON contract")
    parser.add_argument(
        "--schema",
        required=True,
        choices=("task", "stage-result", "case-result", "review", "shard-ledger", "run-provenance"),
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--schema-dir")
    args = parser.parse_args()
    document = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if args.schema == "task":
        validate_task_document(document, schema_dir=args.schema_dir)
    elif args.schema == "case-result":
        validate_case_result_document(document, schema_dir=args.schema_dir)
    elif args.schema == "review":
        validate_review_document(document, schema_dir=args.schema_dir)
    else:
        validate_schema_document(document, args.schema, schema_dir=args.schema_dir)
    print(json.dumps({"schema": args.schema, "status": "PASS"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
