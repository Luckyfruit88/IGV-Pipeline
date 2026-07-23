#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.publication import publish_reviewed


def main() -> int:
    parser = argparse.ArgumentParser(description="Atomically publish reviewed evidence")
    parser.add_argument("--review-package", required=True)
    parser.add_argument("--validated-reviews", required=True)
    parser.add_argument("--destination", required=True)
    args = parser.parse_args()
    result = publish_reviewed(
        args.review_package,
        args.validated_reviews,
        args.destination,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
