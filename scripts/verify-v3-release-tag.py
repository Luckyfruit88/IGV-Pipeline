#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.release_v3 import verify_release_tag


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify that v3.0.0 points to the intended source commit"
    )
    parser.add_argument("--repository", default=".")
    parser.add_argument("--expected-commit")
    parser.add_argument("--tag", default="v3.0.0")
    args = parser.parse_args()
    result = verify_release_tag(
        args.repository,
        args.expected_commit,
        tag=args.tag,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
