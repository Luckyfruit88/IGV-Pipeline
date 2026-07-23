#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.environment import validate_environment


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze an IGV workflow runtime fingerprint")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--phase", required=True)
    parser.add_argument("--pipeline-commit", required=True)
    parser.add_argument("--nextflow-version", required=True)
    parser.add_argument("--require-command", action="append", default=[])
    parser.add_argument("--helper-sif")
    parser.add_argument("--helper-sif-sha256")
    parser.add_argument("--test-mode", action="store_true")
    args = parser.parse_args()
    result = validate_environment(
        args.output_dir,
        phase=args.phase,
        pipeline_commit=args.pipeline_commit,
        nextflow_version=args.nextflow_version,
        required_commands=args.require_command,
        helper_sif=args.helper_sif,
        helper_sif_sha256=args.helper_sif_sha256,
        test_mode=args.test_mode,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
