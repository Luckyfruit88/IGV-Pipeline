#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.normalize import normalize_manifest


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="Create a schema-v2 canonical IGV task bundle")
    value.add_argument("--params", required=True)
    value.add_argument("--output-dir", required=True)
    value.add_argument("--run-id", required=True)
    value.add_argument("--generation-id", required=True)
    value.add_argument("--associations")
    value.add_argument("--prepared-cases")
    value.add_argument("--prepared-samples")
    value.add_argument("--rds-dir")
    value.add_argument("--bam-lookup")
    value.add_argument("--violin-dir")
    value.add_argument("--genome-definition")
    value.add_argument("--fasta")
    value.add_argument("--fai")
    value.add_argument("--cytoband")
    value.add_argument("--annotation")
    value.add_argument("--r-wrapper", required=True)
    value.add_argument("--r-implementation", required=True)
    value.add_argument("--expected-case-count", type=int)
    value.add_argument("--estimated-runtime-seconds", type=float, default=90.0)
    return value


def main() -> int:
    args = parser().parse_args()
    if bool(args.prepared_cases) != bool(args.prepared_samples):
        parser().error("--prepared-cases and --prepared-samples must be supplied together")
    report = normalize_manifest(
        args.params,
        args.output_dir,
        run_id=args.run_id,
        generation_id=args.generation_id,
        associations=args.associations,
        prepared_cases=args.prepared_cases,
        prepared_samples=args.prepared_samples,
        rds_dir=args.rds_dir,
        bam_lookup=args.bam_lookup,
        violin_dir=args.violin_dir,
        genome_definition=args.genome_definition,
        fasta=args.fasta,
        fai=args.fai,
        cytoband=args.cytoband,
        annotation=args.annotation,
        r_wrapper=args.r_wrapper,
        r_implementation=args.r_implementation,
        expected_case_count=args.expected_case_count,
        estimated_runtime_seconds=args.estimated_runtime_seconds,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
