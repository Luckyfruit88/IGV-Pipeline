#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from ssqtl_igv.runtime_identity import (
    RUNTIME_MANIFEST_IMAGE_PATH,
    validate_runtime_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the embedded unsigned IGV Pipeline runtime manifest"
    )
    parser.add_argument(
        "--runtime-manifest",
        default=None,
        help=f"embedded manifest (default: {RUNTIME_MANIFEST_IMAGE_PATH})",
    )
    parser.add_argument("--expected-manifest-sha256")
    parser.add_argument("--expected-fingerprint-sha256")
    parser.add_argument("--manifest-schema")
    parser.add_argument("--materials-lock")
    parser.add_argument("--explicit-lock-dir")
    parser.add_argument("--runtime-config")
    parser.add_argument("--observed-oci-digest")
    parser.add_argument("--observed-sif-sha256")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--allow-staged-symlink", action="store_true")
    args = parser.parse_args()

    report = validate_runtime_manifest(
        args.runtime_manifest or RUNTIME_MANIFEST_IMAGE_PATH,
        expected_manifest_sha256=args.expected_manifest_sha256,
        expected_fingerprint_sha256=args.expected_fingerprint_sha256,
        schema_path=args.manifest_schema,
        materials_path=args.materials_lock,
        explicit_lock_dir=args.explicit_lock_dir,
        runtime_config_path=args.runtime_config,
        observed_oci_digest=args.observed_oci_digest,
        observed_sif_sha256=args.observed_sif_sha256,
        allow_staged_symlink=args.allow_staged_symlink,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
