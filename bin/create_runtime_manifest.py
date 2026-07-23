#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from ssqtl_igv.runtime_identity import create_runtime_manifest


def _write_new(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o444)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except BaseException:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create the unsigned runtime manifest embedded in the OCI"
    )
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--source-tree", required=True)
    parser.add_argument("--materials-lock", required=True)
    parser.add_argument("--runtime-config", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    materials_path = Path(args.materials_lock)
    materials = json.loads(materials_path.read_text(encoding="utf-8"))
    tools = materials.get("tool_contract")
    if not isinstance(tools, dict):
        raise TypeError("runtime material lock lacks tool_contract")
    manifest = create_runtime_manifest(
        source_commit=args.source_commit,
        source_tree=args.source_tree,
        tools={str(key): str(value) for key, value in tools.items()},
        materials_path=materials_path,
        runtime_config_path=args.runtime_config,
    )
    _write_new(Path(args.output), manifest)
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
