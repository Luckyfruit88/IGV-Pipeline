from __future__ import annotations

import json
import os
import re
import shutil
import stat
import uuid
from pathlib import Path
from typing import Any

from .utils import atomic_write_json, sha256_file, sha256_json, utc_now, write_jsonl


_SCHEMA_VERSION_PATTERN = re.compile(
    r'["\']schema_version["\']\s*[:=]\s*["\']([^"\']+)["\']'
)
_PACKAGE_VERSION_PATTERN = re.compile(r"(?m)^\s*version\s*=\s*[\"'](2\.[^\"']+)[\"']")


def _is_within(candidate: Path, root: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _source_files(source: Path) -> list[tuple[Path, Path]]:
    if source.is_symlink():
        raise ValueError(f"v2 source must not be a symlink: {source}")
    if source.is_file():
        return [(source, Path(source.name))]
    if not source.is_dir():
        raise ValueError(f"v2 source must be a regular file or directory: {source}")
    rows: list[tuple[Path, Path]] = []
    for directory, dirnames, filenames in os.walk(source, followlinks=False):
        directory_path = Path(directory)
        for name in list(dirnames):
            path = directory_path / name
            if path.is_symlink():
                raise ValueError(f"v2 source contains a symlinked directory: {path}")
        for name in filenames:
            path = directory_path / name
            if path.is_symlink():
                raise ValueError(f"v2 source contains a symlinked file: {path}")
            metadata = path.stat(follow_symlinks=False)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"v2 source contains a non-regular file: {path}")
            rows.append((path, path.relative_to(source)))
    return sorted(rows, key=lambda row: str(row[1]))


def _stat_identity(path: Path) -> tuple[int, int, int, int, int]:
    value = path.stat(follow_symlinks=False)
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns)


def _declared_versions(path: Path) -> set[str]:
    if path.stat().st_size > 16 * 1024 * 1024:
        return set()
    if path.suffix.lower() not in {".json", ".jsonl", ".toml", ".yaml", ".yml"}:
        return set()
    text = path.read_text(encoding="utf-8", errors="replace")
    versions = {match.group(1).strip() for match in _SCHEMA_VERSION_PATTERN.finditer(text)}
    if path.name == "pyproject.toml":
        versions.update(match.group(1).strip() for match in _PACKAGE_VERSION_PATTERN.finditer(text))
    return versions


def import_v2_read_only(source: str | Path, output: str | Path) -> dict[str, Any]:
    """Inventory a v2 result without copying it or granting v3 execution rights.

    The receipt is deliberately an audit record, not a migration product.  It
    cannot be used as a v3 cache, human-review generation, or publication gate.
    Every source file is re-statted after hashing so a concurrent source change
    fails closed instead of producing a misleading snapshot receipt.
    """

    source_value = Path(source).expanduser()
    if source_value.is_symlink():
        raise ValueError(f"v2 source must not be a symlink: {source_value}")
    source_root = source_value.resolve(strict=True)
    destination_value = Path(output).expanduser()
    if destination_value.is_symlink():
        raise ValueError(f"v2 import output must not be a symlink: {destination_value}")
    destination = destination_value.resolve(strict=False)
    if _is_within(destination, source_root):
        raise ValueError("v2 import output must be outside the read-only source")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"v2 import output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError(f"v2 import parent must be a regular directory: {destination.parent}")

    files = _source_files(source_root)
    if not files:
        raise ValueError("v2 source contains no regular files")
    inventory: list[dict[str, Any]] = []
    source_identities: dict[Path, tuple[int, int, int, int, int]] = {}
    versions: set[str] = set()
    for path, relative in files:
        before = _stat_identity(path)
        digest = sha256_file(path)
        versions.update(_declared_versions(path))
        after = _stat_identity(path)
        if before != after:
            raise RuntimeError(f"v2 source changed while it was being inventoried: {path}")
        source_identities[path] = after
        inventory.append(
            {
                "relative_path": str(relative),
                "size": after[3],
                "mode": oct(stat.S_IMODE(after[2])),
                "mtime_ns": after[4],
                "sha256": digest,
            }
        )
    v2_versions = sorted(version for version in versions if version.startswith("2."))
    if not v2_versions:
        raise ValueError(
            "source does not declare a schema/package version beginning with 2.; "
            "refusing to label it as a v2 import"
        )
    if any(version.startswith("3.") for version in versions):
        raise ValueError("source mixes v2 and v3 declarations")

    source_fingerprint = sha256_json(
        [
            {
                "relative_path": row["relative_path"],
                "size": row["size"],
                "sha256": row["sha256"],
            }
            for row in inventory
        ]
    )
    receipt = {
        "schema_version": "3.0-v2-read-only-import-receipt",
        "receipt_id": f"v2import_{source_fingerprint}",
        "created_at": utc_now(),
        "status": "AUDITED_READ_ONLY",
        "source": str(source_root),
        "source_kind": "directory" if source_root.is_dir() else "file",
        "source_file_count": len(inventory),
        "source_total_bytes": sum(int(row["size"]) for row in inventory),
        "source_fingerprint": source_fingerprint,
        "declared_v2_versions": v2_versions,
        "source_copied": False,
        "permissions": {
            "resume_v3": False,
            "review_v3": False,
            "publish_v3": False,
            "write_source": False,
        },
        "meaning": "audit-only v2 inventory; not a v3 cache, review, or publication receipt",
    }
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        write_jsonl(staging / "source_inventory.jsonl", inventory)
        receipt["inventory_sha256"] = sha256_file(staging / "source_inventory.jsonl")
        atomic_write_json(staging / "import_receipt.json", receipt)
        final_files = _source_files(source_root)
        if [relative for _path, relative in final_files] != [
            relative for _path, relative in files
        ]:
            raise RuntimeError("v2 source file set changed before receipt finalization")
        for path, _relative in files:
            if (
                not path.exists()
                or path.is_symlink()
                or _stat_identity(path) != source_identities[path]
            ):
                raise RuntimeError(f"v2 source changed before receipt finalization: {path}")
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**receipt, "output_dir": str(destination)}
