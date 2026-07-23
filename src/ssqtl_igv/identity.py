from __future__ import annotations

from pathlib import Path
from typing import Any

from .utils import safe_name, sha256_json


def file_identity(path: str | Path, *, sha256: str | None = None) -> dict[str, Any]:
    """Return the stable metadata identity used in canonical provenance.

    Standard Nextflow ``path`` caching observes file metadata rather than
    hashing large track contents. This record mirrors that boundary for
    planning/execution drift and remains useful in ledgers; callers may add a
    SHA-256 when a resource requires content-level pinning.
    """

    resolved = Path(path).expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"input target must be a regular file: {resolved}")
    stat = resolved.stat()
    identity: dict[str, Any] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
    if sha256:
        identity["sha256"] = sha256.lower()
    return identity


def staged_name(source: str | Path, *, role: str, discriminator: str = "") -> str:
    """Create a deterministic collision-resistant task-local basename."""

    path = Path(source)
    suffix = "".join(path.suffixes)
    stem = path.name[: -len(suffix)] if suffix else path.name
    digest = sha256_json(
        {"source": str(path.expanduser().resolve(strict=False)), "role": role, "key": discriminator}
    )[:12]
    safe_stem = safe_name(stem)[:100]
    safe_suffix = "".join(character for character in suffix if character.isalnum() or character in "._-")
    return f"{safe_name(role)[:30]}_{safe_stem}_{digest}{safe_suffix}"


def canonical_fingerprint(value: dict[str, Any], *, field: str = "input_fingerprint") -> str:
    return sha256_json({key: item for key, item in value.items() if key != field})


def task_set_fingerprint(tasks: list[dict[str, Any]]) -> str:
    return sha256_json(
        [
            {
                "task_id": task["task_id"],
                "manifest_order": task["manifest_order"],
                "input_fingerprint": task["input_fingerprint"],
            }
            for task in sorted(tasks, key=lambda row: int(row["manifest_order"]))
        ]
    )
