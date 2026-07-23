from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import stat
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


def optional_text(value: Any) -> str:
    """Normalize an optional textual configuration value."""

    return "" if value is None else str(value).strip()


def command_prefix(value: Any, *, default: str | None = None) -> list[str]:
    """Return an argv prefix without interpreting shell syntax."""

    configured = default if value in (None, "") else value
    if (
        isinstance(configured, str)
        and configured.strip()
        and configured == configured.strip()
    ):
        return [configured]
    if (
        isinstance(configured, list)
        and configured
        and all(isinstance(item, str) and item.strip() for item in configured)
    ):
        return configured.copy()
    raise ValueError(f"invalid command configuration: {configured!r}")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_regular_file_bytes(
    path: str | Path,
    *,
    expected_sha256: str | None = None,
    label: str = "artifact",
) -> bytes:
    """Read exact bytes from one regular file without following its final symlink."""

    source = Path(path)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(source, flags)
    except OSError as exc:
        raise ValueError(f"{label} is missing, symlinked, or unreadable: {source}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"{label} is not a regular file: {source}")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        os.close(descriptor)
    payload = b"".join(chunks)
    if expected_sha256 is not None:
        normalized = str(expected_sha256).strip().lower()
        if len(normalized) != 64 or any(c not in "0123456789abcdef" for c in normalized):
            raise ValueError(f"expected {label} SHA-256 is malformed")
        if hashlib.sha256(payload).hexdigest() != normalized:
            raise ValueError(f"{label} SHA-256 mismatch: {source}")
    return payload


def sha256_json(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def resource_contains_remote_url(path: str | Path) -> bool:
    """Inspect local IGV JSON/.genome metadata without expanding large payloads."""

    def contains_remote_reference(text: str) -> bool:
        return re.search(r"(?i)(?:^|[^A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]*://", text) is not None

    source = Path(path)
    if source.suffix.lower() == ".json":
        text = source.read_text(encoding="utf-8", errors="replace")
        return contains_remote_reference(text)
    if source.suffix.lower() != ".genome":
        return False
    if zipfile.is_zipfile(source):
        inspected = 0
        with zipfile.ZipFile(source) as archive:
            for member in archive.infolist():
                if member.is_dir() or member.file_size > 5 * 1024 * 1024:
                    continue
                inspected += member.file_size
                if inspected > 20 * 1024 * 1024:
                    break
                text = archive.read(member).decode("utf-8", errors="replace")
                if contains_remote_reference(text):
                    return True
        return False
    with source.open("rb") as handle:
        text = handle.read(5 * 1024 * 1024).decode("utf-8", errors="replace")
    return contains_remote_reference(text)


def atomic_write_text(path: str | Path, text: str) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n")


def read_jsonl(path: str | Path) -> Iterator[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        for line_number, raw in enumerate(handle, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSONL at {path}:{line_number}: {exc}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"JSONL row must be an object at {path}:{line_number}")
            yield value


def write_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    text = "".join(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n" for row in rows)
    atomic_write_text(path, text)


def write_tsv(path: str | Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def nearest_existing_parent(path: str | Path) -> Path:
    current = Path(path).expanduser().resolve(strict=False)
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def reject_symlink_path_components(path: str | Path, *, label: str) -> Path:
    """Reject writable targets whose lexical path traverses any symlink."""

    declared = Path(path).expanduser()
    if ".." in declared.parts:
        raise ValueError(f"{label} path must not contain '..': {declared}")
    absolute = declared if declared.is_absolute() else Path.cwd() / declared
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{label} path traverses a symlink: {current}")
    return declared


def safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)
