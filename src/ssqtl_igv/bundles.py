from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from .contracts import validate_schema_document
from .utils import atomic_write_json, sha256_file


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class StageBundle:
    """Build one always-emitted stage directory with a final completion marker.

    Nextflow gives every process invocation an isolated task directory.  Files are
    therefore written at their stable final paths and ``stage_result.json`` is
    written last.  Infrastructure exceptions remove the incomplete directory.
    This preserves absolute paths recorded by native IGV while retaining an
    unambiguous completion boundary.
    """

    def __init__(
        self,
        output_dir: str | Path,
        *,
        run_id: str,
        generation_id: str,
        shard_id: str,
        session_id: str,
        task_id: str,
        manifest_order: int,
        attempt: int,
        stage: str,
        input_fingerprint: str,
        schema_dir: str | Path | None = None,
    ) -> None:
        self.destination = Path(output_dir).expanduser().resolve(strict=False)
        self.metadata = {
            "schema_version": "2.0",
            "bundle_version": "1.0",
            "run_id": run_id,
            "generation_id": generation_id,
            "shard_id": shard_id,
            "session_id": session_id,
            "task_id": task_id,
            "manifest_order": int(manifest_order),
            "attempt": int(attempt),
            "stage": stage,
            "input_fingerprint": input_fingerprint,
        }
        self.schema_dir = schema_dir
        self.started_at = _utc_now()
        self.started_monotonic = time.monotonic()
        self.warnings: list[dict[str, str]] = []
        self.failures: list[dict[str, Any]] = []
        self._artifacts: list[tuple[str, Path]] = []
        self._finished = False
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        if self.destination.exists() or self.destination.is_symlink():
            raise FileExistsError(f"stage bundle already exists: {self.destination}")
        self.staging = self.destination
        self.staging.mkdir(mode=0o700)

    def path(self, relative_path: str) -> Path:
        relative = Path(relative_path)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError(f"bundle path must be safe and relative: {relative_path}")
        path = self.staging / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def add_artifact(self, role: str, relative_path: str) -> Path:
        path = self.path(relative_path)
        self._artifacts.append((role, path))
        return path

    def register_existing_artifact(self, role: str, path: str | Path) -> Path:
        value = Path(path).resolve(strict=False)
        try:
            value.relative_to(self.staging)
        except ValueError as exc:
            raise ValueError(f"artifact is outside the stage bundle: {value}") from exc
        self._artifacts.append((role, value))
        return value

    def add_warning(self, code: str, message: str) -> None:
        self.warnings.append({"code": code, "message": message})

    def add_domain_failure(
        self, code: str, message: str, *, rerun_eligible: bool
    ) -> None:
        self.failures.append(
            {
                "class": "DOMAIN",
                "code": code,
                "message": message,
                "rerun_eligible": bool(rerun_eligible),
            }
        )

    def finish(
        self,
        status: str,
        *,
        peak_rss_gb: float | None = None,
        exit_code: int | None = 0,
    ) -> dict[str, Any]:
        artifacts: list[dict[str, Any]] = []
        roles: set[str] = set()
        for role, path in self._artifacts:
            if role in roles:
                raise ValueError(f"duplicate artifact role: {role}")
            roles.add(role)
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"bundle artifact is missing or not a regular file: {path}")
            relative = path.relative_to(self.staging)
            artifacts.append(
                {
                    "role": role,
                    "relative_path": str(relative),
                    "sha256": sha256_file(path),
                    "size": path.stat().st_size,
                }
            )
        result = {
            **self.metadata,
            "status": status,
            "started_at": self.started_at,
            "finished_at": _utc_now(),
            "artifacts": artifacts,
            "warnings": self.warnings,
            "failures": self.failures,
            "telemetry": {
                "wall_time_seconds": max(0.0, time.monotonic() - self.started_monotonic),
                "peak_rss_gb": peak_rss_gb,
                "exit_code": exit_code,
            },
        }
        validate_schema_document(result, "stage-result", schema_dir=self.schema_dir)
        atomic_write_json(self.staging / "stage_result.json", result)
        self._finished = True
        return result

    def cleanup(self) -> None:
        if not self._finished:
            shutil.rmtree(self.staging, ignore_errors=True)

    def __enter__(self) -> "StageBundle":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.cleanup()


def verify_stage_bundle(
    bundle_dir: str | Path,
    *,
    expected_stage: str | None = None,
    expected_task_id: str | None = None,
    expected_input_fingerprint: str | None = None,
    expected_metadata: Mapping[str, Any] | None = None,
    required_roles: tuple[str, ...] = (),
    schema_dir: str | Path | None = None,
) -> tuple[dict[str, Any], dict[str, Path]]:
    """Validate a completed bundle and return its content-addressed artifacts."""

    root = Path(bundle_dir).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise RuntimeError(f"stage bundle is not a directory: {root}")
    result_path = root / "stage_result.json"
    if result_path.is_symlink() or not result_path.is_file():
        raise RuntimeError(f"stage bundle lacks a regular completion marker: {result_path}")
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read stage completion marker: {result_path}: {exc}") from exc
    if not isinstance(result, dict):
        raise RuntimeError(f"stage completion marker is not an object: {result_path}")
    validate_schema_document(result, "stage-result", schema_dir=schema_dir)
    expectations = {
        "stage": expected_stage,
        "task_id": expected_task_id,
        "input_fingerprint": expected_input_fingerprint,
    }
    for field, expected in expectations.items():
        if expected is not None and result[field] != expected:
            raise RuntimeError(
                f"stage bundle {field} mismatch: expected {expected!r}, observed {result[field]!r}"
            )
    for field, expected in dict(expected_metadata or {}).items():
        if field not in result:
            raise ValueError(f"unknown stage-result lineage field: {field}")
        if result[field] != expected:
            raise RuntimeError(
                f"stage bundle {field} mismatch: expected {expected!r}, observed {result[field]!r}"
            )

    artifacts: dict[str, Path] = {}
    for artifact in result["artifacts"]:
        role = artifact["role"]
        if role in artifacts:
            raise RuntimeError(f"duplicate artifact role in stage bundle: {role}")
        candidate = root / artifact["relative_path"]
        if candidate.is_symlink() or not candidate.is_file():
            raise RuntimeError(f"stage artifact is missing or not regular: {candidate}")
        resolved = candidate.resolve(strict=True)
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise RuntimeError(f"stage artifact escapes its bundle: {candidate}") from exc
        if resolved.stat().st_size != int(artifact["size"]):
            raise RuntimeError(f"stage artifact size drift: {candidate}")
        if sha256_file(resolved) != artifact["sha256"]:
            raise RuntimeError(f"stage artifact checksum drift: {candidate}")
        artifacts[role] = resolved
    missing = sorted(set(required_roles) - set(artifacts))
    if missing:
        raise RuntimeError(f"stage bundle lacks required artifacts: {missing}")
    return result, artifacts


def logical_bundle_reference(
    result: dict[str, Any], bundle_dir: str | Path, logical_relative_path: str
) -> dict[str, Any]:
    """Build a stable case-result reference to a prior completion marker."""

    relative = Path(logical_relative_path)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError(f"bundle reference must be safe and relative: {logical_relative_path}")
    result_path = Path(bundle_dir).expanduser().resolve(strict=True) / "stage_result.json"
    return {
        "relative_path": str(relative),
        "sha256": sha256_file(result_path),
        "status": result["status"],
    }
