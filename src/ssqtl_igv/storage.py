from __future__ import annotations

import fcntl
import json
import math
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .contracts import STORAGE_GATE_SCHEMA
from .utils import atomic_write_json, command_prefix, optional_text, utc_now


def parse_pquota(output: str, quota_path: str) -> dict[str, float | int | str]:
    """Parse one exact project row from five-column pquota output."""

    matches = [line.split() for line in output.splitlines() if line.split()[:1] == [quota_path]]
    if len(matches) != 1 or len(matches[0]) != 5:
        raise ValueError(f"pquota did not return exactly one five-column row for {quota_path}")
    fields = matches[0]
    try:
        quota_gb = float(fields[1])
        quota_files = int(fields[2])
        used_gb = float(fields[3])
        used_files = int(fields[4])
    except ValueError as exc:
        raise ValueError(f"non-numeric pquota row for {quota_path}: {fields}") from exc
    if quota_gb <= 0 or quota_files <= 0 or used_gb < 0 or used_files < 0:
        raise ValueError(f"invalid pquota values for {quota_path}: {fields}")
    return {
        "quota_path": quota_path,
        "quota_total_gb": quota_gb,
        "quota_used_gb": used_gb,
        "quota_free_gb": max(0.0, quota_gb - used_gb),
        "quota_total_files": quota_files,
        "quota_used_files": used_files,
        "quota_free_files": max(0, quota_files - used_files),
    }


def _existing_ancestor(path: Path) -> Path:
    candidate = path.resolve(strict=False)
    while not candidate.exists():
        parent = candidate.parent
        if parent == candidate:
            raise FileNotFoundError(f"no existing ancestor for storage path: {path}")
        candidate = parent
    return candidate


def _filesystem_observation(path: Path) -> dict[str, int | float | str]:
    ancestor = _existing_ancestor(path)
    stat = ancestor.stat()
    fs = os.statvfs(ancestor)
    return {
        "requested_path": str(path.resolve(strict=False)),
        "observed_at_path": str(ancestor),
        "device": int(stat.st_dev),
        "free_gb": float(fs.f_bavail * fs.f_frsize) / (1024.0**3),
        "free_inodes": int(fs.f_favail),
    }


def _requirements(
    config: WorkflowConfig,
    *,
    total_cases: int,
    remaining_cases: int,
) -> dict[str, int | float | str]:
    if total_cases < 0 or remaining_cases < 0 or remaining_cases > total_cases:
        raise ValueError("storage case counts are inconsistent")
    buffer_factor = float(config.get("storage.remaining_case_buffer_factor", 1.25))
    work_gb = float(config.get("storage.work_gb_per_case", 0.25))
    publish_gb = float(config.get("storage.publish_gb_per_case", 0.03125))
    parallel_gb = float(config.get("storage.scratch_gb_per_parallel_task", 0.5))
    work_inodes = int(config.get("storage.work_inodes_per_case", 64))
    publish_inodes = int(config.get("storage.publish_inodes_per_case", 4))
    reserve_gb = float(config.get("storage.reserve_gb", 1.0))
    reserve_inodes = int(config.get("storage.reserve_inodes", 1000))
    parallel_tasks = int(config.get("scheduler.max_parallel", 1))
    if min(
        buffer_factor,
        work_gb,
        publish_gb,
        parallel_gb,
        work_inodes,
        publish_inodes,
        reserve_gb,
        reserve_inodes,
    ) < 0:
        raise ValueError("storage sizing parameters cannot be negative")
    buffered_remaining = math.ceil(buffer_factor * remaining_cases)
    calculated_gb = (
        buffered_remaining * work_gb
        + total_cases * publish_gb
        + parallel_tasks * parallel_gb
        + reserve_gb
    )
    calculated_inodes = (
        buffered_remaining * work_inodes
        + total_cases * publish_inodes
        + reserve_inodes
    )
    required_gb = max(float(config.get("storage.minimum_free_gb", 0)), calculated_gb)
    required_inodes = max(
        int(config.get("storage.minimum_free_inodes", 0)), calculated_inodes
    )
    return {
        "total_cases": total_cases,
        "remaining_cases": remaining_cases,
        "buffered_remaining_cases": buffered_remaining,
        "parallel_tasks": parallel_tasks,
        "calculated_required_gb": calculated_gb,
        "calculated_required_inodes": calculated_inodes,
        "required_free_gb": required_gb,
        "required_free_inodes": required_inodes,
        "storage_formula": (
            "ceil(buffer_factor*remaining)*work_gb_per_case + "
            "total*publish_gb_per_case + max_parallel*scratch_gb_per_parallel_task + reserve_gb"
        ),
        "inode_formula": (
            "ceil(buffer_factor*remaining)*work_inodes_per_case + "
            "total*publish_inodes_per_case + reserve_inodes"
        ),
    }


def collect_storage_evidence(
    config: WorkflowConfig,
    *,
    execution_root: str | Path | None = None,
    publish_root: str | Path | None = None,
    remaining_cases: int,
    total_cases: int | None = None,
) -> dict[str, Any]:
    """Collect filesystem evidence and optional project-quota evidence."""

    execution = Path(execution_root) if execution_root is not None else config.output_root
    publication = Path(publish_root) if publish_root is not None else config.publish_root
    total = remaining_cases if total_cases is None else total_cases
    requirements = _requirements(
        config,
        total_cases=int(total),
        remaining_cases=int(remaining_cases),
    )
    execution_fs = _filesystem_observation(execution)
    publication_fs = _filesystem_observation(publication)
    provider = optional_text(config.get("storage.provider", "filesystem")).lower()
    quota: dict[str, Any] | None = None
    free_gb = min(float(execution_fs["free_gb"]), float(publication_fs["free_gb"]))
    free_inodes = min(
        int(execution_fs["free_inodes"]), int(publication_fs["free_inodes"])
    )
    if provider == "pquota":
        command = command_prefix(config.get("binaries.pquota"))
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=float(config.get("storage.query_timeout_seconds", 30)),
        )
        if completed.returncode != 0:
            raise RuntimeError(
                "pquota failed: "
                + (completed.stderr.strip() or completed.stdout.strip() or repr(command))
            )
        quota = parse_pquota(
            completed.stdout, optional_text(config.get("storage.project_quota_path"))
        )
        free_gb = min(free_gb, float(quota["quota_free_gb"]))
        free_inodes = min(free_inodes, int(quota["quota_free_files"]))

    same_filesystem = execution_fs["device"] == publication_fs["device"]
    require_same = bool(config.get("storage.require_same_filesystem", False))
    checks = {
        "same_filesystem": same_filesystem or not require_same,
        "required_free_gb": free_gb >= float(requirements["required_free_gb"]),
        "required_free_inodes": free_inodes
        >= int(requirements["required_free_inodes"]),
    }
    evidence = {
        "schema_version": STORAGE_GATE_SCHEMA,
        "observed_at": utc_now(),
        "observed_at_epoch": time.time(),
        "provider": provider,
        "status": "PASS" if all(checks.values()) else "FAIL",
        "execution_root": str(execution.resolve(strict=False)),
        "publish_root": str(publication.resolve(strict=False)),
        "quota": quota,
        "execution_filesystem": execution_fs,
        "publication_filesystem": publication_fs,
        "same_filesystem": same_filesystem,
        "require_same_filesystem": require_same,
        "effective_free_gb": free_gb,
        "effective_free_inodes": free_inodes,
        "minimum_free_gb": float(config.get("storage.minimum_free_gb", 0)),
        "minimum_free_inodes": int(config.get("storage.minimum_free_inodes", 0)),
        **requirements,
        "checks": checks,
    }
    if evidence["status"] != "PASS":
        failed = [name for name, passed in checks.items() if not passed]
        raise ValueError("storage gate failed: " + ", ".join(failed))
    return evidence


def validate_storage_evidence(
    evidence: dict[str, Any],
    config: WorkflowConfig,
    *,
    now_epoch: float | None = None,
    remaining_cases: int,
    total_cases: int | None = None,
) -> None:
    total = remaining_cases if total_cases is None else total_cases
    requirements = _requirements(
        config,
        total_cases=int(total),
        remaining_cases=int(remaining_cases),
    )
    expected = {
        "schema_version": STORAGE_GATE_SCHEMA,
        "provider": optional_text(config.get("storage.provider", "filesystem")).lower(),
        "status": "PASS",
        "execution_root": str(config.output_root.resolve(strict=False)),
        "publish_root": str(config.publish_root.resolve(strict=False)),
        "minimum_free_gb": float(config.get("storage.minimum_free_gb", 0)),
        "minimum_free_inodes": int(config.get("storage.minimum_free_inodes", 0)),
        **requirements,
    }
    failures = [key for key, value in expected.items() if evidence.get(key) != value]
    if evidence.get("require_same_filesystem") and not evidence.get("same_filesystem"):
        failures.append("same_filesystem")
    if float(evidence.get("effective_free_gb", -1)) < float(
        requirements["required_free_gb"]
    ):
        failures.append("effective_free_gb")
    if int(evidence.get("effective_free_inodes", -1)) < int(
        requirements["required_free_inodes"]
    ):
        failures.append("effective_free_inodes")
    if expected["provider"] == "pquota":
        quota = evidence.get("quota") or {}
        if quota.get("quota_path") != optional_text(
            config.get("storage.project_quota_path")
        ):
            failures.append("quota_path")
    observed = float(evidence.get("observed_at_epoch", 0))
    age = (time.time() if now_epoch is None else now_epoch) - observed
    maximum_age = float(config.get("storage.gate_max_age_seconds", 1800))
    if observed <= 0 or age < -5 or age > maximum_age:
        failures.append("freshness")
    if failures:
        raise ValueError(
            "storage evidence is invalid: " + ", ".join(sorted(set(failures)))
        )


def cached_storage_low_watermark(
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    remaining_cases: int,
    total_cases: int | None = None,
    cache_seconds: float = 300.0,
) -> dict[str, Any]:
    """Serialize case checks and reuse only a fresh, conservative PASS observation."""

    root = Path(run_root)
    gate_root = root / ".work" / "gates"
    gate_root.mkdir(parents=True, exist_ok=True)
    evidence_path = gate_root / "storage_low_watermark.json"
    lock_path = gate_root / "storage_low_watermark.lock"
    total = remaining_cases if total_cases is None else total_cases
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            if evidence_path.is_file():
                try:
                    cached = json.loads(evidence_path.read_text(encoding="utf-8"))
                    age = time.time() - float(cached.get("observed_at_epoch", 0))
                    if (
                        int(cached.get("remaining_cases", -1)) >= remaining_cases
                        and int(cached.get("total_cases", -1)) == total
                        and 0 <= age <= cache_seconds
                    ):
                        validate_storage_evidence(
                            cached,
                            config,
                            remaining_cases=int(cached["remaining_cases"]),
                            total_cases=total,
                        )
                        return {**cached, "cache": "HIT", "cache_age_seconds": age}
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    pass
            evidence = collect_storage_evidence(
                config,
                execution_root=config.output_root,
                publish_root=config.publish_root,
                remaining_cases=remaining_cases,
                total_cases=total,
            )
            validate_storage_evidence(
                evidence,
                config,
                remaining_cases=remaining_cases,
                total_cases=total,
            )
            atomic_write_json(evidence_path, evidence)
            return {**evidence, "cache": "MISS", "cache_age_seconds": 0.0}
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
