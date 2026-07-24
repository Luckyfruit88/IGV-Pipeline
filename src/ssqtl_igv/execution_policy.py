from __future__ import annotations

import math
import os
import re
import subprocess
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .contracts import validate_schema_document
from .utils import atomic_write_json, sha256_json


GIB = 1024**3
MIB = 1024**2
MAX_PARALLEL = 8
RENDER_RETRY_EXIT_CODE = 75
RENDER_RETRY_EXIT_STATUSES = (75, 137, 143)
RENDER_ATTEMPT_FACTORS = (1, 2, 3)
MINIMUM_IGV_MEMORY_BYTES = 3 * GIB
IGV_NON_HEAP_RESERVE_BYTES = 2 * GIB

_MEMORY = re.compile(
    r"^\s*(?P<value>[0-9]+(?:\.[0-9]+)?)\s*"
    r"(?P<unit>b|kb|kib|mb|mib|gb|gib|tb|tib)?\s*$",
    re.IGNORECASE,
)
_DURATION = re.compile(
    r"^\s*(?P<value>[0-9]+(?:\.[0-9]+)?)\s*"
    r"(?P<unit>s|sec|secs|second|seconds|m|min|mins|minute|minutes|"
    r"h|hr|hrs|hour|hours|d|day|days)?\s*$",
    re.IGNORECASE,
)
_SCC_ENVELOPE_KEYS = (
    "IGV_SCC_SLOTS",
    "IGV_SCC_MEMORY_PER_SLOT",
    "IGV_SCC_WALLTIME",
)


def parse_memory_bytes(value: str | int | float, *, label: str) -> int:
    """Parse an explicit memory value without silently assuming decimal GB."""

    if isinstance(value, bool):
        raise ValueError(f"{label} must be a memory value")
    if isinstance(value, int):
        if value <= 0:
            raise ValueError(f"{label} must be positive")
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or value <= 0 or not value.is_integer():
            raise ValueError(f"{label} numeric bytes must be a positive integer")
        return int(value)
    match = _MEMORY.fullmatch(str(value))
    if not match:
        raise ValueError(f"{label} has an invalid memory value: {value!r}")
    number = float(match.group("value"))
    unit = (match.group("unit") or "b").lower()
    factors = {
        "b": 1,
        "kb": 1000,
        "kib": 1024,
        "mb": 1000**2,
        "mib": 1024**2,
        "gb": 1000**3,
        "gib": GIB,
        "tb": 1000**4,
        "tib": 1024**4,
    }
    result = number * factors[unit]
    if not math.isfinite(result) or result <= 0 or not result.is_integer():
        raise ValueError(f"{label} must resolve to a positive whole number of bytes")
    return int(result)


def parse_duration_seconds(value: str | int | float, *, label: str) -> int:
    """Parse Nextflow-style durations and SGE HH:MM:SS walltimes."""

    if isinstance(value, bool):
        raise ValueError(f"{label} must be a duration")
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)) or float(value) <= 0:
            raise ValueError(f"{label} must be positive")
        seconds = float(value)
    else:
        raw = str(value).strip()
        if re.fullmatch(r"[0-9]+:[0-5][0-9]:[0-5][0-9]", raw):
            hours, minutes, seconds_part = (int(part) for part in raw.split(":"))
            seconds = float(hours * 3600 + minutes * 60 + seconds_part)
        else:
            match = _DURATION.fullmatch(raw)
            if not match:
                raise ValueError(f"{label} has an invalid duration: {value!r}")
            unit = (match.group("unit") or "s").lower()
            factors = {
                "s": 1,
                "sec": 1,
                "secs": 1,
                "second": 1,
                "seconds": 1,
                "m": 60,
                "min": 60,
                "mins": 60,
                "minute": 60,
                "minutes": 60,
                "h": 3600,
                "hr": 3600,
                "hrs": 3600,
                "hour": 3600,
                "hours": 3600,
                "d": 86400,
                "day": 86400,
                "days": 86400,
            }
            seconds = float(match.group("value")) * factors[unit]
    if not math.isfinite(seconds) or seconds <= 0 or not seconds.is_integer():
        raise ValueError(f"{label} must resolve to a positive whole number of seconds")
    return int(seconds)


def _positive_int(value: Any, *, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a positive integer")
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a positive integer") from exc
    if parsed <= 0 or str(parsed) != str(value).strip():
        raise ValueError(f"{label} must be a positive integer")
    return parsed


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, PermissionError, OSError, UnicodeDecodeError):
        return None


def _cpuset_count(value: str | None) -> int | None:
    if not value:
        return None
    selected: set[int] = set()
    try:
        for item in value.split(","):
            bounds = item.strip().split("-", 1)
            start = int(bounds[0])
            end = int(bounds[-1])
            if start < 0 or end < start:
                return None
            selected.update(range(start, end + 1))
    except (TypeError, ValueError):
        return None
    return len(selected) or None


def _cgroup_cpu_candidates(root: Path) -> list[tuple[str, int]]:
    candidates: list[tuple[str, int]] = []
    cpu_max = _read_text(root / "cpu.max")
    if cpu_max:
        fields = cpu_max.split()
        if len(fields) == 2 and fields[0] != "max":
            try:
                quota, period = (int(field) for field in fields)
            except ValueError:
                pass
            else:
                if quota > 0 and period > 0:
                    candidates.append(("cgroup_v2_cpu_quota", max(1, quota // period)))
    else:
        quota_text = _read_text(root / "cpu" / "cpu.cfs_quota_us")
        period_text = _read_text(root / "cpu" / "cpu.cfs_period_us")
        try:
            quota = int(quota_text or "-1")
            period = int(period_text or "0")
        except ValueError:
            pass
        else:
            if quota > 0 and period > 0:
                candidates.append(("cgroup_v1_cpu_quota", max(1, quota // period)))
    for label, relative in (
        ("cgroup_v2_cpuset", "cpuset.cpus.effective"),
        ("cgroup_cpuset", "cpuset.cpus"),
        ("cgroup_v1_cpuset", "cpuset/cpuset.cpus"),
    ):
        count = _cpuset_count(_read_text(root / relative))
        if count:
            candidates.append((label, count))
            break
    return candidates


def _physical_memory_bytes() -> tuple[str, int] | None:
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        page_count = int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, TypeError, ValueError):
        pass
    else:
        if page_size > 0 and page_count > 0:
            return "sysconf_physical_memory", page_size * page_count
    try:
        completed = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        value = int(completed.stdout.strip()) if completed.returncode == 0 else 0
    except (FileNotFoundError, OSError, subprocess.TimeoutExpired, ValueError):
        return None
    return ("sysctl_hw_memsize", value) if value > 0 else None


def _cgroup_memory_candidates(root: Path) -> list[tuple[str, int]]:
    candidates: list[tuple[str, int]] = []
    for label, relative in (
        ("cgroup_v2_memory_max", "memory.max"),
        ("cgroup_v1_memory_limit", "memory/memory.limit_in_bytes"),
    ):
        raw = _read_text(root / relative)
        if not raw or raw == "max":
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        # v1 uses very large page-aligned values to represent "unlimited".
        if 0 < value < 2**60:
            candidates.append((label, value))
    return candidates


def _scc_envelope(
    environ: Mapping[str, str],
    *,
    execution_mode: str,
) -> dict[str, Any] | None:
    present = {key for key in _SCC_ENVELOPE_KEYS if str(environ.get(key, "")).strip()}
    requested = execution_mode == "scc" or bool(present)
    if not requested:
        return None
    if present != set(_SCC_ENVELOPE_KEYS):
        missing = sorted(set(_SCC_ENVELOPE_KEYS) - present)
        raise ValueError(
            "SCC allocation envelope is incomplete; missing " + ", ".join(missing)
        )
    slots = _positive_int(environ["IGV_SCC_SLOTS"], label="IGV_SCC_SLOTS")
    memory_per_slot = parse_memory_bytes(
        environ["IGV_SCC_MEMORY_PER_SLOT"], label="IGV_SCC_MEMORY_PER_SLOT"
    )
    walltime = parse_duration_seconds(
        environ["IGV_SCC_WALLTIME"], label="IGV_SCC_WALLTIME"
    )
    native_slots = str(environ.get("NSLOTS", "")).strip()
    if native_slots and _positive_int(native_slots, label="NSLOTS") != slots:
        raise ValueError("IGV_SCC_SLOTS differs from the scheduler NSLOTS value")
    return {
        "slots": slots,
        "memory_per_slot_bytes": memory_per_slot,
        "total_memory_bytes": slots * memory_per_slot,
        "walltime_seconds": walltime,
    }


def detect_resource_envelope(
    *,
    execution_mode: str = "standalone",
    environ: Mapping[str, str] | None = None,
    cgroup_root: str | Path = "/sys/fs/cgroup",
) -> dict[str, Any]:
    """Detect the smallest observable CPU/memory ceiling.

    The SCC allocation contract is authoritative when present, while a smaller
    cgroup limit remains a real runtime ceiling and is therefore also honored.
    """

    environment = dict(os.environ if environ is None else environ)
    cpu_candidates: list[tuple[str, int]] = []
    try:
        affinity = len(os.sched_getaffinity(0))
    except (AttributeError, OSError):
        affinity = 0
    if affinity > 0:
        cpu_candidates.append(("sched_affinity", affinity))
    cpu_count = os.cpu_count()
    if cpu_count and cpu_count > 0:
        cpu_candidates.append(("os_cpu_count", int(cpu_count)))
    root = Path(cgroup_root)
    cpu_candidates.extend(_cgroup_cpu_candidates(root))

    memory_candidates = _cgroup_memory_candidates(root)
    physical = _physical_memory_bytes()
    if physical:
        memory_candidates.append(physical)

    scc = _scc_envelope(environment, execution_mode=execution_mode)
    if scc:
        cpu_candidates.append(("scc_allocation", scc["slots"]))
        memory_candidates.append(("scc_allocation", scc["total_memory_bytes"]))
    if not cpu_candidates:
        raise ValueError("available CPU slots could not be detected")

    cpu_slots = min(value for _source, value in cpu_candidates)
    memory_bytes = (
        min(value for _source, value in memory_candidates) if memory_candidates else None
    )
    return {
        "execution_mode": execution_mode,
        "cpu_slots": cpu_slots,
        "memory_bytes": memory_bytes,
        "cpu_observations": [
            {"source": source, "slots": value}
            for source, value in sorted(cpu_candidates)
        ],
        "memory_observations": [
            {"source": source, "bytes": value}
            for source, value in sorted(memory_candidates)
        ],
        "scc_allocation": scc,
    }


def _heap_argument(memory_bytes: int) -> tuple[int, str]:
    heap_bytes = memory_bytes - IGV_NON_HEAP_RESERVE_BYTES
    if heap_bytes <= 0:
        raise ValueError("IGV memory leaves no room for the fixed 2 GiB non-heap reserve")
    if heap_bytes % GIB == 0:
        return heap_bytes, f"{heap_bytes // GIB}g"
    if heap_bytes % MIB == 0:
        return heap_bytes, f"{heap_bytes // MIB}m"
    raise ValueError("IGV memory must resolve to a whole number of MiB")


def resolve_execution_policy(
    *,
    max_parallel: str | int = "auto",
    igv_cpus: int | str = 1,
    igv_memory: str | int = "8GiB",
    igv_timeout: str | int = "30m",
    normalization_cpus: int | str = 1,
    normalization_memory: str | int = "12GiB",
    normalization_timeout: str | int = "36h",
    execution_mode: str = "standalone",
    environ: Mapping[str, str] | None = None,
    cgroup_root: str | Path = "/sys/fs/cgroup",
    envelope: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    mode = str(execution_mode).strip().lower()
    if mode not in {"standalone", "docker", "scc", "test"}:
        raise ValueError("execution_mode must be standalone, docker, scc, or test")
    render_cpus = _positive_int(igv_cpus, label="igv_cpus")
    render_memory = parse_memory_bytes(igv_memory, label="igv_memory")
    render_timeout = parse_duration_seconds(igv_timeout, label="igv_timeout")
    normalize_cpus = _positive_int(normalization_cpus, label="normalization_cpus")
    normalize_memory = parse_memory_bytes(
        normalization_memory, label="normalization_memory"
    )
    normalize_timeout = parse_duration_seconds(
        normalization_timeout, label="normalization_timeout"
    )
    if render_memory < MINIMUM_IGV_MEMORY_BYTES:
        raise ValueError("igv_memory must be at least 3 GiB")
    observed = dict(
        envelope
        if envelope is not None
        else detect_resource_envelope(
            execution_mode=mode, environ=environ, cgroup_root=cgroup_root
        )
    )
    cpu_slots = _positive_int(observed.get("cpu_slots"), label="detected CPU slots")
    detected_memory = observed.get("memory_bytes")
    if detected_memory is not None:
        detected_memory = _positive_int(
            detected_memory, label="detected memory bytes"
        )
        reserve = max(2 * GIB, math.ceil(detected_memory * 0.10))
        usable_memory = detected_memory - reserve
        if usable_memory < render_memory:
            memory_capacity = 0
        else:
            memory_capacity = usable_memory // render_memory
    else:
        reserve = None
        usable_memory = None
        memory_capacity = 1
    cpu_capacity = cpu_slots // render_cpus
    safe_parallel = min(MAX_PARALLEL, cpu_capacity, memory_capacity)
    if safe_parallel < 1:
        raise ValueError(
            "available resource envelope cannot admit one base IGV render task"
        )
    requested = str(max_parallel).strip().lower()
    if requested == "auto":
        effective_parallel = safe_parallel
    else:
        explicit = _positive_int(max_parallel, label="max_parallel")
        if explicit > MAX_PARALLEL:
            raise ValueError(f"max_parallel must not exceed {MAX_PARALLEL}")
        if explicit > safe_parallel:
            raise ValueError(
                f"max_parallel={explicit} exceeds the detected safe envelope "
                f"of {safe_parallel}"
            )
        effective_parallel = explicit

    attempts: list[dict[str, Any]] = []
    for attempt, factor in enumerate(RENDER_ATTEMPT_FACTORS, 1):
        memory_bytes = render_memory * factor
        timeout_seconds = render_timeout * factor
        heap_bytes, heap_argument = _heap_argument(memory_bytes)
        attempts.append(
            {
                "attempt": attempt,
                "cpus": render_cpus,
                "memory_bytes": memory_bytes,
                "timeout_seconds": timeout_seconds,
                "igv_heap_bytes": heap_bytes,
                "igv_heap_argument": heap_argument,
            }
        )

    body = {
        "schema_version": "3.0-execution-policy",
        "execution_mode": mode,
        "resource_envelope": {
            "cpu_slots": cpu_slots,
            "memory_bytes": detected_memory,
            "memory_reserve_bytes": reserve,
            "usable_memory_bytes": usable_memory,
            "memory_detection_reliable": detected_memory is not None,
            "cpu_observations": list(observed.get("cpu_observations", [])),
            "memory_observations": list(observed.get("memory_observations", [])),
            "scc_allocation": observed.get("scc_allocation"),
        },
        "concurrency": {
            "requested": requested,
            "hard_max": MAX_PARALLEL,
            "cpu_capacity": cpu_capacity,
            "memory_capacity": memory_capacity,
            "effective_max_parallel": effective_parallel,
        },
        "render": {
            "max_retries": 2,
            "retry_worker_exit_code": RENDER_RETRY_EXIT_CODE,
            "retry_exit_statuses": list(RENDER_RETRY_EXIT_STATUSES),
            "retryable_failure_classes": ["OOM", "TIMEOUT"],
            "attempts": attempts,
        },
        "normalization": {
            "cpus": normalize_cpus,
            "memory_bytes": normalize_memory,
            "timeout_seconds": normalize_timeout,
            "max_retries": 0,
            "error_strategy": "terminate",
        },
    }
    policy = {**body, "execution_policy_sha256": sha256_json(body)}
    validate_execution_policy(policy)
    return policy


def validate_execution_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    document = dict(policy)
    validate_schema_document(document, "execution-policy-v3")
    digest = document.pop("execution_policy_sha256")
    if digest != sha256_json(document):
        raise ValueError("execution policy SHA-256 does not match its content")
    attempts = policy["render"]["attempts"]
    if [row["attempt"] for row in attempts] != [1, 2, 3]:
        raise ValueError("execution policy attempts must be exactly 1, 2, 3")
    for row in attempts:
        heap_bytes, heap_argument = _heap_argument(int(row["memory_bytes"]))
        if (
            row["igv_heap_bytes"] != heap_bytes
            or row["igv_heap_argument"] != heap_argument
        ):
            raise ValueError("execution policy IGV heap does not match memory minus 2 GiB")
    return dict(policy)


def load_execution_policy(path: str | Path) -> dict[str, Any]:
    import json

    source = Path(path).expanduser()
    if source.is_symlink() or not source.resolve(strict=True).is_file():
        raise ValueError(f"execution policy must be a regular non-symlink file: {source}")
    try:
        document = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read execution policy {source}: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError("execution policy must contain one JSON object")
    return validate_execution_policy(document)


def write_execution_policy(
    path: str | Path,
    **kwargs: Any,
) -> dict[str, Any]:
    destination = Path(path).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"execution policy already exists: {destination}")
    # Reserve the public target before the atomic writer to make concurrent
    # policy creation fail closed. The reservation is removed before rename.
    reservation = destination.parent / f".{destination.name}.reserve-{uuid.uuid4().hex}"
    descriptor = os.open(reservation, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    try:
        policy = resolve_execution_policy(**kwargs)
        atomic_write_json(destination, policy)
    finally:
        reservation.unlink(missing_ok=True)
    return policy
