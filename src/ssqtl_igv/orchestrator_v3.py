from __future__ import annotations

import fcntl
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .accounting import collect_local_accounting, collect_scc_accounting, finalize_scc_accounting
from .artifact_admission_v3 import (
    assert_not_debug_metadata,
    assert_production_artifact_tree,
)
from .campaign_v3 import load_and_validate_batch_request
from .contracts import (
    validate_v3_task_document,
    validate_v3_case_result_document,
    validate_v3_terminal_bundle_document,
)
from .identity import task_set_fingerprint
from .controller_runtime_v3 import (
    collect_controller_runtime_identity,
    freeze_controller_runtime_identity,
    normalized_nextflow_environment,
    validate_controller_source_identity,
)
from .docker_worker_v3 import docker_worker_identity
from .review_package_v3 import build_review_package_v3
from .rerun_v3 import freeze_case_failure_rerun, prepare_rerun_task_set
from .runtime_identity import validate_runtime_manifest
from .sharding_v3 import create_bounded_shards
from .utils import (
    atomic_write_json,
    read_jsonl,
    reject_symlink_path_components,
    sha256_file,
    sha256_json,
    utc_now,
    write_tsv,
)
from .v3_manifest import _relative_path, normalize_generic_manifest


_SGE_SITE_TOKEN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_ADVANCED_BIND_PATH = re.compile(r"^/[A-Za-z0-9_./+@=-]+$")
_SSQTL_BUNDLE_FILES = {
    "tasks.jsonl",
    "normalized_manifest.tsv",
    "parameters.json",
    "ssqtl_preparation.json",
    "validation.json",
    "prepared_cases.tsv",
    "prepared_samples.tsv",
    "r_prepare.stdout.log",
    "r_prepare.stderr.log",
    "r_prepare.json",
}


def _available_cpu_count() -> int:
    try:
        affinity = os.sched_getaffinity(0)
    except (AttributeError, OSError):
        affinity = None
    return max(1, len(affinity) if affinity else int(os.cpu_count() or 1))


def _available_memory_bytes() -> int | None:
    """Return a conservative cgroup/host available-memory observation."""

    candidates: list[int] = []
    cgroup_pairs = (
        (Path("/sys/fs/cgroup/memory.max"), Path("/sys/fs/cgroup/memory.current")),
        (
            Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
            Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
        ),
    )
    for limit_path, usage_path in cgroup_pairs:
        try:
            limit_text = limit_path.read_text(encoding="utf-8").strip()
            if limit_text == "max":
                continue
            limit = int(limit_text)
            usage = int(usage_path.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            continue
        # cgroup v1 reports a near-maximum integer when no limit is active.
        if 0 < limit < (1 << 60):
            candidates.append(max(0, limit - usage))
    try:
        with Path("/proc/meminfo").open(encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemAvailable:"):
                    candidates.append(int(line.split()[1]) * 1024)
                    break
    except (OSError, ValueError, IndexError):
        pass
    return min(candidates) if candidates else None


def resolve_max_parallel(value: str | int | None = "auto") -> int:
    """Resolve the public ``auto|N`` concurrency contract to an integer 1..8."""

    normalized = "auto" if value is None else str(value).strip().lower()
    if normalized != "auto":
        try:
            explicit = int(normalized)
        except ValueError as exc:
            raise ValueError("--max-parallel must be auto or an integer from 1 to 8") from exc
        if str(explicit) != normalized or not 1 <= explicit <= 8:
            raise ValueError("--max-parallel must be auto or an integer from 1 to 8")
        return explicit
    memory = _available_memory_bytes()
    if memory is None:
        return 1
    memory_slots = memory // (8 * 1024**3)
    return max(1, min(8, _available_cpu_count(), int(memory_slots)))


def _validate_scc_site_options(
    project: str | None,
    qname: str | None,
) -> tuple[str, str | None]:
    project_value = "" if project is None else str(project)
    qname_value = "" if qname is None else str(qname)
    if not project_value or not _SGE_SITE_TOKEN.fullmatch(project_value):
        raise ValueError(
            "SCC project is required and must match "
            "[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
        )
    if qname_value and not _SGE_SITE_TOKEN.fullmatch(qname_value):
        raise ValueError(
            "SCC qname must match [A-Za-z0-9][A-Za-z0-9._-]{0,127}"
        )
    return project_value, qname_value or None


def _freeze_scc_site_adapter(
    run_dir: Path,
    *,
    project: str,
    qname: str | None,
) -> dict[str, Any]:
    contract = _scc_site_adapter_contract(project, qname)
    path = run_dir / "contract" / "scc_site_adapter.json"
    if path.is_symlink():
        raise ValueError("SCC site-adapter contract cannot be a symlink")
    if path.exists():
        if not path.is_file() or _read_json_object(path, "SCC site adapter") != contract:
            raise ValueError("SCC site-adapter options differ from the immutable run contract")
    else:
        atomic_write_json(path, contract)
    return contract


def _scc_site_adapter_contract(project: str, qname: str | None) -> dict[str, Any]:
    cluster_options = ["-P", project]
    if qname:
        cluster_options.extend(("-q", qname))
    return {
        "schema_version": "3.0-scc-site-adapter",
        "executor": "sge",
        "project": project,
        "qname": qname,
        "cluster_options": cluster_options,
        "active_shard_limit": 1,
        "portable_render_max_forks": 8,
        "max_cases_per_shard_limit": 256,
    }


def _project_root() -> Path:
    candidates = []
    configured = os.environ.get("IGV_SNAPSHOT_PIPELINE_DIR")
    if configured:
        candidates.append(Path(configured).expanduser().resolve(strict=False))
    candidates.extend(
        (
            Path(__file__).resolve().parents[2],
            Path.cwd().resolve(strict=False),
            Path(sys.prefix) / "share" / "igv-snapshot-workflow" / "pipeline",
        )
    )
    for candidate in candidates:
        if (candidate / "main.nf").is_file() and (candidate / "nextflow.config").is_file():
            return candidate
    raise FileNotFoundError(
        "Nextflow pipeline assets are unavailable; set IGV_SNAPSHOT_PIPELINE_DIR"
    )


def _nextflow_executable(value: str | None = None) -> str:
    if value:
        candidate = Path(value).expanduser().resolve(strict=False)
        if not candidate.is_file():
            raise FileNotFoundError(f"Nextflow executable is unavailable: {candidate}")
        return str(candidate)
    observed = shutil.which("nextflow")
    if observed:
        return observed
    bundled = _project_root() / ".tools" / "nextflow-25.04.7" / "nextflow"
    if bundled.is_file():
        return str(bundled)
    raise FileNotFoundError("Nextflow 25.04.7 is unavailable")


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def _validated_terminal_case_results(
    run_dir: Path,
    canonical_tasks: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """Reload the checksum-bound terminal case set in canonical task order."""

    task_ids = [str(task["task_id"]) for task in canonical_tasks]
    if len(task_ids) != len(set(task_ids)):
        raise ValueError("canonical task set contains duplicate task IDs")
    case_results: list[dict[str, Any]] = []
    for task in canonical_tasks:
        task_id = str(task["task_id"])
        case_root = run_dir / "results" / "cases" / task_id
        result_path = case_root / "case_result.json"
        bundle_path = case_root / "terminal_bundle.json"
        if result_path.is_symlink() or not result_path.is_file():
            raise ValueError(f"case result must be a regular non-symlink file: {result_path}")
        if bundle_path.is_symlink() or not bundle_path.is_file():
            raise ValueError(
                f"terminal bundle must be a regular non-symlink file: {bundle_path}"
            )
        result = _read_json_object(result_path, "case result")
        bundle = _read_json_object(bundle_path, "terminal bundle")
        validate_v3_case_result_document(result)
        validate_v3_terminal_bundle_document(bundle, result)
        if (
            bundle.get("case_result_sha256") != sha256_file(result_path)
            or int(bundle.get("case_result_size", -1)) != result_path.stat().st_size
        ):
            raise ValueError(f"terminal bundle no longer binds case result {task_id}")
        for field in (
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
        ):
            if result.get(field) != task.get(field):
                raise ValueError(
                    f"case result differs from canonical task {task_id}: {field}"
                )
        case_results.append(result)
    if {str(result["task_id"]) for result in case_results} != set(task_ids):
        raise ValueError("terminal case result set differs from canonical tasks")
    failures = [str(result["task_id"]) for result in case_results if not result["eligible"]]
    return case_results, failures


def _write_direct_output_tables(
    run_dir: Path,
    canonical_tasks: list[dict[str, Any]],
    case_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Write rebuildable, non-authoritative user indexes in manifest order."""

    if [row["task_id"] for row in canonical_tasks] != [
        row["task_id"] for row in case_results
    ]:
        raise ValueError("case-result order differs from canonical tasks")
    snapshot_fields = [
        "manifest_order",
        "task_id",
        "status",
        "adapter_type",
        "scientific_interpretation",
        "review_png",
        "review_sha256",
        "raw_igv_png",
        "raw_igv_sha256",
        "case_result_json",
        "input_fingerprint",
    ]
    failure_fields = [
        "manifest_order",
        "task_id",
        "failure_code",
        "message",
        "case_result_json",
        "input_fingerprint",
    ]
    snapshot_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    for result in case_results:
        task_id = str(result["task_id"])
        artifacts = result["artifacts"]
        review = artifacts.get("review_image") or {}
        raw = artifacts.get("raw_igv") or {}
        case_result_relative = f"results/cases/{task_id}/case_result.json"
        snapshot_rows.append(
            {
                "manifest_order": result["manifest_order"],
                "task_id": task_id,
                "status": "SNAPSHOT_READY" if result["eligible"] else "CASE_FAILED",
                "adapter_type": result["adapter_type"],
                "scientific_interpretation": result["scientific_interpretation"],
                "review_png": review.get("relative_path", ""),
                "review_sha256": review.get("sha256", ""),
                "raw_igv_png": raw.get("relative_path", ""),
                "raw_igv_sha256": raw.get("sha256", ""),
                "case_result_json": case_result_relative,
                "input_fingerprint": result["input_fingerprint"],
            }
        )
        for failure in result["failures"]:
            failure_rows.append(
                {
                    "manifest_order": result["manifest_order"],
                    "task_id": task_id,
                    "failure_code": failure["code"],
                    "message": failure["message"],
                    "case_result_json": case_result_relative,
                    "input_fingerprint": result["input_fingerprint"],
                }
            )
    snapshots_path = run_dir / "snapshots.tsv"
    failures_path = run_dir / "failed_cases.tsv"
    write_tsv(snapshots_path, snapshot_fields, snapshot_rows)
    write_tsv(failures_path, failure_fields, failure_rows)
    return {
        "snapshots_relative_path": "snapshots.tsv",
        "snapshots_sha256": sha256_file(snapshots_path),
        "failed_cases_relative_path": "failed_cases.tsv",
        "failed_cases_sha256": sha256_file(failures_path),
        "snapshot_count": sum(row["status"] == "SNAPSHOT_READY" for row in snapshot_rows),
        "failed_case_count": sum(not row["eligible"] for row in case_results),
    }


def _write_trace_report(run_dir: Path, trace_paths: list[Path]) -> dict[str, Any]:
    """Combine exact Nextflow rows into one convenience report with source pins."""

    if not trace_paths:
        raise ValueError("cannot build reports/trace.txt without Nextflow traces")
    fieldnames: list[str] | None = None
    rows: list[dict[str, str]] = []
    sources: list[dict[str, Any]] = []
    for source_value in trace_paths:
        source = source_value.resolve(strict=True)
        if source.is_symlink() or not source.is_file():
            raise ValueError(f"Nextflow trace is not a regular file: {source}")
        with source.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            current = list(reader.fieldnames or [])
            if not current:
                raise ValueError(f"Nextflow trace has no header: {source}")
            if fieldnames is None:
                fieldnames = current
            elif current != fieldnames:
                raise ValueError("Nextflow traces use different field sets")
            rows.extend(
                {key: str(value or "") for key, value in row.items()}
                for row in reader
            )
        sources.append(
            {
                "path": str(source),
                "sha256": sha256_file(source),
                "size": source.stat().st_size,
            }
        )
    report = run_dir / "reports" / "trace.txt"
    write_tsv(report, fieldnames or [], rows)
    source_receipt = {
        "schema_version": "3.0-trace-projection-sources",
        "authoritative": False,
        "projection": "reports/trace.txt",
        "projection_sha256": sha256_file(report),
        "sources": sources,
        "source_set_sha256": sha256_json(sources),
    }
    atomic_write_json(run_dir / "reports" / "trace.sources.json", source_receipt)
    return {
        "trace_relative_path": "reports/trace.txt",
        "trace_sha256": sha256_file(report),
        "trace_sources_relative_path": "reports/trace.sources.json",
        "trace_sources_sha256": sha256_file(
            run_dir / "reports" / "trace.sources.json"
        ),
    }


def _terminal_execution_state(
    *,
    profile: str,
    accounting_pass: bool,
    failed_case_ids: list[str],
) -> tuple[str, int]:
    """Choose the snapshot state; SCC qacct is optional operator evidence."""

    if failed_case_ids:
        return "CASE_FAILURES", 2
    if profile != "scc" and not accounting_pass:
        return "INFRASTRUCTURE_FATAL", 1
    return "SNAPSHOTS_READY", 0


def _runtime_identity(path: str | Path, expected_sha256: str | None) -> tuple[Path, dict[str, Any]]:
    declared = Path(path).expanduser()
    if declared.is_symlink():
        raise ValueError(f"runtime manifest must be a regular non-symlink file: {declared}")
    source = declared.resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"runtime manifest must be a regular non-symlink file: {source}")
    validation = validate_runtime_manifest(
        source, expected_manifest_sha256=expected_sha256
    )
    manifest = _read_json_object(source, "runtime manifest")
    manifest.update(
        {
            "runtime_manifest_sha256": validation["runtime_manifest_sha256"],
            "runtime_fingerprint_sha256": validation["runtime_fingerprint_sha256"],
            "observed_provenance": validation["observed_provenance"],
        }
    )
    return source, manifest


def _ssqtl_bound_path(
    root: Path,
    declared: str,
    *,
    label: str,
    directory: bool,
) -> tuple[str, Path]:
    relative = _relative_path(declared, label=label)
    resolved = root.joinpath(*relative.parts).resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} escapes the declared ssQTL input root") from exc
    if directory and not resolved.is_dir():
        raise ValueError(f"{label} is not a directory: {relative.as_posix()}")
    if not directory and not resolved.is_file():
        raise ValueError(f"{label} is not a file: {relative.as_posix()}")
    return relative.as_posix(), resolved


def _ssqtl_bind_contract(
    *,
    root: Path,
    reference: Path,
    associations: str,
    rds_dir: str,
    bam_lookup: str,
    violin_dir: str,
    adapter_config: str | None,
    runtime_identity: dict[str, Any],
) -> dict[str, Any]:
    """Freeze lightweight bind inputs without recursively copying BAM/RDS trees."""

    association_declared, association_path = _ssqtl_bound_path(
        root, associations, label="ssQTL associations", directory=False
    )
    rds_declared, rds_path = _ssqtl_bound_path(
        root, rds_dir, label="ssQTL RDS directory", directory=True
    )
    lookup_declared, lookup_path = _ssqtl_bound_path(
        root, bam_lookup, label="ssQTL BAM lookup", directory=False
    )
    violin_declared, violin_path = _ssqtl_bound_path(
        root, violin_dir, label="ssQTL violin directory", directory=True
    )
    config_record: dict[str, Any] | None = None
    if adapter_config:
        config_declared, config_path = _ssqtl_bound_path(
            root, adapter_config, label="ssQTL adapter config", directory=False
        )
        config_record = {
            "declared_path": config_declared,
            "source_path": str(config_path),
            "sha256": sha256_file(config_path),
            "mount": "input_root:ro",
        }
    contract = {
        "schema_version": "3.0-ssqtl-bind-contract",
        "mount_policy": "read_only_roots_no_recursive_controller_copy",
        "input_root": str(root),
        "reference_root": str(reference.parent),
        "resources": {
            "associations": {
                "declared_path": association_declared,
                "source_path": str(association_path),
                "sha256": sha256_file(association_path),
                "mount": "input_root:ro",
            },
            "bam_lookup": {
                "declared_path": lookup_declared,
                "source_path": str(lookup_path),
                "sha256": sha256_file(lookup_path),
                "mount": "input_root:ro",
            },
            "rds_directory": {
                "declared_path": rds_declared,
                "source_path": str(rds_path),
                "recursive_controller_copy": False,
                "mount": "input_root:ro",
            },
            "violin_directory": {
                "declared_path": violin_declared,
                "source_path": str(violin_path),
                "recursive_controller_copy": False,
                "mount": "input_root:ro",
            },
            "adapter_config": config_record,
            "reference": {
                "source_path": str(reference),
                "sha256": sha256_file(reference),
                "mount": "reference_root:ro",
            },
        },
        "runtime_manifest_sha256": runtime_identity["runtime_manifest_sha256"],
        "runtime_fingerprint_sha256": runtime_identity[
            "runtime_fingerprint_sha256"
        ],
    }
    contract["contract_sha256"] = sha256_json(contract)
    return contract


def _nextflow_trace_rows(path: Path, *, label: str) -> list[dict[str, str]]:
    if path.is_symlink() or not path.resolve(strict=True).is_file():
        raise ValueError(f"{label} must be a regular non-symlink trace: {path}")
    source = path.resolve(strict=True)
    with source.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        required = {"task_id", "hash", "native_id", "status"}
        if (
            not reader.fieldnames
            or not required.issubset(reader.fieldnames)
            or not ({"name", "process"} & set(reader.fieldnames))
        ):
            raise ValueError(f"{label} lacks required Nextflow fields: {source}")
        rows = []
        for row in reader:
            normalized = {key: str(value or "") for key, value in row.items()}
            normalized["process"] = normalized.get("process") or normalized.get("name", "")
            normalized["trace_file"] = str(source)
            rows.append(normalized)
    return rows


def _validate_ssqtl_normalization_trace(
    path: Path,
    *,
    profile: str,
    run_id: str,
    generation_id: str,
) -> dict[str, dict[str, str]]:
    rows = _nextflow_trace_rows(path, label="ssQTL normalization trace")
    if len(rows) != 2:
        raise ValueError("ssQTL normalization trace must contain exactly two tasks")
    selected: dict[str, dict[str, str]] = {}
    for row in rows:
        process = row["process"]
        if "VALIDATE_RUNTIME_IDENTITY" in process or "VALIDATE_RUNTIME_MANIFEST" in process:
            role = "runtime_manifest_validation"
        elif "NORMALIZE_SSQTL_V3" in process:
            role = "ssqtl_normalization"
            if f"({run_id}:{generation_id})" not in process:
                raise ValueError("ssQTL normalization trace tag differs from run identity")
        else:
            raise ValueError(f"ssQTL normalization trace has an unexpected process: {process}")
        if role in selected:
            raise ValueError(f"ssQTL normalization trace duplicates role: {role}")
        status = row["status"].strip().upper()
        if status != "COMPLETED" or row.get("exit", "").strip() not in {"", "0", "-"}:
            raise ValueError(f"ssQTL normalization trace role did not complete: {role}:{status}")
        if profile == "scc" and row["native_id"].strip() in {"", "-"}:
            raise ValueError(f"SCC ssQTL normalization trace lacks native_id: {role}")
        selected[role] = row
    if set(selected) != {"runtime_manifest_validation", "ssqtl_normalization"}:
        raise ValueError("ssQTL normalization trace role set is incomplete")
    return selected


def _ssqtl_selected_input_inventory(
    tasks: list[dict[str, Any]],
    *,
    bind_contract: dict[str, Any],
    bind_contract_sha256: str,
    preparation: dict[str, Any],
) -> dict[str, Any]:
    records: dict[tuple[str, str, str], dict[str, Any]] = {}

    def add(role: str, resource: dict[str, Any], mount: str) -> None:
        identity = resource.get("identity") if isinstance(resource.get("identity"), dict) else {}
        declared = str(resource.get("declared_path") or "")
        digest = str(identity.get("sha256") or resource.get("sha256") or "")
        if not declared or len(digest) != 64:
            raise ValueError(f"ssQTL selected input inventory lacks {role} identity")
        key = (role, declared, digest)
        records[key] = {
            "role": role,
            "declared_path": declared,
            "sha256": digest,
            "size": identity.get("size"),
            "mount": mount,
        }

    for rds in preparation.get("rds_resources", []):
        if not isinstance(rds, dict):
            raise ValueError("ssQTL preparation RDS inventory contains a non-object")
        add("rds", rds, "input_root:ro")
    for task in tasks:
        core = task["core"]
        for track in core["tracks"]:
            add("bam", track["bam"], "input_root:ro")
            add("bai", track["bai"], "input_root:ro")
        auxiliary = core["auxiliary"]
        if auxiliary["state"] == "PRESENT":
            add("violin", auxiliary, "input_root:ro")
        for role, resource in core["reference"]["resources"].items():
            add(f"reference:{role}", resource, "reference_root:ro")
    inventory = {
        "schema_version": "3.0-ssqtl-selected-input-inventory",
        "bind_contract_sha256": bind_contract_sha256,
        "bind_contract_identity": bind_contract.get("contract_sha256"),
        "task_count": len(tasks),
        "records": [records[key] for key in sorted(records)],
        "preparation_sources": {
            key: preparation.get(key)
            for key in (
                "association_sha256",
                "bam_lookup_sha256",
                "adapter_config_sha256",
                "r_wrapper_sha256",
                "r_implementation_sha256",
                "prepared_cases_sha256",
                "prepared_samples_sha256",
            )
        },
    }
    inventory["inventory_sha256"] = sha256_json(inventory)
    return inventory


@contextmanager
def _run_lock(run_dir: Path) -> Iterator[None]:
    control = run_dir / "control"
    control.mkdir(parents=True, exist_ok=True)
    lock_path = control / "run.lock"
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"another controller owns this run: {run_dir}") from exc
        yield


def _append_control_event(run_dir: Path, state: str, detail: dict[str, Any]) -> dict[str, Any]:
    """Append a diagnostic controller observation, never execution state.

    Nextflow trace/cache and raw qacct are the execution and scheduler
    authorities.  This journal therefore records only a checksum of the
    controller observation plus immutable digest references; it deliberately
    does not persist task IDs, failed-case sets, native IDs, or state names.
    """

    journal = run_dir / "control" / "control.journal.jsonl"
    previous = "0" * 64
    sequence = 1
    if journal.is_file():
        rows = list(read_jsonl(journal))
        if rows:
            previous = str(rows[-1]["event_hash"])
            sequence = int(rows[-1]["sequence"]) + 1
    digest_references: dict[str, str] = {}

    def collect_digests(value: Any, prefix: str = "") -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                path = f"{prefix}.{key}" if prefix else str(key)
                lowered = str(key).lower()
                if (
                    isinstance(child, str)
                    and (lowered.endswith("sha256") or lowered.endswith("digest"))
                    and (re.fullmatch(r"[a-f0-9]{64}", child) or re.fullmatch(r"sha256:[a-f0-9]{64}", child))
                ):
                    digest_references[path] = child
                else:
                    collect_digests(child, path)
        elif isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                collect_digests(child, f"{prefix}[{index}]")

    collect_digests(detail)
    body = {
        "schema_version": "3.0-controller-diagnostic",
        "authoritative": False,
        "event_type": "CONTROLLER_OBSERVATION",
        "sequence": sequence,
        "observed_at": utc_now(),
        "observation_sha256": sha256_json({"code": state, "detail": detail}),
        "source_digest_references": dict(sorted(digest_references.items())),
        "previous_event_hash": previous,
    }
    body["event_hash"] = sha256_json(body)
    payload = json.dumps(body, sort_keys=True, ensure_ascii=False) + "\n"
    descriptor = os.open(journal, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(descriptor, payload.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return body


def _write_run_summary(run_dir: Path, summary: dict[str, Any]) -> dict[str, Any]:
    """Write aggregates plus source pins, never a second task-state record."""

    source_digests: dict[str, str] = {}
    for label, relative in (
        ("canonical_tasks", Path("contract/tasks.jsonl")),
        ("run_identity", Path("contract/run_identity.json")),
        ("controller_runtime", Path("contract/controller_runtime.json")),
        ("shard_plan", Path("shards/shard_plan.json")),
        ("snapshots", Path("snapshots.tsv")),
        ("failed_cases", Path("failed_cases.tsv")),
        ("trace_projection", Path("reports/trace.txt")),
        ("trace_projection_sources", Path("reports/trace.sources.json")),
    ):
        path = run_dir / relative
        if path.is_file() and not path.is_symlink():
            source_digests[label] = sha256_file(path)
    accounting = summary.get("accounting")
    if isinstance(accounting, Mapping):
        receipt_sha = accounting.get("receipt_sha256")
        if isinstance(receipt_sha, str) and re.fullmatch(r"[a-f0-9]{64}", receipt_sha):
            source_digests["accounting_receipt"] = receipt_sha
    review = summary.get("review_package")
    if isinstance(review, Mapping):
        for key in ("package_json_sha256", "checksums_sha256", "contract_set_sha256"):
            value = review.get(key)
            if isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value):
                source_digests[f"review_package_{key}"] = value
    rerun = summary.get("rerun")
    if isinstance(rerun, Mapping):
        for key in ("rerun_manifest_sha256", "rerun_receipt_sha256", "checksums_sha256"):
            value = rerun.get(key)
            if isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value):
                source_digests[f"rerun_{key}"] = value
    failed_ids = summary.get("failed_case_ids")
    failed_count = len(failed_ids) if isinstance(failed_ids, list) else int(
        summary.get("failed_case_count", 0) or 0
    )
    shards = summary.get("shards")
    accounting_projection = None
    if isinstance(accounting, Mapping):
        accounting_projection = {
            "provider": accounting.get("provider"),
            "status": accounting.get("status"),
            "qacct_used": accounting.get("qacct_used"),
        }
    projection = {
        "schema_version": "3.0",
        "pipeline_version": summary.get("pipeline_version", "3.0.0"),
        "authoritative": False,
        "projection_kind": "UX_ONLY",
        "status": summary.get("status"),
        "exit_code": summary.get("exit_code"),
        "profile": summary.get("profile"),
        "expected_case_count": summary.get("expected_case_count"),
        "observed_case_count": summary.get("observed_case_count"),
        "failed_case_count": failed_count,
        "shard_count": len(shards) if isinstance(shards, list) else None,
        "accounting": accounting_projection,
        "review_gate": bool(summary.get("review_gate", False)),
        "review_package_ready": isinstance(review, Mapping),
        "rerun_required": bool(rerun),
        "publication_state": summary.get("publication_state", "NOT_READY"),
        "human_review_required": bool(summary.get("human_review_required", False)),
        "effective_max_parallel": summary.get("effective_max_parallel"),
        "source_digests": dict(sorted(source_digests.items())),
    }
    summary["authoritative"] = False
    summary["projection_kind"] = "UX_ONLY"
    summary["source_digests"] = dict(sorted(source_digests.items()))
    atomic_write_json(run_dir / "run_summary.json", projection)
    return summary


def _identity_contract(
    *,
    run_id: str,
    generation_id: str,
    profile: str,
    adapter: str,
    tasks_path: Path,
    shard_plan_path: Path,
    runtime_path: Path,
    runtime_identity: dict[str, Any],
    runtime_snapshot_path: Path,
    scc_site_adapter: dict[str, Any] | None = None,
    docker_worker: dict[str, Any] | None = None,
) -> dict[str, Any]:
    identity = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "run_id": run_id,
        "generation_id": generation_id,
        "profile": profile,
        "adapter": adapter,
        "execution_run_root": str(tasks_path.parent.parent.resolve(strict=True)),
        "canonical_tasks_sha256": sha256_file(tasks_path),
        "shard_plan_sha256": sha256_file(shard_plan_path),
        "runtime_manifest": str(runtime_path),
        "runtime_manifest_sha256": runtime_identity["runtime_manifest_sha256"],
        "runtime_manifest_snapshot_sha256": sha256_file(runtime_snapshot_path),
        "runtime_fingerprint_sha256": runtime_identity[
            "runtime_fingerprint_sha256"
        ],
    }
    if scc_site_adapter is not None:
        identity["scc_site_adapter"] = scc_site_adapter
    if docker_worker is not None:
        identity["docker_worker_identity"] = docker_worker
    project_binding_path = tasks_path.parent / "project_binding.json"
    if project_binding_path.exists():
        binding = _read_json_object(project_binding_path, "project source binding")
        _validate_project_binding(binding, adapter=adapter)
        identity["project_binding_sha256"] = sha256_file(project_binding_path)
        identity["project_source_fingerprint_sha256"] = binding["binding_sha256"]
    campaign_binding_path = tasks_path.parent / "campaign_binding.json"
    if campaign_binding_path.exists():
        binding = _read_json_object(campaign_binding_path, "campaign batch binding")
        if (
            binding.get("schema_version") != "3.0-batch-admission"
            or binding.get("tasks_sha256") != sha256_file(tasks_path)
            or binding.get("task_set_sha256")
            != task_set_fingerprint(list(read_jsonl(tasks_path)))
            or binding.get("campaign_id") != run_id
            or binding.get("batch_id") != generation_id
        ):
            raise ValueError("campaign batch binding differs from canonical tasks")
        identity.update(
            {
                "campaign_binding_sha256": sha256_file(campaign_binding_path),
                "campaign_id": binding["campaign_id"],
                "campaign_contract_sha256": binding[
                    "campaign_contract_sha256"
                ],
                "batch_id": binding["batch_id"],
                "batch_request_sha256": binding["batch_request_sha256"],
                "batch_purpose": binding["purpose"],
                "master_tasks_sha256": binding["master_tasks_sha256"],
                "master_task_set_sha256": binding[
                    "master_task_set_sha256"
                ],
                "pilot_selection_sha256": binding.get(
                    "pilot_selection_sha256"
                ),
                "batch_task_set_sha256": binding["task_set_sha256"],
            }
        )
    normalization_execution_path = (
        tasks_path.parent / "ssqtl_normalization_execution.json"
    )
    if normalization_execution_path.exists():
        execution = _read_json_object(
            normalization_execution_path, "ssQTL normalization execution"
        )
        controller_runtime_path = tasks_path.parent / "controller_runtime.json"
        controller_runtime = _read_json_object(
            controller_runtime_path, "ssQTL normalization controller runtime"
        )
        if (
            execution.get("schema_version")
            != "3.0-ssqtl-normalization-execution"
            or execution.get("status") != "SUCCEEDED"
            or execution.get("controller_runtime_sha256")
            != sha256_file(controller_runtime_path)
            or execution.get("controller_runtime_identity_sha256")
            != controller_runtime.get("identity_sha256")
        ):
            raise ValueError(
                "ssQTL normalization controller differs from its execution receipt"
            )
        identity.update(
            {
                "normalization_controller_runtime_identity_sha256": controller_runtime[
                    "identity_sha256"
                ],
                "normalization_controller_runtime_contract_sha256": sha256_file(
                    controller_runtime_path
                ),
                "normalization_execution_receipt_sha256": sha256_file(
                    normalization_execution_path
                ),
            }
        )
    return identity


def _validate_project_binding(
    value: Mapping[str, Any], *, adapter: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("project source binding must be an object")
    normalized = json.loads(json.dumps(dict(value), sort_keys=True, ensure_ascii=False))
    required = {
        "schema_version",
        "adapter",
        "project",
        "inputs",
        "reference",
        "binding_sha256",
    }
    if set(normalized) != required:
        raise ValueError("project source binding fields are invalid")
    if normalized.get("schema_version") != "3.0-project-source-binding":
        raise ValueError("project source binding schema_version is invalid")
    if normalized.get("adapter") != adapter:
        raise ValueError("project source binding adapter differs from the run")
    body = {key: normalized[key] for key in sorted(required - {"binding_sha256"})}
    if normalized.get("binding_sha256") != sha256_json(body):
        raise ValueError("project source binding checksum is invalid")
    return normalized


def _prepare_campaign_batch(
    *,
    batch_request: str | Path,
    contract_dir: Path,
    run_id: str,
    generation_id: str,
) -> dict[str, Any]:
    """Admit exactly one immutable campaign batch without creating execution state."""

    binding = load_and_validate_batch_request(batch_request)
    request = binding["request"]
    if (
        request.get("execution_run_id") != run_id
        or request.get("execution_generation_id") != generation_id
    ):
        raise ValueError("batch-request run/generation differs from requested execution")
    if contract_dir.exists() or contract_dir.is_symlink():
        raise FileExistsError(f"campaign contract destination already exists: {contract_dir}")
    contract_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = contract_dir.parent / f".{contract_dir.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    source_tasks = Path(str(binding["tasks_path"]))
    source_request = Path(str(binding["request_path"]))
    try:
        shutil.copyfile(source_tasks, staging / "tasks.jsonl")
        shutil.copyfile(source_request, staging / "batch-request.json")
        if (
            sha256_file(staging / "tasks.jsonl") != binding["tasks_sha256"]
            or sha256_file(staging / "batch-request.json")
            != binding["request_sha256"]
        ):
            raise RuntimeError("campaign inputs changed while being admitted")
        admission = {
            "schema_version": "3.0-batch-admission",
            "campaign_id": binding["campaign_id"],
            "campaign_contract_sha256": binding[
                "campaign_contract_sha256"
            ],
            "campaign_root": binding["campaign_root"],
            "batch_id": request["batch_id"],
            "batch_index": request["batch_index"],
            "purpose": request["purpose"],
            "batch_request_sha256": binding["request_sha256"],
            "master_task_count": request["master_task_count"],
            "master_tasks_sha256": request["master_tasks_sha256"],
            "master_task_set_sha256": request["master_task_set_sha256"],
            "pilot_selection_sha256": request["pilot_selection_sha256"],
            "task_count": request["task_count"],
            "tasks_sha256": binding["tasks_sha256"],
            "task_set_sha256": binding["task_set_sha256"],
        }
        atomic_write_json(staging / "campaign_binding.json", admission)
        os.replace(staging, contract_dir)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "status": "READY",
        "adapter_id": "ssqtl",
        "run_id": run_id,
        "generation_id": generation_id,
        "task_count": int(request["task_count"]),
        "task_set_sha256": binding["task_set_sha256"],
        "tasks_sha256": binding["tasks_sha256"],
        "tasks": str(contract_dir / "tasks.jsonl"),
        "campaign_binding": str(contract_dir / "campaign_binding.json"),
        "campaign_binding_sha256": sha256_file(
            contract_dir / "campaign_binding.json"
        ),
        "batch_request": str(contract_dir / "batch-request.json"),
        "batch_request_sha256": binding["request_sha256"],
        "campaign_id": binding["campaign_id"],
        "batch_id": request["batch_id"],
        "batch_purpose": request["purpose"],
        "master_tasks_sha256": request["master_tasks_sha256"],
        "master_task_set_sha256": request["master_task_set_sha256"],
        "pilot_selection_sha256": request["pilot_selection_sha256"],
    }


def _validate_normalization_controller_contract(
    run_dir: Path,
    *,
    observed_controller: dict[str, Any],
) -> None:
    """Require render to use the exact controller admitted by native normalization."""

    execution_path = run_dir / "contract" / "ssqtl_normalization_execution.json"
    identity = _read_json_object(
        run_dir / "contract" / "run_identity.json", "run identity"
    )
    bound_fields = {
        "normalization_controller_runtime_identity_sha256",
        "normalization_controller_runtime_contract_sha256",
        "normalization_execution_receipt_sha256",
    }
    if not execution_path.exists():
        # A rerun generation imports canonical tasks and source-lineage hashes but
        # owns no normalization task in this generation.  Do not attach the old
        # trace/controller as if they ran again.
        if bound_fields.intersection(identity):
            raise ValueError(
                "run identity declares normalization controller evidence without "
                "a same-generation normalization execution"
            )
        return
    controller_path = run_dir / "contract" / "controller_runtime.json"
    execution = _read_json_object(execution_path, "ssQTL normalization execution")
    if (
        set(identity).intersection(bound_fields) != bound_fields
        or identity.get("normalization_controller_runtime_identity_sha256")
        != observed_controller.get("identity_sha256")
        or identity.get("normalization_controller_runtime_contract_sha256")
        != sha256_file(controller_path)
        or identity.get("normalization_execution_receipt_sha256")
        != sha256_file(execution_path)
        or execution.get("controller_runtime_identity_sha256")
        != observed_controller.get("identity_sha256")
        or execution.get("controller_runtime_sha256") != sha256_file(controller_path)
    ):
        raise ValueError(
            "render controller differs from the immutable ssQTL normalization controller"
        )


def _prepare_generic(
    *,
    manifest: str | Path,
    input_root: str | Path,
    reference: str | Path,
    contract_dir: Path,
    run_id: str,
    generation_id: str,
) -> dict[str, Any]:
    return normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        contract_dir,
        run_id,
        generation_id,
    )


def run_portable_ssqtl_normalization(
    *,
    run_dir: str | Path,
    run_id: str,
    generation_id: str,
    profile: str,
    associations: str,
    rds_dir: str,
    bam_lookup: str,
    violin_dir: str,
    input_root: str | Path,
    reference: str | Path,
    adapter_config: str | None,
    runtime_identity_path: str | Path,
    runtime_identity_sha256: str | None = None,
    runtime_image: str | None = None,
    runtime_sif: str | None = None,
    nextflow: str | None = None,
    work_dir: str | Path | None = None,
    scc_project: str | None = None,
    scc_qname: str | None = None,
    docker_worker_uid: object | None = None,
    docker_worker_gid: object | None = None,
    max_parallel: int = 1,
) -> dict[str, Any]:
    """Run raw ssQTL preparation as a bounded portable Nextflow process."""

    if profile not in {"standalone", "docker", "scc", "test"}:
        raise ValueError("ssQTL normalization profile is unsupported")
    if not isinstance(max_parallel, int) or isinstance(max_parallel, bool) or not 1 <= max_parallel <= 8:
        raise ValueError("max_parallel must be an integer from 1 to 8")
    for value, label in (
        (associations, "associations"),
        (rds_dir, "rds directory"),
        (bam_lookup, "BAM lookup"),
        (violin_dir, "violin directory"),
    ):
        _relative_path(value, label=f"ssQTL {label}")
    if adapter_config:
        _relative_path(adapter_config, label="ssQTL adapter config")
    root_value = Path(input_root).expanduser()
    reference_value = Path(reference).expanduser()
    if root_value.is_symlink() or reference_value.is_symlink():
        raise ValueError("ssQTL input root/reference cannot be a symlink")
    root = root_value.resolve(strict=True)
    reference_path = reference_value.resolve(strict=True)
    if not root.is_dir() or not reference_path.is_file():
        raise ValueError("ssQTL input root/reference is unavailable")
    for value, label in (
        (str(root), "input root"),
        (str(reference_path), "reference"),
        (run_id, "run_id"),
        (generation_id, "generation_id"),
        (associations, "associations"),
        (rds_dir, "rds directory"),
        (bam_lookup, "BAM lookup"),
        (violin_dir, "violin directory"),
        (adapter_config or "", "adapter config"),
    ):
        if "'" in value or any(ord(character) < 32 for character in value):
            raise ValueError(f"ssQTL {label} contains a shell-unsafe character")
    if profile in {"docker", "scc"} and (
        _ADVANCED_BIND_PATH.fullmatch(str(root)) is None
        or _ADVANCED_BIND_PATH.fullmatch(str(reference_path.parent)) is None
    ):
        raise ValueError(
            "advanced ssQTL container bind roots must use only safe absolute-path characters"
        )

    runtime_path, runtime_value = _runtime_identity(
        runtime_identity_path, runtime_identity_sha256
    )
    observed_sif_sha = ""
    if runtime_sif:
        sif = Path(runtime_sif).expanduser()
        if sif.is_symlink() or not sif.resolve(strict=True).is_file():
            raise ValueError("runtime SIF must be a regular non-symlink file")
        sif = sif.resolve(strict=True)
        observed_sif_sha = sha256_file(sif)

    source = runtime_value.get("source") if isinstance(runtime_value.get("source"), dict) else {}
    controller_source = validate_controller_source_identity(
        profile,
        source,
        _project_root(),
        allow_test_runtime=profile == "test",
    )

    worker = docker_worker_identity(
        profile, docker_worker_uid, docker_worker_gid
    )
    site_project: str | None = None
    site_qname: str | None = None
    if profile == "scc":
        site_project, site_qname = _validate_scc_site_options(scc_project, scc_qname)
    executable = _nextflow_executable(nextflow)
    controller_runtime = collect_controller_runtime_identity(executable)
    requested_run = Path(run_dir).expanduser().resolve(strict=False)
    requested_run.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(
        tempfile.mkdtemp(
            prefix=f".{requested_run.name}.ssqtl-normalize-",
            dir=requested_run.parent,
        )
    )
    try:
        output_parent = temporary / "published"
        session = temporary / "session"
        launch = temporary / "launch"
        output_parent.mkdir()
        session.mkdir()
        launch.mkdir()
        work = (
            reject_symlink_path_components(work_dir, label="work directory").resolve(
                strict=False
            )
            if work_dir
            else temporary / "work"
        )
        work.mkdir(parents=True, exist_ok=True)
        controller_runtime_path = temporary / "controller_runtime.json"
        atomic_write_json(controller_runtime_path, controller_runtime)
        bind_contract = _ssqtl_bind_contract(
            root=root,
            reference=reference_path,
            associations=associations,
            rds_dir=rds_dir,
            bam_lookup=bam_lookup,
            violin_dir=violin_dir,
            adapter_config=adapter_config,
            runtime_identity=runtime_value,
        )
        bind_contract_path = temporary / "ssqtl_bind_contract.json"
        atomic_write_json(bind_contract_path, bind_contract)
        command = [
            executable,
            "run",
            str(_project_root()),
            "-entry",
            "SSQTL_NORMALIZE",
            "-profile",
            profile,
            "-work-dir",
            str(work),
            "--run_id",
            run_id,
            "--generation_id",
            generation_id,
            "--ssqtl_associations",
            associations,
            "--ssqtl_rds_dir",
            rds_dir,
            "--ssqtl_bam_lookup",
            bam_lookup,
            "--ssqtl_violin_dir",
            violin_dir,
            "--ssqtl_input_root",
            str(root),
            "--ssqtl_reference",
            str(reference_path),
            "--ssqtl_reference_root",
            str(reference_path.parent),
            "--ssqtl_bind_contract",
            str(bind_contract_path),
            "--ssqtl_normalization_output",
            str(output_parent),
            "--runtime_manifest",
            str(runtime_path),
            "--runtime_manifest_sha256",
            runtime_value["runtime_manifest_sha256"],
            "--runtime_fingerprint_sha256",
            runtime_value["runtime_fingerprint_sha256"],
            "--max_parallel",
            str(max_parallel),
            "--enable_reports",
            "true",
            "--session_output",
            str(session),
        ]
        if adapter_config:
            command.extend(["--ssqtl_config", adapter_config])
        if runtime_image:
            command.extend(["--runtime_image", runtime_image])
        if runtime_sif:
            command.extend(["--runtime_sif", str(Path(runtime_sif).resolve(strict=True))])
            command.extend(["--runtime_sif_sha256", observed_sif_sha])
        if worker:
            command.extend(
                ["--host_uid", str(worker["uid"]), "--host_gid", str(worker["gid"])]
            )
        if site_project:
            command.extend(["--scc_project", site_project])
        if site_qname:
            command.extend(["--scc_qname", site_qname])
        stdout = temporary / "controller.stdout.log"
        stderr = temporary / "controller.stderr.log"
        launch_environment = normalized_nextflow_environment(
            controller_runtime["java"]["executable"],
            base={**os.environ, "NXF_ANSI_LOG": "false"},
        )
        with stdout.open("w", encoding="utf-8") as out, stderr.open(
            "w", encoding="utf-8"
        ) as err:
            completed = subprocess.run(
                command,
                check=False,
                text=True,
                stdout=out,
                stderr=err,
                cwd=launch,
                env=launch_environment,
            )
        bundle = output_parent / "normalization_bundle"
        if completed.returncode != 0 or not bundle.is_dir():
            raise RuntimeError(
                f"portable ssQTL normalization failed ({completed.returncode}); logs: {stderr}"
            )
        trace = session / "trace.txt"
        _validate_ssqtl_normalization_trace(
            trace,
            profile=profile,
            run_id=run_id,
            generation_id=generation_id,
        )
        runtime_validation = (
            session
            / "runtime_manifest"
            / "runtime_manifest_validation"
            / "validation.json"
        )
        validation_receipt = _read_json_object(
            runtime_validation, "ssQTL runtime identity validation"
        )
        if (
            validation_receipt.get("schema_version")
            != "3.0-runtime-manifest-validation"
            or validation_receipt.get("status") != "PASS"
            or validation_receipt.get("runtime_manifest_sha256")
            != runtime_value["runtime_manifest_sha256"]
            or validation_receipt.get("runtime_fingerprint_sha256")
            != runtime_value["runtime_fingerprint_sha256"]
        ):
            raise ValueError("ssQTL runtime manifest validation receipt drift")
        required_bundle_files = _SSQTL_BUNDLE_FILES
        if {path.name for path in bundle.iterdir()} != required_bundle_files:
            raise ValueError("ssQTL normalization bundle file set is incomplete")
        bundle_inventory = {
            name: sha256_file(bundle / name) for name in sorted(required_bundle_files)
        }
        return {
            "schema_version": "3.0-ssqtl-normalization-execution",
            "status": "SUCCEEDED",
            "profile": profile,
            "process_label": "portable_runtime",
            "executor": "sge" if profile == "scc" else "local",
            "bundle": str(bundle),
            "temporary_root": str(temporary),
            "command": command,
            "controller_stdout": str(stdout),
            "controller_stdout_sha256": sha256_file(stdout),
            "controller_stderr": str(stderr),
            "controller_stderr_sha256": sha256_file(stderr),
            "controller_runtime": str(controller_runtime_path),
            "controller_runtime_sha256": sha256_file(controller_runtime_path),
            "controller_runtime_identity_sha256": controller_runtime["identity_sha256"],
            "controller_source_validation": controller_source,
            "bind_contract": str(bind_contract_path),
            "bind_contract_sha256": sha256_file(bind_contract_path),
            "runtime_validation": str(runtime_validation),
            "runtime_validation_sha256": sha256_file(runtime_validation),
            "trace": str(trace),
            "trace_sha256": sha256_file(trace),
            "bundle_inventory": bundle_inventory,
            "runtime_manifest_sha256": runtime_value["runtime_manifest_sha256"],
            "runtime_fingerprint_sha256": runtime_value[
                "runtime_fingerprint_sha256"
            ],
            "runtime_sif_sha256": observed_sif_sha or None,
            "max_parallel": max_parallel,
        }
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _prepare_ssqtl(
    *,
    normalization_bundle: str | Path,
    normalization_execution: dict[str, Any],
    contract_dir: Path,
    run_id: str,
    generation_id: str,
    profile: str,
    runtime_identity: dict[str, Any],
    max_parallel: int,
) -> dict[str, Any]:
    source_value = Path(normalization_bundle).expanduser()
    if source_value.is_symlink():
        raise ValueError("ssQTL normalization bundle cannot be a symlink")
    source = source_value.resolve(strict=True)
    if not source.is_dir():
        raise ValueError("ssQTL normalization bundle must be a directory")
    required = _SSQTL_BUNDLE_FILES
    if {path.name for path in source.iterdir()} != required:
        raise ValueError("ssQTL normalization bundle file set differs from v3 contract")
    for path in source.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ValueError("ssQTL normalization bundle contains a non-regular file")
    validation = _read_json_object(source / "validation.json", "ssQTL validation")
    if (
        validation.get("schema_version") != "3.0"
        or validation.get("adapter_schema_version") != "3.0-ssqtl"
        or validation.get("adapter_id") != "ssqtl"
        or validation.get("run_id") != run_id
        or validation.get("generation_id") != generation_id
        or validation.get("status")
        not in {"PASS", "PASS_WITH_CASE_INPUT_ERRORS"}
    ):
        raise ValueError("ssQTL normalization validation identity is invalid")
    tasks = list(read_jsonl(source / "tasks.jsonl"))
    if not tasks:
        raise ValueError("native ssQTL canonical task set is empty")
    for task in tasks:
        validate_v3_task_document(task)
        if (
            task.get("adapter_id") != "ssqtl"
            or task.get("adapter_data", {}).get("adapter_schema_version") != "3.0-ssqtl"
            or task.get("run_id") != run_id
            or task.get("generation_id") != generation_id
        ):
            raise ValueError("native ssQTL task identity differs from requested run")
    if validation.get("tasks_sha256") != sha256_file(source / "tasks.jsonl"):
        raise ValueError("ssQTL tasks checksum differs from normalization validation")
    if validation.get("task_set_sha256") != task_set_fingerprint(tasks):
        raise ValueError("ssQTL task-set checksum differs from normalization validation")
    invalid_task_count = sum(
        task["core"]["preflight"]["state"] != "READY" for task in tasks
    )
    if (
        validation.get("task_count") != len(tasks)
        or validation.get("ready_task_count") != len(tasks) - invalid_task_count
        or validation.get("case_input_invalid_count") != invalid_task_count
        or (
            validation.get("status") == "PASS"
            and invalid_task_count != 0
        )
        or (
            validation.get("status") == "PASS_WITH_CASE_INPUT_ERRORS"
            and invalid_task_count == 0
        )
    ):
        raise ValueError("ssQTL validation status/count contract is inconsistent")
    bundle_inventory = {
        name: sha256_file(source / name) for name in sorted(required)
    }
    if normalization_execution.get("bundle_inventory") != bundle_inventory:
        raise ValueError("ssQTL normalization bundle differs from its launch receipt")
    expected_sif = normalization_execution.get("runtime_sif_sha256")
    expected_executor = "sge" if profile == "scc" else "local"
    if (
        normalization_execution.get("schema_version")
        != "3.0-ssqtl-normalization-execution"
        or normalization_execution.get("status") != "SUCCEEDED"
        or normalization_execution.get("profile") != profile
        or normalization_execution.get("process_label") != "portable_runtime"
        or normalization_execution.get("executor") != expected_executor
        or normalization_execution.get("runtime_manifest_sha256")
        != runtime_identity["runtime_manifest_sha256"]
        or normalization_execution.get("runtime_fingerprint_sha256")
        != runtime_identity["runtime_fingerprint_sha256"]
        or normalization_execution.get("runtime_sif_sha256") != expected_sif
        or normalization_execution.get("max_parallel") != max_parallel
    ):
        raise ValueError("ssQTL normalization execution/runtime manifest is invalid")

    def execution_file(key: str, digest_key: str) -> Path:
        declared = Path(str(normalization_execution.get(key, ""))).expanduser()
        if declared.is_symlink() or not declared.resolve(strict=True).is_file():
            raise ValueError(f"ssQTL normalization {key} is unavailable")
        resolved = declared.resolve(strict=True)
        if sha256_file(resolved) != normalization_execution.get(digest_key):
            raise ValueError(f"ssQTL normalization {key} checksum drift")
        return resolved

    stdout = execution_file("controller_stdout", "controller_stdout_sha256")
    stderr = execution_file("controller_stderr", "controller_stderr_sha256")
    controller_runtime_path = execution_file(
        "controller_runtime", "controller_runtime_sha256"
    )
    controller_runtime = _read_json_object(
        controller_runtime_path, "ssQTL normalization controller runtime"
    )
    if (
        controller_runtime.get("schema_version") != "3.0-controller-runtime"
        or controller_runtime.get("identity_sha256")
        != normalization_execution.get("controller_runtime_identity_sha256")
        or controller_runtime.get("identity_sha256") != sha256_json(
            {key: value for key, value in controller_runtime.items() if key != "identity_sha256"}
        )
    ):
        raise ValueError("ssQTL normalization controller identity is invalid")
    source_validation = normalization_execution.get("controller_source_validation")
    if not isinstance(source_validation, dict) or source_validation.get("status") not in {
        "PASS",
        "TEST_ONLY",
    }:
        raise ValueError("ssQTL normalization controller source validation is invalid")
    if profile != "test" and source_validation.get("status") != "PASS":
        raise ValueError("production ssQTL normalization controller source is unverified")

    bind_path = execution_file("bind_contract", "bind_contract_sha256")
    bind_contract = _read_json_object(bind_path, "ssQTL bind contract")
    bind_claim = {key: value for key, value in bind_contract.items() if key != "contract_sha256"}
    if (
        bind_contract.get("schema_version") != "3.0-ssqtl-bind-contract"
        or bind_contract.get("contract_sha256") != sha256_json(bind_claim)
        or bind_contract.get("runtime_manifest_sha256")
        != runtime_identity["runtime_manifest_sha256"]
        or bind_contract.get("runtime_fingerprint_sha256")
        != runtime_identity["runtime_fingerprint_sha256"]
    ):
        raise ValueError("ssQTL bind contract runtime manifest is invalid")

    runtime_validation_path = execution_file(
        "runtime_validation", "runtime_validation_sha256"
    )
    runtime_validation = _read_json_object(
        runtime_validation_path, "ssQTL runtime validation receipt"
    )
    if (
        runtime_validation.get("schema_version")
        != "3.0-runtime-manifest-validation"
        or runtime_validation.get("status") != "PASS"
        or runtime_validation.get("runtime_manifest_sha256")
        != runtime_identity["runtime_manifest_sha256"]
        or runtime_validation.get("runtime_fingerprint_sha256")
        != runtime_identity["runtime_fingerprint_sha256"]
    ):
        raise ValueError("ssQTL runtime validation receipt manifest drift")

    trace_path = execution_file("trace", "trace_sha256")
    trace_rows = _validate_ssqtl_normalization_trace(
        trace_path,
        profile=profile,
        run_id=run_id,
        generation_id=generation_id,
    )
    command = normalization_execution.get("command")
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        raise ValueError("ssQTL normalization command contract is invalid")

    def command_value(option: str) -> str:
        positions = [index for index, value in enumerate(command) if value == option]
        if len(positions) != 1 or positions[0] + 1 >= len(command):
            raise ValueError(f"ssQTL normalization command lacks unique {option}")
        return command[positions[0] + 1]

    if (
        command_value("-entry") != "SSQTL_NORMALIZE"
        or command_value("-profile") != profile
        or command_value("--run_id") != run_id
        or command_value("--generation_id") != generation_id
        or command_value("--runtime_manifest_sha256")
        != runtime_identity["runtime_manifest_sha256"]
        or command_value("--runtime_fingerprint_sha256")
        != runtime_identity["runtime_fingerprint_sha256"]
        or command_value("--max_parallel") != str(max_parallel)
        or command_value("--ssqtl_bind_contract") != str(bind_path)
        or command[0] != controller_runtime["nextflow"]["executable"]
        or "-resume" in command
    ):
        raise ValueError("ssQTL normalization launch command differs from frozen identity")

    preparation = _read_json_object(
        source / "ssqtl_preparation.json", "ssQTL preparation receipt"
    )
    if (
        preparation.get("schema_version") != "3.0-ssqtl-preparation"
        or preparation.get("status") != "PASS"
    ):
        raise ValueError("ssQTL preparation receipt is not passing")
    parameters = _read_json_object(source / "parameters.json", "ssQTL parameters")
    if (
        parameters.get("run_id") != run_id
        or parameters.get("generation_id") != generation_id
        or parameters.get("input_root") != bind_contract.get("input_root")
        or Path(str(parameters.get("source_reference", ""))).resolve(strict=True)
        != Path(str(bind_contract["resources"]["reference"]["source_path"])).resolve(
            strict=True
        )
    ):
        raise ValueError("ssQTL normalization parameters differ from bind contract")

    expected_artifacts = {
        "prepared_cases": "prepared_cases.tsv",
        "prepared_samples": "prepared_samples.tsv",
        "r_stdout": "r_prepare.stdout.log",
        "r_stderr": "r_prepare.stderr.log",
        "r_report": "r_prepare.json",
    }
    artifacts = preparation.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(expected_artifacts):
        raise ValueError("ssQTL preparation artifact inventory is incomplete")
    for role, filename in expected_artifacts.items():
        record = artifacts[role]
        artifact = source / filename
        if (
            not isinstance(record, dict)
            or set(record) != {"relative_path", "sha256", "size"}
            or record.get("relative_path") != filename
            or record.get("sha256") != sha256_file(artifact)
            or record.get("size") != artifact.stat().st_size
        ):
            raise ValueError(f"ssQTL preparation artifact binding drift: {role}")
    if (
        preparation.get("artifact_set_sha256") != sha256_json(artifacts)
        or validation.get("preparation_artifact_set_sha256")
        != preparation.get("artifact_set_sha256")
        or validation.get("preparation_receipt_sha256")
        != sha256_file(source / "ssqtl_preparation.json")
        or validation.get("normalized_manifest_sha256")
        != sha256_file(source / "normalized_manifest.tsv")
        or validation.get("parameters_sha256")
        != sha256_file(source / "parameters.json")
    ):
        raise ValueError("ssQTL validation/preparation checksum chain is invalid")
    r_report = _read_json_object(source / "r_prepare.json", "ssQTL R preparation report")
    if (
        r_report.get("schema_version") != "3.0-ssqtl-r-prepare"
        or r_report.get("status") != "PASS"
        or r_report.get("prepared_cases_sha256")
        != artifacts["prepared_cases"]["sha256"]
        or r_report.get("prepared_samples_sha256")
        != artifacts["prepared_samples"]["sha256"]
        or r_report.get("stdout_sha256") != artifacts["r_stdout"]["sha256"]
        or r_report.get("stderr_sha256") != artifacts["r_stderr"]["sha256"]
        or preparation.get("prepared_cases_sha256")
        != r_report.get("prepared_cases_sha256")
        or preparation.get("prepared_samples_sha256")
        != r_report.get("prepared_samples_sha256")
        or preparation.get("r_stdout_sha256") != r_report.get("stdout_sha256")
        or preparation.get("r_stderr_sha256") != r_report.get("stderr_sha256")
        or preparation.get("association_row_count") != r_report.get("case_count")
        or preparation.get("selected_sample_count") != r_report.get("sample_count")
    ):
        raise ValueError("ssQTL R preparation evidence chain is invalid")

    if contract_dir.exists() or contract_dir.is_symlink():
        raise FileExistsError(f"ssQTL contract destination already exists: {contract_dir}")
    contract_dir.parent.mkdir(parents=True, exist_ok=True)
    staging = contract_dir.parent / f".{contract_dir.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        for name in required:
            shutil.copy2(source / name, staging / name)
            if sha256_file(staging / name) != bundle_inventory[name]:
                raise RuntimeError(f"ssQTL bundle changed during admission: {name}")
        log_dir = staging / "ssqtl_normalization_nextflow"
        validation_dir = log_dir / "runtime_manifest" / "runtime_manifest_validation"
        validation_dir.mkdir(parents=True)
        for source_path, target in (
            (stdout, log_dir / "controller.stdout.log"),
            (stderr, log_dir / "controller.stderr.log"),
            (trace_path, log_dir / "trace.txt"),
            (runtime_validation_path, validation_dir / "validation.json"),
        ):
            shutil.copy2(source_path, target)
        shutil.copy2(controller_runtime_path, staging / "controller_runtime.json")
        shutil.copy2(bind_path, staging / "ssqtl_bind_contract.json")
        selected_inventory = _ssqtl_selected_input_inventory(
            tasks,
            bind_contract=bind_contract,
            bind_contract_sha256=sha256_file(staging / "ssqtl_bind_contract.json"),
            preparation=preparation,
        )
        atomic_write_json(staging / "ssqtl_input_inventory.json", selected_inventory)
        future_log = contract_dir / "ssqtl_normalization_nextflow"
        receipt = {
            "schema_version": "3.0-ssqtl-normalization-execution",
            "status": "SUCCEEDED",
            "profile": profile,
            "process_label": "portable_runtime",
            "executor": expected_executor,
            "runtime_manifest_sha256": runtime_identity["runtime_manifest_sha256"],
            "runtime_fingerprint_sha256": runtime_identity[
                "runtime_fingerprint_sha256"
            ],
            "runtime_sif_sha256": expected_sif,
            "max_parallel": max_parallel,
            "controller_runtime_identity_sha256": controller_runtime["identity_sha256"],
            "controller_runtime_sha256": sha256_file(staging / "controller_runtime.json"),
            "controller_source_validation": source_validation,
            "bind_contract_sha256": sha256_file(staging / "ssqtl_bind_contract.json"),
            "input_inventory_sha256": sha256_file(staging / "ssqtl_input_inventory.json"),
            "runtime_validation_relative_path": str(
                (future_log / "runtime_manifest" / "runtime_manifest_validation" / "validation.json")
                .relative_to(contract_dir.parent)
            ),
            "runtime_validation_sha256": sha256_file(
                validation_dir / "validation.json"
            ),
            "trace_relative_path": str((future_log / "trace.txt").relative_to(contract_dir.parent)),
            "trace_sha256": sha256_file(log_dir / "trace.txt"),
            "controller_stdout_sha256": sha256_file(log_dir / "controller.stdout.log"),
            "controller_stderr_sha256": sha256_file(log_dir / "controller.stderr.log"),
            "normalization_tasks_sha256": sha256_file(staging / "tasks.jsonl"),
            "normalization_task_set_sha256": task_set_fingerprint(tasks),
            "preparation_receipt_sha256": sha256_file(staging / "ssqtl_preparation.json"),
            "validation_receipt_sha256": sha256_file(staging / "validation.json"),
            "parameters_sha256": sha256_file(staging / "parameters.json"),
            "normalized_manifest_sha256": sha256_file(staging / "normalized_manifest.tsv"),
            "bundle_inventory": bundle_inventory,
            "trace_roles": {
                role: {
                    key: row[key]
                    for key in ("task_id", "process", "hash", "native_id", "status")
                }
                for role, row in sorted(trace_rows.items())
            },
            "launch_command_sha256": sha256_json(command),
        }
        atomic_write_json(staging / "ssqtl_normalization_execution.json", receipt)
        os.replace(staging, contract_dir)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    result = dict(validation)
    result.update(
        {
            "tasks": str(contract_dir / "tasks.jsonl"),
            "normalized_manifest": str(contract_dir / "normalized_manifest.tsv"),
            "validation": str(contract_dir / "validation.json"),
            "parameters": str(contract_dir / "parameters.json"),
            "preparation": str(contract_dir / "ssqtl_preparation.json"),
            "input_inventory": str(contract_dir / "ssqtl_input_inventory.json"),
            "normalization_execution": str(
                contract_dir / "ssqtl_normalization_execution.json"
            ),
            "normalization_trace": str(
                contract_dir / "ssqtl_normalization_nextflow" / "trace.txt"
            ),
        }
    )
    return result


def prepare_portable_run(
    *,
    run_dir: str | Path,
    run_id: str,
    generation_id: str,
    profile: str,
    adapter: str,
    runtime_identity_path: str | Path,
    runtime_identity_sha256: str | None = None,
    project_binding: Mapping[str, Any] | None = None,
    manifest: str | Path | None = None,
    input_root: str | Path | None = None,
    reference: str | Path | None = None,
    ssqtl_normalization_bundle: str | Path | None = None,
    ssqtl_normalization_execution: dict[str, Any] | None = None,
    batch_request: str | Path | None = None,
    rerun_source_run: str | Path | None = None,
    rerun_receipt: str | Path | None = None,
    max_cases_per_shard: int = 256,
    max_parallel: int = 1,
    scc_project: str | None = None,
    scc_qname: str | None = None,
    docker_worker_uid: object | None = None,
    docker_worker_gid: object | None = None,
    resume: bool = False,
) -> dict[str, Any]:
    if not isinstance(max_parallel, int) or isinstance(max_parallel, bool) or not 1 <= max_parallel <= 8:
        raise ValueError("max_parallel must be an integer from 1 to 8")
    docker_worker = docker_worker_identity(
        profile, docker_worker_uid, docker_worker_gid
    )
    scc_site_adapter = None
    if profile == "scc":
        site_project, site_qname = _validate_scc_site_options(scc_project, scc_qname)
        scc_site_adapter = _scc_site_adapter_contract(site_project, site_qname)
    root_value = reject_symlink_path_components(run_dir, label="run directory")
    root = root_value.resolve(strict=False)
    if root.exists() and not root.is_dir():
        raise ValueError(f"run path is not a directory: {root}")
    preexisting_entries = list(root.iterdir()) if root.exists() else []
    bootstrap_directories = {".runtime", ".work"}
    preexisting = any(
        path.name not in bootstrap_directories or path.is_symlink() or not path.is_dir()
        for path in preexisting_entries
    )
    if preexisting and not resume:
        raise FileExistsError(f"non-empty run directory requires --resume: {root}")
    root.mkdir(parents=True, exist_ok=True)
    runtime_path, runtime_value = _runtime_identity(
        runtime_identity_path, runtime_identity_sha256
    )
    with _run_lock(root):
        contract_dir = root / "contract"
        identity_path = contract_dir / "run_identity.json"
        if resume:
            if not identity_path.is_file():
                raise ValueError("resume requires the original run_identity.json")
            tasks_path = contract_dir / "tasks.jsonl"
            canonical_tasks = list(read_jsonl(tasks_path))
            task_ids = [str(task["task_id"]) for task in canonical_tasks]
            terminal_set_complete = bool(task_ids) and all(
                (root / "results" / "cases" / task_id / "case_result.json").is_file()
                and (root / "results" / "cases" / task_id / "terminal_bundle.json").is_file()
                for task_id in task_ids
            )
            if terminal_set_complete:
                _results, failures = _validated_terminal_case_results(
                    root, canonical_tasks
                )
                if failures:
                    raise ValueError(
                        "terminal case failures require a new generation; "
                        "same-generation resume is prohibited"
                    )
            if (root / "review" / "review_package").is_dir() or (
                root / "review" / "finalized_review.json"
            ).is_file():
                raise ValueError("immutable review evidence closes this generation for execution")
            if any((root / "accounting" / "scc").glob("*/request/request.json")):
                raise ValueError(
                    "frozen SCC accounting request closes rendering; finalize qacct accounting"
                )
            previous = _read_json_object(identity_path, "run identity")
            project_binding_path = contract_dir / "project_binding.json"
            if project_binding is not None:
                live_binding = _validate_project_binding(
                    project_binding, adapter=adapter
                )
                if not project_binding_path.is_file() or project_binding_path.is_symlink():
                    raise ValueError(
                        "resume requires the original project source binding; "
                        "start a new output directory"
                    )
                frozen_binding = _validate_project_binding(
                    _read_json_object(
                        project_binding_path, "frozen project source binding"
                    ),
                    adapter=adapter,
                )
                if live_binding != frozen_binding:
                    raise ValueError(
                        "project metadata changed since the run was prepared; "
                        "start a new output directory"
                    )
            elif project_binding_path.exists():
                raise ValueError(
                    "resume requires a live project source binding for comparison"
                )
            if "campaign_binding_sha256" in previous:
                if batch_request is None:
                    raise ValueError(
                        "campaign run resume requires the original --batch-request"
                    )
                live_batch = load_and_validate_batch_request(batch_request)
                frozen_binding = _read_json_object(
                    contract_dir / "campaign_binding.json",
                    "campaign batch binding",
                )
                if (
                    live_batch["request_sha256"]
                    != frozen_binding.get("batch_request_sha256")
                    or live_batch["tasks_sha256"]
                    != frozen_binding.get("tasks_sha256")
                ):
                    raise ValueError(
                        "resume batch-request differs from the immutable campaign admission"
                    )
            elif batch_request is not None:
                raise ValueError(
                    "--batch-request cannot be attached to a non-campaign resume"
                )
            shard_plan_path = root / "shards" / "shard_plan.json"
            expected = _identity_contract(
                run_id=run_id,
                generation_id=generation_id,
                profile=profile,
                adapter=adapter,
                tasks_path=tasks_path,
                shard_plan_path=shard_plan_path,
                runtime_path=runtime_path,
                runtime_identity=runtime_value,
                runtime_snapshot_path=contract_dir / "runtime_manifest.snapshot.json",
                scc_site_adapter=scc_site_adapter,
                docker_worker=docker_worker,
            )
            if previous != expected:
                raise ValueError("resume identity differs from the immutable original run")
            normalization = _read_json_object(contract_dir / "normalization.json", "normalization")
        else:
            if (rerun_source_run is None) != (rerun_receipt is None):
                raise ValueError(
                    "rerun generation requires both rerun_source_run and rerun_receipt"
                )
            if batch_request is not None:
                if (
                    adapter != "ssqtl"
                    or rerun_source_run is not None
                    or ssqtl_normalization_bundle is not None
                    or ssqtl_normalization_execution is not None
                    or manifest is not None
                    or input_root is not None
                    or reference is not None
                ):
                    raise ValueError(
                        "batch-request is an ssQTL execution intent and is mutually "
                        "exclusive with normalization, generic, and rerun inputs"
                    )
                normalization = _prepare_campaign_batch(
                    batch_request=batch_request,
                    contract_dir=contract_dir,
                    run_id=run_id,
                    generation_id=generation_id,
                )
            elif rerun_source_run is not None and rerun_receipt is not None:
                normalization = prepare_rerun_task_set(
                    rerun_source_run,
                    rerun_receipt,
                    contract_dir,
                    run_id=run_id,
                    generation_id=generation_id,
                )
                if normalization.get("adapter_id") != adapter:
                    raise ValueError(
                        "rerun source adapter differs from the requested adapter"
                    )
            elif adapter == "generic":
                if manifest is None or input_root is None or reference is None:
                    raise ValueError("generic adapter requires manifest, input_root, and reference")
                normalization = _prepare_generic(
                    manifest=manifest,
                    input_root=input_root,
                    reference=reference,
                    contract_dir=contract_dir,
                    run_id=run_id,
                    generation_id=generation_id,
                )
            elif adapter == "ssqtl":
                if ssqtl_normalization_bundle is None or ssqtl_normalization_execution is None:
                    raise ValueError(
                        "ssqtl adapter requires the completed portable raw-input normalization stage"
                    )
                normalization = _prepare_ssqtl(
                    normalization_bundle=ssqtl_normalization_bundle,
                    normalization_execution=ssqtl_normalization_execution,
                    contract_dir=contract_dir,
                    run_id=run_id,
                    generation_id=generation_id,
                    profile=profile,
                    runtime_identity=runtime_value,
                    max_parallel=max_parallel,
                )
            else:
                raise ValueError("adapter must be generic or ssqtl")
            tasks_path = Path(normalization["tasks"]).resolve(strict=True)
            create_bounded_shards(
                tasks_path,
                root / "shards",
                max_cases_per_shard=max_cases_per_shard,
            )
            shard_plan_path = root / "shards" / "shard_plan.json"
            contract_dir.mkdir(parents=True, exist_ok=True)
            if project_binding is not None:
                atomic_write_json(
                    contract_dir / "project_binding.json",
                    _validate_project_binding(project_binding, adapter=adapter),
                )
            runtime_snapshot_path = contract_dir / "runtime_manifest.snapshot.json"
            atomic_write_json(runtime_snapshot_path, runtime_value)
            identity = _identity_contract(
                run_id=run_id,
                generation_id=generation_id,
                profile=profile,
                adapter=adapter,
                tasks_path=tasks_path,
                shard_plan_path=shard_plan_path,
                runtime_path=runtime_path,
                runtime_identity=runtime_value,
                runtime_snapshot_path=runtime_snapshot_path,
                scc_site_adapter=scc_site_adapter,
                docker_worker=docker_worker,
            )
            atomic_write_json(contract_dir / "normalization.json", normalization)
            atomic_write_json(identity_path, identity)
            _append_control_event(
                root,
                "PREPARED",
                {
                    "task_count": normalization["task_count"],
                    "task_set_sha256": normalization.get("task_set_sha256")
                    or normalization.get("tasks_sha256"),
                },
            )
        return {
            "run_dir": str(root),
            "normalization": normalization,
            "identity": _read_json_object(identity_path, "run identity"),
            "shards": _read_json_object(root / "shards" / "shard_plan.json", "shard plan"),
            "runtime_manifest": runtime_value,
        }


def _verify_shard_plan(run_dir: Path) -> dict[str, Any]:
    tasks_path = run_dir / "contract" / "tasks.jsonl"
    identity = _read_json_object(run_dir / "contract" / "run_identity.json", "run identity")
    plan_path = run_dir / "shards" / "shard_plan.json"
    plan = _read_json_object(plan_path, "shard plan")
    if sha256_file(tasks_path) != identity.get("canonical_tasks_sha256"):
        raise ValueError("canonical task set differs from immutable run identity")
    if sha256_file(plan_path) != identity.get("shard_plan_sha256"):
        raise ValueError("shard plan differs from immutable run identity")
    if plan.get("source_sha256") != sha256_file(tasks_path):
        raise ValueError("shard plan source identity differs from canonical tasks")
    master = list(read_jsonl(tasks_path))
    observed: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for expected_order, row in enumerate(plan.get("shards", [])):
        if int(row.get("shard_order", -1)) != expected_order:
            raise ValueError("shard_order is not contiguous")
        path = Path(str(row.get("path", ""))).resolve(strict=True)
        try:
            path.relative_to((run_dir / "shards").resolve(strict=True))
        except ValueError as exc:
            raise ValueError(f"shard path escapes run shard root: {path}") from exc
        if path.is_symlink() or sha256_file(path) != row.get("sha256"):
            raise ValueError(f"shard file checksum drift: {path}")
        subset = list(read_jsonl(path))
        if len(subset) != int(row.get("case_count", -1)):
            raise ValueError(f"shard case count drift: {path}")
        ids = [str(task["task_id"]) for task in subset]
        if ids != list(row.get("task_ids", [])):
            raise ValueError(f"shard task ID list drift: {path}")
        duplicate = seen_ids.intersection(ids)
        if duplicate:
            raise ValueError(f"task appears in multiple shards: {sorted(duplicate)}")
        seen_ids.update(ids)
        observed.extend(subset)
    if observed != master:
        raise ValueError("ordered shard concatenation differs from canonical task set")
    if len(master) != int(plan.get("case_count", -1)):
        raise ValueError("shard plan total case count differs from canonical task set")
    return plan


def _run_nextflow_shard(
    *,
    shard: dict[str, Any],
    run_dir: Path,
    profile: str,
    runtime_identity: Path,
    runtime_identity_value: dict[str, Any],
    nextflow: str,
    work_dir: Path,
    resume: bool,
    fake_runtime: bool,
    runtime_image: str | None,
    runtime_sif: str | None,
    runtime_sif_sha256: str | None,
    run_id: str,
    generation_id: str,
    scc_project: str | None = None,
    scc_qname: str | None = None,
    docker_worker_uid: object | None = None,
    docker_worker_gid: object | None = None,
    controller_java: str | Path | None = None,
    max_parallel: int = 1,
) -> dict[str, Any]:
    worker_identity = docker_worker_identity(
        profile, docker_worker_uid, docker_worker_gid
    )
    shard_id = shard["shard_id"]
    shard_sessions = run_dir / "sessions" / shard_id
    prior_attempts = sorted(shard_sessions.glob("attempt-*")) if shard_sessions.is_dir() else []
    attempt = len(prior_attempts) + 1
    session = shard_sessions / f"attempt-{attempt:04d}"
    if session.exists() or session.is_symlink():
        raise FileExistsError(f"Nextflow attempt output already exists: {session}")
    session.mkdir(parents=True, exist_ok=True)
    launch_dir = run_dir / "control" / "nextflow" / shard_id
    launch_dir.mkdir(parents=True, exist_ok=True)
    stable_session_id = f"{run_id}__{generation_id}__{shard_id}"
    command = [
        nextflow,
        "run",
        str(_project_root()),
        "-entry",
        "PORTABLE_RUN",
        "-profile",
        profile,
        "-work-dir",
        str(work_dir),
        "--canonical_tasks",
        shard["path"],
        "--run_output",
        str(run_dir),
        "--session_id",
        stable_session_id,
        "--runtime_manifest",
        str(runtime_identity),
        "--runtime_manifest_sha256",
        runtime_identity_value["runtime_manifest_sha256"],
        "--runtime_fingerprint_sha256",
        runtime_identity_value["runtime_fingerprint_sha256"],
        "--max_parallel",
        str(max_parallel),
        "--enable_reports",
        "true",
        "--session_output",
        str(session),
    ]
    if runtime_image:
        command.extend(["--runtime_image", runtime_image])
    if runtime_sif:
        command.extend(["--runtime_sif", runtime_sif])
    if runtime_sif_sha256:
        command.extend(["--runtime_sif_sha256", runtime_sif_sha256])
    if worker_identity is not None:
        command.extend(
            [
                "--host_uid",
                str(worker_identity["uid"]),
                "--host_gid",
                str(worker_identity["gid"]),
            ]
        )
    if profile == "scc":
        site_project, site_qname = _validate_scc_site_options(scc_project, scc_qname)
        command.extend(["--scc_project", site_project])
        if site_qname:
            command.extend(["--scc_qname", site_qname])
    if fake_runtime:
        command.extend(["--test_mode", "true", "--fake_runtime", "true"])
    if resume:
        command.append("-resume")
    stdout = session / "controller.stdout.log"
    stderr = session / "controller.stderr.log"
    with stdout.open("w", encoding="utf-8") as out, stderr.open("w", encoding="utf-8") as err:
        launch_environment = {**os.environ, "NXF_ANSI_LOG": "false"}
        if controller_java is not None:
            launch_environment = normalized_nextflow_environment(
                controller_java,
                base=launch_environment,
            )
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=out,
            stderr=err,
            env=launch_environment,
            cwd=launch_dir,
        )
    return {
        "shard_id": shard_id,
        "attempt": attempt,
        "stable_session_id": stable_session_id,
        "launch_dir": str(launch_dir),
        "command": command,
        "exit_code": completed.returncode,
        "stdout": str(stdout),
        "stderr": str(stderr),
        "trace": str(session / "trace.txt"),
        "stdout_relative_path": str(stdout.relative_to(run_dir)),
        "stderr_relative_path": str(stderr.relative_to(run_dir)),
        "trace_relative_path": str((session / "trace.txt").relative_to(run_dir)),
    }


def _expected_trace_bindings(
    trace_paths: list[Path],
    expected_case_ids: list[str],
    *,
    runtime_validation_receipts: dict[Path, Path] | None = None,
) -> list[dict[str, Any]]:
    """Bind every case ID to exactly one Nextflow task identity from its process tag."""

    rows: list[dict[str, Any]] = []
    case_rows: list[dict[str, Any]] = []
    for path in trace_paths:
        validation_count = 0
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                process = str(row.get("process") or row.get("name") or "")
                base = {
                    "task_id": str(row.get("task_id") or ""),
                    "process": process,
                    "hash": str(row.get("hash") or ""),
                    "trace_file": str(path.resolve(strict=True)),
                }
                if "VALIDATE_RUNTIME_IDENTITY" in process or "VALIDATE_RUNTIME_MANIFEST" in process:
                    validation_count += 1
                    validation_path = (runtime_validation_receipts or {}).get(
                        path.resolve(strict=True),
                        path.parent
                        / "runtime_manifest"
                        / "runtime_manifest_validation"
                        / "validation.json",
                    )
                    validation = _read_json_object(
                        validation_path, "runtime manifest validation receipt"
                    )
                    if validation.get("status") != "PASS":
                        raise ValueError("runtime manifest validation did not pass")
                    rows.append(
                        {
                            **base,
                            "control_role": "runtime_manifest_validation",
                            "control_receipt": str(validation_path.resolve(strict=True)),
                            "control_receipt_sha256": sha256_file(validation_path),
                            "runtime_manifest_sha256": str(
                                validation.get("runtime_manifest_sha256") or ""
                            ),
                            "runtime_fingerprint_sha256": str(
                                validation.get("runtime_fingerprint_sha256") or ""
                            ),
                        }
                    )
                    continue
                if "RUN_PORTABLE_CASE" not in process:
                    raise ValueError(f"portable trace contains an unexpected process: {process}")
                match = re.search(r"\(([^()]*)\)\s*$", process)
                if not match:
                    raise ValueError(f"portable trace process lacks its case tag: {process}")
                case_row = {**base, "case_id": match.group(1)}
                rows.append(case_row)
                case_rows.append(case_row)
        if validation_count != 1:
            raise ValueError(
                f"portable trace must contain exactly one runtime validation task: {path}"
            )
    observed = [row["case_id"] for row in case_rows]
    if len(observed) != len(set(observed)):
        raise ValueError("portable trace contains duplicate case tags")
    if set(observed) != set(expected_case_ids):
        raise ValueError(
            "portable trace case set differs from canonical tasks; "
            f"missing={sorted(set(expected_case_ids) - set(observed))} "
            f"unexpected={sorted(set(observed) - set(expected_case_ids))}"
        )
    return rows


def _expected_ssqtl_normalization_trace_bindings(
    run_dir: Path,
    *,
    runtime_identity: dict[str, Any],
    run_id: str,
    generation_id: str,
    profile: str,
) -> tuple[list[Path], list[dict[str, Any]]]:
    """Admit the one-time native ssQTL prepare session into final accounting."""

    execution_path = run_dir / "contract" / "ssqtl_normalization_execution.json"
    if not execution_path.exists():
        return [], []
    if execution_path.is_symlink() or not execution_path.is_file():
        raise ValueError("ssQTL normalization execution receipt is not a regular file")
    execution = _read_json_object(execution_path, "ssQTL normalization execution")
    trace = run_dir / "contract" / "ssqtl_normalization_nextflow" / "trace.txt"
    validation_path = (
        run_dir
        / "contract"
        / "ssqtl_normalization_nextflow"
        / "runtime_manifest"
        / "runtime_manifest_validation"
        / "validation.json"
    )
    if (
        sha256_file(trace) != execution.get("trace_sha256")
        or sha256_file(validation_path) != execution.get("runtime_validation_sha256")
        or sha256_file(run_dir / "contract" / "tasks.jsonl")
        != execution.get("normalization_tasks_sha256")
        or task_set_fingerprint(list(read_jsonl(run_dir / "contract" / "tasks.jsonl")))
        != execution.get("normalization_task_set_sha256")
    ):
        raise ValueError("ssQTL normalization accounting artifacts drifted")
    validation = _read_json_object(validation_path, "ssQTL normalization runtime validation")
    if (
        validation.get("status") != "PASS"
        or validation.get("runtime_manifest_sha256")
        != runtime_identity["runtime_manifest_sha256"]
        or validation.get("runtime_fingerprint_sha256")
        != runtime_identity["runtime_fingerprint_sha256"]
    ):
        raise ValueError("ssQTL normalization accounting runtime manifest drifted")
    trace_rows = _validate_ssqtl_normalization_trace(
        trace,
        profile=profile,
        run_id=run_id,
        generation_id=generation_id,
    )
    validation_row = trace_rows["runtime_manifest_validation"]
    prepare_row = trace_rows["ssqtl_normalization"]
    base_validation = {
        key: validation_row[key] for key in ("task_id", "process", "hash", "trace_file")
    }
    base_prepare = {
        key: prepare_row[key] for key in ("task_id", "process", "hash", "trace_file")
    }
    return [trace.resolve(strict=True)], [
        {
            **base_validation,
            "control_role": "runtime_manifest_validation",
            "control_receipt": str(validation_path.resolve(strict=True)),
            "control_receipt_sha256": sha256_file(validation_path),
            "runtime_manifest_sha256": validation["runtime_manifest_sha256"],
            "runtime_fingerprint_sha256": validation[
                "runtime_fingerprint_sha256"
            ],
        },
        {
            **base_prepare,
            "prepare_role": "ssqtl_normalization",
            "prepare_receipt": str(execution_path.resolve(strict=True)),
            "prepare_receipt_sha256": sha256_file(execution_path),
            "prepared_tasks_sha256": execution["normalization_tasks_sha256"],
            "prepared_task_set_sha256": execution["normalization_task_set_sha256"],
            "preparation_receipt_sha256": execution["preparation_receipt_sha256"],
            "input_inventory_sha256": execution["input_inventory_sha256"],
            "runtime_manifest_sha256": execution["runtime_manifest_sha256"],
            "runtime_fingerprint_sha256": execution[
                "runtime_fingerprint_sha256"
            ],
            "runtime_sif_sha256": execution.get("runtime_sif_sha256"),
            "trace_sha256": execution["trace_sha256"],
        },
    ]


def _canonical_resources(
    task: dict[str, Any],
) -> list[tuple[str, dict[str, Any], bool]]:
    resources: list[tuple[str, dict[str, Any], bool]] = []
    core = task["core"]
    for track in core["tracks"]:
        resources.extend(
            (
                ("bam", track["bam"], False),
                ("bai", track["bai"], False),
            )
        )
    resources.extend(
        (f"reference:{role}", resource, True)
        for role, resource in core["reference"]["resources"].items()
    )
    if core["auxiliary"]["state"] == "PRESENT":
        resources.append(("auxiliary", core["auxiliary"], True))
    return resources


def _verify_canonical_input_identities(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    """Re-observe each unique READY input once before launching Nextflow.

    Large BAM/BAI resources intentionally use size and mtime identities. Fixed
    reference and auxiliary resources retain content SHA-256 identities, which
    are verified here once per unique source rather than once per render task.
    """

    observed: dict[str, dict[str, Any]] = {}
    expected_by_path: dict[str, tuple[int, int, str]] = {}
    for task in tasks:
        if task["core"]["preflight"]["state"] != "READY":
            continue
        for role, resource, require_sha in _canonical_resources(task):
            identity = resource["identity"]
            expected_sha = str(identity.get("sha256") or "").lower()
            if require_sha and not expected_sha:
                raise ValueError(
                    f"READY task {task['task_id']} {role} resource lacks content SHA-256"
                )
            if expected_sha and not re.fullmatch(r"[a-f0-9]{64}", expected_sha):
                raise ValueError(
                    f"READY task {task['task_id']} {role} resource has an invalid SHA-256"
                )
            source = Path(str(resource["source_path"])).expanduser()
            if source.is_symlink():
                raise ValueError(f"canonical source path became a symlink: {source}")
            resolved = source.resolve(strict=True)
            if not resolved.is_file():
                raise ValueError(f"canonical source is not a regular file: {resolved}")
            expected_size = int(identity.get("size", -1))
            expected_mtime = int(identity.get("mtime_ns", -1))
            key = str(resolved)
            expected_tuple = (expected_size, expected_mtime, expected_sha)
            prior_expected = expected_by_path.get(key)
            if prior_expected is not None and prior_expected != expected_tuple:
                raise ValueError(f"canonical source has conflicting identities: {resolved}")
            if key in observed:
                continue
            stat = resolved.stat()
            if expected_size != stat.st_size:
                raise ValueError(f"canonical source size changed: {resolved}")
            if expected_mtime != stat.st_mtime_ns:
                raise ValueError(f"canonical source mtime changed: {resolved}")
            actual_sha = sha256_file(resolved) if expected_sha else ""
            if expected_sha and actual_sha != expected_sha:
                raise ValueError(f"canonical source content changed: {resolved}")
            expected_by_path[key] = expected_tuple
            observed[key] = {
                "path": str(resolved),
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "identity_mode": "sha256" if expected_sha else "metadata",
                **({"sha256": actual_sha} if expected_sha else {}),
            }
    return {
        "schema_version": "3.0-input-revalidation",
        "status": "PASS",
        "unique_resource_count": len(observed),
        "resource_set_sha256": sha256_json(sorted(observed.values(), key=lambda row: row["path"])),
    }


def _case_tree_identity(root: Path) -> dict[str, Any]:
    if root.is_symlink() or not root.resolve(strict=True).is_dir():
        raise ValueError(f"case output must be a regular non-symlink directory: {root}")
    resolved = root.resolve(strict=True)
    files: list[dict[str, Any]] = []
    for path in sorted(resolved.rglob("*"), key=lambda item: str(item.relative_to(resolved))):
        if path.is_symlink():
            raise ValueError(f"case output contains a symlink: {path}")
        if path.is_file():
            files.append(
                {
                    "relative_path": str(path.relative_to(resolved)),
                    "size": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
            )
    if not files:
        raise ValueError(f"case output is empty: {resolved}")
    return {
        "root": str(resolved),
        "file_count": len(files),
        "tree_sha256": sha256_json(files),
        "files": files,
    }


def _admit_session_case_outputs(
    run_dir: Path,
    session_dir: Path,
    shard: dict[str, Any],
    canonical_tasks: dict[str, dict[str, Any]],
    *,
    allow_debug_only: bool = False,
) -> dict[str, Any]:
    """Promote one immutable attempt only when it cannot replace different bytes."""

    source_root = session_dir / "case_outputs"
    destination_root = run_dir / "results" / "cases"
    destination_root.mkdir(parents=True, exist_ok=True)
    admissions: list[dict[str, Any]] = []
    for task_id in shard["task_ids"]:
        source = source_root / task_id
        source_identity = _case_tree_identity(source)
        result_path = source / "case_result.json"
        terminal_path = source / "terminal_bundle.json"
        result = _read_json_object(result_path, "attempt case result")
        terminal = _read_json_object(terminal_path, "attempt terminal bundle")
        validate_v3_case_result_document(result)
        validate_v3_terminal_bundle_document(terminal, result)
        if terminal["case_result_sha256"] != sha256_file(result_path):
            raise ValueError(f"attempt terminal bundle case-result checksum drift: {task_id}")
        if int(terminal["case_result_size"]) != result_path.stat().st_size:
            raise ValueError(f"attempt terminal bundle case-result size drift: {task_id}")
        if not allow_debug_only:
            assert_production_artifact_tree(source, label=f"case output {task_id}")
            assert_not_debug_metadata(result, label=f"case result {task_id}")
        task = canonical_tasks[task_id]
        for key in (
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
        ):
            if result.get(key) != task.get(key):
                raise ValueError(f"attempt case output identity drift for {task_id}: {key}")
        artifact_prefix = Path("results") / "cases" / task_id
        for role, record in result["artifacts"].items():
            relative = Path(str(record["relative_path"]))
            try:
                attempt_relative = relative.relative_to(artifact_prefix)
            except ValueError as exc:
                raise ValueError(
                    f"attempt artifact is outside its canonical case root: {task_id}:{role}"
                ) from exc
            if not attempt_relative.parts or ".." in attempt_relative.parts:
                raise ValueError(f"attempt artifact path is unsafe: {task_id}:{role}")
            artifact = source / attempt_relative
            if artifact.is_symlink() or not artifact.is_file():
                raise ValueError(f"attempt artifact is unavailable: {task_id}:{role}")
            resolved_artifact = artifact.resolve(strict=True)
            try:
                resolved_artifact.relative_to(source.resolve(strict=True))
            except ValueError as exc:
                raise ValueError(
                    f"attempt artifact escapes its case root: {task_id}:{role}"
                ) from exc
            if int(record["size"]) != resolved_artifact.stat().st_size:
                raise ValueError(f"attempt artifact size drift: {task_id}:{role}")
            if str(record["sha256"]) != sha256_file(resolved_artifact):
                raise ValueError(f"attempt artifact checksum drift: {task_id}:{role}")
        target = destination_root / task_id
        action = "ADMITTED"
        if target.exists() or target.is_symlink():
            target_identity = _case_tree_identity(target)
            if target_identity["tree_sha256"] != source_identity["tree_sha256"]:
                raise ValueError(
                    f"same-generation case output differs from the admitted result: {task_id}"
                )
            action = "IDENTICAL_ALREADY_ADMITTED"
        else:
            staging = destination_root / f".{task_id}.tmp-{uuid.uuid4().hex}"
            try:
                shutil.copytree(source, staging, symlinks=False)
                copied = _case_tree_identity(staging)
                if copied["tree_sha256"] != source_identity["tree_sha256"]:
                    raise ValueError(f"case output copy checksum drift: {task_id}")
                os.replace(staging, target)
            except BaseException:
                shutil.rmtree(staging, ignore_errors=True)
                raise
        admissions.append(
            {
                "task_id": task_id,
                "action": action,
                "attempt_tree_sha256": source_identity["tree_sha256"],
                "admitted_path": str(target),
            }
        )
    receipt = {
        "schema_version": "3.0-case-output-admission",
        "status": "PASS",
        "shard_id": shard["shard_id"],
        "session_dir": str(session_dir.resolve(strict=True)),
        "case_count": len(admissions),
        "admissions": admissions,
        "admission_set_sha256": sha256_json(admissions),
    }
    atomic_write_json(session_dir / "case_output_admission.json", receipt)
    return receipt


def _accounting_generation(run_dir: Path) -> tuple[Path, list[dict[str, Any]] | None]:
    root = run_dir / "accounting" / "local"
    existing = sorted(path for path in root.glob("generation-*") if path.is_dir()) if root.is_dir() else []
    next_index = len(existing) + 1
    output = root / f"generation-{next_index:04d}"
    lineage: dict[tuple[str, str], dict[str, Any]] = {}
    for generation in existing:
        path = generation / "cache_lineage.jsonl"
        if not path.is_file() or path.is_symlink():
            continue
        for row in read_jsonl(path):
            key = (str(row.get("process", "")), str(row.get("hash", "")))
            if all(key):
                lineage[key] = row
    return output, list(lineage.values()) or None


def _known_lineage_keys(run_dir: Path) -> set[tuple[str, str]]:
    _output, lineage = _accounting_generation(run_dir)
    return {
        (str(row.get("process", "")), str(row.get("hash", "")))
        for row in (lineage or [])
        if row.get("process") and row.get("hash")
    }


def _freeze_partial_accounting(
    run_dir: Path,
    trace_paths: list[Path],
    canonical_tasks: list[dict[str, Any]],
    *,
    shard_id: str,
    attempt: int,
) -> dict[str, Any] | None:
    """Freeze successful work from a failed controller attempt for later cache proof."""

    available = [path for path in trace_paths if path.is_file() and not path.is_symlink()]
    if not available:
        return None
    if len(available) != 1:
        raise ValueError("partial accounting freezes exactly one controller trace at a time")
    fieldnames: list[str] = []
    successful: list[dict[str, str]] = []
    known = _known_lineage_keys(run_dir)
    new_identity_found = False
    for trace_path in available:
        with trace_path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            observed_fields = list(reader.fieldnames or [])
            if not fieldnames:
                fieldnames = observed_fields
            elif observed_fields != fieldnames:
                raise ValueError("partial accounting traces use different field sets")
            for row in reader:
                normalized = {key: str(value or "") for key, value in row.items()}
                status = str(row.get("status") or "").upper()
                exit_value = str(row.get("exit") or "")
                process = str(row.get("process") or row.get("name") or "")
                key = (process, str(row.get("hash") or ""))
                is_runtime_validation = (
                    "VALIDATE_RUNTIME_IDENTITY" in process
                    or "VALIDATE_RUNTIME_MANIFEST" in process
                )
                is_case = "RUN_PORTABLE_CASE" in process
                if is_case and status in {"COMPLETED", "CACHED"} and exit_value in {
                    "",
                    "0",
                    "-",
                }:
                    match = re.search(r"\(([^()]*)\)\s*$", process)
                    case_id = match.group(1) if match else ""
                    admitted = run_dir / "results" / "cases" / case_id / "terminal_bundle.json"
                    if not case_id or not admitted.is_file():
                        # A killed controller may have a valid work output that
                        # Nextflow has not republished/admitted yet.  It is not
                        # cache evidence until that terminal bundle is bound.
                        continue
                if (
                    status in {"COMPLETED", "CACHED"}
                    and exit_value in {"", "0", "-"}
                    and key not in known
                ):
                    new_identity_found = True
                if (
                    status in {"COMPLETED", "CACHED"}
                    and exit_value in {"", "0", "-"}
                    and (key not in known or is_runtime_validation)
                ):
                    successful.append(normalized)
    if not successful or not new_identity_found:
        return None
    trace_root = run_dir / "accounting" / "attempt_traces" / shard_id
    filtered = trace_root / f"attempt-{attempt:04d}.successful.trace.tsv"
    if filtered.exists() or filtered.is_symlink():
        raise FileExistsError(f"partial accounting trace already exists: {filtered}")
    write_tsv(filtered, fieldnames, successful)
    case_ids: list[str] = []
    for row in successful:
        process = str(row.get("process") or row.get("name") or "")
        if "VALIDATE_RUNTIME_IDENTITY" in process or "VALIDATE_RUNTIME_MANIFEST" in process:
            continue
        if "RUN_PORTABLE_CASE" not in process:
            raise ValueError(f"successful portable trace has an unexpected process: {process}")
        match = re.search(r"\(([^()]*)\)\s*$", process)
        if not match:
            raise ValueError(f"successful portable process lacks case tag: {process}")
        case_ids.append(match.group(1))
    task_map = {str(task["task_id"]): task for task in canonical_tasks}
    if any(case_id not in task_map for case_id in case_ids):
        raise ValueError("partial trace contains a case outside canonical tasks")
    bundles = [
        run_dir / "results" / "cases" / case_id / "terminal_bundle.json"
        for case_id in case_ids
    ]
    if any(not path.is_file() for path in bundles):
        raise ValueError("successful partial trace lacks its terminal case bundle")
    original_validation = (
        available[0].parent
        / "runtime_manifest"
        / "runtime_manifest_validation"
        / "validation.json"
    )
    expected_rows = _expected_trace_bindings(
        [filtered],
        case_ids,
        runtime_validation_receipts={
            filtered.resolve(strict=True): original_validation.resolve(strict=True)
        },
    )
    output, prior_lineage = _accounting_generation(run_dir)
    result = collect_local_accounting(
        [filtered],
        output,
        expected_tasks=expected_rows,
        expected_cases=[task_map[case_id] for case_id in case_ids],
        terminal_bundles=bundles,
        cached_lineage=prior_lineage,
    )
    atomic_write_json(
        output / "provisional.json",
        {
            "schema_version": "3.0-provisional-accounting",
            "status": "PROVISIONAL_ONLY",
            "shard_id": shard_id,
            "attempt": attempt,
            "successful_case_ids": case_ids,
            "review_gate": False,
            "meaning": "cache lineage retained after controller failure; not a completed run gate",
        },
    )
    return result


def _recover_prior_attempt_accounting(
    run_dir: Path,
    canonical_tasks: list[dict[str, Any]],
    *,
    allow_debug_only: bool = False,
) -> list[dict[str, Any]]:
    """Recover completed lineage left by a killed controller before using -resume."""

    recovered: list[dict[str, Any]] = []
    sessions = run_dir / "sessions"
    if not sessions.is_dir():
        return recovered
    task_map = {str(task["task_id"]): task for task in canonical_tasks}
    for shard_dir in sorted(path for path in sessions.iterdir() if path.is_dir()):
        for attempt_dir in sorted(path for path in shard_dir.glob("attempt-*") if path.is_dir()):
            trace = attempt_dir / "trace.txt"
            if not trace.is_file() or trace.is_symlink():
                continue
            try:
                attempt = int(attempt_dir.name.rsplit("-", 1)[1])
            except (IndexError, ValueError):
                raise ValueError(f"invalid prior attempt directory: {attempt_dir}")
            successful_case_ids: list[str] = []
            with trace.open(encoding="utf-8", newline="") as handle:
                for row in csv.DictReader(handle, delimiter="\t"):
                    process = str(row.get("process") or row.get("name") or "")
                    if (
                        "RUN_PORTABLE_CASE" not in process
                        or str(row.get("status") or "").upper()
                        not in {"COMPLETED", "CACHED"}
                        or str(row.get("exit") or "") not in {"", "0", "-"}
                    ):
                        continue
                    match = re.search(r"\(([^()]*)\)\s*$", process)
                    if match:
                        successful_case_ids.append(match.group(1))
            available_case_ids = [
                task_id
                for task_id in successful_case_ids
                if (attempt_dir / "case_outputs" / task_id).is_dir()
            ]
            if available_case_ids:
                unknown = sorted(set(available_case_ids) - set(task_map))
                if unknown:
                    raise ValueError(f"prior attempt contains unknown cases: {unknown}")
                _admit_session_case_outputs(
                    run_dir,
                    attempt_dir,
                    {"shard_id": shard_dir.name, "task_ids": available_case_ids},
                    task_map,
                    allow_debug_only=allow_debug_only,
                )
            result = _freeze_partial_accounting(
                run_dir,
                [trace],
                canonical_tasks,
                shard_id=shard_dir.name,
                attempt=attempt,
            )
            if result is not None:
                recovered.append(result)
    return recovered


def _validate_execution_runtime(
    prepared: dict[str, Any],
    *,
    profile: str,
    runtime_identity_path: Path,
    runtime_image: str | None,
    runtime_sif: str | None,
    fake_runtime: bool,
) -> tuple[dict[str, Any], str | None]:
    run_dir = Path(prepared["run_dir"]).resolve(strict=True)
    run_identity = _read_json_object(
        run_dir / "contract" / "run_identity.json", "run identity"
    )
    observed_path, observed = _runtime_identity(
        runtime_identity_path, run_identity["runtime_manifest_sha256"]
    )
    snapshot = _read_json_object(
        run_dir / "contract" / "runtime_manifest.snapshot.json",
        "runtime manifest snapshot",
    )
    snapshot_path = run_dir / "contract" / "runtime_manifest.snapshot.json"
    if sha256_file(snapshot_path) != run_identity.get("runtime_manifest_snapshot_sha256"):
        raise ValueError("runtime manifest snapshot differs from immutable run identity")
    if observed != snapshot or observed != prepared.get("runtime_manifest"):
        raise ValueError("runtime manifest changed between planning and execution")
    if str(observed_path) != run_identity.get("runtime_manifest"):
        raise ValueError("runtime manifest path differs from immutable run identity")
    if observed.get("runtime_fingerprint_sha256") != run_identity.get(
        "runtime_fingerprint_sha256"
    ):
        raise ValueError("runtime fingerprint differs from immutable run identity")
    observed_sif_sha: str | None = None
    if runtime_sif:
        sif_path = Path(runtime_sif).expanduser()
        if sif_path.is_symlink() or not sif_path.resolve(strict=True).is_file():
            raise ValueError(f"runtime SIF must be a regular non-symlink file: {sif_path}")
        observed_sif_sha = sha256_file(sif_path.resolve(strict=True))
    if profile == "test":
        if not fake_runtime:
            raise ValueError("test profile requires fake_runtime")
    elif profile not in {"standalone", "docker", "scc"}:
        raise ValueError("profile must be standalone, docker, scc, or internal test")

    source = observed.get("source") if isinstance(observed.get("source"), dict) else {}
    validate_controller_source_identity(
        profile,
        source,
        _project_root(),
        allow_test_runtime=fake_runtime,
    )
    return observed, observed_sif_sha


def execute_portable_run(
    prepared: dict[str, Any],
    *,
    profile: str,
    runtime_identity_path: str | Path,
    nextflow: str | None = None,
    work_dir: str | Path | None = None,
    resume: bool = False,
    fake_runtime: bool = False,
    runtime_image: str | None = None,
    runtime_sif: str | None = None,
    dry_run: bool = False,
    scc_controller: dict[str, Any] | None = None,
    scc_project: str | None = None,
    scc_qname: str | None = None,
    docker_worker_uid: object | None = None,
    docker_worker_gid: object | None = None,
    qacct_command: str = "qacct",
    max_parallel: int = 1,
) -> dict[str, Any]:
    if not isinstance(max_parallel, int) or isinstance(max_parallel, bool) or not 1 <= max_parallel <= 8:
        raise ValueError("max_parallel must be an integer from 1 to 8")
    worker_identity = docker_worker_identity(
        profile, docker_worker_uid, docker_worker_gid
    )
    if prepared.get("identity", {}).get("docker_worker_identity") != worker_identity:
        raise ValueError(
            "Docker worker UID/GID differs from the immutable run identity"
        )
    site_project: str | None = None
    site_qname: str | None = None
    if profile == "scc":
        site_project, site_qname = _validate_scc_site_options(scc_project, scc_qname)
    run_dir = Path(prepared["run_dir"]).resolve(strict=True)
    identity_path = Path(runtime_identity_path).expanduser().resolve(strict=True)
    plan = _verify_shard_plan(run_dir)
    runtime_value, runtime_sif_sha256 = _validate_execution_runtime(
        prepared,
        profile=profile,
        runtime_identity_path=identity_path,
        runtime_image=runtime_image,
        runtime_sif=runtime_sif,
        fake_runtime=fake_runtime,
    )
    work = (
        reject_symlink_path_components(work_dir, label="work directory").resolve(
            strict=False
        )
        if work_dir
        else run_dir / "work"
    )
    work.mkdir(parents=True, exist_ok=True)
    executable = _nextflow_executable(nextflow)
    commands: list[dict[str, Any]] = []
    tasks = list(read_jsonl(run_dir / "contract" / "tasks.jsonl"))
    task_map = {str(task["task_id"]): task for task in tasks}
    allow_debug_only = profile == "test" and fake_runtime
    with _run_lock(run_dir):
        controller_runtime = freeze_controller_runtime_identity(run_dir, executable)
        _validate_normalization_controller_contract(
            run_dir,
            observed_controller=controller_runtime,
        )
        site_adapter = None
        if profile == "scc":
            if plan.get("active_shard_limit") != 1 or int(
                plan.get("max_cases_per_shard", 257)
            ) > 256:
                raise ValueError("SCC shard bounds differ from the fixed v3 control contract")
            site_adapter = _freeze_scc_site_adapter(
                run_dir,
                project=site_project,
                qname=site_qname,
            )
            if prepared["identity"].get("scc_site_adapter") != site_adapter:
                raise ValueError("SCC site adapter differs from the immutable run identity")
        input_revalidation = _verify_canonical_input_identities(tasks)
        recovered_accounting = (
            _recover_prior_attempt_accounting(
                run_dir, tasks, allow_debug_only=allow_debug_only
            )
            if resume and not dry_run
            else []
        )
        _append_control_event(
            run_dir,
            "EXECUTION_PLANNED",
            {
                "profile": profile,
                "controller_runtime_identity_sha256": controller_runtime[
                    "identity_sha256"
                ],
                "docker_worker_identity": worker_identity,
                "scc_site_adapter": site_adapter,
                "input_revalidation": input_revalidation,
                "recovered_accounting_generations": len(recovered_accounting),
            },
        )
        for shard in plan["shards"]:
            if dry_run:
                commands.append(
                    {
                        "shard_id": shard["shard_id"],
                        "status": "DRY_RUN",
                        "canonical_tasks": shard["path"],
                    }
                )
                continue
            result = _run_nextflow_shard(
                shard=shard,
                run_dir=run_dir,
                profile=profile,
                runtime_identity=identity_path,
                runtime_identity_value=runtime_value,
                nextflow=executable,
                work_dir=work,
                resume=resume,
                fake_runtime=fake_runtime,
                runtime_image=runtime_image,
                runtime_sif=runtime_sif,
                runtime_sif_sha256=runtime_sif_sha256,
                run_id=prepared["identity"]["run_id"],
                generation_id=prepared["identity"]["generation_id"],
                scc_project=site_project,
                scc_qname=site_qname,
                docker_worker_uid=(worker_identity or {}).get("uid"),
                docker_worker_gid=(worker_identity or {}).get("gid"),
                controller_java=controller_runtime["java"]["executable"],
                max_parallel=max_parallel,
            )
            commands.append(result)
            if result["exit_code"] != 0:
                provisional: list[dict[str, Any]] = []
                provisional_error: str | None = None
                try:
                    provisional = _recover_prior_attempt_accounting(
                        run_dir, tasks, allow_debug_only=allow_debug_only
                    )
                except (OSError, ValueError, RuntimeError) as exc:
                    provisional_error = f"{type(exc).__name__}: {exc}"
                _append_control_event(run_dir, "INFRASTRUCTURE_FATAL", result)
                summary = {
                    "schema_version": "3.0",
                    "status": "INFRASTRUCTURE_FATAL",
                    "exit_code": 1,
                    "controller_runtime": controller_runtime,
                    "shards": commands,
                    "provisional_accounting": provisional,
                    "provisional_accounting_error": provisional_error,
                }
                _write_run_summary(run_dir, summary)
                return summary
            session_dir = Path(result["trace"]).parent.resolve(strict=True)
            try:
                result["case_output_admission"] = _admit_session_case_outputs(
                    run_dir,
                    session_dir,
                    shard,
                    task_map,
                    allow_debug_only=allow_debug_only,
                )
            except (OSError, ValueError, RuntimeError) as exc:
                _append_control_event(
                    run_dir,
                    "INFRASTRUCTURE_FATAL",
                    {"shard_id": shard["shard_id"], "case_output_admission": str(exc)},
                )
                summary = {
                    "schema_version": "3.0",
                    "status": "INFRASTRUCTURE_FATAL",
                    "exit_code": 1,
                    "controller_runtime": controller_runtime,
                    "shards": commands,
                    "reason": f"case output admission failed: {exc}",
                }
                _write_run_summary(run_dir, summary)
                return summary

        if resume and not dry_run:
            # A cached attempt can republish output that was unavailable during
            # the pre-launch recovery window.  Freeze its original COMPLETED
            # lineage now, before admitting current CACHED rows.
            recovered_accounting.extend(
                _recover_prior_attempt_accounting(
                    run_dir, tasks, allow_debug_only=allow_debug_only
                )
            )

        if dry_run:
            summary = {
                "schema_version": "3.0",
                "status": "DRY_RUN",
                "exit_code": 0,
                "controller_runtime": controller_runtime,
                "shards": commands,
            }
            _write_run_summary(run_dir, summary)
            return summary

        render_trace_paths = [Path(row["trace"]) for row in commands]
        expected = [str(task["task_id"]) for task in tasks]
        bundles = [
            run_dir / "results" / "cases" / task_id / "terminal_bundle.json"
            for task_id in expected
        ]
        render_expected_trace_rows = _expected_trace_bindings(
            render_trace_paths, expected
        )
        normalization_trace_paths, normalization_expected_trace_rows = (
            _expected_ssqtl_normalization_trace_bindings(
                run_dir,
                runtime_identity=runtime_value,
                run_id=str(prepared["identity"]["run_id"]),
                generation_id=str(prepared["identity"]["generation_id"]),
                profile=profile,
            )
        )
        trace_paths = [*normalization_trace_paths, *render_trace_paths]
        expected_trace_rows = [
            *normalization_expected_trace_rows,
            *render_expected_trace_rows,
        ]
        if profile in {"standalone", "docker", "test"}:
            accounting_output, prior_lineage = _accounting_generation(run_dir)
            accounting = collect_local_accounting(
                trace_paths,
                accounting_output,
                expected_tasks=expected_trace_rows,
                expected_cases=tasks,
                terminal_bundles=bundles,
                cached_lineage=prior_lineage,
            )
        else:
            if not scc_controller or not site_adapter:
                raise ValueError(
                    "SCC execution requires controller native ID and project before accounting admission"
                )
            accounting = collect_scc_accounting(
                trace_paths,
                run_dir / "accounting" / "scc" / str(prepared["identity"]["generation_id"]),
                controller=scc_controller,
                expected_project=site_adapter["project"],
                expected_qname=site_adapter["qname"],
                expected_tasks=expected_trace_rows,
                expected_cases=tasks,
                terminal_bundles=bundles,
                qacct_command=qacct_command,
            )
        accounting_root = Path(str(accounting.get("output_dir", ""))).resolve(strict=True)
        try:
            accounting["output_relative_path"] = str(accounting_root.relative_to(run_dir))
        except ValueError as exc:
            raise ValueError("accounting output is outside the run root") from exc
        case_results, failures = _validated_terminal_case_results(run_dir, tasks)
        direct_outputs = _write_direct_output_tables(run_dir, tasks, case_results)
        trace_report = _write_trace_report(run_dir, trace_paths)
        status, exit_code = _terminal_execution_state(
            profile=profile,
            accounting_pass=accounting.get("status") == "PASS",
            failed_case_ids=failures,
        )
        summary = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "status": status,
            "exit_code": exit_code,
            "profile": profile,
            "controller_runtime": controller_runtime,
            "docker_worker_identity": worker_identity,
            "scc_site_adapter": site_adapter,
            "expected_case_count": len(expected),
            "observed_case_count": len(case_results),
            "failed_case_ids": failures,
            "accounting": accounting,
            "shards": commands,
            "direct_outputs": {**direct_outputs, **trace_report},
            "effective_max_parallel": max_parallel,
            "publication_state": "NOT_READY",
            "human_review_required": False,
        }
        if status == "SNAPSHOTS_READY":
            _write_run_summary(run_dir, summary)
            try:
                summary["review_package"] = build_review_package_v3(run_dir)
            except (OSError, ValueError, RuntimeError) as exc:
                summary["review_package"] = None
                summary["review_package_warning"] = f"{type(exc).__name__}: {exc}"
            _write_run_summary(run_dir, summary)
        elif status == "CASE_FAILURES":
            _write_run_summary(run_dir, summary)
            try:
                summary["rerun"] = freeze_case_failure_rerun(
                    run_dir, tasks, case_results
                )
            except (OSError, ValueError, RuntimeError) as exc:
                summary.update(
                    {
                        "status": "INFRASTRUCTURE_FATAL",
                        "exit_code": 1,
                        "rerun": None,
                        "reason": f"case-failure rerun generation failed: {exc}",
                    }
                )
            _write_run_summary(run_dir, summary)
        else:
            _write_run_summary(run_dir, summary)
        _append_control_event(
            run_dir,
            str(summary["status"]),
            {
                "failed_case_count": len(failures),
                "snapshots_sha256": direct_outputs["snapshots_sha256"],
                "failed_cases_sha256": direct_outputs["failed_cases_sha256"],
                "trace_sha256": trace_report["trace_sha256"],
            },
        )
        return summary


def finalize_scc_run_accounting(
    run_dir: str | Path,
    *,
    qacct_command: str = "qacct",
) -> dict[str, Any]:
    """Close SCC accounting from trace/qacct/case evidence, never run_summary."""

    run_root = Path(run_dir).expanduser()
    if run_root.is_symlink() or not run_root.resolve(strict=True).is_dir():
        raise ValueError(f"run directory must be a regular non-symlink directory: {run_root}")
    run_root = run_root.resolve(strict=True)
    with _run_lock(run_root):
        identity = _read_json_object(
            run_root / "contract" / "run_identity.json", "run identity"
        )
        if identity.get("profile") != "scc":
            raise ValueError("SCC accounting finalization requires an SCC run identity")
        tasks = list(read_jsonl(run_root / "contract" / "tasks.jsonl"))
        case_results, failures = _validated_terminal_case_results(run_root, tasks)
        output = (
            run_root
            / "accounting"
            / "scc"
            / str(identity.get("generation_id", ""))
        )
        if output.is_symlink() or not output.is_dir():
            raise ValueError("SCC accounting request directory is unavailable")
        finalized = finalize_scc_accounting(output, qacct_command=qacct_command)
        finalized["output_relative_path"] = str(output.relative_to(run_root))
        controller_path = run_root / "contract" / "controller_runtime.json"
        controller_runtime = _read_json_object(
            controller_path, "controller runtime identity"
        )
        shard_plan_path = run_root / "shards" / "shard_plan.json"
        shard_plan = (
            _read_json_object(shard_plan_path, "shard plan")
            if shard_plan_path.is_file() and not shard_plan_path.is_symlink()
            else {"shards": []}
        )
        summary: dict[str, Any] = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "profile": "scc",
            "controller_runtime": controller_runtime,
            "expected_case_count": len(tasks),
            "observed_case_count": len(case_results),
            "failed_case_ids": failures,
            "accounting": finalized,
            "shards": [{} for _ in shard_plan.get("shards", [])],
            "publication_state": "NOT_READY",
            "human_review_required": False,
            "review_gate": False,
        }
        if failures:
            rerun = freeze_case_failure_rerun(run_root, tasks, case_results)
            if rerun is None:
                raise ValueError("failed SCC case set produced no rerun generation")
            summary.update(
                {
                    "status": "CASE_FAILURES",
                    "exit_code": 2,
                    "rerun": rerun,
                }
            )
        else:
            summary.update({"status": "SNAPSHOTS_READY", "exit_code": 0})
            _write_run_summary(run_root, summary)
            try:
                summary["review_package"] = build_review_package_v3(run_root)
            except (OSError, ValueError, RuntimeError) as exc:
                summary["review_package"] = None
                summary["review_package_warning"] = f"{type(exc).__name__}: {exc}"
        _write_run_summary(run_root, summary)
        _append_control_event(
            run_root,
            str(summary["status"]),
            {
                "accounting_receipt_sha256": finalized.get("receipt_sha256"),
                "review_package": summary.get("review_package"),
                "rerun": summary.get("rerun"),
            },
        )
        return summary
