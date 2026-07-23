from __future__ import annotations

import copy
import json
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .publication import verify_checksum_tree
from .contracts import validate_unique_task_set, validate_v3_task_document, v3_task_fingerprint
from .identity import task_set_fingerprint
from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_jsonl,
    reject_symlink_path_components,
    sha256_file,
    sha256_json,
    write_jsonl,
)


def _object(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object")
    return value


def _write_checksums(root: Path) -> None:
    files = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS"),
        key=lambda path: str(path.relative_to(root)),
    )
    atomic_write_text(
        root / "SHA256SUMS",
        "".join(f"{sha256_file(path)}  {path.relative_to(root)}\n" for path in files),
    )


def freeze_case_failure_rerun(
    run_dir: str | Path,
    canonical_tasks: list[dict[str, Any]],
    case_results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Freeze terminal case failures as requests for a different generation."""

    run_root = Path(run_dir).expanduser().resolve(strict=True)
    task_map = {str(task["task_id"]): task for task in canonical_tasks}
    result_map = {str(result["task_id"]): result for result in case_results}
    if set(task_map) != set(result_map):
        raise ValueError("rerun source result set differs from canonical tasks")
    rows: list[dict[str, Any]] = []
    for task in canonical_tasks:
        task_id = str(task["task_id"])
        result = result_map[task_id]
        if result.get("eligible") is True:
            continue
        for field in (
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
        ):
            if result.get(field) != task.get(field):
                raise ValueError(f"rerun source differs from canonical task {task_id}: {field}")
        failures = result.get("failures")
        if not isinstance(failures, list) or not failures:
            failures = [{"code": "CASE_INELIGIBLE", "message": "case is not review eligible"}]
        rows.append(
            {
                "schema_version": "3.0-rerun-request",
                "source_run_id": task["run_id"],
                "source_generation_id": task["generation_id"],
                "source_task_id": task_id,
                "source_manifest_order": task["manifest_order"],
                "source_input_fingerprint": task["input_fingerprint"],
                "failure_codes": [str(row.get("code", "CASE_FAILURE")) for row in failures],
                "failure_set_sha256": sha256_json(failures),
                "control_action": "CREATE_NEW_GENERATION",
                "target_generation_policy": "MUST_DIFFER_FROM_SOURCE_GENERATION",
                "same_generation_resume_allowed": False,
            }
        )
    if not rows:
        return None
    identity = _object(run_root / "contract" / "run_identity.json", label="run identity")
    tasks_path = run_root / "contract" / "tasks.jsonl"
    if identity.get("canonical_tasks_sha256") != sha256_file(tasks_path):
        raise ValueError("run identity no longer binds the rerun source task set")
    request_set_sha256 = sha256_json(rows)
    rerun_id = "case_failures_" + request_set_sha256
    destination = run_root / "rerun" / "generations" / rerun_id
    receipt = {
        "schema_version": "3.0-rerun-receipt",
        "rerun_id": rerun_id,
        "state": "RERUN_REQUIRED",
        "source_run_id": identity["run_id"],
        "source_generation_id": identity["generation_id"],
        "canonical_tasks_sha256": identity["canonical_tasks_sha256"],
        "rerun_case_count": len(rows),
        "rerun_request_set_sha256": request_set_sha256,
        "rerun_manifest": "rerun_manifest.jsonl",
        "target_generation_policy": "MUST_DIFFER_FROM_SOURCE_GENERATION",
        "same_generation_resume_allowed": False,
    }
    if destination.exists() or destination.is_symlink():
        if destination.is_symlink() or not destination.is_dir():
            raise ValueError("existing case-failure rerun generation is not a regular directory")
        verify_checksum_tree(destination)
        frozen = _object(destination / "rerun_receipt.json", label="rerun receipt")
        frozen_rows = list(read_jsonl(destination / "rerun_manifest.jsonl"))
        if any(frozen.get(key) != value for key, value in receipt.items()) or frozen_rows != rows:
            raise ValueError("existing case-failure rerun generation differs from terminal results")
        receipt = frozen
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = destination.parent / f".{rerun_id}.tmp-{uuid.uuid4().hex}"
        staging.mkdir(mode=0o700)
        try:
            write_jsonl(staging / "rerun_manifest.jsonl", rows)
            receipt["rerun_manifest_sha256"] = sha256_file(staging / "rerun_manifest.jsonl")
            atomic_write_json(staging / "rerun_receipt.json", receipt)
            _write_checksums(staging)
            verify_checksum_tree(staging)
            os.replace(staging, destination)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
    return {
        "schema_version": "3.0-rerun-pointer",
        "rerun_id": rerun_id,
        "relative_path": str(destination.relative_to(run_root)),
        "rerun_case_count": len(rows),
        "rerun_manifest_sha256": sha256_file(destination / "rerun_manifest.jsonl"),
        "rerun_receipt_sha256": sha256_file(destination / "rerun_receipt.json"),
        "checksums_sha256": sha256_file(destination / "SHA256SUMS"),
        "target_generation_policy": "MUST_DIFFER_FROM_SOURCE_GENERATION",
        "same_generation_resume_allowed": False,
    }


def prepare_rerun_task_set(
    source_run_dir: str | Path,
    rerun_receipt_path: str | Path,
    output_dir: str | Path,
    *,
    run_id: str,
    generation_id: str,
) -> dict[str, Any]:
    """Materialize a checksum-bound rerun request as a different generation."""

    source_value = Path(source_run_dir).expanduser()
    if source_value.is_symlink():
        raise ValueError("rerun source run must not be a symlink")
    source = source_value.resolve(strict=True)
    if not source.is_dir():
        raise ValueError("rerun source run must be a directory")
    receipt_value = Path(rerun_receipt_path).expanduser()
    if receipt_value.is_symlink():
        raise ValueError("rerun receipt must not be a symlink")
    receipt_path = receipt_value.resolve(strict=True)
    if not receipt_path.is_file() or receipt_path.name != "rerun_receipt.json":
        raise ValueError("rerun receipt must be a regular rerun_receipt.json file")
    generation_root = receipt_path.parent
    verify_checksum_tree(generation_root)
    receipt = _object(receipt_path, label="rerun receipt")
    required_receipt = {
        "schema_version",
        "rerun_id",
        "state",
        "source_run_id",
        "source_generation_id",
        "canonical_tasks_sha256",
        "rerun_case_count",
        "rerun_request_set_sha256",
        "rerun_manifest",
        "rerun_manifest_sha256",
        "target_generation_policy",
        "same_generation_resume_allowed",
    }
    if set(receipt) != required_receipt:
        raise ValueError("rerun receipt fields differ from the schema 3.0 contract")
    if (
        receipt.get("schema_version") != "3.0-rerun-receipt"
        or receipt.get("state") != "RERUN_REQUIRED"
        or receipt.get("target_generation_policy") != "MUST_DIFFER_FROM_SOURCE_GENERATION"
        or receipt.get("same_generation_resume_allowed") is not False
    ):
        raise ValueError("rerun receipt does not require a different generation")
    manifest_name = str(receipt.get("rerun_manifest", ""))
    if manifest_name != "rerun_manifest.jsonl":
        raise ValueError("rerun receipt manifest name is invalid")
    manifest_path = generation_root / manifest_name
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("rerun manifest is unavailable")
    if sha256_file(manifest_path) != receipt.get("rerun_manifest_sha256"):
        raise ValueError("rerun manifest checksum differs from its receipt")
    requests = list(read_jsonl(manifest_path))
    if (
        len(requests) != receipt.get("rerun_case_count")
        or sha256_json(requests) != receipt.get("rerun_request_set_sha256")
    ):
        raise ValueError("rerun request set differs from its receipt")
    if not requests:
        raise ValueError("rerun request set is empty")

    identity = _object(source / "contract" / "run_identity.json", label="source run identity")
    tasks_path = source / "contract" / "tasks.jsonl"
    if tasks_path.is_symlink() or not tasks_path.is_file():
        raise ValueError("source canonical tasks are unavailable")
    if (
        identity.get("run_id") != receipt.get("source_run_id")
        or identity.get("generation_id") != receipt.get("source_generation_id")
        or identity.get("canonical_tasks_sha256") != receipt.get("canonical_tasks_sha256")
        or sha256_file(tasks_path) != receipt.get("canonical_tasks_sha256")
    ):
        raise ValueError("rerun receipt differs from the immutable source run")
    if run_id != receipt["source_run_id"]:
        raise ValueError("rerun generation must retain the source run_id")
    if generation_id == receipt["source_generation_id"]:
        raise ValueError("rerun generation_id must differ from the source generation")

    source_tasks = {str(row["task_id"]): row for row in read_jsonl(tasks_path)}
    selected: list[dict[str, Any]] = []
    observed_ids: set[str] = set()
    for order, request in enumerate(requests, 1):
        if not isinstance(request, Mapping):
            raise ValueError("rerun request must be an object")
        task_id = str(request.get("source_task_id", ""))
        if not task_id or task_id in observed_ids or task_id not in source_tasks:
            raise ValueError("rerun request task set is duplicate or absent from source tasks")
        observed_ids.add(task_id)
        source_task = source_tasks[task_id]
        for request_key, task_key in (
            ("source_run_id", "run_id"),
            ("source_generation_id", "generation_id"),
            ("source_task_id", "task_id"),
            ("source_manifest_order", "manifest_order"),
            ("source_input_fingerprint", "input_fingerprint"),
        ):
            if request.get(request_key) != source_task.get(task_key):
                raise ValueError(f"rerun request differs from source task {task_id}: {request_key}")
        if (
            request.get("control_action") != "CREATE_NEW_GENERATION"
            or request.get("same_generation_resume_allowed") is not False
        ):
            raise ValueError("rerun request does not require a new generation")
        task = copy.deepcopy(source_task)
        task["run_id"] = run_id
        task["generation_id"] = generation_id
        task["manifest_order"] = order
        task.pop("input_fingerprint", None)
        task["input_fingerprint"] = v3_task_fingerprint(task)
        validate_v3_task_document(task)
        selected.append(task)
    selected = validate_unique_task_set(selected)

    destination_value = reject_symlink_path_components(
        output_dir, label="rerun normalization output"
    )
    destination = destination_value.resolve(strict=False)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"rerun normalization output already exists: {destination}")
    destination.mkdir(parents=True, mode=0o700)
    output_tasks = destination / "tasks.jsonl"
    write_jsonl(output_tasks, selected)
    result = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "status": "READY",
        "adapter_id": selected[0]["adapter_id"],
        "run_id": run_id,
        "generation_id": generation_id,
        "task_count": len(selected),
        "task_set_sha256": task_set_fingerprint(selected),
        "tasks_sha256": sha256_file(output_tasks),
        "tasks": str(output_tasks),
        "source_run": str(source),
        "source_generation_id": receipt["source_generation_id"],
        "source_canonical_tasks_sha256": receipt["canonical_tasks_sha256"],
        "source_rerun_id": receipt["rerun_id"],
        "source_rerun_receipt": str(receipt_path),
        "source_rerun_receipt_sha256": sha256_file(receipt_path),
        "source_rerun_manifest_sha256": receipt["rerun_manifest_sha256"],
        "same_generation_resume_allowed": False,
    }
    atomic_write_json(destination / "rerun_import.json", result)
    return result
