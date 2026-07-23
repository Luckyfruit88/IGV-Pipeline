from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .artifact_admission_v3 import (
    assert_not_debug_metadata,
    assert_production_artifact,
    assert_production_artifact_tree,
)
from .contracts import validate_v3_case_result_document
from .evidence_v3 import locate_verified_accounting
from .publication import verify_checksum_tree
from .review_server import (
    GENERIC_ASSERTIONS,
    SSQTL_ASSERTIONS,
    _authoritative_tasks,
    _runtime_binding,
)
from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_jsonl,
    sha256_file,
    sha256_json,
    utc_now,
    write_jsonl,
    write_tsv,
)


def _object(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    assert_not_debug_metadata(value, label=label)
    return value


def _artifact(
    run_root: Path,
    case_result: Mapping[str, Any],
    role: str,
    *,
    required: bool,
) -> tuple[Path | None, str | None]:
    artifacts = case_result.get("artifacts")
    record = artifacts.get(role) if isinstance(artifacts, Mapping) else None
    if record is None:
        if required:
            raise ValueError(f"eligible case lacks {role}: {case_result.get('task_id')}")
        return None, None
    if not isinstance(record, Mapping):
        raise ValueError(f"case artifact record is not an object: {case_result.get('task_id')}:{role}")
    relative = Path(str(record.get("relative_path", "")))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"unsafe case artifact path: {relative}")
    candidate = run_root / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"case artifact is unavailable or symlinked: {candidate}")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(run_root)
    except ValueError as exc:
        raise ValueError(f"case artifact escapes the run root: {candidate}") from exc
    digest = str(record.get("sha256", "")).strip().lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"case artifact SHA-256 is malformed: {candidate}")
    if sha256_file(resolved) != digest:
        raise ValueError(f"case artifact checksum drift: {candidate}")
    if int(record.get("size", -1)) != resolved.stat().st_size:
        raise ValueError(f"case artifact size drift: {candidate}")
    assert_production_artifact(resolved, label=f"review package {role}")
    return resolved, digest


def _case_contracts(
    run_root: Path,
    tasks: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cases_root = run_root / "results" / "cases"
    assert_production_artifact_tree(cases_root, label="review package case-result tree")
    contracts: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for task in tasks:
        task_id = str(task["task_id"])
        result_path = cases_root / task_id / "case_result.json"
        result = _object(result_path, label=f"case result {task_id}")
        validate_v3_case_result_document(result)
        for field in (
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
        ):
            if result.get(field) != task.get(field):
                raise ValueError(f"case result differs from canonical task {task_id}: {field}")
        if (
            result.get("schema_version") != "3.0"
            or result.get("eligible") is not True
            or result.get("artifact_review_state") != "REVIEW_PENDING"
            or result.get("publication_state") != "NOT_READY"
        ):
            raise ValueError(f"case is not eligible for the review package: {task_id}")
        adapter = str(result.get("adapter_type", "")).lower()
        if adapter not in {"generic", "ssqtl"}:
            raise ValueError(f"case has an unsupported adapter type: {task_id}:{adapter}")
        assertions = result.get("required_manual_assertions")
        if assertions is None:
            assertions = GENERIC_ASSERTIONS if adapter == "generic" else SSQTL_ASSERTIONS
        if (
            not isinstance(assertions, (list, tuple))
            or not assertions
            or any(not isinstance(item, str) or not item.strip() for item in assertions)
            or len(set(assertions)) != len(assertions)
        ):
            raise ValueError(f"case has invalid required manual assertions: {task_id}")
        for role in result["artifacts"]:
            _artifact(run_root, result, role, required=True)
        review_image, review_sha = _artifact(
            run_root, result, "review_image", required=True
        )
        scientific_qc, scientific_qc_sha = _artifact(
            run_root, result, "scientific_qc", required=False
        )
        review_relative = Path("cases") / task_id / "review.png"
        qc_relative = (
            Path("cases") / task_id / "scientific_qc.json"
            if scientific_qc is not None
            else None
        )
        contract = {
            "schema_version": "3.0-review-contract",
            "run_id": str(result["run_id"]),
            "generation_id": str(result["generation_id"]),
            "task_id": task_id,
            "manifest_order": int(result["manifest_order"]),
            "input_fingerprint": str(result["input_fingerprint"]),
            "case_result_sha256": sha256_file(result_path),
            "review_image_sha256": review_sha,
            "scientific_qc_sha256": scientific_qc_sha,
            "artifact_set_sha256": sha256_json(result["artifacts"]),
            "adapter_type": adapter,
            "evidence_state": str(result.get("evidence_state", "")),
            "required_manual_assertions": list(assertions),
            "review_image_relative_path": str(review_relative),
            "scientific_qc_relative_path": str(qc_relative) if qc_relative else None,
        }
        assert_not_debug_metadata(contract, label=f"review contract {task_id}")
        contracts.append(contract)
        sources.append(
            {
                "task_id": task_id,
                "review_source": review_image,
                "review_relative": review_relative,
                "qc_source": scientific_qc,
                "qc_relative": qc_relative,
            }
        )
    return contracts, sources


def _write_checksums(root: Path) -> Path:
    files = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS"),
        key=lambda path: str(path.relative_to(root)),
    )
    target = root / "SHA256SUMS"
    atomic_write_text(
        target,
        "".join(f"{sha256_file(path)}  {path.relative_to(root)}\n" for path in files),
    )
    return target


def _source_paths(value: Any) -> set[str]:
    observed: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) == "source_path" and str(child):
                observed.add(str(child))
            observed.update(_source_paths(child))
    elif isinstance(value, (list, tuple)):
        for child in value:
            observed.update(_source_paths(child))
    return observed


def _assert_source_paths_redacted(root: Path, forbidden: set[str]) -> None:
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() == ".png":
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"unexpected non-text review-package artifact: {path}") from exc
        leaked = next((value for value in forbidden if value and value in text), None)
        if leaked is not None:
            raise ValueError(f"review package exposes a canonical source path: {path}")


def _result(destination: Path, package: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "3.0-review-package-pointer",
        "relative_path": "review/review_package",
        "package_id": package["package_id"],
        "package_json_sha256": sha256_file(destination / "package.json"),
        "checksums_sha256": sha256_file(destination / "SHA256SUMS"),
        "contract_set_sha256": package["contract_set_sha256"],
        "eligible_review_count": package["eligible_review_count"],
    }


def build_review_package_v3(run_dir: str | Path) -> dict[str, Any]:
    """Freeze authoritative run evidence into a path-redacted, checksum-bound package."""

    run_value = Path(run_dir).expanduser()
    if run_value.is_symlink() or not run_value.resolve(strict=True).is_dir():
        raise ValueError(f"run directory must be a regular non-symlink directory: {run_value}")
    run_root = run_value.resolve(strict=True)
    tasks = _authoritative_tasks(run_root)
    accounting = locate_verified_accounting(run_root, tasks)
    runtime_binding = _runtime_binding(run_root)
    contracts, sources = _case_contracts(run_root, tasks)
    contract_set_sha256 = sha256_json(contracts)
    run_identity = _object(run_root / "contract" / "run_identity.json", label="run identity")
    controller_path = run_root / "contract" / "controller_runtime.json"
    controller_runtime = _object(controller_path, label="controller runtime identity")
    package_identity = {
        "run_id": run_identity["run_id"],
        "generation_id": run_identity["generation_id"],
        "canonical_tasks_sha256": run_identity["canonical_tasks_sha256"],
        "contract_set_sha256": contract_set_sha256,
        "accounting_provider": accounting["provider"],
        "accounting_receipt_sha256": accounting["receipt_sha256"],
        "runtime_binding_sha256": sha256_json(dict(runtime_binding)),
        "controller_runtime_identity_sha256": controller_runtime[
            "identity_sha256"
        ],
        "controller_runtime_contract_sha256": sha256_file(controller_path),
    }
    package_id = "review_package_" + sha256_json(package_identity)
    package = {
        "schema_version": "3.0-review-package",
        "package_id": package_id,
        "created_at": utc_now(),
        "status": "SNAPSHOTS_READY",
        "review_gate": "OPTIONAL_HUMAN_REVIEW",
        **package_identity,
        "runtime_manifest_sha256": runtime_binding["runtime_manifest_sha256"],
        "runtime_fingerprint_sha256": runtime_binding[
            "runtime_fingerprint_sha256"
        ],
        "canonical_task_count": len(tasks),
        "eligible_review_count": len(contracts),
        "excluded_count": 0,
        "ordering": "manifest_order",
        "contains_bam_or_bai_files": False,
        "contains_private_source_paths": False,
    }
    if "runtime_oci_digest" in runtime_binding:
        package["runtime_oci_digest"] = runtime_binding["runtime_oci_digest"]
    destination = run_root / "review" / "review_package"
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError("review root must be a regular non-symlink directory")
    if destination.exists() or destination.is_symlink():
        if destination.is_symlink() or not destination.is_dir():
            raise ValueError("existing review package must be a regular non-symlink directory")
        verify_checksum_tree(destination)
        frozen = _object(destination / "package.json", label="existing review package")
        if any(frozen.get(key) != value for key, value in package_identity.items()) or frozen.get(
            "package_id"
        ) != package_id:
            raise ValueError("existing review package differs from the closed run gate")
        if list(read_jsonl(destination / "review_contract.jsonl")) != contracts:
            raise ValueError("existing review package contract set differs from case results")
        assert_production_artifact_tree(destination, label="existing review package")
        _assert_source_paths_redacted(destination, _source_paths(tasks))
        return _result(destination, frozen)

    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        for source in sources:
            review_target = staging / source["review_relative"]
            review_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source["review_source"], review_target)
            qc_source = source["qc_source"]
            if qc_source is not None:
                qc_target = staging / source["qc_relative"]
                shutil.copyfile(qc_source, qc_target)
        write_jsonl(staging / "review_contract.jsonl", contracts)
        write_tsv(
            staging / "review_manifest.tsv",
            [
                "manifest_order",
                "task_id",
                "adapter_type",
                "evidence_state",
                "review_image_relative_path",
                "review_image_sha256",
                "scientific_qc_relative_path",
                "scientific_qc_sha256",
            ],
            contracts,
        )
        atomic_write_json(staging / "package.json", package)
        assert_production_artifact_tree(staging, label="review package staging")
        _assert_source_paths_redacted(staging, _source_paths(tasks))
        _write_checksums(staging)
        verify_checksum_tree(staging)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return _result(destination, package)
