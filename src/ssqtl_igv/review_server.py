from __future__ import annotations

import fcntl
import html
import json
import os
import re
import secrets
import shutil
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, quote, urlparse

from .artifact_admission_v3 import (
    assert_not_debug_metadata,
    assert_production_artifact,
    assert_production_artifact_tree,
)
from .campaign_v3 import LEDGER_EVENT_TYPES, append_campaign_event, verify_campaign_ledger
from .evidence_v3 import locate_verified_accounting
from .publication import verify_checksum_tree
from .contracts import V3_GENERIC_MANUAL_ASSERTIONS, V3_SSQTL_MANUAL_ASSERTIONS
from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_jsonl,
    read_regular_file_bytes,
    sha256_file,
    sha256_json,
    utc_now,
    write_jsonl,
)


GENERIC_ASSERTIONS = V3_GENERIC_MANUAL_ASSERTIONS
SSQTL_ASSERTIONS = V3_SSQTL_MANUAL_ASSERTIONS
REVIEW_BINDING_FIELDS_V3 = (
    "run_id",
    "generation_id",
    "task_id",
    "manifest_order",
    "input_fingerprint",
    "case_result_sha256",
    "artifact_set_sha256",
    "review_image_sha256",
    "scientific_qc_sha256",
    "adapter_type",
    "evidence_state",
)
_JOURNAL_LOCKS: dict[Path, threading.RLock] = {}
_JOURNAL_LOCKS_GUARD = threading.Lock()
LEGACY_REVIEW_LEDGER_KIND = "LEGACY_REVIEW_JOURNAL"
CAMPAIGN_REVIEW_LEDGER_KIND = "CAMPAIGN_LEDGER_PREFIX"


@dataclass(frozen=True)
class ReviewContext:
    run_root: Path
    review_root: Path
    contracts: tuple[dict[str, Any], ...]
    artifact_paths: Mapping[str, Path]
    contract_set_sha256: str
    runtime_binding: Mapping[str, Any]

    @property
    def contract_map(self) -> dict[str, dict[str, Any]]:
        return {str(row["task_id"]): row for row in self.contracts}

    @property
    def journal_path(self) -> Path:
        return self.review_root / "review_journal.jsonl"

    @property
    def campaign_binding_path(self) -> Path:
        return self.run_root / "contract" / "campaign_binding.json"

    @property
    def finalized_path(self) -> Path:
        return self.review_root / "finalized_review.json"


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    assert_not_debug_metadata(value, label=label)
    return value


def _safe_artifact(root: Path, relative_value: Any, expected_sha256: Any) -> tuple[Path, str]:
    root = root.resolve(strict=True)
    relative = Path(str(relative_value or ""))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"review artifact path must be safe and relative: {relative}")
    candidate = root / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"review artifact is unavailable or symlinked: {candidate}")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"review artifact escapes its run/package root: {candidate}") from exc
    digest = str(expected_sha256 or "").strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"review artifact SHA-256 is malformed: {candidate}")
    if sha256_file(resolved) != digest:
        raise ValueError(f"review artifact checksum drift: {candidate}")
    assert_production_artifact(resolved, label="review artifact")
    return resolved, digest


def _artifact_record(
    case_result: Mapping[str, Any], key: str, *, required: bool
) -> tuple[str | None, str | None]:
    artifacts = case_result.get("artifacts")
    if not isinstance(artifacts, Mapping):
        if required:
            raise ValueError(f"eligible case {case_result.get('task_id')} lacks artifacts")
        return None, None
    raw = artifacts.get(key)
    if raw is None and key == "review_image":
        raw = artifacts.get("combined_png")
    if raw is None:
        if required:
            raise ValueError(f"eligible case {case_result.get('task_id')} lacks {key}")
        return None, None
    if isinstance(raw, Mapping):
        return (
            str(raw.get("relative_path", raw.get("path", ""))),
            str(raw.get("sha256", "")),
        )
    digest_key = "combined_sha256" if key == "review_image" else f"{key}_sha256"
    return str(raw), str(artifacts.get(digest_key, case_result.get(digest_key, "")))


def _authoritative_tasks(run_root: Path) -> list[dict[str, Any]]:
    tasks_path = run_root / "contract" / "tasks.jsonl"
    identity_path = run_root / "contract" / "run_identity.json"
    if tasks_path.is_symlink() or not tasks_path.is_file():
        raise ValueError("v3 review requires the immutable contract/tasks.jsonl")
    identity = _load_json_object(identity_path, label="v3 run identity")
    if (
        identity.get("schema_version") != "3.0"
        or identity.get("canonical_tasks_sha256") != sha256_file(tasks_path)
    ):
        raise ValueError("run identity does not bind the canonical task manifest")
    tasks = list(read_jsonl(tasks_path))
    if not tasks:
        raise ValueError("canonical task manifest is empty")
    task_ids: set[str] = set()
    orders: set[int] = set()
    for task in tasks:
        required = (
            "schema_version",
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
        )
        missing = [field for field in required if task.get(field) in (None, "")]
        if missing or str(task.get("schema_version")) != "3.0":
            raise ValueError(f"canonical task lacks v3 identity fields {missing}")
        task_id = str(task["task_id"])
        order = int(task["manifest_order"])
        if task_id in task_ids or order in orders:
            raise ValueError("canonical task manifest has duplicate task/order identity")
        if (
            task["run_id"] != identity.get("run_id")
            or task["generation_id"] != identity.get("generation_id")
        ):
            raise ValueError(f"canonical task differs from run identity: {task_id}")
        task_ids.add(task_id)
        orders.add(order)
    if orders != set(range(1, len(tasks) + 1)):
        raise ValueError("canonical task manifest_order is not contiguous one-based")
    return sorted(tasks, key=lambda row: int(row["manifest_order"]))


def _runtime_binding(run_root: Path) -> dict[str, Any]:
    """Bind the unsigned runtime manifest and every successful validation receipt."""

    identity_path = run_root / "contract" / "run_identity.json"
    snapshot_path = run_root / "contract" / "runtime_manifest.snapshot.json"
    if identity_path.is_symlink() or snapshot_path.is_symlink():
        raise ValueError("run identity/runtime manifest snapshot must not be symlinked")
    identity = _load_json_object(identity_path, label="v3 run identity")
    snapshot = _load_json_object(snapshot_path, label="runtime manifest snapshot")
    snapshot_sha = sha256_file(snapshot_path)
    manifest_sha = str(identity.get("runtime_manifest_sha256", ""))
    fingerprint_sha = str(identity.get("runtime_fingerprint_sha256", ""))
    if (
        identity.get("schema_version") != "3.0"
        or identity.get("runtime_manifest_snapshot_sha256") != snapshot_sha
        or snapshot.get("runtime_manifest_sha256") != manifest_sha
        or snapshot.get("runtime_fingerprint_sha256") != fingerprint_sha
        or not re.fullmatch(r"[a-f0-9]{64}", manifest_sha)
        or not re.fullmatch(r"[a-f0-9]{64}", fingerprint_sha)
    ):
        raise ValueError("run identity does not bind a valid runtime manifest snapshot")
    accounting = locate_verified_accounting(run_root, _authoritative_tasks(run_root))
    controls = accounting.get("runtime_validations")
    if not isinstance(controls, list) or not controls:
        raise ValueError("verified accounting lacks runtime validation controls")
    validations: list[dict[str, Any]] = []
    for control in controls:
        if not isinstance(control, Mapping):
            raise ValueError("verified accounting runtime control is malformed")
        if (
            control.get("runtime_manifest_sha256") != manifest_sha
            or control.get("runtime_fingerprint_sha256") != fingerprint_sha
            or not re.fullmatch(
                r"[a-f0-9]{64}", str(control.get("control_receipt_sha256", ""))
            )
        ):
            raise ValueError("accounting runtime validation differs from run identity")
        validations.append(
            {
                "trace_file": str(control.get("trace_file", "")),
                "control_receipt": str(control.get("control_receipt", "")),
                "validation_sha256": str(control["control_receipt_sha256"]),
            }
        )
    validations.sort(key=lambda row: (row["trace_file"], row["control_receipt"]))
    binding = {
        "schema_version": "3.0-runtime-review-binding",
        "profile": str(identity.get("profile", "")),
        "runtime_manifest_audit_path": str(identity.get("runtime_manifest", "")),
        "run_identity_sha256": sha256_file(identity_path),
        "runtime_manifest_sha256": manifest_sha,
        "runtime_manifest_snapshot_sha256": snapshot_sha,
        "runtime_fingerprint_sha256": fingerprint_sha,
        "runtime_validation_count": len(validations),
        "runtime_validation_set_sha256": sha256_json(validations),
        "runtime_validations": validations,
    }
    provenance = snapshot.get("observed_provenance")
    if isinstance(provenance, Mapping):
        oci = provenance.get("oci")
        if isinstance(oci, Mapping):
            digest = str(oci.get("digest", ""))
            if re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
                binding["runtime_oci_digest"] = digest
        sif_sha = str(provenance.get("sif_sha256") or "")
        if re.fullmatch(r"[a-f0-9]{64}", sif_sha):
            binding["runtime_sif_sha256"] = sif_sha
    return binding


def _contracts_from_case_results(run_root: Path) -> tuple[list[dict[str, Any]], dict[str, Path]]:
    cases_root = run_root / "results" / "cases"
    if cases_root.is_symlink() or not cases_root.is_dir():
        return [], {}
    try:
        cases_root.resolve(strict=True).relative_to(run_root)
    except ValueError as exc:
        raise ValueError("results/cases escapes run_dir through a symlink") from exc
    assert_production_artifact_tree(cases_root, label="review case-result tree")
    tasks = _authoritative_tasks(run_root)
    expected_tasks = {str(task["task_id"]): task for task in tasks}
    case_paths = sorted(cases_root.glob("*/case_result.json"))
    case_documents: list[tuple[Path, dict[str, Any]]] = []
    observed_tasks: dict[str, dict[str, Any]] = {}
    for path in case_paths:
        if path.parent.is_symlink():
            raise ValueError(f"case result parent must not be a symlink: {path.parent}")
        case = _load_json_object(path, label="v3 case result")
        task_id = str(case.get("task_id", ""))
        if not task_id or task_id in observed_tasks:
            raise ValueError(f"case results contain a missing/duplicate task_id: {task_id}")
        if path.parent.name != task_id:
            raise ValueError(f"case result directory differs from task_id: {path}")
        observed_tasks[task_id] = case
        case_documents.append((path, case))
    if set(observed_tasks) != set(expected_tasks):
        raise ValueError(
            "case result set differs from canonical tasks; "
            f"missing={sorted(set(expected_tasks) - set(observed_tasks))[:10]} "
            f"unexpected={sorted(set(observed_tasks) - set(expected_tasks))[:10]}"
        )
    locate_verified_accounting(run_root, tasks)
    _runtime_binding(run_root)
    contracts: list[dict[str, Any]] = []
    artifact_paths: dict[str, Path] = {}
    for path, case in case_documents:
        if str(case.get("schema_version")) != "3.0":
            raise ValueError(f"review service requires a schema 3.0 case result: {path}")
        if case.get("eligible") is not True:
            raise ValueError(f"SNAPSHOTS_READY run contains an ineligible case: {path}")
        required = (
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
            "evidence_state",
            "adapter_type",
        )
        missing = [field for field in required if case.get(field) in (None, "")]
        if missing:
            raise ValueError(f"eligible case result lacks fields {missing}: {path}")
        if case.get("artifact_review_state") != "REVIEW_PENDING":
            raise ValueError(f"eligible case is not REVIEW_PENDING: {case['task_id']}")
        if case.get("publication_state") != "NOT_READY":
            raise ValueError(f"eligible case is unexpectedly publication-ready: {case['task_id']}")
        task_id = str(case["task_id"])
        if task_id in artifact_paths:
            raise ValueError(f"duplicate eligible task_id: {task_id}")
        task = expected_tasks[task_id]
        identity_fields = (
            "run_id",
            "generation_id",
            "task_id",
            "manifest_order",
            "input_fingerprint",
        )
        drift = [field for field in identity_fields if case.get(field) != task.get(field)]
        if drift:
            raise ValueError(f"case result differs from canonical task {task_id}: {drift}")
        image_relative, image_sha = _artifact_record(case, "review_image", required=True)
        image_path, image_digest = _safe_artifact(run_root, image_relative, image_sha)
        qc_relative, qc_sha = _artifact_record(case, "scientific_qc", required=False)
        qc_digest: str | None = None
        if qc_relative is not None:
            _qc_path, qc_digest = _safe_artifact(run_root, qc_relative, qc_sha)
        adapter = str(case["adapter_type"]).lower()
        if adapter not in {"generic", "ssqtl"}:
            raise ValueError(f"unsupported review adapter_type: {adapter}")
        assertions = case.get("required_manual_assertions")
        if assertions is None:
            assertions = GENERIC_ASSERTIONS if adapter == "generic" else SSQTL_ASSERTIONS
        if (
            not isinstance(assertions, (list, tuple))
            or not assertions
            or any(not isinstance(item, str) or not item.strip() for item in assertions)
            or len(set(assertions)) != len(assertions)
        ):
            raise ValueError(f"invalid required_manual_assertions for {task_id}")
        contracts.append(
            {
                "schema_version": "3.0-review-contract",
                "run_id": str(case["run_id"]),
                "generation_id": str(case["generation_id"]),
                "task_id": task_id,
                "manifest_order": int(case["manifest_order"]),
                "input_fingerprint": str(case["input_fingerprint"]),
                "case_result_sha256": sha256_file(path),
                "review_image_sha256": image_digest,
                "scientific_qc_sha256": qc_digest,
                "adapter_type": adapter,
                "evidence_state": str(case["evidence_state"]),
                "required_manual_assertions": list(assertions),
            }
        )
        artifact_paths[task_id] = image_path
    return contracts, artifact_paths


def _contracts_from_review_package(
    run_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Path]]:
    candidates = (
        run_root / "review" / "review_package",
        run_root / "review_package",
    )
    package_root = next((path for path in candidates if path.is_dir() and not path.is_symlink()), None)
    if package_root is None:
        return [], {}
    try:
        package_root.resolve(strict=True).relative_to(run_root)
    except ValueError as exc:
        raise ValueError("review package escapes run_dir through a symlink") from exc
    assert_production_artifact_tree(package_root, label="review package")
    package = _load_json_object(package_root / "package.json", label="v3 review package")
    if package.get("schema_version") != "3.0-review-package":
        raise ValueError("v2 review packages are audit-only and cannot enter v3 review")
    checksum_manifest = package_root / "SHA256SUMS"
    if checksum_manifest.is_symlink() or not checksum_manifest.is_file():
        raise ValueError("v3 review package requires an immutable SHA256SUMS tree")
    verify_checksum_tree(package_root)
    contract_path = package_root / "review_contract.jsonl"
    if contract_path.is_symlink() or not contract_path.is_file():
        raise ValueError(f"review package lacks review_contract.jsonl: {package_root}")
    tasks = _authoritative_tasks(run_root)
    accounting = locate_verified_accounting(run_root, tasks)
    run_identity = _load_json_object(
        run_root / "contract" / "run_identity.json", label="v3 run identity"
    )
    runtime_binding = _runtime_binding(run_root)
    source_contracts = list(read_jsonl(contract_path))
    contract_set_sha = sha256_json(source_contracts)
    controller_path = run_root / "contract" / "controller_runtime.json"
    controller_runtime = _load_json_object(
        controller_path, label="controller runtime identity"
    )
    package_identity = {
        "run_id": run_identity["run_id"],
        "generation_id": run_identity["generation_id"],
        "canonical_tasks_sha256": run_identity["canonical_tasks_sha256"],
        "contract_set_sha256": contract_set_sha,
        "accounting_provider": accounting["provider"],
        "accounting_receipt_sha256": accounting["receipt_sha256"],
        "runtime_binding_sha256": sha256_json(dict(runtime_binding)),
        "controller_runtime_identity_sha256": controller_runtime[
            "identity_sha256"
        ],
        "controller_runtime_contract_sha256": sha256_file(controller_path),
    }
    expected_package_id = "review_package_" + sha256_json(package_identity)
    if (
        package.get("package_id") != expected_package_id
        or any(package.get(key) != value for key, value in package_identity.items())
        or package.get("status") != "SNAPSHOTS_READY"
        or package.get("review_gate") != "OPTIONAL_HUMAN_REVIEW"
        or package.get("runtime_manifest_sha256")
        != runtime_binding["runtime_manifest_sha256"]
        or package.get("runtime_fingerprint_sha256")
        != runtime_binding["runtime_fingerprint_sha256"]
        or package.get("runtime_oci_digest")
        != runtime_binding.get("runtime_oci_digest")
        or package.get("canonical_task_count") != len(tasks)
        or package.get("eligible_review_count") != len(tasks)
        or package.get("excluded_count") != 0
    ):
        raise ValueError("review package identity differs from authoritative evidence")
    task_map = {str(task["task_id"]): task for task in tasks}
    contracts: list[dict[str, Any]] = []
    artifacts: dict[str, Path] = {}
    for source in source_contracts:
        if source.get("schema_version") != "3.0-review-contract":
            raise ValueError("v2 review contracts are audit-only and cannot enter v3 review")
        task_id = str(source.get("task_id", ""))
        if not task_id or task_id in artifacts:
            raise ValueError(f"review package has a missing/duplicate task_id: {task_id}")
        relative = source.get("review_image_relative_path", source.get("combined_relative_path"))
        digest = source.get("review_image_sha256", source.get("combined_sha256"))
        artifact, image_digest = _safe_artifact(package_root, relative, digest)
        adapter = str(source.get("adapter_type", "ssqtl")).lower()
        if adapter not in {"generic", "ssqtl"}:
            raise ValueError(f"unsupported review adapter_type: {adapter}")
        assertions = source.get(
            "required_manual_assertions",
            GENERIC_ASSERTIONS if adapter == "generic" else SSQTL_ASSERTIONS,
        )
        if (
            not isinstance(assertions, (list, tuple))
            or not assertions
            or any(not isinstance(item, str) or not item.strip() for item in assertions)
            or len(set(assertions)) != len(assertions)
        ):
            raise ValueError(f"invalid required_manual_assertions for {task_id}")
        contract = {
            "schema_version": "3.0-review-contract",
            "run_id": str(source.get("run_id", "")),
            "generation_id": str(source.get("generation_id", "")),
            "task_id": task_id,
            "manifest_order": int(source.get("manifest_order", 0)),
            "input_fingerprint": str(source.get("input_fingerprint", "")),
            "case_result_sha256": str(source.get("case_result_sha256", "")),
            "artifact_set_sha256": str(source.get("artifact_set_sha256", "")),
            "review_image_sha256": image_digest,
            "scientific_qc_sha256": source.get("scientific_qc_sha256"),
            "adapter_type": adapter,
            "evidence_state": str(source.get("evidence_state", "COMPLETE")),
            "required_manual_assertions": list(assertions),
        }
        missing = [field for field in REVIEW_BINDING_FIELDS_V3 if contract.get(field) in (None, "")]
        # A missing scientific QC hash is valid for the generic adapter.
        if missing == ["scientific_qc_sha256"] and adapter == "generic":
            missing = []
        if missing:
            raise ValueError(f"review contract lacks binding fields {missing}: {task_id}")
        task = task_map.get(task_id)
        if task is None or any(
            contract.get(field) != task.get(field)
            for field in (
                "run_id",
                "generation_id",
                "task_id",
                "manifest_order",
                "input_fingerprint",
            )
        ):
            raise ValueError(f"review package contract differs from canonical task: {task_id}")
        contracts.append(contract)
        artifacts[task_id] = artifact
    if set(artifacts) != set(task_map):
        raise ValueError("review package does not exactly cover the canonical task set")
    return contracts, artifacts


def _resolve_context(run_dir: str | Path) -> ReviewContext:
    run_root = Path(run_dir).expanduser()
    if run_root.is_symlink() or not run_root.resolve(strict=True).is_dir():
        raise ValueError(f"run_dir must be a regular non-symlink directory: {run_root}")
    run_root = run_root.resolve(strict=True)
    contracts, artifacts = _contracts_from_review_package(run_root)
    if not contracts:
        legacy_package = run_root / "review" / "review_package" / "package.json"
        if legacy_package.is_file() and not legacy_package.is_symlink():
            package = _load_json_object(legacy_package, label="review package")
            if str(package.get("schema_version", "")).startswith("2"):
                raise ValueError("v2 review packages are audit-only and cannot enter v3 review")
        raise ValueError(
            "v3 review requires the immutable checksum-bound review package"
        )
    identities = [(row["task_id"], int(row["manifest_order"])) for row in contracts]
    if len(identities) != len(set(identities)):
        raise ValueError("review contracts contain duplicate task/order identities")
    run_ids = {row["run_id"] for row in contracts}
    generation_ids = {row["generation_id"] for row in contracts}
    if len(run_ids) > 1 or len(generation_ids) > 1:
        raise ValueError("review contracts mix run or generation identities")
    ordered = tuple(sorted(contracts, key=lambda row: int(row["manifest_order"])))
    review_root = run_root / "review"
    review_root.mkdir(mode=0o700, exist_ok=True)
    if review_root.is_symlink() or not review_root.is_dir():
        raise ValueError(f"review root must be a regular directory: {review_root}")
    return ReviewContext(
        run_root=run_root,
        review_root=review_root,
        contracts=ordered,
        artifact_paths=artifacts,
        contract_set_sha256=sha256_json(list(ordered)),
        runtime_binding=_runtime_binding(run_root),
    )


def _binding(contract: Mapping[str, Any]) -> dict[str, Any]:
    return {field: contract.get(field) for field in REVIEW_BINDING_FIELDS_V3}


def _campaign_binding(context: ReviewContext) -> dict[str, Any] | None:
    """Return the immutable campaign admission bound to this run, if present."""

    path = context.campaign_binding_path
    if not path.exists():
        return None
    binding = _load_json_object(path, label="campaign batch binding")
    required = {
        "schema_version",
        "campaign_root",
        "campaign_id",
        "campaign_contract_sha256",
        "batch_id",
        "purpose",
        "batch_request_sha256",
        "master_tasks_sha256",
        "master_task_set_sha256",
        "task_count",
        "tasks_sha256",
        "task_set_sha256",
    }
    missing = sorted(required - set(binding))
    if missing or binding.get("schema_version") != "3.0-batch-admission":
        raise ValueError(f"campaign batch binding is incomplete: {missing}")
    if binding.get("purpose") not in {
        "PILOT_QA",
        "PRODUCTION_CONTINUATION",
    }:
        raise ValueError("campaign batch binding has an invalid purpose")
    for field in (
        "campaign_contract_sha256",
        "batch_request_sha256",
        "master_tasks_sha256",
        "master_task_set_sha256",
        "tasks_sha256",
        "task_set_sha256",
    ):
        if not re.fullmatch(r"[a-f0-9]{64}", str(binding.get(field, ""))):
            raise ValueError(f"campaign batch binding has malformed {field}")

    campaign_root_value = Path(str(binding["campaign_root"])).expanduser()
    if campaign_root_value.is_symlink() or not campaign_root_value.resolve(strict=True).is_dir():
        raise ValueError("campaign root must be a regular non-symlink directory")
    campaign_root = campaign_root_value.resolve(strict=True)
    local_request = context.run_root / "contract" / "batch-request.json"
    if local_request.is_symlink() or not local_request.is_file():
        raise ValueError("campaign run lacks its immutable local batch-request")
    request = _load_json_object(local_request, label="campaign batch-request")
    if sha256_file(local_request) != binding["batch_request_sha256"]:
        raise ValueError("campaign batch-request checksum differs from its admission")
    if (
        request.get("schema_version") != "3.0-batch-request"
        or request.get("campaign_id") != binding["campaign_id"]
        or request.get("campaign_contract_sha256")
        != binding["campaign_contract_sha256"]
        or request.get("batch_id") != binding["batch_id"]
        or request.get("purpose") != binding["purpose"]
        or request.get("master_tasks_sha256") != binding["master_tasks_sha256"]
        or request.get("master_task_set_sha256")
        != binding["master_task_set_sha256"]
        or request.get("tasks_sha256") != binding["tasks_sha256"]
        or request.get("task_set_sha256") != binding["task_set_sha256"]
        or int(request.get("task_count", -1)) != int(binding["task_count"])
    ):
        raise ValueError("campaign batch-request differs from its admitted binding")
    sources = request.get("source_tasks")
    if not isinstance(sources, list):
        raise ValueError("campaign batch-request lacks its source task mapping")
    source_ids = [str(row.get("task_id", "")) for row in sources if isinstance(row, Mapping)]
    contract_ids = [str(row["task_id"]) for row in context.contracts]
    if len(source_ids) != len(sources) or set(source_ids) != set(contract_ids):
        raise ValueError("campaign batch-request does not exactly cover review contracts")
    if any(
        contract.get("run_id") != binding["campaign_id"]
        or contract.get("generation_id") != binding["batch_id"]
        for contract in context.contracts
    ):
        raise ValueError("campaign review contracts differ from campaign/batch identity")

    identity = _load_json_object(
        context.run_root / "contract" / "run_identity.json", label="v3 run identity"
    )
    expected_identity = {
        "campaign_binding_sha256": sha256_file(path),
        "campaign_id": binding["campaign_id"],
        "campaign_contract_sha256": binding["campaign_contract_sha256"],
        "batch_id": binding["batch_id"],
        "batch_purpose": binding["purpose"],
        "batch_request_sha256": binding["batch_request_sha256"],
        "master_tasks_sha256": binding["master_tasks_sha256"],
        "master_task_set_sha256": binding["master_task_set_sha256"],
        "batch_task_set_sha256": binding["task_set_sha256"],
    }
    if any(identity.get(field) != value for field, value in expected_identity.items()):
        raise ValueError("run identity does not bind the admitted scientific campaign")

    package_root = next(
        (
            candidate
            for candidate in (
                context.run_root / "review" / "review_package",
                context.run_root / "review_package",
            )
            if candidate.is_dir() and not candidate.is_symlink()
        ),
        None,
    )
    if package_root is None:
        raise ValueError("campaign review lacks its immutable review package")
    package = _load_json_object(package_root / "package.json", label="v3 review package")
    accounting_sha = str(package.get("accounting_receipt_sha256", ""))
    if not re.fullmatch(r"[a-f0-9]{64}", accounting_sha):
        raise ValueError("campaign review package lacks its accounting receipt checksum")
    runtime = dict(context.runtime_binding)
    result = {
        **binding,
        "campaign_root": str(campaign_root),
        "campaign_binding_sha256": sha256_file(path),
        "batch_purpose": binding["purpose"],
        "batch_task_set_sha256": binding["task_set_sha256"],
        "accounting_receipt_sha256": accounting_sha,
        "runtime_manifest_sha256": runtime["runtime_manifest_sha256"],
        "runtime_fingerprint_sha256": runtime["runtime_fingerprint_sha256"],
    }
    return result


def _campaign_decision_view(
    event: Mapping[str, Any], context: ReviewContext, binding: Mapping[str, Any]
) -> dict[str, Any]:
    if (
        event.get("schema_version") != "3.0-campaign-ledger-event"
        or event.get("campaign_id") != binding["campaign_id"]
        or event.get("campaign_contract_sha256")
        != binding["campaign_contract_sha256"]
        or event.get("batch_id") != binding["batch_id"]
        or event.get("event_type") != "HUMAN_DECISION"
    ):
        raise ValueError("campaign human-decision event identity is invalid")
    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("campaign human-decision payload is invalid")
    task_id = str(payload.get("task_id", ""))
    contract = context.contract_map.get(task_id)
    if contract is None or payload.get("contract_binding") != _binding(contract):
        raise ValueError("campaign human decision differs from its review contract")
    state = str(payload.get("artifact_review_state", ""))
    interpretation = str(payload.get("scientific_interpretation", ""))
    allowed = (
        {"NOT_APPLICABLE"}
        if contract["adapter_type"] == "generic"
        else {"SUPPORTED", "NOT_SUPPORTED", "INDETERMINATE"}
    )
    assertions = payload.get("manual_assertions")
    if (
        state not in {"APPROVE", "REJECT"}
        or interpretation not in allowed
        or not str(event.get("actor", "")).strip()
        or not isinstance(assertions, Mapping)
        or set(assertions) != set(contract["required_manual_assertions"])
        or any(not isinstance(value, bool) for value in assertions.values())
        or (state == "APPROVE" and not all(assertions.values()))
    ):
        raise ValueError("campaign human-decision payload is invalid")
    if (
        contract["adapter_type"] == "ssqtl"
        and contract["evidence_state"] in {"EVIDENCE_INCOMPLETE", "UNAVAILABLE"}
        and interpretation != "INDETERMINATE"
    ):
        raise ValueError("campaign decision conflicts with incomplete ssQTL evidence")
    event_sha = str(event.get("event_sha256", ""))
    if not re.fullmatch(r"[a-f0-9]{64}", event_sha):
        raise ValueError("campaign human-decision event checksum is malformed")
    return {
        "event_id": f"campaign_evt_{event_sha}",
        "event_sha256": event_sha,
        "sequence": int(event.get("sequence", 0)),
        "recorded_at": event.get("recorded_at"),
        "task_id": task_id,
        "artifact_review_state": state,
        "scientific_interpretation": interpretation,
        "reviewer": str(event["actor"]),
        "notes": str(payload.get("notes", "")),
        "manual_assertions": dict(assertions),
        "contract_binding": dict(payload["contract_binding"]),
    }


def _campaign_ledger_decisions(
    context: ReviewContext, binding: Mapping[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = verify_campaign_ledger(binding["campaign_root"])
    decisions = [
        _campaign_decision_view(event, context, binding)
        for event in rows
        if event.get("batch_id") == binding["batch_id"]
        and event.get("event_type") == "HUMAN_DECISION"
    ]
    return rows, decisions


def _journal_lock(path: Path) -> threading.RLock:
    with _JOURNAL_LOCKS_GUARD:
        return _JOURNAL_LOCKS.setdefault(path, threading.RLock())


@contextmanager
def _review_operation_lock(run_dir: str | Path):
    run_value = Path(run_dir).expanduser()
    if run_value.is_symlink() or not run_value.resolve(strict=True).is_dir():
        raise ValueError(f"run_dir must be a regular non-symlink directory: {run_value}")
    run_root = run_value.resolve(strict=True)
    review_root = run_root / "review"
    review_root.mkdir(mode=0o700, exist_ok=True)
    if review_root.is_symlink() or not review_root.is_dir():
        raise ValueError(f"review root must be a regular directory: {review_root}")
    lock_path = review_root / ".review-operation.lock"
    process_lock = _journal_lock(lock_path)
    with process_lock:
        descriptor = os.open(
            lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield run_root
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)


def _load_journal(context: ReviewContext) -> list[dict[str, Any]]:
    path = context.journal_path
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"review journal must be a regular non-symlink file: {path}")
    contracts = context.contract_map
    rows = list(read_jsonl(path))
    previous: str | None = None
    for index, row in enumerate(rows, 1):
        if row.get("schema_version") != "3.0-review-journal" or row.get("event_type") != "DECISION":
            raise ValueError(f"invalid review journal event at sequence {index}")
        if row.get("sequence") != index or row.get("previous_event_sha256") != previous:
            raise ValueError(f"review journal chain/order failure at sequence {index}")
        payload = {
            key: value
            for key, value in row.items()
            if key not in {"event_id", "event_sha256"}
        }
        event_sha = sha256_json(payload)
        if row.get("event_sha256") != event_sha or row.get("event_id") != f"review_evt_{event_sha}":
            raise ValueError(f"review journal event checksum failure at sequence {index}")
        task_id = str(row.get("task_id", ""))
        if task_id not in contracts or row.get("contract_binding") != _binding(contracts[task_id]):
            raise ValueError(f"review journal contract binding failure at sequence {index}")
        contract = contracts[task_id]
        state = row.get("artifact_review_state")
        interpretation = row.get("scientific_interpretation")
        allowed_interpretations = (
            {"NOT_APPLICABLE"}
            if contract["adapter_type"] == "generic"
            else {"SUPPORTED", "NOT_SUPPORTED", "INDETERMINATE"}
        )
        assertions = row.get("manual_assertions")
        if (
            state not in {"APPROVE", "REJECT"}
            or interpretation not in allowed_interpretations
            or not str(row.get("reviewer", "")).strip()
            or not isinstance(assertions, dict)
            or set(assertions) != set(contract["required_manual_assertions"])
            or any(not isinstance(value, bool) for value in assertions.values())
        ):
            raise ValueError(f"invalid review decision payload at sequence {index}")
        if state == "APPROVE" and not all(assertions.values()):
            raise ValueError(f"incomplete approval assertions at sequence {index}")
        if (
            contract["adapter_type"] == "ssqtl"
            and contract["evidence_state"] in {"EVIDENCE_INCOMPLETE", "UNAVAILABLE"}
            and interpretation != "INDETERMINATE"
        ):
            raise ValueError(f"invalid incomplete-evidence interpretation at sequence {index}")
        previous = event_sha
    return rows


def append_review_decision(
    run_dir: str | Path,
    *,
    task_id: str,
    artifact_review_state: str,
    scientific_interpretation: str,
    reviewer: str,
    notes: str = "",
    manual_assertions: Mapping[str, bool] | None = None,
) -> dict[str, Any]:
    """Append one checksum-chained review event; existing events are never edited."""

    with _review_operation_lock(run_dir) as locked_root:
        context = _resolve_context(locked_root)
        if context.finalized_path.exists() or context.finalized_path.is_symlink():
            raise RuntimeError("review generation is already finalized")
        campaign = _campaign_binding(context)
        if campaign is not None:
            campaign_rows = verify_campaign_ledger(campaign["campaign_root"])
            if any(
                event.get("batch_id") == campaign["batch_id"]
                and event.get("event_type") == "REVIEW_FINALIZED"
                for event in campaign_rows
            ):
                raise RuntimeError("campaign review generation is already finalized")
        contract = context.contract_map.get(task_id)
        if contract is None:
            raise ValueError(f"task is not eligible for review: {task_id}")
        state = artifact_review_state.strip().upper()
        if state not in {"APPROVE", "REJECT"}:
            raise ValueError("artifact_review_state must be APPROVE or REJECT")
        interpretation = scientific_interpretation.strip().upper()
        adapter = contract["adapter_type"]
        allowed = (
            {"NOT_APPLICABLE"}
            if adapter == "generic"
            else {"SUPPORTED", "NOT_SUPPORTED", "INDETERMINATE"}
        )
        if interpretation not in allowed:
            raise ValueError(
                f"scientific_interpretation for {adapter} must be one of {sorted(allowed)}"
            )
        if (
            adapter == "ssqtl"
            and contract["evidence_state"] in {"EVIDENCE_INCOMPLETE", "UNAVAILABLE"}
            and interpretation != "INDETERMINATE"
        ):
            raise ValueError("incomplete/unavailable ssQTL evidence requires INDETERMINATE")
        reviewer_name = reviewer.strip()
        if not reviewer_name:
            raise ValueError("reviewer is required")
        assertions = {
            name: bool((manual_assertions or {}).get(name))
            for name in contract["required_manual_assertions"]
        }
        if state == "APPROVE" and not all(assertions.values()):
            missing = sorted(name for name, value in assertions.items() if not value)
            raise ValueError("approval requires every manual assertion: " + ", ".join(missing))
        if campaign is not None:
            event = append_campaign_event(
                campaign["campaign_root"],
                event_type="HUMAN_DECISION",
                batch_id=str(campaign["batch_id"]),
                actor=reviewer_name,
                payload={
                    "task_id": task_id,
                    "artifact_review_state": state,
                    "scientific_interpretation": interpretation,
                    "notes": str(notes),
                    "manual_assertions": assertions,
                    "contract_binding": _binding(contract),
                },
            )
            return _campaign_decision_view(event, context, campaign)
        rows = _load_journal(context)
        previous = rows[-1]["event_sha256"] if rows else None
        payload = {
            "schema_version": "3.0-review-journal",
            "event_type": "DECISION",
            "sequence": len(rows) + 1,
            "previous_event_sha256": previous,
            "recorded_at": utc_now(),
            "task_id": task_id,
            "artifact_review_state": state,
            "scientific_interpretation": interpretation,
            "reviewer": reviewer_name,
            "notes": str(notes),
            "manual_assertions": assertions,
            "contract_binding": _binding(contract),
        }
        event_sha = sha256_json(payload)
        event = {
            **payload,
            "event_id": f"review_evt_{event_sha}",
            "event_sha256": event_sha,
        }
        line = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
        descriptor = os.open(
            context.journal_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(descriptor, line.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return event


def _latest_decisions(context: ReviewContext) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    campaign = _campaign_binding(context)
    if campaign is not None:
        rows, decisions = _campaign_ledger_decisions(context, campaign)
        latest: dict[str, dict[str, Any]] = {}
        for decision in decisions:
            latest[str(decision["task_id"])] = decision
        return rows, latest
    rows = _load_journal(context)
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        latest[str(row["task_id"])] = row
    return rows, latest


def _write_checksums(root: Path) -> None:
    files = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS"),
        key=lambda path: str(path.relative_to(root)),
    )
    atomic_write_text(
        root / "SHA256SUMS",
        "".join(f"{sha256_file(path)}  {path.relative_to(root)}\n" for path in files),
    )


def verify_finalized_review_generation(
    generation_root: str | Path,
    supplied_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Verify every receipt-bound byte in one immutable review generation."""

    root_value = Path(generation_root).expanduser()
    if root_value.is_symlink() or not root_value.resolve(strict=True).is_dir():
        raise ValueError(f"review generation must be a regular non-symlink directory: {root_value}")
    root = root_value.resolve(strict=True)
    verify_checksum_tree(root)
    receipt_path = root / "review_receipt.json"
    receipt = _load_json_object(receipt_path, label="finalized review receipt")
    if supplied_receipt is not None and dict(supplied_receipt) != receipt:
        raise ValueError("supplied review receipt differs from the frozen review generation")

    ledger_kind = str(receipt.get("ledger_kind") or LEGACY_REVIEW_LEDGER_KIND)
    required_files = {
        "review_records_sha256": root / "review_records.jsonl",
        "rerun_manifest_sha256": root / "rerun_manifest.jsonl",
        "rerun_receipt_sha256": root / "rerun_receipt.json",
    }
    if ledger_kind == CAMPAIGN_REVIEW_LEDGER_KIND:
        required_files["campaign_ledger_sha256"] = root / "campaign_ledger.jsonl"
        ledger_path = root / "campaign_ledger.jsonl"
        if (
            receipt.get("campaign_ledger") != "campaign_ledger.jsonl"
            or receipt.get("review_journal") is not None
            or receipt.get("review_journal_sha256") is not None
        ):
            raise ValueError("campaign review receipt contains an unexpected ledger path")
    elif ledger_kind == LEGACY_REVIEW_LEDGER_KIND:
        required_files["review_journal_sha256"] = root / "review_journal.jsonl"
        ledger_path = root / "review_journal.jsonl"
        if receipt.get("review_journal") != "review_journal.jsonl":
            raise ValueError("legacy review receipt contains an unexpected journal path")
    else:
        raise ValueError(f"unsupported finalized review ledger kind: {ledger_kind}")
    for field, path in required_files.items():
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"finalized review generation lacks {path.name}")
        if receipt.get(field) != sha256_file(path):
            raise ValueError(f"finalized review {path.name} checksum differs from receipt")
    if (
        receipt.get("rerun_manifest") != "rerun_manifest.jsonl"
        or receipt.get("rerun_receipt") != "rerun_receipt.json"
    ):
        raise ValueError("finalized review receipt contains unexpected rerun paths")

    records = list(read_jsonl(root / "review_records.jsonl"))
    rerun_rows = list(read_jsonl(root / "rerun_manifest.jsonl"))
    journal_rows = list(read_jsonl(ledger_path))
    rerun_receipt = _load_json_object(root / "rerun_receipt.json", label="rerun receipt")
    previous_event_sha: str | None = None
    latest_events: dict[str, dict[str, Any]] = {}
    decision_event_count = 0
    if ledger_kind == CAMPAIGN_REVIEW_LEDGER_KIND:
        campaign_id = str(receipt.get("campaign_id", ""))
        campaign_contract_sha = str(receipt.get("campaign_contract_sha256", ""))
        batch_id = str(receipt.get("batch_id", ""))
        if not campaign_id or not batch_id or not re.fullmatch(
            r"[a-f0-9]{64}", campaign_contract_sha
        ):
            raise ValueError("campaign review receipt lacks its campaign identity")
        if receipt.get("batch_purpose") not in {
            "PILOT_QA",
            "PRODUCTION_CONTINUATION",
        }:
            raise ValueError("campaign review receipt has an invalid batch purpose")
        required_campaign_hashes = (
            "master_tasks_sha256",
            "master_task_set_sha256",
            "batch_request_sha256",
            "batch_task_set_sha256",
            "accounting_receipt_sha256",
            "runtime_manifest_sha256",
            "runtime_fingerprint_sha256",
        )
        if any(
            not re.fullmatch(r"[a-f0-9]{64}", str(receipt.get(field, "")))
            for field in required_campaign_hashes
        ):
            raise ValueError("campaign review receipt has a malformed immutable binding")
        for sequence, event in enumerate(journal_rows, start=1):
            required_event_fields = {
                "schema_version",
                "campaign_id",
                "campaign_contract_sha256",
                "sequence",
                "recorded_at",
                "actor",
                "batch_id",
                "event_type",
                "payload",
                "previous_event_sha256",
                "event_sha256",
            }
            expected_event_sha = sha256_json(
                {key: value for key, value in event.items() if key != "event_sha256"}
            )
            if (
                set(event) != required_event_fields
                or event.get("schema_version") != "3.0-campaign-ledger-event"
                or event.get("campaign_id") != campaign_id
                or event.get("campaign_contract_sha256") != campaign_contract_sha
                or event.get("sequence") != sequence
                or event.get("previous_event_sha256") != previous_event_sha
                or event.get("event_sha256") != expected_event_sha
                or event.get("event_type") not in LEDGER_EVENT_TYPES
                or not str(event.get("actor", "")).strip()
                or not isinstance(event.get("payload"), Mapping)
            ):
                raise ValueError(
                    f"frozen campaign ledger chain failure at sequence {sequence}"
                )
            previous_event_sha = expected_event_sha
            if event.get("batch_id") != batch_id:
                continue
            if event.get("event_type") == "REVIEW_FINALIZED":
                raise ValueError("campaign decision-prefix snapshot includes finalization")
            if event.get("event_type") != "HUMAN_DECISION":
                continue
            payload = event.get("payload")
            if not isinstance(payload, Mapping):
                raise ValueError("frozen campaign decision payload is invalid")
            task_id = str(payload.get("task_id", ""))
            view = {
                "event_id": f"campaign_evt_{expected_event_sha}",
                "event_sha256": expected_event_sha,
                "recorded_at": event.get("recorded_at"),
                "task_id": task_id,
                "artifact_review_state": payload.get("artifact_review_state"),
                "scientific_interpretation": payload.get("scientific_interpretation"),
                "reviewer": event.get("actor"),
                "notes": str(payload.get("notes", "")),
                "manual_assertions": payload.get("manual_assertions"),
                "contract_binding": payload.get("contract_binding"),
            }
            latest_events[task_id] = view
            decision_event_count += 1
        if (
            receipt.get("campaign_ledger_sha256") != sha256_file(ledger_path)
            or receipt.get("journal_sha256") != sha256_file(ledger_path)
            or int(receipt.get("campaign_ledger_event_count", -1)) != len(journal_rows)
            or int(receipt.get("journal_event_count", -1)) != len(journal_rows)
            or int(receipt.get("decision_event_count", -1)) != decision_event_count
            or receipt.get("campaign_ledger_head_sha256") != previous_event_sha
        ):
            raise ValueError("campaign decision-prefix snapshot differs from receipt")
        runtime_binding = receipt.get("runtime_binding")
        if (
            not isinstance(runtime_binding, Mapping)
            or receipt.get("runtime_manifest_sha256")
            != runtime_binding.get("runtime_manifest_sha256")
            or receipt.get("runtime_fingerprint_sha256")
            != runtime_binding.get("runtime_fingerprint_sha256")
        ):
            raise ValueError("campaign receipt runtime manifest hashes differ")
    else:
        for sequence, event in enumerate(journal_rows, start=1):
            payload = {
                key: value
                for key, value in event.items()
                if key not in {"event_id", "event_sha256"}
            }
            expected_event_sha = sha256_json(payload)
            if (
                event.get("schema_version") != "3.0-review-journal"
                or event.get("event_type") != "DECISION"
                or event.get("sequence") != sequence
                or event.get("previous_event_sha256") != previous_event_sha
                or event.get("event_sha256") != expected_event_sha
                or event.get("event_id") != f"review_evt_{expected_event_sha}"
            ):
                raise ValueError(f"frozen review journal chain failure at sequence {sequence}")
            previous_event_sha = expected_event_sha
            latest_events[str(event.get("task_id", ""))] = event
    for record in records:
        assert_not_debug_metadata(record, label="finalized review record")
    decisions = [str(record.get("artifact_review_state", "")) for record in records]
    task_ids = [str(record.get("task_id", "")) for record in records]
    rejected_ids = {
        str(record.get("task_id", ""))
        for record in records
        if record.get("artifact_review_state") == "REJECT"
    }
    rerun_ids = {str(row.get("source_task_id", "")) for row in rerun_rows}
    record_event_mismatch = False
    for record in records:
        event = latest_events.get(str(record.get("task_id", "")))
        if event is None or any(
            (
                record.get("review_record_id") != event.get("event_id"),
                record.get("journal_event_sha256") != event.get("event_sha256"),
                record.get("artifact_review_state")
                != event.get("artifact_review_state"),
                record.get("scientific_interpretation")
                != event.get("scientific_interpretation"),
                record.get("reviewer") != event.get("reviewer"),
                record.get("reviewed_at") != event.get("recorded_at"),
                record.get("notes") != event.get("notes"),
                record.get("manual_assertions") != event.get("manual_assertions"),
                {field: record.get(field) for field in REVIEW_BINDING_FIELDS_V3}
                != event.get("contract_binding"),
            )
        ):
            record_event_mismatch = True
            break
    if (
        len(records) != int(receipt.get("decision_count", -1))
        or len(records) != int(receipt.get("eligible_count", -1))
        or not all(task_ids)
        or len(task_ids) != len(set(task_ids))
        or any(decision not in {"APPROVE", "REJECT"} for decision in decisions)
        or decisions.count("APPROVE") != int(receipt.get("approved_count", -1))
        or decisions.count("REJECT") != int(receipt.get("rejected_count", -1))
        or rejected_ids != rerun_ids
        or record_event_mismatch
        or len(journal_rows) != int(receipt.get("journal_event_count", -1))
        or receipt.get("journal_sha256") != sha256_file(ledger_path)
    ):
        raise ValueError("finalized review record/rerun set differs from receipt")
    run_identity = _load_json_object(
        root.parents[2] / "contract" / "run_identity.json",
        label="v3 run identity",
    )
    rerun_request_set_sha256 = sha256_json(rerun_rows)
    expected_rerun = {
        "rerun_id": "review_rejects_" + rerun_request_set_sha256,
        "source_run_id": receipt.get("run_id"),
        "source_generation_id": receipt.get("generation_id"),
        "canonical_tasks_sha256": run_identity.get("canonical_tasks_sha256"),
        "rerun_case_count": len(rerun_rows),
        "rerun_request_set_sha256": rerun_request_set_sha256,
        "rerun_manifest_sha256": receipt.get("rerun_manifest_sha256"),
    }
    if (
        any(rerun_receipt.get(key) != value for key, value in expected_rerun.items())
        or receipt.get("rerun_case_count") != len(rerun_rows)
        or receipt.get("rerun_required") is not bool(rerun_rows)
        or rerun_receipt.get("state")
        != ("RERUN_REQUIRED" if rerun_rows else "NOT_REQUIRED")
        or rerun_receipt.get("same_generation_resume_allowed") is not False
    ):
        raise ValueError("finalized rerun receipt differs from review decisions")
    return {
        "receipt": receipt,
        "receipt_sha256": sha256_file(receipt_path),
        "checksums_sha256": sha256_file(root / "SHA256SUMS"),
        "records": records,
        "rerun_rows": rerun_rows,
        "rerun_receipt": rerun_receipt,
        "ledger_kind": ledger_kind,
        "ledger_rows": journal_rows,
    }


def _campaign_finalization_payload(
    receipt: Mapping[str, Any], receipt_sha256: str
) -> dict[str, Any]:
    return {
        "review_generation_id": receipt["review_generation_id"],
        "decision_count": int(receipt["decision_count"]),
        "approved_count": int(receipt["approved_count"]),
        "rejected_count": int(receipt["rejected_count"]),
        "all_eligible_decided": True,
        "review_receipt_sha256": receipt_sha256,
        "decision_prefix_sha256": receipt["campaign_ledger_sha256"],
        "decision_prefix_head_sha256": receipt[
            "campaign_ledger_head_sha256"
        ],
        "decision_prefix_event_count": int(
            receipt["campaign_ledger_event_count"]
        ),
    }


def _matching_campaign_finalization_event(
    context: ReviewContext,
    binding: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_sha256: str,
) -> dict[str, Any]:
    rows = verify_campaign_ledger(binding["campaign_root"])
    events = [
        event
        for event in rows
        if event.get("batch_id") == binding["batch_id"]
        and event.get("event_type") == "REVIEW_FINALIZED"
    ]
    if len(events) != 1:
        raise ValueError(
            "campaign review must have exactly one REVIEW_FINALIZED event"
        )
    event = events[0]
    expected_payload = _campaign_finalization_payload(receipt, receipt_sha256)
    if (
        event.get("payload") != expected_payload
        or event.get("previous_event_sha256")
        != receipt.get("campaign_ledger_head_sha256")
        or any(
            later.get("batch_id") == binding["batch_id"]
            and later.get("event_type") == "HUMAN_DECISION"
            and int(later.get("sequence", 0)) > int(event.get("sequence", 0))
            for later in rows
        )
    ):
        raise ValueError("campaign finalization event conflicts with frozen review")
    if any(
        receipt.get(field) != binding.get(field)
        for field in (
            "campaign_id",
            "campaign_contract_sha256",
            "master_tasks_sha256",
            "master_task_set_sha256",
            "batch_id",
            "batch_purpose",
            "batch_request_sha256",
            "batch_task_set_sha256",
            "accounting_receipt_sha256",
            "runtime_manifest_sha256",
            "runtime_fingerprint_sha256",
        )
    ):
        raise ValueError("campaign finalization receipt identity drift")
    return event


def _finalized_pointer(
    context: ReviewContext, generation_root: Path, receipt: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": "3.0-finalized-review-pointer",
        "review_generation_id": receipt["review_generation_id"],
        "receipt_relative_path": str(
            (generation_root / "review_receipt.json").relative_to(context.review_root)
        ),
        "receipt_sha256": sha256_file(generation_root / "review_receipt.json"),
        "checksums_sha256": sha256_file(generation_root / "SHA256SUMS"),
    }


def _write_finalized_pointer(context: ReviewContext, pointer: Mapping[str, Any]) -> None:
    descriptor = os.open(
        context.finalized_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        payload = json.dumps(dict(pointer), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        os.write(descriptor, payload.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _recover_campaign_finalization(
    context: ReviewContext, binding: Mapping[str, Any]
) -> dict[str, Any] | None:
    rows = verify_campaign_ledger(binding["campaign_root"])
    events = [
        event
        for event in rows
        if event.get("batch_id") == binding["batch_id"]
        and event.get("event_type") == "REVIEW_FINALIZED"
    ]
    if not events:
        return None
    if len(events) != 1:
        raise ValueError("campaign has conflicting REVIEW_FINALIZED events")
    payload = events[0].get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("campaign finalization payload is invalid")
    generation_id = str(payload.get("review_generation_id", ""))
    if not re.fullmatch(r"review_[a-f0-9]{64}", generation_id):
        raise ValueError("campaign finalization generation ID is invalid")
    generation_root = context.review_root / "generations" / generation_id
    verified = verify_finalized_review_generation(generation_root)
    receipt = verified["receipt"]
    _matching_campaign_finalization_event(
        context, binding, receipt, verified["receipt_sha256"]
    )
    if context.finalized_path.exists() or context.finalized_path.is_symlink():
        raise ValueError("campaign finalized pointer appeared during recovery")
    _write_finalized_pointer(
        context, _finalized_pointer(context, generation_root, receipt)
    )
    return receipt


def _existing_finalized_receipt(context: ReviewContext) -> dict[str, Any] | None:
    if not context.finalized_path.exists():
        return None
    pointer = _load_json_object(context.finalized_path, label="finalized review pointer")
    relative = Path(str(pointer.get("receipt_relative_path", "")))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError("finalized review pointer contains an unsafe receipt path")
    receipt_path = context.review_root / relative
    receipt = _load_json_object(receipt_path, label="finalized review receipt")
    if sha256_file(receipt_path) != pointer.get("receipt_sha256"):
        raise ValueError("finalized review receipt checksum drift")
    verified = verify_finalized_review_generation(receipt_path.parent, receipt)
    if pointer.get("checksums_sha256") != verified["checksums_sha256"]:
        raise ValueError("finalized review checksum-tree pointer drift")
    common_mismatch = (
        receipt.get("review_generation_id") != pointer.get("review_generation_id")
        or receipt.get("contract_set_sha256") != context.contract_set_sha256
        or receipt.get("runtime_binding_sha256") != sha256_json(dict(context.runtime_binding))
        or receipt.get("runtime_binding") != dict(context.runtime_binding)
    )
    campaign = _campaign_binding(context)
    if campaign is not None:
        if receipt.get("ledger_kind") != CAMPAIGN_REVIEW_LEDGER_KIND:
            raise ValueError("campaign finalized receipt uses the wrong ledger kind")
        _matching_campaign_finalization_event(
            context, campaign, receipt, verified["receipt_sha256"]
        )
    else:
        if context.journal_path.is_symlink():
            raise ValueError("finalized review journal must not be a symlink")
        current_journal_sha = (
            sha256_file(context.journal_path) if context.journal_path.is_file() else None
        )
        common_mismatch = common_mismatch or (
            receipt.get("journal_sha256") != current_journal_sha
        )
    if common_mismatch:
        raise ValueError("finalized review pointer no longer matches run/journal identity")
    return receipt


def finalize_review(run_dir: str | Path) -> dict[str, Any]:
    """Freeze the latest decision for every eligible case into one generation."""

    with _review_operation_lock(run_dir) as locked_root:
        context = _resolve_context(locked_root)
        existing = _existing_finalized_receipt(context)
        if existing is not None:
            return existing
        campaign = _campaign_binding(context)
        if campaign is not None:
            recovered = _recover_campaign_finalization(context, campaign)
            if recovered is not None:
                return recovered
        elif not context.journal_path.exists():
            atomic_write_text(context.journal_path, "")
        rows, latest = _latest_decisions(context)
        expected = set(context.contract_map)
        if set(latest) != expected:
            missing = sorted(expected - set(latest))
            unexpected = sorted(set(latest) - expected)
            raise ValueError(
                f"review decisions do not exactly cover eligible tasks; "
                f"missing={missing[:10]} unexpected={unexpected[:10]}"
            )
        if campaign is not None:
            if any(
                event.get("batch_id") == campaign["batch_id"]
                and event.get("event_type") == "REVIEW_FINALIZED"
                for event in rows
            ):
                raise ValueError("campaign has a conflicting finalization event")
            live_ledger_path = (
                Path(str(campaign["campaign_root"]))
                / "ledger"
                / "campaign-ledger.jsonl"
            )
            if live_ledger_path.is_symlink() or not live_ledger_path.is_file():
                raise ValueError("campaign ledger is unavailable for review finalization")
            journal_sha = sha256_file(live_ledger_path)
            ledger_kind = CAMPAIGN_REVIEW_LEDGER_KIND
            ledger_head_sha = rows[-1]["event_sha256"] if rows else None
            if ledger_head_sha is None:
                raise ValueError("campaign ledger cannot be empty at review finalization")
            decision_event_count = sum(
                event.get("batch_id") == campaign["batch_id"]
                and event.get("event_type") == "HUMAN_DECISION"
                for event in rows
            )
        else:
            live_ledger_path = context.journal_path
            journal_sha = sha256_file(live_ledger_path)
            ledger_kind = LEGACY_REVIEW_LEDGER_KIND
            ledger_head_sha = rows[-1]["event_sha256"] if rows else None
            decision_event_count = len(rows)
        generation_digest = sha256_json(
            {
                "ledger_kind": ledger_kind,
                "contract_set_sha256": context.contract_set_sha256,
                "journal_sha256": journal_sha,
                "runtime_binding_sha256": sha256_json(dict(context.runtime_binding)),
                "latest_event_sha256": [latest[row["task_id"]]["event_sha256"] for row in context.contracts],
            }
        )
        generation_id = f"review_{generation_digest}"
        generation_root = context.review_root / "generations" / generation_id
        generation_root.parent.mkdir(parents=True, exist_ok=True)
        records = [
            {
                "schema_version": "3.0",
                "review_record_id": latest[contract["task_id"]]["event_id"],
                **_binding(contract),
                "artifact_review_state": latest[contract["task_id"]]["artifact_review_state"],
                "scientific_interpretation": latest[contract["task_id"]][
                    "scientific_interpretation"
                ],
                "reviewer": latest[contract["task_id"]]["reviewer"],
                "reviewed_at": latest[contract["task_id"]]["recorded_at"],
                "notes": latest[contract["task_id"]]["notes"],
                "manual_assertions": latest[contract["task_id"]]["manual_assertions"],
                "journal_event_sha256": latest[contract["task_id"]]["event_sha256"],
            }
            for contract in context.contracts
        ]
        approved = sum(row["artifact_review_state"] == "APPROVE" for row in records)
        rerun_rows = [
            {
                "schema_version": "3.0-rerun-request",
                "source_run_id": record["run_id"],
                "source_generation_id": record["generation_id"],
                "source_review_generation_id": generation_id,
                "source_review_record_id": record["review_record_id"],
                "source_task_id": record["task_id"],
                "source_manifest_order": record["manifest_order"],
                "source_input_fingerprint": record["input_fingerprint"],
                "artifact_review_state": "REJECT",
                "review_notes": record["notes"],
                "control_action": "CREATE_NEW_GENERATION",
                "same_generation_resume_allowed": False,
            }
            for record in records
            if record["artifact_review_state"] == "REJECT"
        ]
        receipt = {
            "schema_version": "3.0-review-receipt",
            "review_generation_id": generation_id,
            "created_at": utc_now(),
            "status": "FINALIZED",
            "run_id": context.contracts[0]["run_id"] if context.contracts else context.run_root.name,
            "generation_id": (
                context.contracts[0]["generation_id"] if context.contracts else None
            ),
            "contract_set_sha256": context.contract_set_sha256,
            "ledger_kind": ledger_kind,
            "journal_sha256": journal_sha,
            "runtime_binding": dict(context.runtime_binding),
            "runtime_binding_sha256": sha256_json(dict(context.runtime_binding)),
            "journal_event_count": len(rows),
            "eligible_count": len(context.contracts),
            "decision_count": len(records),
            "approved_count": approved,
            "rejected_count": len(records) - approved,
            "rerun_required": bool(rerun_rows),
            "rerun_case_count": len(rerun_rows),
            "all_eligible_decided": True,
            "publication_gate": "READY_FOR_STAGING",
            "meaning": "artifact approval gates publication; scientific interpretation remains separate",
        }
        if campaign is not None:
            receipt.update(
                {
                    "campaign_id": campaign["campaign_id"],
                    "campaign_contract_sha256": campaign[
                        "campaign_contract_sha256"
                    ],
                    "master_tasks_sha256": campaign["master_tasks_sha256"],
                    "master_task_set_sha256": campaign[
                        "master_task_set_sha256"
                    ],
                    "batch_id": campaign["batch_id"],
                    "batch_purpose": campaign["batch_purpose"],
                    "batch_request_sha256": campaign["batch_request_sha256"],
                    "batch_task_set_sha256": campaign[
                        "batch_task_set_sha256"
                    ],
                    "accounting_receipt_sha256": campaign[
                        "accounting_receipt_sha256"
                    ],
                    "runtime_manifest_sha256": campaign[
                        "runtime_manifest_sha256"
                    ],
                    "runtime_fingerprint_sha256": campaign[
                        "runtime_fingerprint_sha256"
                    ],
                    "campaign_ledger": "campaign_ledger.jsonl",
                    "campaign_ledger_sha256": journal_sha,
                    "campaign_ledger_event_count": len(rows),
                    "campaign_ledger_head_sha256": ledger_head_sha,
                    "decision_event_count": decision_event_count,
                }
            )
        if generation_root.exists():
            verify_checksum_tree(generation_root)
            frozen = _load_json_object(
                generation_root / "review_receipt.json", label="review generation receipt"
            )
            stable_fields = (
                "schema_version",
                "review_generation_id",
                "run_id",
                "generation_id",
                "contract_set_sha256",
                "ledger_kind",
                "journal_sha256",
                "runtime_binding_sha256",
                "eligible_count",
                "decision_count",
                "approved_count",
                "rejected_count",
                "rerun_required",
                "rerun_case_count",
            )
            if campaign is not None:
                stable_fields += (
                    "campaign_id",
                    "campaign_contract_sha256",
                    "master_tasks_sha256",
                    "master_task_set_sha256",
                    "batch_id",
                    "batch_purpose",
                    "batch_request_sha256",
                    "batch_task_set_sha256",
                    "accounting_receipt_sha256",
                    "runtime_manifest_sha256",
                    "runtime_fingerprint_sha256",
                    "campaign_ledger_sha256",
                    "campaign_ledger_event_count",
                    "campaign_ledger_head_sha256",
                    "decision_event_count",
                )
            if any(frozen.get(field) != receipt.get(field) for field in stable_fields):
                raise ValueError("existing review generation differs from finalized decisions")
            receipt = frozen
        else:
            staging = generation_root.parent / f".{generation_id}.tmp-{uuid.uuid4().hex}"
            staging.mkdir(mode=0o700)
            try:
                write_jsonl(staging / "review_records.jsonl", records)
                receipt["review_records_sha256"] = sha256_file(staging / "review_records.jsonl")
                if campaign is not None:
                    frozen_ledger = staging / "campaign_ledger.jsonl"
                    shutil.copyfile(live_ledger_path, frozen_ledger)
                    if sha256_file(frozen_ledger) != journal_sha:
                        raise ValueError(
                            "campaign ledger changed while freezing its decision prefix"
                        )
                else:
                    frozen_ledger = staging / "review_journal.jsonl"
                    shutil.copyfile(live_ledger_path, frozen_ledger)
                    receipt["review_journal"] = "review_journal.jsonl"
                    receipt["review_journal_sha256"] = sha256_file(frozen_ledger)
                    if receipt["review_journal_sha256"] != journal_sha:
                        raise ValueError(
                            "review journal changed while finalizing its generation"
                        )
                write_jsonl(staging / "rerun_manifest.jsonl", rerun_rows)
                rerun_manifest_sha256 = sha256_file(staging / "rerun_manifest.jsonl")
                run_identity = _load_json_object(
                    context.run_root / "contract" / "run_identity.json",
                    label="v3 run identity",
                )
                request_set_sha256 = sha256_json(rerun_rows)
                rerun_id = "review_rejects_" + request_set_sha256
                rerun_receipt = {
                    "schema_version": "3.0-rerun-receipt",
                    "rerun_id": rerun_id,
                    "state": "RERUN_REQUIRED" if rerun_rows else "NOT_REQUIRED",
                    "source_run_id": receipt["run_id"],
                    "source_generation_id": receipt["generation_id"],
                    "canonical_tasks_sha256": run_identity[
                        "canonical_tasks_sha256"
                    ],
                    "rerun_case_count": len(rerun_rows),
                    "rerun_request_set_sha256": request_set_sha256,
                    "rerun_manifest": "rerun_manifest.jsonl",
                    "rerun_manifest_sha256": rerun_manifest_sha256,
                    "target_generation_policy": "MUST_DIFFER_FROM_SOURCE_GENERATION",
                    "same_generation_resume_allowed": False,
                }
                atomic_write_json(staging / "rerun_receipt.json", rerun_receipt)
                receipt["rerun_manifest"] = "rerun_manifest.jsonl"
                receipt["rerun_manifest_sha256"] = rerun_manifest_sha256
                receipt["rerun_receipt"] = "rerun_receipt.json"
                receipt["rerun_receipt_sha256"] = sha256_file(
                    staging / "rerun_receipt.json"
                )
                atomic_write_json(staging / "review_receipt.json", receipt)
                _write_checksums(staging)
                verify_checksum_tree(staging)
                os.replace(staging, generation_root)
            except BaseException:
                shutil.rmtree(staging, ignore_errors=True)
                raise
        verified = verify_finalized_review_generation(generation_root, receipt)
        if campaign is not None:
            current_rows = verify_campaign_ledger(campaign["campaign_root"])
            current_ledger_sha = sha256_file(live_ledger_path)
            current_head = current_rows[-1]["event_sha256"] if current_rows else None
            if current_ledger_sha != journal_sha or current_head != ledger_head_sha:
                raise ValueError(
                    "campaign ledger changed while finalizing its decision prefix"
                )
            finalization_payload = _campaign_finalization_payload(
                receipt, verified["receipt_sha256"]
            )
            finalization = append_campaign_event(
                campaign["campaign_root"],
                event_type="REVIEW_FINALIZED",
                batch_id=str(campaign["batch_id"]),
                actor="igv-snapshot-review-controller",
                payload=finalization_payload,
            )
            if (
                finalization.get("previous_event_sha256") != ledger_head_sha
                or finalization.get("payload") != finalization_payload
            ):
                raise ValueError(
                    "campaign ledger advanced before review finalization was appended"
                )
            _matching_campaign_finalization_event(
                context, campaign, receipt, verified["receipt_sha256"]
            )
        pointer = _finalized_pointer(context, generation_root, receipt)
        _write_finalized_pointer(context, pointer)
        return receipt


class ReviewHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        context: ReviewContext,
        reviewer: str,
    ) -> None:
        super().__init__(address, ReviewRequestHandler)
        self.context = context
        self.reviewer = reviewer
        self.access_token = secrets.token_urlsafe(32)
        self.session_cookie = secrets.token_urlsafe(32)
        self.csrf_token = secrets.token_urlsafe(32)
        self.access_token_used = False
        self.auth_lock = threading.Lock()


class ReviewRequestHandler(BaseHTTPRequestHandler):
    server: ReviewHTTPServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # pragma: no cover - CLI log
        return

    def _cookie_authenticated(self) -> bool:
        cookie = SimpleCookie()
        try:
            cookie.load(self.headers.get("Cookie", ""))
        except Exception:
            return False
        morsel = cookie.get("igv_review_session")
        return bool(
            morsel
            and secrets.compare_digest(morsel.value, self.server.session_cookie)
        )

    def _authenticate_get(self) -> tuple[bool, bool]:
        if self._cookie_authenticated():
            return True, False
        token = parse_qs(urlparse(self.path).query).get("token", [""])[0]
        with self.server.auth_lock:
            if self.server.access_token_used or not secrets.compare_digest(
                token, self.server.access_token
            ):
                return False, False
            self.server.access_token_used = True
        return True, True

    def _security_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Security-Policy", "default-src 'self'; img-src 'self'; style-src 'unsafe-inline'")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")

    def _send(self, status: HTTPStatus, body: bytes, content_type: str, *, set_cookie: bool = False) -> None:
        self.send_response(status)
        self._security_headers()
        if set_cookie:
            self.send_header(
                "Set-Cookie",
                f"igv_review_session={self.server.session_cookie}; HttpOnly; SameSite=Strict; Path=/",
            )
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: HTTPStatus, message: str) -> None:
        body = f"<!doctype html><h1>{status.value}</h1><p>{html.escape(message)}</p>".encode()
        self._send(status, body, "text/html; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802
        authenticated, set_cookie = self._authenticate_get()
        if not authenticated:
            self._error(HTTPStatus.FORBIDDEN, "invalid or already-used review access token")
            return
        parsed = urlparse(self.path)
        if parsed.path == "/artifact":
            task_id = parse_qs(parsed.query).get("task_id", [""])[0]
            path = self.server.context.artifact_paths.get(task_id)
            if path is None:
                self._error(HTTPStatus.NOT_FOUND, "unknown review artifact")
                return
            try:
                contract = self.server.context.contract_map[task_id]
                payload = read_regular_file_bytes(
                    path,
                    expected_sha256=contract["review_image_sha256"],
                    label="review artifact",
                )
            except (OSError, ValueError):
                self._error(
                    HTTPStatus.CONFLICT,
                    "review artifact changed after the review contract was created",
                )
                return
            self._send(HTTPStatus.OK, payload, "image/png", set_cookie=set_cookie)
            return
        if parsed.path != "/":
            self._error(HTTPStatus.NOT_FOUND, "unknown endpoint")
            return
        self._send(
            HTTPStatus.OK,
            self._render_index().encode("utf-8"),
            "text/html; charset=utf-8",
            set_cookie=set_cookie,
        )

    def _render_index(self) -> str:
        _rows, latest = _latest_decisions(self.server.context)
        cards: list[str] = []
        for contract in self.server.context.contracts:
            task_id = str(contract["task_id"])
            current = latest.get(task_id)
            if contract["adapter_type"] == "generic":
                options = '<option value="NOT_APPLICABLE">NOT_APPLICABLE</option>'
            else:
                options = "".join(
                    f'<option value="{value}">{value}</option>'
                    for value in ("SUPPORTED", "NOT_SUPPORTED", "INDETERMINATE")
                )
            assertions = "".join(
                f'<label><input type="checkbox" name="assertion.{html.escape(name)}" value="true"> '
                f"{html.escape(name)}</label><br>"
                for name in contract["required_manual_assertions"]
            )
            state = (
                f"{current['artifact_review_state']} / {current['scientific_interpretation']}"
                if current
                else "PENDING"
            )
            cards.append(
                f"<section><h2>{html.escape(task_id)}</h2>"
                f"<p>Current: {html.escape(state)}</p>"
                f'<img alt="IGV evidence for {html.escape(task_id)}" '
                f'src="/artifact?task_id={quote(task_id)}" style="max-width:100%;height:auto">'
                '<form method="post" action="/decision">'
                f'<input type="hidden" name="csrf" value="{self.server.csrf_token}">'
                f'<input type="hidden" name="task_id" value="{html.escape(task_id)}">'
                '<label>Artifact <select name="artifact_review_state">'
                '<option value="APPROVE">APPROVE</option><option value="REJECT">REJECT</option>'
                "</select></label><br>"
                f'<label>Scientific <select name="scientific_interpretation">{options}</select></label><br>'
                f"{assertions}"
                '<label>Notes <textarea name="notes"></textarea></label><br>'
                '<button type="submit">Record decision</button></form></section>'
            )
        return (
            "<!doctype html><html><head><meta charset=\"utf-8\"><title>IGV review</title>"
            "<style>body{font-family:sans-serif;max-width:1200px;margin:auto;padding:1rem;}"
            "section{border:1px solid #bbb;padding:1rem;margin:1rem 0}label{line-height:1.7}</style>"
            "</head><body><h1>IGV Snapshot Review</h1>"
            "<p>Decisions append to a checksum-chained journal. Publication remains a separate gate.</p>"
            + "".join(cards)
            + "</body></html>"
        )

    def do_POST(self) -> None:  # noqa: N802
        if not self._cookie_authenticated():
            self._error(HTTPStatus.FORBIDDEN, "authenticated review session required")
            return
        if urlparse(self.path).path != "/decision":
            self._error(HTTPStatus.NOT_FOUND, "unknown endpoint")
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._error(HTTPStatus.BAD_REQUEST, "invalid content length")
            return
        if length <= 0 or length > 64 * 1024:
            self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "invalid request size")
            return
        raw = self.rfile.read(length)
        try:
            if self.headers.get("Content-Type", "").split(";", 1)[0] == "application/json":
                document = json.loads(raw.decode("utf-8"))
                if not isinstance(document, dict):
                    raise ValueError("JSON request must be an object")
                values = {key: str(value) for key, value in document.items() if not isinstance(value, dict)}
                assertion_values = document.get("manual_assertions", {})
                if not isinstance(assertion_values, dict):
                    raise ValueError("manual_assertions must be an object")
                if any(not isinstance(value, bool) for value in assertion_values.values()):
                    raise ValueError("manual_assertions values must be booleans")
            else:
                parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
                values = {key: items[-1] for key, items in parsed.items() if not key.startswith("assertion.")}
                assertion_values = {
                    key.removeprefix("assertion."): items[-1].lower() == "true"
                    for key, items in parsed.items()
                    if key.startswith("assertion.")
                }
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
            return
        supplied_csrf = values.get("csrf", self.headers.get("X-CSRF-Token", ""))
        if not secrets.compare_digest(supplied_csrf, self.server.csrf_token):
            self._error(HTTPStatus.FORBIDDEN, "invalid CSRF token")
            return
        try:
            append_review_decision(
                self.server.context.run_root,
                task_id=values.get("task_id", ""),
                artifact_review_state=values.get("artifact_review_state", ""),
                scientific_interpretation=values.get("scientific_interpretation", ""),
                reviewer=self.server.reviewer,
                notes=values.get("notes", ""),
                manual_assertions={key: bool(value) for key, value in assertion_values.items()},
            )
            _rows, latest = _latest_decisions(self.server.context)
            complete = set(latest) == set(self.server.context.contract_map)
        except (ValueError, RuntimeError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, str(exc))
            return
        if complete:
            body = b"<!doctype html><h1>Review complete</h1><p>The immutable generation will now be finalized.</p>"
            self._send(HTTPStatus.OK, body, "text/html; charset=utf-8")
            threading.Thread(target=self.server.shutdown, daemon=True).start()
            return
        self.send_response(HTTPStatus.SEE_OTHER)
        self._security_headers()
        self.send_header("Location", "/")
        self.send_header("Content-Length", "0")
        self.end_headers()


def create_review_server(
    run_dir: str | Path,
    host: str = "127.0.0.1",
    port: int = 0,
    reviewer: str = "",
) -> tuple[ReviewHTTPServer, str]:
    """Create a localhost-only review server and return its one-time URL."""

    if host != "127.0.0.1":
        raise ValueError("review server must bind exactly to 127.0.0.1")
    if not reviewer.strip():
        raise ValueError("reviewer is required")
    if not isinstance(port, int) or not 0 <= port <= 65535:
        raise ValueError("port must be an integer from 0 through 65535")
    context = _resolve_context(run_dir)
    server = ReviewHTTPServer((host, port), context, reviewer.strip())
    actual_port = int(server.server_address[1])
    url = f"http://127.0.0.1:{actual_port}/?token={quote(server.access_token)}"
    return server, url


def serve_review(
    run_dir: str | Path,
    host: str = "127.0.0.1",
    port: int = 0,
    reviewer: str = "",
) -> dict[str, Any]:
    """Serve until every eligible task has a decision, then finalize and return its receipt."""

    context = _resolve_context(run_dir)
    if not context.contracts:
        return finalize_review(run_dir)
    server, url = create_review_server(run_dir, host, port, reviewer)
    print(f"IGV review URL (one-time token): {url}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        server.server_close()
    return finalize_review(run_dir)
