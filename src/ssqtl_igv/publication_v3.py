from __future__ import annotations

import json
import re
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
from .publication import (
    assert_public_tree_safe,
    atomic_rename_noreplace,
    verify_checksum_tree,
)
from .review_server import verify_finalized_review_generation
from .utils import (
    atomic_write_json,
    atomic_write_text,
    reject_symlink_path_components,
    sha256_file,
    sha256_json,
    utc_now,
    write_jsonl,
    write_tsv,
)


_SHA256_PATTERN = re.compile(r"^[a-f0-9]{64}$")
_CAMPAIGN_PURPOSES = {"PILOT_QA", "PRODUCTION_CONTINUATION"}
_CAMPAIGN_BINDING_FIELDS = (
    "batch_purpose",
    "master_task_set_sha256",
    "batch_task_set_sha256",
)
_PUBLIC_RUNTIME_BINDING_FIELDS = (
    "schema_version",
    "profile",
    "run_identity_sha256",
    "runtime_fingerprint_sha256",
    "runtime_manifest_sha256",
    "runtime_oci_digest",
)


def _public_runtime_binding(runtime_binding: Mapping[str, Any]) -> dict[str, Any]:
    """Project runtime provenance into a path-free public allowlist."""

    return {
        field: runtime_binding[field]
        for field in _PUBLIC_RUNTIME_BINDING_FIELDS
        if field in runtime_binding
    }


def _object(value: Mapping[str, Any] | str | Path, *, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    path = Path(value).expanduser()
    if path.is_symlink() or not path.resolve(strict=True).is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        document = json.loads(path.resolve(strict=True).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}: {path}: {exc}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{label} must contain one JSON object")
    assert_not_debug_metadata(document, label=label)
    return document


def _sha256(value: object, *, label: str) -> str:
    token = str(value).strip().lower()
    if not _SHA256_PATTERN.fullmatch(token):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return token


def _frozen_runtime_binding(
    receipt: Mapping[str, Any],
) -> tuple[dict[str, Any], str]:
    binding = receipt.get("runtime_binding")
    if not isinstance(binding, Mapping):
        raise ValueError("publication requires a frozen runtime binding")
    frozen = dict(binding)
    if receipt.get("runtime_binding_sha256") != sha256_json(frozen):
        raise ValueError("publication runtime binding checksum is invalid")
    fingerprint = _sha256(
        frozen.get("runtime_fingerprint_sha256"),
        label="runtime_fingerprint_sha256",
    )
    manifest_sha = frozen.get("runtime_manifest_sha256")
    if manifest_sha is not None:
        _sha256(manifest_sha, label="runtime_manifest_sha256")
    return frozen, fingerprint


def _campaign_binding(receipt: Mapping[str, Any]) -> dict[str, str]:
    present = {field for field in _CAMPAIGN_BINDING_FIELDS if field in receipt}
    if not present:
        return {}
    if present != set(_CAMPAIGN_BINDING_FIELDS):
        raise ValueError("review receipt has an incomplete campaign binding")
    purpose = str(receipt["batch_purpose"])
    if purpose not in _CAMPAIGN_PURPOSES:
        raise ValueError("review receipt batch_purpose is invalid")
    return {
        "batch_purpose": purpose,
        "master_task_set_sha256": _sha256(
            receipt["master_task_set_sha256"],
            label="master_task_set_sha256",
        ),
        "batch_task_set_sha256": _sha256(
            receipt["batch_task_set_sha256"],
            label="batch_task_set_sha256",
        ),
    }


def _safe_artifact(run_root: Path, record: Mapping[str, Any]) -> Path:
    relative = Path(str(record.get("relative_path", "")))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"unsafe review artifact path: {relative}")
    candidate = run_root / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"review artifact is unavailable or symlinked: {candidate}")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(run_root)
    except ValueError as exc:
        raise ValueError(f"review artifact escapes run root: {candidate}") from exc
    if sha256_file(resolved) != str(record.get("sha256", "")):
        raise ValueError(f"review artifact checksum drift: {candidate}")
    assert_production_artifact(resolved, label="publication source artifact")
    return resolved


def _write_checksums(root: Path) -> Path:
    paths = sorted(
        (
            path
            for path in root.rglob("*")
            if path.is_file() and path.name != "SHA256SUMS"
        ),
        key=lambda path: str(path.relative_to(root)),
    )
    target = root / "SHA256SUMS"
    atomic_write_text(
        target,
        "".join(f"{sha256_file(path)}  {path.relative_to(root)}\n" for path in paths),
    )
    return target


def _verify_review_coverage(
    receipt: Mapping[str, Any], records: list[dict[str, Any]]
) -> None:
    eligible = receipt.get("eligible_count")
    decided = receipt.get("decision_count")
    approved = receipt.get("approved_count")
    rejected = receipt.get("rejected_count")
    task_ids = [str(record.get("task_id", "")) for record in records]
    if (
        not isinstance(eligible, int)
        or isinstance(eligible, bool)
        or eligible < 1
        or decided != eligible
        or not isinstance(approved, int)
        or isinstance(approved, bool)
        or not isinstance(rejected, int)
        or isinstance(rejected, bool)
        or approved + rejected != eligible
        or len(records) != eligible
        or len(task_ids) != len(set(task_ids))
        or any(not task_id for task_id in task_ids)
        or sum(record.get("artifact_review_state") == "APPROVE" for record in records)
        != approved
        or sum(record.get("artifact_review_state") == "REJECT" for record in records)
        != rejected
    ):
        raise ValueError(
            "finalized review does not exactly cover its eligible task set"
        )


def build_publication_staging(
    run_dir: str | Path,
    review_receipt: Mapping[str, Any] | str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Build an immutable public staging tree from one finalized review generation."""

    run_root = Path(run_dir).expanduser()
    if run_root.is_symlink() or not run_root.resolve(strict=True).is_dir():
        raise ValueError(
            f"run directory must be a regular non-symlink directory: {run_root}"
        )
    run_root = run_root.resolve(strict=True)
    receipt = _object(review_receipt, label="review receipt")
    if (
        receipt.get("schema_version") != "3.0-review-receipt"
        or receipt.get("status") != "FINALIZED"
        or receipt.get("publication_gate") != "READY_FOR_STAGING"
        or receipt.get("all_eligible_decided") is not True
    ):
        raise ValueError("review receipt is not a finalized v3 publication gate")
    runtime_binding, runtime_fingerprint = _frozen_runtime_binding(receipt)
    campaign_binding = _campaign_binding(receipt)

    generation_id = str(receipt.get("review_generation_id", ""))
    generation_root = run_root / "review" / "generations" / generation_id
    if generation_root.is_symlink():
        raise ValueError("review generation must not be a symlink")
    pointer = _object(
        run_root / "review" / "finalized_review.json",
        label="finalized review pointer",
    )
    expected_receipt_relative = str(
        (generation_root / "review_receipt.json").relative_to(run_root / "review")
    )
    verified_generation = verify_finalized_review_generation(generation_root, receipt)
    if (
        pointer.get("schema_version") != "3.0-finalized-review-pointer"
        or pointer.get("review_generation_id") != generation_id
        or pointer.get("receipt_relative_path") != expected_receipt_relative
        or pointer.get("receipt_sha256") != verified_generation["receipt_sha256"]
        or pointer.get("checksums_sha256") != verified_generation["checksums_sha256"]
    ):
        raise ValueError("finalized review pointer differs from the frozen generation")
    frozen_receipt = verified_generation["receipt"]
    if frozen_receipt != receipt:
        raise ValueError("supplied review receipt differs from the frozen generation")
    records = list(verified_generation["records"])
    _verify_review_coverage(receipt, records)

    destination = reject_symlink_path_components(
        output_dir, label="publication staging"
    ).resolve(strict=False)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"publication staging already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError(
            "publication staging parent must be a regular directory: "
            f"{destination.parent}"
        )
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    status_rows: list[dict[str, Any]] = []
    try:
        approved_count = 0
        rejected_count = 0
        for record in sorted(records, key=lambda row: int(row["manifest_order"])):
            task_id = str(record["task_id"])
            result_path = run_root / "results" / "cases" / task_id / "case_result.json"
            result = _object(result_path, label="v3 case result")
            if sha256_file(result_path) != record.get("case_result_sha256"):
                raise ValueError(f"case result differs from review binding: {task_id}")
            for key in (
                "run_id",
                "generation_id",
                "task_id",
                "manifest_order",
                "input_fingerprint",
            ):
                if result.get(key) != record.get(key):
                    raise ValueError(
                        f"case result identity differs from review record: {task_id}:{key}"
                    )
            decision = str(record["artifact_review_state"])
            public_state = "WITHHELD"
            review_relative: str | None = None
            review_sha: str | None = None
            if decision == "APPROVE":
                artifact = result.get("artifacts", {}).get("review_image")
                if not isinstance(artifact, Mapping):
                    raise ValueError(f"approved case lacks review image: {task_id}")
                source = _safe_artifact(run_root, artifact)
                if artifact.get("sha256") != record.get("review_image_sha256"):
                    raise ValueError(
                        f"review image differs from review binding: {task_id}"
                    )
                case_root = staging / "cases" / task_id
                case_root.mkdir(parents=True)
                target = case_root / "review.png"
                shutil.copyfile(source, target)
                review_relative = str(target.relative_to(staging))
                review_sha = sha256_file(target)
                metadata = {
                    "schema_version": "3.0-public-case",
                    "run_id": result["run_id"],
                    "generation_id": result["generation_id"],
                    "task_id": task_id,
                    "manifest_order": result["manifest_order"],
                    "input_fingerprint": result["input_fingerprint"],
                    "adapter_type": result["adapter_type"],
                    "evidence_state": result["evidence_state"],
                    "artifact_review_state": decision,
                    "scientific_interpretation": record[
                        "scientific_interpretation"
                    ],
                    "publication_state": "PUBLISHED",
                    "review_image": review_relative,
                    "review_image_sha256": review_sha,
                }
                atomic_write_json(case_root / "metadata.json", metadata)
                public_state = "PUBLISHED"
                approved_count += 1
            elif decision == "REJECT":
                rejected_count += 1
            else:  # pragma: no cover - guarded by _verify_review_coverage
                raise ValueError(f"unexpected finalized review decision: {decision}")
            status_rows.append(
                {
                    "manifest_order": int(record["manifest_order"]),
                    "task_id": task_id,
                    "artifact_review_state": decision,
                    "scientific_interpretation": record[
                        "scientific_interpretation"
                    ],
                    "publication_state": public_state,
                    "review_image": review_relative or "",
                    "review_image_sha256": review_sha or "",
                }
            )

        fields = [
            "manifest_order",
            "task_id",
            "artifact_review_state",
            "scientific_interpretation",
            "publication_state",
            "review_image",
            "review_image_sha256",
        ]
        write_tsv(staging / "final_status.tsv", fields, status_rows)
        write_jsonl(staging / "public_case_status.jsonl", status_rows)
        public_binding = _public_runtime_binding(runtime_binding)
        publication = {
            "schema_version": "3.0-publication-staging",
            "created_at": utc_now(),
            "status": "STAGED",
            "run_id": receipt["run_id"],
            "generation_id": receipt["generation_id"],
            "review_generation_id": generation_id,
            "review_receipt_sha256": verified_generation["receipt_sha256"],
            "contract_set_sha256": receipt["contract_set_sha256"],
            "journal_sha256": receipt["journal_sha256"],
            "runtime_binding": public_binding,
            "public_runtime_binding_sha256": sha256_json(public_binding),
            "runtime_binding_sha256": receipt["runtime_binding_sha256"],
            "runtime_fingerprint_sha256": runtime_fingerprint,
            "terminal_case_count": len(records),
            "published_case_count": approved_count,
            "withheld_case_count": rejected_count,
            "meaning": (
                "PUBLISHED means human-approved evidence delivery, "
                "not biological support"
            ),
            **campaign_binding,
        }
        if "runtime_manifest_sha256" in runtime_binding:
            publication["runtime_manifest_sha256"] = runtime_binding[
                "runtime_manifest_sha256"
            ]
        if "runtime_oci_digest" in runtime_binding:
            publication["runtime_oci_digest"] = runtime_binding[
                "runtime_oci_digest"
            ]
        atomic_write_json(staging / "publication.json", publication)
        assert_production_artifact_tree(staging, label="publication staging tree")
        assert_public_tree_safe(staging)
        checksums = _write_checksums(staging)
        verify_checksum_tree(staging)
        atomic_rename_noreplace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **publication,
        "output_dir": str(destination),
        "checksums_sha256": sha256_file(destination / checksums.name),
    }
