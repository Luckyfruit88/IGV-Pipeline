from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from .contracts import validate_case_result_document, validate_review_document
from .utils import atomic_write_json, read_jsonl, sha256_file, utc_now, write_jsonl


REVIEW_BINDING_FIELDS = (
    "run_id",
    "generation_id",
    "task_id",
    "manifest_order",
    "input_fingerprint",
    "case_result_sha256",
    "combined_sha256",
    "left_pixel_sha256",
    "scientific_qc_sha256",
    "figure_contract_id",
    "gui_settle_contract_id",
)


def _unique_by(rows: list[dict[str, Any]], field: str, label: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row[field])
        if key in result:
            raise ValueError(f"duplicate {label}: {key}")
        result[key] = row
    return result


def validate_reviews(
    review_contract_path: str | Path,
    review_jsonl_path: str | Path,
    aggregate_case_results: str | Path,
    output_dir: str | Path,
    *,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Bind one append-only human review record to every eligible case."""

    contract_path = Path(review_contract_path).expanduser()
    review_path = Path(review_jsonl_path).expanduser()
    results_path = Path(aggregate_case_results).expanduser()
    for label, path in (
        ("review contract", contract_path),
        ("review records", review_path),
        ("aggregate results", results_path),
    ):
        if path.is_symlink() or not path.resolve(strict=True).is_file():
            raise ValueError(f"{label} must be a regular non-symlink file: {path}")

    contracts = list(read_jsonl(contract_path.resolve(strict=True)))
    contract_map = _unique_by(contracts, "task_id", "review contract task_id")
    reviews = list(read_jsonl(review_path.resolve(strict=True)))
    for review in reviews:
        validate_review_document(review, schema_dir=schema_dir)
    review_map = _unique_by(reviews, "task_id", "review task_id")
    _unique_by(reviews, "review_record_id", "review_record_id")
    if set(review_map) != set(contract_map):
        missing = sorted(set(contract_map) - set(review_map))
        unexpected = sorted(set(review_map) - set(contract_map))
        raise ValueError(
            f"review set differs from eligible contract; missing={missing[:10]} "
            f"unexpected={unexpected[:10]}"
        )

    case_results = list(read_jsonl(results_path.resolve(strict=True)))
    for result in case_results:
        validate_case_result_document(result, schema_dir=schema_dir)
    result_map = _unique_by(case_results, "task_id", "aggregate task_id")
    if not set(contract_map).issubset(result_map):
        raise ValueError("review contract contains task IDs outside aggregate results")

    accepted: list[dict[str, Any]] = []
    reviewed_results: list[dict[str, Any]] = []
    for result in sorted(case_results, key=lambda row: int(row["manifest_order"])):
        task_id = result["task_id"]
        if task_id not in contract_map:
            reviewed_results.append(result)
            continue
        contract = contract_map[task_id]
        review = review_map[task_id]
        mismatched = [
            field for field in REVIEW_BINDING_FIELDS if review.get(field) != contract.get(field)
        ]
        if mismatched:
            raise ValueError(
                f"review binding differs from package contract for {task_id}: "
                + ", ".join(mismatched)
            )
        if result["input_fingerprint"] != contract["input_fingerprint"]:
            raise ValueError(f"review contract input identity drift: {task_id}")
        if contract.get("evidence_state") != result["evidence_state"]:
            raise ValueError(f"review contract evidence state drift: {task_id}")
        if (
            result["evidence_state"] == "EVIDENCE_INCOMPLETE"
            and review["scientific_interpretation"] != "INDETERMINATE"
        ):
            raise ValueError(
                f"incomplete evidence requires INDETERMINATE interpretation: {task_id}"
            )

        reviewed = dict(result)
        reviewed["artifact_review_state"] = review["artifact_review_state"]
        reviewed["scientific_interpretation"] = review["scientific_interpretation"]
        if review["artifact_review_state"] == "APPROVE":
            reviewed["publication_state"] = "READY"
        else:
            reviewed["publication_state"] = "WITHHELD"
            reviewed["warnings"] = [
                *reviewed["warnings"],
                {
                    "code": "MANUAL_ARTIFACT_REJECTED",
                    "message": "human artifact review rejected this evidence package",
                },
            ]
            reviewed["failures"] = [
                *reviewed["failures"],
                {
                    "stage": "MANUAL_REVIEW",
                    "class": "DOMAIN",
                    "code": "MANUAL_ARTIFACT_REJECTED",
                    "message": "human artifact review rejected this evidence package",
                    "rerun_eligible": True,
                },
            ]
            reviewed["rerun_eligible"] = True
            reviewed["rerun_reason"] = "MANUAL_REVIEW:MANUAL_ARTIFACT_REJECTED"
        validate_case_result_document(reviewed, schema_dir=schema_dir)
        accepted.append(review)
        reviewed_results.append(reviewed)

    report = {
        "schema_version": "2.0-review-validation",
        "created_at": utc_now(),
        "status": "PASS",
        "review_contract_sha256": sha256_file(contract_path),
        "review_records_sha256": sha256_file(review_path),
        "aggregate_case_results_sha256": sha256_file(results_path),
        "eligible_count": len(contracts),
        "accepted_count": len(accepted),
        "artifact_approved_count": sum(
            review["artifact_review_state"] == "APPROVE" for review in accepted
        ),
        "artifact_rejected_count": sum(
            review["artifact_review_state"] == "REJECT" for review in accepted
        ),
    }
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"review validation output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        write_jsonl(
            staging / "accepted_reviews.jsonl",
            sorted(accepted, key=lambda row: int(row["manifest_order"])),
        )
        write_jsonl(staging / "reviewed_case_results.jsonl", reviewed_results)
        atomic_write_json(staging / "review_validation.json", report)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**report, "output_dir": str(destination)}
