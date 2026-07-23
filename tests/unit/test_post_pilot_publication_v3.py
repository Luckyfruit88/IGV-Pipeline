from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

import pytest

from ssqtl_igv import publication
from ssqtl_igv.publication import (
    build_publication_promotion_receipt,
    promote_publication,
    verify_checksum_tree,
)
from ssqtl_igv.publication_v3 import (
    _frozen_runtime_binding,
    _verify_review_coverage,
    build_publication_staging,
)
from ssqtl_igv.utils import sha256_file, sha256_json


REVIEW_BYTES = b"synthetic review image\n"
REVIEW_SHA256 = hashlib.sha256(REVIEW_BYTES).hexdigest()


def _write_json(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _review() -> dict:
    runtime_binding = {
        "schema_version": "3.0-runtime-review-binding",
        "profile": "scc",
        "run_identity_sha256": "a" * 64,
        "runtime_fingerprint_sha256": "b" * 64,
        "runtime_manifest_sha256": "c" * 64,
        "runtime_oci_digest": "sha256:" + "d" * 64,
    }
    return {
        "schema_version": "3.0-review-receipt",
        "status": "FINALIZED",
        "publication_gate": "READY_FOR_STAGING",
        "all_eligible_decided": True,
        "review_generation_id": "review-001",
        "run_id": "run-001",
        "generation_id": "generation-001",
        "contract_set_sha256": "e" * 64,
        "journal_sha256": "f" * 64,
        "runtime_binding": runtime_binding,
        "runtime_binding_sha256": sha256_json(runtime_binding),
        "eligible_count": 1,
        "decision_count": 1,
        "approved_count": 1,
        "rejected_count": 0,
    }


def _staging(tmp_path: Path, review_path: Path, *, fingerprint: str = "b" * 64) -> Path:
    review = json.loads(review_path.read_text(encoding="utf-8"))
    root = tmp_path / "staging"
    root.mkdir()
    metadata = root / "publication.json"
    _write_json(
        metadata,
        {
            "schema_version": "3.0-publication-staging",
            "status": "STAGED",
            "run_id": review["run_id"],
            "generation_id": review["generation_id"],
            "review_generation_id": review["review_generation_id"],
            "review_receipt_sha256": sha256_file(review_path),
            "contract_set_sha256": review["contract_set_sha256"],
            "journal_sha256": review["journal_sha256"],
            "terminal_case_count": 1,
            "runtime_binding_sha256": review["runtime_binding_sha256"],
            "runtime_fingerprint_sha256": fingerprint,
            "runtime_manifest_sha256": review["runtime_binding"][
                "runtime_manifest_sha256"
            ],
            "runtime_oci_digest": review["runtime_binding"]["runtime_oci_digest"],
            "published_case_count": 1,
            "withheld_case_count": 0,
        },
    )
    case_root = root / "cases" / "case-1"
    case_root.mkdir(parents=True)
    image = case_root / "review.png"
    image.write_bytes(REVIEW_BYTES)
    case_metadata = _write_json(
        case_root / "metadata.json",
        {
            "task_id": "case-1",
            "manifest_order": 1,
            "artifact_review_state": "APPROVE",
            "publication_state": "PUBLISHED",
            "review_image": "cases/case-1/review.png",
            "review_image_sha256": REVIEW_SHA256,
        },
    )
    status = root / "public_case_status.jsonl"
    status.write_text(
        json.dumps(
            {
                "task_id": "case-1",
                "manifest_order": 1,
                "artifact_review_state": "APPROVE",
                "scientific_interpretation": "NOT_APPLICABLE",
                "publication_state": "PUBLISHED",
                "review_image": "cases/case-1/review.png",
                "review_image_sha256": REVIEW_SHA256,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "SHA256SUMS").write_text(
        "".join(
            f"{sha256_file(path)}  {path.relative_to(root)}\n"
            for path in sorted(
                (metadata, image, case_metadata, status),
                key=lambda path: str(path.relative_to(root)),
            )
        ),
        encoding="utf-8",
    )
    return root


def _verified_generation(review_path: Path) -> dict:
    return {
        "receipt_sha256": sha256_file(review_path),
        "records": [
            {
                "task_id": "case-1",
                "manifest_order": 1,
                "artifact_review_state": "APPROVE",
                "scientific_interpretation": "NOT_APPLICABLE",
                "review_image_sha256": REVIEW_SHA256,
            }
        ],
    }


def test_publication_api_has_no_certification_inputs() -> None:
    parameters = inspect.signature(build_publication_staging).parameters
    assert set(parameters) == {"run_dir", "review_receipt", "output_dir"}


def test_frozen_runtime_binding_requires_fingerprint_not_certificate() -> None:
    review = _review()
    binding, fingerprint = _frozen_runtime_binding(review)
    assert fingerprint == "b" * 64
    assert binding["runtime_manifest_sha256"] == "c" * 64

    review["runtime_binding"].pop("runtime_fingerprint_sha256")
    review["runtime_binding_sha256"] = sha256_json(review["runtime_binding"])
    with pytest.raises(ValueError, match="runtime_fingerprint_sha256"):
        _frozen_runtime_binding(review)


def test_review_coverage_is_exact_and_unique() -> None:
    review = _review()
    records = [{"task_id": "case-1", "artifact_review_state": "APPROVE"}]
    _verify_review_coverage(review, records)
    with pytest.raises(ValueError, match="exactly cover"):
        _verify_review_coverage(
            {**review, "eligible_count": 2, "decision_count": 2}, records
        )


def test_any_profile_can_promote_with_frozen_fingerprint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    review_path = _write_json(
        tmp_path / "review" / "generations" / "review-001" / "review_receipt.json",
        _review(),
    )
    monkeypatch.setattr(
        publication,
        "_verify_frozen_review_receipt",
        lambda *_args: _verified_generation(review_path),
    )
    staging = _staging(tmp_path, review_path)
    destination = tmp_path / "published"
    promotion_path = tmp_path / "promotion.json"

    receipt = build_publication_promotion_receipt(
        staging,
        destination,
        review_path,
        output=promotion_path,
    )
    assert receipt["runtime_fingerprint_sha256"] == "b" * 64
    result = promote_publication(staging, destination, promotion_path)

    assert result["status"] == "PUBLISHED"
    assert result["runtime_fingerprint_sha256"] == "b" * 64
    completion = json.loads(
        Path(result["completion_receipt"]).read_text(encoding="utf-8")
    )
    assert completion["runtime_fingerprint_sha256"] == "b" * 64
    assert not any("certif" in key or "qualification" in key for key in completion)
    verify_checksum_tree(destination)


def test_promotion_rejects_runtime_fingerprint_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    review_path = _write_json(
        tmp_path / "review" / "generations" / "review-001" / "review_receipt.json",
        _review(),
    )
    monkeypatch.setattr(
        publication,
        "_verify_frozen_review_receipt",
        lambda *_args: _verified_generation(review_path),
    )
    staging = _staging(tmp_path, review_path, fingerprint="9" * 64)

    with pytest.raises(ValueError, match="runtime_fingerprint_sha256"):
        build_publication_promotion_receipt(
            staging,
            tmp_path / "published",
            review_path,
        )
