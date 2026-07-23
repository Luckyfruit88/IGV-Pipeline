from __future__ import annotations

import csv
import ctypes
import errno
import fcntl
import json
import os
import re
import shutil
import tempfile
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .contracts import validate_case_result_document, validate_review_document
from .review_records import REVIEW_BINDING_FIELDS
from .utils import atomic_write_json, atomic_write_text, read_jsonl, sha256_file, sha256_json, utc_now, write_jsonl, write_tsv


FINAL_STATUS_FIELDS = [
    "manifest_order",
    "task_id",
    "render_state",
    "evidence_state",
    "artifact_review_state",
    "scientific_interpretation",
    "publication_state",
    "failure_codes",
]


def _json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON value is not an object: {path}")
    return value


def verify_checksum_tree(root_value: str | Path) -> dict[str, str]:
    unresolved = Path(root_value).expanduser()
    if unresolved.is_symlink():
        raise ValueError(f"checksum root must not be a symlink: {unresolved}")
    root = unresolved.resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"checksum root is not a directory: {root}")
    manifest = root / "SHA256SUMS"
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError(f"checksum manifest is missing or symlinked: {manifest}")
    recorded: dict[str, str] = {}
    for line_number, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
        try:
            digest, relative_text = line.split("  ", 1)
        except ValueError as exc:
            raise ValueError(f"malformed SHA256SUMS line {line_number}") from exc
        relative = Path(relative_text)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise ValueError(f"unsafe checksum path: {relative_text}")
        if relative_text in recorded:
            raise ValueError(f"duplicate checksum path: {relative_text}")
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"checksummed artifact is missing or symlinked: {path}")
        if sha256_file(path) != digest:
            raise ValueError(f"checksummed artifact drift: {path}")
        recorded[relative_text] = digest
    actual = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != "SHA256SUMS"
    }
    if any(path.is_symlink() for path in root.rglob("*")):
        raise ValueError("checksum tree contains a symlink")
    if set(recorded) != actual:
        missing = sorted(actual - set(recorded))
        unexpected = sorted(set(recorded) - actual)
        raise ValueError(
            f"checksum file set mismatch; missing={missing[:10]} unexpected={unexpected[:10]}"
        )
    return recorded


def _tsv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _unique(rows: list[dict[str, Any]], field: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row[field])
        if key in result:
            raise ValueError(f"duplicate {field}: {key}")
        result[key] = row
    return result


def _write_checksums(root: Path) -> Path:
    files = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS"),
        key=lambda path: str(path.relative_to(root)),
    )
    target = root / "SHA256SUMS"
    atomic_write_text(
        target,
        "\n".join(f"{sha256_file(path)}  {path.relative_to(root)}" for path in files)
        + "\n",
    )
    return target


def assert_public_tree_safe(root: Path) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"publication staging contains a symlink: {path}")
        if not path.is_file() or path.suffix.lower() == ".png":
            continue
        try:
            text = path.read_text(encoding="utf-8").lower()
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"publication contains an unexpected non-text artifact: {path}"
            ) from exc
        if re.search(r"(?:^|[^a-z0-9_])[a-z0-9._-]+\.ba[mi](?:$|[^a-z0-9_])", text):
            raise ValueError(f"publication exposes a BAM/BAI token: {path}")
        if re.search(
            r"(?:^|[\s\"'=:\[])/(?:restricted|users|private|tmp|var|home|input|reference|run|work)(?:/|\b)",
            text,
        ) or re.search(r"(?:^|[\s\"'=:\[])[a-z]:\\", text):
            raise ValueError(f"publication exposes an absolute source path: {path}")


# Backward-compatible internal name used by the v2 publisher.
_assert_public_tree_safe = assert_public_tree_safe


def atomic_rename_noreplace(source: Path, destination: Path) -> None:
    """Atomically rename one directory without ever replacing the destination."""

    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    if sys.platform.startswith("linux"):
        rename = getattr(libc, "renameat2", None)
        if rename is None:
            raise RuntimeError("production publication requires renameat2(RENAME_NOREPLACE)")
        rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(-100, source_bytes, -100, destination_bytes, 1)
    elif sys.platform == "darwin":
        rename = getattr(libc, "renamex_np", None)
        if rename is None:
            raise RuntimeError("publication test platform lacks renamex_np(RENAME_EXCL)")
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(source_bytes, destination_bytes, 0x00000004)
    else:
        raise RuntimeError(
            "atomic no-replace publication is unsupported on this platform"
        )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(
            error, "publication destination already exists", str(destination)
        )
    raise OSError(error, os.strerror(error), str(destination))


def publish_reviewed(
    review_package_dir: str | Path,
    validated_review_dir: str | Path,
    destination_dir: str | Path,
) -> dict[str, Any]:
    """Publish reviewed evidence through a verified same-filesystem atomic rename."""

    package_root = Path(review_package_dir).expanduser().resolve(strict=True)
    validation_root = Path(validated_review_dir).expanduser().resolve(strict=True)
    verify_checksum_tree(package_root)
    required_validation = (
        validation_root / "accepted_reviews.jsonl",
        validation_root / "reviewed_case_results.jsonl",
        validation_root / "review_validation.json",
    )
    if any(path.is_symlink() or not path.is_file() for path in required_validation):
        raise ValueError("validated review bundle is incomplete or symlinked")
    validation = _json_object(validation_root / "review_validation.json")
    if validation.get("status") != "PASS":
        raise ValueError("review validation did not pass")
    contract_path = package_root / "review_contract.jsonl"
    if validation.get("review_contract_sha256") != sha256_file(contract_path):
        raise ValueError("validated review contract differs from the review package")

    contracts = list(read_jsonl(contract_path))
    contract_map = _unique(contracts, "task_id")
    reviews = list(read_jsonl(validation_root / "accepted_reviews.jsonl"))
    for review in reviews:
        validate_review_document(review)
    review_map = _unique(reviews, "task_id")
    if set(review_map) != set(contract_map):
        raise ValueError("accepted review set differs from review package contract")
    results = list(read_jsonl(validation_root / "reviewed_case_results.jsonl"))
    for result in results:
        validate_case_result_document(result)
    result_map = _unique(results, "task_id")
    if not set(contract_map).issubset(result_map):
        raise ValueError("reviewed result set omits an eligible task")
    for task_id, review in review_map.items():
        contract = contract_map[task_id]
        mismatched = [
            field for field in REVIEW_BINDING_FIELDS if review.get(field) != contract.get(field)
        ]
        if mismatched:
            raise ValueError(
                f"accepted review binding drift for {task_id}: {', '.join(mismatched)}"
            )
        result = result_map[task_id]
        expected_publication = (
            "READY" if review["artifact_review_state"] == "APPROVE" else "WITHHELD"
        )
        if (
            result["artifact_review_state"] != review["artifact_review_state"]
            or result["scientific_interpretation"] != review["scientific_interpretation"]
            or result["publication_state"] != expected_publication
        ):
            raise ValueError(f"reviewed case result differs from accepted review: {task_id}")

    manifest_rows = _tsv_rows(package_root / "review_manifest.tsv")
    manifest_map = _unique(manifest_rows, "task_id")
    if set(manifest_map) != set(contract_map):
        raise ValueError("review manifest differs from review contract")
    destination = Path(destination_dir).expanduser().resolve(strict=False)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"publication destination already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError(f"publication parent must be a regular directory: {destination.parent}")
    staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent))
    if staging.stat().st_dev != destination.parent.stat().st_dev:
        shutil.rmtree(staging, ignore_errors=True)
        raise ValueError("publication staging and destination are not on the same filesystem")
    try:
        published_count = 0
        final_rows: list[dict[str, Any]] = []
        public_results: list[dict[str, Any]] = []
        for result in sorted(results, key=lambda row: int(row["manifest_order"])):
            task_id = result["task_id"]
            public_state = result["publication_state"]
            if public_state == "READY":
                row = manifest_map[task_id]
                contract = contract_map[task_id]
                for field, hash_field in (
                    ("combined_relative_path", "combined_sha256"),
                    ("sample_table_relative_path", "sample_table_sha256"),
                    ("metadata_relative_path", None),
                ):
                    relative = Path(row[field])
                    if relative.is_absolute() or ".." in relative.parts:
                        raise ValueError(f"unsafe reviewed artifact path: {relative}")
                    source = package_root / relative
                    if source.is_symlink() or not source.is_file():
                        raise ValueError(f"reviewed artifact is unavailable: {source}")
                    if hash_field and sha256_file(source) != row[hash_field]:
                        raise ValueError(f"reviewed artifact hash drift: {source}")
                    if field == "combined_relative_path" and row[hash_field] != contract[hash_field]:
                        raise ValueError(f"review package contract hash drift: {task_id}")
                    target = staging / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
                public_state = "PUBLISHED"
                published_count += 1
            final_rows.append(
                {
                    "manifest_order": result["manifest_order"],
                    "task_id": task_id,
                    "render_state": result["render_state"],
                    "evidence_state": result["evidence_state"],
                    "artifact_review_state": result["artifact_review_state"],
                    "scientific_interpretation": result["scientific_interpretation"],
                    "publication_state": public_state,
                    "failure_codes": ",".join(
                        failure["code"] for failure in result["failures"]
                    ),
                }
            )
            public_results.append(
                {
                    "schema_version": "2.0-public-case-status",
                    "run_id": result["run_id"],
                    "generation_id": result["generation_id"],
                    "task_id": task_id,
                    "manifest_order": result["manifest_order"],
                    "render_state": result["render_state"],
                    "evidence_state": result["evidence_state"],
                    "artifact_review_state": result["artifact_review_state"],
                    "scientific_interpretation": result["scientific_interpretation"],
                    "publication_state": public_state,
                }
            )

        write_tsv(staging / "final_status.tsv", FINAL_STATUS_FIELDS, final_rows)
        write_jsonl(staging / "public_case_status.jsonl", public_results)
        write_jsonl(
            staging / "review_records.jsonl",
            sorted(reviews, key=lambda row: int(row["manifest_order"])),
        )
        publication = {
            "schema_version": "2.0-publication",
            "created_at": utc_now(),
            "run_id": results[0]["run_id"] if results else None,
            "generation_id": results[0]["generation_id"] if results else None,
            "result_root": ".",
            "terminal_case_count": len(results),
            "reviewed_case_count": len(reviews),
            "published_case_count": published_count,
            "withheld_case_count": sum(
                row["publication_state"] == "WITHHELD" for row in final_rows
            ),
            "not_ready_case_count": sum(
                row["publication_state"] == "NOT_READY" for row in final_rows
            ),
            "meaning": "PUBLISHED means reviewed evidence delivery, not biological support",
        }
        atomic_write_json(staging / "publication.json", publication)
        _assert_public_tree_safe(staging)
        checksums = _write_checksums(staging)
        verify_checksum_tree(staging)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **publication,
        "output_dir": str(destination),
        "checksums_sha256": sha256_file(destination / checksums.name),
    }


def _tree_identity(root: Path) -> str:
    rows = [
        {
            "relative_path": str(path.relative_to(root)),
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(
            (candidate for candidate in root.rglob("*") if candidate.is_file()),
            key=lambda candidate: str(candidate.relative_to(root)),
        )
    ]
    from .utils import sha256_json

    return sha256_json(rows)


def _receipt_object(
    value: Mapping[str, Any] | str | Path, *, label: str
) -> tuple[dict[str, Any], str, Path]:
    if isinstance(value, Mapping):
        raise ValueError(f"{label} must be supplied as a frozen receipt file, not an in-memory object")
    path = Path(value).expanduser()
    if path.is_symlink() or not path.resolve(strict=True).is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    path = path.resolve(strict=True)
    return _json_object(path), sha256_file(path), path


def _verify_frozen_review_receipt(
    path: Path, receipt: Mapping[str, Any], digest: str
) -> dict[str, Any]:
    if path.name != "review_receipt.json":
        raise ValueError("review receipt must be the immutable generation review_receipt.json")
    generation_root = path.parent
    if generation_root.is_symlink() or generation_root.name != receipt.get("review_generation_id"):
        raise ValueError("review receipt is not inside its named immutable generation")
    if generation_root.parent.name != "generations":
        raise ValueError("review receipt is outside a review/generations tree")
    # Delayed import avoids the review_server -> publication checksum-helper
    # import cycle while keeping one authoritative v3 generation verifier.
    from .review_server import verify_finalized_review_generation

    verified = verify_finalized_review_generation(generation_root, receipt)
    if verified["receipt_sha256"] != digest:
        raise ValueError("review receipt digest differs from frozen generation")
    review_root = generation_root.parent.parent
    pointer_path = review_root / "finalized_review.json"
    if pointer_path.is_symlink() or not pointer_path.is_file():
        raise ValueError("review receipt lacks its finalized review pointer")
    pointer = _json_object(pointer_path)
    expected_relative = str(path.relative_to(review_root))
    if (
        pointer.get("schema_version") != "3.0-finalized-review-pointer"
        or pointer.get("review_generation_id") != receipt.get("review_generation_id")
        or pointer.get("receipt_relative_path") != expected_relative
        or pointer.get("receipt_sha256") != digest
        or pointer.get("checksums_sha256") != verified["checksums_sha256"]
    ):
        raise ValueError("review receipt differs from its finalized review pointer")
    return verified


def _runtime_fingerprint(runtime_binding: Mapping[str, Any]) -> str:
    value = str(runtime_binding.get("runtime_fingerprint_sha256", ""))
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("frozen runtime binding lacks runtime_fingerprint_sha256")
    manifest_sha = runtime_binding.get("runtime_manifest_sha256")
    if manifest_sha is not None and not re.fullmatch(r"[a-f0-9]{64}", str(manifest_sha)):
        raise ValueError("frozen runtime binding has an invalid runtime_manifest_sha256")
    return value


def _optional_publication_binding(source: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in (
        "runtime_manifest_sha256",
        "runtime_oci_digest",
        "batch_purpose",
        "master_task_set_sha256",
        "batch_task_set_sha256",
    ):
        if field in source:
            result[field] = source[field]
    return result


def _verify_staged_review_coverage(
    staging_root: Path,
    review: Mapping[str, Any],
    verified_generation: Mapping[str, Any],
) -> None:
    records = verified_generation.get("records")
    if not isinstance(records, list):
        raise ValueError("frozen review generation lacks review records")
    status_path = staging_root / "public_case_status.jsonl"
    if status_path.is_symlink() or not status_path.is_file():
        raise ValueError("publication staging lacks public case status records")
    status_rows = list(read_jsonl(status_path))
    record_map = _unique([dict(row) for row in records], "task_id")
    status_map = _unique(status_rows, "task_id")
    if (
        set(status_map) != set(record_map)
        or len(record_map) != int(review.get("decision_count", -1))
    ):
        raise ValueError("publication staging task set differs from finalized review")
    for task_id, record in record_map.items():
        status = status_map[task_id]
        decision = record.get("artifact_review_state")
        expected_state = "PUBLISHED" if decision == "APPROVE" else "WITHHELD"
        if (
            status.get("manifest_order") != record.get("manifest_order")
            or status.get("artifact_review_state") != decision
            or status.get("scientific_interpretation")
            != record.get("scientific_interpretation")
            or status.get("publication_state") != expected_state
        ):
            raise ValueError(
                f"publication status differs from finalized review: {task_id}"
            )
        case_root = staging_root / "cases" / task_id
        if decision == "APPROVE":
            image = case_root / "review.png"
            metadata_path = case_root / "metadata.json"
            expected_relative = str(image.relative_to(staging_root))
            expected_sha = str(record.get("review_image_sha256", ""))
            if (
                status.get("review_image") != expected_relative
                or status.get("review_image_sha256") != expected_sha
                or image.is_symlink()
                or not image.is_file()
                or sha256_file(image) != expected_sha
                or metadata_path.is_symlink()
                or not metadata_path.is_file()
            ):
                raise ValueError(
                    f"published case artifact differs from finalized review: {task_id}"
                )
            metadata = _json_object(metadata_path)
            if (
                metadata.get("task_id") != task_id
                or metadata.get("manifest_order") != record.get("manifest_order")
                or metadata.get("artifact_review_state") != "APPROVE"
                or metadata.get("publication_state") != "PUBLISHED"
                or metadata.get("review_image") != expected_relative
                or metadata.get("review_image_sha256") != expected_sha
            ):
                raise ValueError(
                    f"published case metadata differs from finalized review: {task_id}"
                )
        elif decision == "REJECT":
            if case_root.exists() or status.get("review_image") or status.get(
                "review_image_sha256"
            ):
                raise ValueError(
                    f"withheld case exposes a publication artifact: {task_id}"
                )
        else:
            raise ValueError(f"unexpected finalized review decision: {task_id}")


def build_publication_promotion_receipt(
    staging: str | Path,
    destination: str | Path,
    review_receipt: Mapping[str, Any] | str | Path,
    *,
    output: str | Path | None = None,
) -> dict[str, Any]:
    """Authorize one exact staging tree for one exact launcher destination.

    This is intentionally a launcher operation.  The receipt must live outside
    the staging tree so writing it cannot invalidate the tree identity it binds.
    """

    staging_value = Path(staging).expanduser()
    if staging_value.is_symlink():
        raise ValueError(f"publication staging must not be a symlink: {staging_value}")
    staging_root = staging_value.resolve(strict=True)
    if not staging_root.is_dir():
        raise ValueError(f"publication staging must be a directory: {staging_root}")
    verify_checksum_tree(staging_root)
    review, review_sha, review_path = _receipt_object(review_receipt, label="review receipt")
    if (
        review.get("schema_version") != "3.0-review-receipt"
        or review.get("status") != "FINALIZED"
        or review.get("publication_gate") != "READY_FOR_STAGING"
        or review.get("all_eligible_decided") is not True
    ):
        raise ValueError("review receipt is not a finalized v3 publication gate")
    verified_generation = _verify_frozen_review_receipt(
        review_path, review, review_sha
    )
    runtime_binding = review.get("runtime_binding")
    if (
        not isinstance(runtime_binding, Mapping)
        or review.get("runtime_binding_sha256") != sha256_json(dict(runtime_binding))
    ):
        raise ValueError("publication promotion requires a frozen runtime binding")
    runtime_fingerprint = _runtime_fingerprint(runtime_binding)
    publication_path = staging_root / "publication.json"
    publication = _json_object(publication_path)
    _verify_staged_review_coverage(staging_root, review, verified_generation)
    expected_publication = {
        "schema_version": "3.0-publication-staging",
        "status": "STAGED",
        "run_id": review.get("run_id"),
        "generation_id": review.get("generation_id"),
        "review_generation_id": review.get("review_generation_id"),
        "review_receipt_sha256": review_sha,
        "contract_set_sha256": review.get("contract_set_sha256"),
        "journal_sha256": review.get("journal_sha256"),
        "terminal_case_count": int(review.get("decision_count", 0)),
        "runtime_binding_sha256": review.get("runtime_binding_sha256"),
        "runtime_fingerprint_sha256": runtime_fingerprint,
        "published_case_count": int(review.get("approved_count", 0)),
        "withheld_case_count": int(review.get("rejected_count", 0)),
        **_optional_publication_binding(review),
        **_optional_publication_binding(runtime_binding),
    }
    mismatched = [
        field
        for field, expected in expected_publication.items()
        if publication.get(field) != expected
    ]
    if mismatched:
        raise ValueError(
            "publication staging metadata differs from finalized review: "
            + ", ".join(mismatched)
        )
    destination_value = Path(destination).expanduser()
    if destination_value.is_symlink():
        raise ValueError(f"publication destination must not be a symlink: {destination_value}")
    destination_root = destination_value.resolve(strict=False)
    try:
        destination_root.relative_to(staging_root)
    except ValueError:
        pass
    else:
        raise ValueError("publication destination must be outside the staging tree")
    receipt = {
        "schema_version": "3.0-publication-staging-receipt",
        "created_at": utc_now(),
        "status": "READY_FOR_PROMOTION",
        "authorized_destination": str(destination_root),
        "staging_tree_sha256": _tree_identity(staging_root),
        "checksums_sha256": sha256_file(staging_root / "SHA256SUMS"),
        "review_receipt_sha256": review_sha,
        "review_receipt_path": str(review_path),
        "contract_set_sha256": review.get("contract_set_sha256"),
        "journal_sha256": review.get("journal_sha256"),
        "runtime_binding_sha256": review.get("runtime_binding_sha256"),
        "runtime_fingerprint_sha256": runtime_fingerprint,
        "review_generation_id": review.get("review_generation_id"),
        "run_id": review.get("run_id"),
        "generation_id": review.get("generation_id"),
        "approved_count": int(review.get("approved_count", 0)),
        "rejected_count": int(review.get("rejected_count", 0)),
        **_optional_publication_binding(review),
        **_optional_publication_binding(runtime_binding),
    }
    if output is not None:
        output_value = Path(output).expanduser()
        if output_value.is_symlink():
            raise ValueError(f"promotion receipt output must not be a symlink: {output_value}")
        output_path = output_value.resolve(strict=False)
        try:
            output_path.relative_to(staging_root)
        except ValueError:
            pass
        else:
            raise ValueError("promotion receipt must be written outside the staging tree")
        if output_path.exists() or output_path.is_symlink():
            raise FileExistsError(f"promotion receipt already exists: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(
            output_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            payload = json.dumps(receipt, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
            os.write(descriptor, payload.encode("utf-8"))
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return receipt


def promote_publication(
    staging: str | Path,
    destination: str | Path,
    receipt: Mapping[str, Any] | str | Path,
) -> dict[str, Any]:
    """Validate and atomically promote a pre-built publication staging tree.

    No files are copied or generated here.  The exact checksum tree, finalized
    review generation, and authorized destination were frozen in a separate
    promotion receipt.  Existing destinations always fail, preventing duplicate
    publication through this launcher helper.
    """

    promotion, promotion_sha, _promotion_path = _receipt_object(
        receipt, label="promotion receipt"
    )
    if (
        promotion.get("schema_version") != "3.0-publication-staging-receipt"
        or promotion.get("status") != "READY_FOR_PROMOTION"
    ):
        raise ValueError("receipt is not a ready v3 publication staging receipt")
    review_path = Path(str(promotion.get("review_receipt_path", ""))).expanduser()
    review, review_sha, review_path = _receipt_object(review_path, label="review receipt")
    _verify_frozen_review_receipt(review_path, review, review_sha)
    runtime_binding = review.get("runtime_binding")
    if (
        not isinstance(runtime_binding, Mapping)
        or review.get("runtime_binding_sha256") != sha256_json(dict(runtime_binding))
    ):
        raise ValueError("promotion receipt lacks a frozen runtime binding")
    runtime_fingerprint = _runtime_fingerprint(runtime_binding)
    if (
        promotion.get("review_receipt_sha256") != review_sha
        or promotion.get("review_generation_id") != review.get("review_generation_id")
        or promotion.get("contract_set_sha256") != review.get("contract_set_sha256")
        or promotion.get("journal_sha256") != review.get("journal_sha256")
        or promotion.get("runtime_binding_sha256") != review.get("runtime_binding_sha256")
        or promotion.get("runtime_fingerprint_sha256") != runtime_fingerprint
        or any(
            promotion.get(field) != value
            for field, value in {
                **_optional_publication_binding(review),
                **_optional_publication_binding(runtime_binding),
            }.items()
        )
    ):
        raise ValueError("promotion receipt differs from the frozen review generation")
    destination_value = Path(destination).expanduser()
    if destination_value.is_symlink():
        raise ValueError(f"publication destination must not be a symlink: {destination_value}")
    destination_root = destination_value.resolve(strict=False)
    if promotion.get("authorized_destination") != str(destination_root):
        raise ValueError("promotion receipt does not authorize this destination")
    completion_path = (
        destination_root.parent / f".{destination_root.name}.publication-completion.json"
    )
    expected_completion = {
        "schema_version": "3.0-publication-completion-receipt",
        "status": "ATOMIC_RENAME_COMMIT_RECORD",
        "authorized_destination": str(destination_root),
        "promotion_receipt_sha256": promotion_sha,
        "staging_tree_sha256": promotion.get("staging_tree_sha256"),
        "checksums_sha256": promotion.get("checksums_sha256"),
        "review_receipt_sha256": promotion.get("review_receipt_sha256"),
        "review_generation_id": promotion.get("review_generation_id"),
        "runtime_binding_sha256": promotion.get("runtime_binding_sha256"),
        "runtime_fingerprint_sha256": promotion.get(
            "runtime_fingerprint_sha256"
        ),
        **_optional_publication_binding(promotion),
    }

    def verified_completion() -> dict[str, Any]:
        if completion_path.is_symlink() or not completion_path.is_file():
            raise ValueError(
                "existing publication destination lacks its sidecar completion receipt"
            )
        observed = _json_object(completion_path)
        if observed != expected_completion:
            raise ValueError("publication sidecar completion receipt differs from authorization")
        return observed

    staging_value = Path(staging).expanduser()
    if staging_value.is_symlink():
        raise ValueError(f"publication staging must not be a symlink: {staging_value}")
    if not staging_value.exists():
        if destination_root.is_dir() and not destination_root.is_symlink():
            verified_completion()
            verify_checksum_tree(destination_root)
            if (
                promotion.get("checksums_sha256")
                != sha256_file(destination_root / "SHA256SUMS")
                or promotion.get("staging_tree_sha256") != _tree_identity(destination_root)
            ):
                raise ValueError("existing destination is not the receipt-bound promoted tree")
            return {
                "schema_version": "3.0-publication-promotion-receipt",
                "promoted_at": utc_now(),
                "status": "PUBLISHED",
                "destination": str(destination_root),
                "promotion_receipt_sha256": promotion_sha,
                "staging_tree_sha256": promotion["staging_tree_sha256"],
                "review_generation_id": promotion.get("review_generation_id"),
                "run_id": promotion.get("run_id"),
                "generation_id": promotion.get("generation_id"),
                "approved_count": promotion.get("approved_count", 0),
                "rejected_count": promotion.get("rejected_count", 0),
                "recovered_after_atomic_rename": True,
                "completion_receipt": str(completion_path),
                "completion_receipt_sha256": sha256_file(completion_path),
                "runtime_fingerprint_sha256": runtime_fingerprint,
            }
        raise FileNotFoundError(f"publication staging is unavailable: {staging_value}")
    staging_root = staging_value.resolve(strict=True)
    if not staging_root.is_dir():
        raise ValueError(f"publication staging must be a directory: {staging_root}")
    try:
        destination_root.relative_to(staging_root)
    except ValueError:
        pass
    else:
        raise ValueError("publication destination must be outside the staging tree")
    verify_checksum_tree(staging_root)
    if promotion.get("checksums_sha256") != sha256_file(staging_root / "SHA256SUMS"):
        raise ValueError("publication staging checksum manifest differs from its receipt")
    if promotion.get("staging_tree_sha256") != _tree_identity(staging_root):
        raise ValueError("publication staging tree differs from its receipt")
    if destination_root.exists() or destination_root.is_symlink():
        raise FileExistsError(f"publication destination already exists: {destination_root}")
    destination_root.parent.mkdir(parents=True, exist_ok=True)
    if destination_root.parent.is_symlink() or not destination_root.parent.is_dir():
        raise ValueError(f"publication parent must be a regular directory: {destination_root.parent}")
    if staging_root.stat().st_dev != destination_root.parent.stat().st_dev:
        raise ValueError("publication staging and destination are not on the same filesystem")
    lock_path = destination_root.parent / f".{destination_root.name}.promotion.lock"
    if lock_path.is_symlink():
        raise ValueError(f"publication promotion lock must not be a symlink: {lock_path}")
    lock_descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
        if destination_root.exists() or destination_root.is_symlink():
            raise FileExistsError(f"publication destination already exists: {destination_root}")
        if completion_path.is_symlink():
            raise ValueError(
                f"publication completion receipt must not be a symlink: {completion_path}"
            )
        if completion_path.exists():
            verified_completion()
        else:
            completion_descriptor = os.open(
                completion_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                os.write(
                    completion_descriptor,
                    (
                        json.dumps(
                            expected_completion,
                            indent=2,
                            sort_keys=True,
                            ensure_ascii=False,
                        )
                        + "\n"
                    ).encode("utf-8"),
                )
                os.fsync(completion_descriptor)
            finally:
                os.close(completion_descriptor)
        atomic_rename_noreplace(staging_root, destination_root)
        verify_checksum_tree(destination_root)
    finally:
        fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
        os.close(lock_descriptor)
    return {
        "schema_version": "3.0-publication-promotion-receipt",
        "promoted_at": utc_now(),
        "status": "PUBLISHED",
        "destination": str(destination_root),
        "promotion_receipt_sha256": promotion_sha,
        "staging_tree_sha256": promotion["staging_tree_sha256"],
        "review_generation_id": promotion.get("review_generation_id"),
        "run_id": promotion.get("run_id"),
        "generation_id": promotion.get("generation_id"),
        "approved_count": promotion.get("approved_count", 0),
        "rejected_count": promotion.get("rejected_count", 0),
        "completion_receipt": str(completion_path),
        "completion_receipt_sha256": sha256_file(completion_path),
        "runtime_fingerprint_sha256": runtime_fingerprint,
    }
