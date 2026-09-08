from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssqtl_igv.contracts import V3_GENERIC_MANUAL_ASSERTIONS
from ssqtl_igv.identity import task_set_fingerprint
from ssqtl_igv.public_rerun_v3 import (
    reconcile_failed_rerun,
    validate_live_project_binding,
)
from ssqtl_igv.rerun_v3 import freeze_case_failure_rerun
from ssqtl_igv.utils import read_jsonl, sha256_file, sha256_json, write_jsonl

FAILURE_HEADER = (
    "manifest_order\ttask_id\tchromosome\tfailure_code\tmessage\tinput_fingerprint\n"
)
SNAPSHOT_HEADER = "manifest_order\ttask_id\tchromosome\trelative_path\tsha256\tstatus\n"


def _write_case_evidence(
    root: Path,
    task: dict,
    *,
    eligible: bool,
    review_image: Path | None = None,
) -> None:
    task_id = task["task_id"]
    case_root = root / ".igv-pipeline" / "cases" / task_id
    case_root.mkdir(parents=True)
    artifacts: dict[str, dict[str, object]] = {}
    if eligible:
        assert review_image is not None
        scientific_qc = case_root / "scientific_qc.json"
        scientific_qc.write_text("{}\n", encoding="utf-8")
        artifacts = {
            "review_image": {
                "relative_path": f"results/cases/{task_id}/review.png",
                "sha256": sha256_file(review_image),
                "size": review_image.stat().st_size,
            },
            "scientific_qc": {
                "relative_path": f"results/cases/{task_id}/scientific_qc.json",
                "sha256": sha256_file(scientific_qc),
                "size": scientific_qc.stat().st_size,
            },
        }
        for role in (
            "raw_igv",
            "capture_metadata",
            "layout",
            "raw_qc",
            "review_qc",
            "track_display_contract",
            "igv_session",
        ):
            artifact = case_root / f"{role}.json"
            artifact.write_text(json.dumps({"role": role}) + "\n", encoding="utf-8")
            artifacts[role] = {
                "relative_path": f"results/cases/{task_id}/{role}.json",
                "sha256": sha256_file(artifact),
                "size": artifact.stat().st_size,
            }
    result = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task_id,
        "manifest_order": task["manifest_order"],
        "input_fingerprint": task["input_fingerprint"],
        "adapter_type": "generic",
        "adapter_evidence": {
            "adapter_schema_version": "3.0-generic",
            "scientific_interpretation": "NOT_APPLICABLE",
        },
        "eligible": eligible,
        "render_state": "SUCCEEDED" if eligible else "FAILED",
        "evidence_state": "COMPLETE" if eligible else "UNAVAILABLE",
        "artifact_review_state": "REVIEW_PENDING",
        "scientific_interpretation": "NOT_APPLICABLE",
        "publication_state": "NOT_READY",
        "debug_only": False,
        "required_manual_assertions": list(V3_GENERIC_MANUAL_ASSERTIONS),
        "artifacts": artifacts,
        "pixel_identity": (
            {
                "source_igv_decoded_pixel_sha256": "d" * 64,
                "final_igv_decoded_pixel_sha256": "d" * 64,
                "igv_pixel_identity": True,
            }
            if eligible
            else None
        ),
        "failures": (
            [] if eligible else [{"code": "CASE_RENDER_FAILED", "message": "fixture"}]
        ),
        "created_at": "2026-07-28T00:00:00Z",
    }
    result_path = case_root / "case_result.json"
    result_path.write_text(json.dumps(result) + "\n", encoding="utf-8")
    bundle = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task_id,
        "manifest_order": task["manifest_order"],
        "input_fingerprint": task["input_fingerprint"],
        "status": "SUCCEEDED" if eligible else "DOMAIN_FAILED",
        "case_result_sha256": sha256_file(result_path),
        "case_result_size": result_path.stat().st_size,
        "artifact_set_sha256": sha256_json(artifacts),
    }
    (case_root / "terminal_bundle.json").write_text(
        json.dumps(bundle) + "\n", encoding="utf-8"
    )


def _source_product(root: Path) -> tuple[dict, Path]:
    contract = root / ".igv-pipeline" / "contract"
    contract.mkdir(parents=True)
    task = {
        "run_id": "run-1",
        "generation_id": "generation-001",
        "task_id": "case_1",
        "manifest_order": 7,
        "input_fingerprint": "a" * 64,
    }
    write_jsonl(contract / "tasks.jsonl", [task])
    (contract / "run_identity.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "generation_id": "generation-001",
                "canonical_tasks_sha256": sha256_file(contract / "tasks.jsonl"),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "snapshots").mkdir()
    (root / "snapshots.tsv").write_text(
        SNAPSHOT_HEADER + "7\tcase_1\tchr11\t\t\tCASE_FAILED\n",
        encoding="utf-8",
    )
    (root / "failed_cases.tsv").write_text(
        FAILURE_HEADER + f"7\tcase_1\tchr11\tCASE_RENDER_FAILED\tfixture\t{'a' * 64}\n",
        encoding="utf-8",
    )
    (root / "run_summary.json").write_text(
        '{"authoritative":false,"status":"CASE_FAILURES","exit_code":2}\n',
        encoding="utf-8",
    )
    _write_case_evidence(root, task, eligible=False)
    failed = {
        **task,
        "eligible": False,
        "failures": [
            {
                "code": "CASE_RENDER_FAILED",
                "message": "fixture",
                "rerun_eligible": True,
            }
        ],
    }
    pointer = freeze_case_failure_rerun(root, [task], [failed])
    assert pointer is not None
    receipt = root / pointer["relative_path"] / "rerun_receipt.json"
    return task, receipt


def _add_campaign_source_contract(root: Path, task: dict) -> None:
    contract = root / ".igv-pipeline" / "contract"
    tasks_path = contract / "tasks.jsonl"
    tasks = list(read_jsonl(tasks_path))
    tasks_sha = sha256_file(tasks_path)
    tasks_set_sha = task_set_fingerprint(tasks)
    request = {
        "schema_version": "3.0-batch-request",
        "campaign_id": task["run_id"],
        "batch_id": task["generation_id"],
        "purpose": "PILOT_QA",
        "execution_run_id": task["run_id"],
        "execution_generation_id": task["generation_id"],
        "task_count": 1,
        "tasks_sha256": tasks_sha,
        "task_set_sha256": tasks_set_sha,
        "source_tasks": [
            {
                "task_id": task["task_id"],
                "batch_manifest_order": 1,
                "batch_input_fingerprint": task["input_fingerprint"],
            }
        ],
    }
    request["request_sha256"] = sha256_json(request)
    request_path = contract / "batch-request.json"
    request_path.write_text(json.dumps(request) + "\n", encoding="utf-8")
    binding = {
        "schema_version": "3.0-batch-admission",
        "campaign_id": request["campaign_id"],
        "batch_id": request["batch_id"],
        "purpose": request["purpose"],
        "batch_request_sha256": sha256_file(request_path),
        "task_count": 1,
        "tasks_sha256": tasks_sha,
        "task_set_sha256": tasks_set_sha,
    }
    binding_path = contract / "campaign_binding.json"
    binding_path.write_text(json.dumps(binding) + "\n", encoding="utf-8")
    identity = json.loads(
        (contract / "run_identity.json").read_text(encoding="utf-8")
    )
    identity.update(
        adapter="ssqtl",
        canonical_task_set_sha256=tasks_set_sha,
        batch_request_sha256=sha256_file(request_path),
        campaign_binding_sha256=sha256_file(binding_path),
    )
    (contract / "run_identity.json").write_text(
        json.dumps(identity) + "\n", encoding="utf-8"
    )


def test_campaign_source_authorizes_without_live_project_binding(tmp_path: Path) -> None:
    product = tmp_path / "output"
    task, _receipt = _source_product(product)
    _add_campaign_source_contract(product, task)

    validate_live_project_binding(product, tmp_path / "not-used.yaml")


def test_campaign_source_rejects_task_mapping_tamper(tmp_path: Path) -> None:
    product = tmp_path / "output"
    task, _receipt = _source_product(product)
    _add_campaign_source_contract(product, task)
    request_path = product / ".igv-pipeline" / "contract" / "batch-request.json"
    request = json.loads(request_path.read_text(encoding="utf-8"))
    request["source_tasks"][0]["task_id"] = "case_other"
    request["request_sha256"] = sha256_json(
        {key: value for key, value in request.items() if key != "request_sha256"}
    )
    request_path.write_text(json.dumps(request) + "\n", encoding="utf-8")
    identity_path = product / ".igv-pipeline" / "contract" / "run_identity.json"
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    identity["batch_request_sha256"] = sha256_file(request_path)
    identity_path.write_text(json.dumps(identity) + "\n", encoding="utf-8")
    binding_path = product / ".igv-pipeline" / "contract" / "campaign_binding.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding["batch_request_sha256"] = sha256_file(request_path)
    binding_path.write_text(json.dumps(binding) + "\n", encoding="utf-8")
    identity["campaign_binding_sha256"] = sha256_file(binding_path)
    identity_path.write_text(json.dumps(identity) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="source task task_id differs"):
        validate_live_project_binding(product, tmp_path / "not-used.yaml")


def _incoming_product(
    product: Path, source_task: dict, receipt: Path, *, successful: bool
) -> Path:
    generation_id = "rerun-" + "b" * 64
    root = product / ".igv-pipeline" / "rerun" / "executions" / generation_id
    contract = root / ".igv-pipeline" / "contract"
    contract.mkdir(parents=True)
    task = {
        **source_task,
        "generation_id": generation_id,
        "manifest_order": 1,
        "input_fingerprint": "c" * 64,
    }
    write_jsonl(contract / "tasks.jsonl", [task])
    source_receipt = json.loads(receipt.read_text(encoding="utf-8"))
    binding = {
        "schema_version": "3.0-rerun-binding",
        "source_run_id": source_task["run_id"],
        "source_generation_id": source_task["generation_id"],
        "source_canonical_tasks_sha256": source_receipt["canonical_tasks_sha256"],
        "source_rerun_id": source_receipt["rerun_id"],
        "source_rerun_receipt_sha256": sha256_file(receipt),
        "source_rerun_manifest_sha256": source_receipt["rerun_manifest_sha256"],
        "target_run_id": source_task["run_id"],
        "target_generation_id": generation_id,
        "target_tasks_sha256": sha256_file(contract / "tasks.jsonl"),
        "target_task_set_sha256": task_set_fingerprint([task]),
        "same_generation_resume_allowed": False,
    }
    (contract / "rerun_binding.json").write_text(
        json.dumps(binding) + "\n", encoding="utf-8"
    )
    (contract / "run_identity.json").write_text(
        json.dumps(
            {
                "run_id": source_task["run_id"],
                "generation_id": generation_id,
                "rerun_binding_sha256": sha256_file(contract / "rerun_binding.json"),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (root / "snapshots").mkdir(parents=True)
    review_image: Path | None = None
    if successful:
        image = root / "snapshots" / "chr11" / "case_1.png"
        image.parent.mkdir()
        image.write_bytes(b"recovered")
        review_image = image
        (root / "snapshots.tsv").write_text(
            SNAPSHOT_HEADER
            + "1\tcase_1\tchr11\tsnapshots/chr11/case_1.png\t"
            + sha256_file(image)
            + "\tSNAPSHOT_READY\n",
            encoding="utf-8",
        )
        (root / "failed_cases.tsv").write_text(FAILURE_HEADER, encoding="utf-8")
    else:
        (root / "snapshots.tsv").write_text(
            SNAPSHOT_HEADER + "1\tcase_1\tchr11\t\t\tCASE_FAILED\n",
            encoding="utf-8",
        )
        (root / "failed_cases.tsv").write_text(
            FAILURE_HEADER
            + f"1\tcase_1\tchr11\tIGV_TIMEOUT\tretry failed\t{'c' * 64}\n",
            encoding="utf-8",
        )
    (root / "run_summary.json").write_text(
        json.dumps(
            {
                "authoritative": False,
                "status": "SNAPSHOTS_READY" if successful else "CASE_FAILURES",
                "exit_code": 0 if successful else 2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    _write_case_evidence(
        root,
        task,
        eligible=successful,
        review_image=review_image,
    )
    return root


def test_public_rerun_receipt_is_hidden_but_importable(tmp_path: Path) -> None:
    product = tmp_path / "output"
    task, receipt = _source_product(product)

    assert receipt.relative_to(product).parts[:3] == (
        ".igv-pipeline",
        "rerun",
        "generations",
    )
    requests = list(read_jsonl(receipt.parent / "rerun_manifest.jsonl"))
    assert [row["source_task_id"] for row in requests] == [task["task_id"]]


def test_failed_task_can_transition_to_ready_atomically_and_idempotently(
    tmp_path: Path,
) -> None:
    product = tmp_path / "output"
    task, receipt = _source_product(product)
    incoming = _incoming_product(product, task, receipt, successful=True)

    result = reconcile_failed_rerun(
        product,
        source_run=product,
        rerun_receipt=receipt,
        incoming_output=incoming,
    )
    replay = reconcile_failed_rerun(
        product,
        source_run=product,
        rerun_receipt=receipt,
        incoming_output=incoming,
    )

    assert result["status"] == "PUBLISHED"
    assert result["commit_mode"] == "IMMUTABLE_OBJECTS_ATOMIC_CURRENT"
    assert result["recovered_case_count"] == 1
    from ssqtl_igv.public_rerun_v3 import validate_projected_rerun_replacements
    validate_projected_rerun_replacements(product, {"case_1": task["input_fingerprint"]})
    with pytest.raises(ValueError, match="canonical case"):
        validate_projected_rerun_replacements(product, {"case_1": "f" * 64})
    assert replay["status"] == "IDEMPOTENT"
    assert replay["remaining_failed_case_count"] == 0
    snapshot_row = (product / "snapshots.tsv").read_text(encoding="utf-8")
    assert "7\tcase_1\tchr11\tsnapshots/chr11/case_1.png" in snapshot_row
    assert (product / "failed_cases.tsv").read_text(encoding="utf-8") == FAILURE_HEADER
    assert (product / "snapshots/chr11/case_1.png").read_bytes() == b"recovered"
    projected_case_root = product / ".igv-pipeline" / "cases" / "case_1"
    projected_result = json.loads(
        (projected_case_root / "case_result.json").read_text(encoding="utf-8")
    )
    projected_bundle = json.loads(
        (projected_case_root / "terminal_bundle.json").read_text(encoding="utf-8")
    )
    projection = json.loads(
        (projected_case_root / "rerun_projection.json").read_text(encoding="utf-8")
    )
    assert projected_result["eligible"] is True
    assert projected_result["generation_id"] == task["generation_id"]
    assert projected_result["manifest_order"] == task["manifest_order"]
    assert projected_bundle["case_result_sha256"] == sha256_file(
        projected_case_root / "case_result.json"
    )
    assert projection["authoritative"] is False
    assert projection["source_identity"]["generation_id"].startswith("rerun-")
    state = json.loads(
        (product / ".igv-pipeline" / "rerun" / "failed-only-state.json").read_text(
            encoding="utf-8"
        )
    )
    assert state["status"] == "SNAPSHOTS_READY"
    assert len(state["generations"]) == 1


def test_failed_task_migrates_known_legacy_failure_column_order(
    tmp_path: Path,
) -> None:
    product = tmp_path / "output"
    task, receipt = _source_product(product)
    product.joinpath("failed_cases.tsv").write_text(
        "manifest_order\ttask_id\tfailure_code\tmessage\tchromosome\tinput_fingerprint\n"
        + f"7\tcase_1\tCASE_RENDER_FAILED\tfixture\tchr11\t{'a' * 64}\n",
        encoding="utf-8",
    )
    incoming = _incoming_product(product, task, receipt, successful=True)

    result = reconcile_failed_rerun(
        product,
        source_run=product,
        rerun_receipt=receipt,
        incoming_output=incoming,
    )

    assert result["status"] == "PUBLISHED"
    assert product.joinpath("failed_cases.tsv").read_text(encoding="utf-8") == FAILURE_HEADER


def test_failed_rerun_remains_failed_and_points_to_latest_generation(
    tmp_path: Path,
) -> None:
    product = tmp_path / "output"
    task, receipt = _source_product(product)
    incoming = _incoming_product(product, task, receipt, successful=False)

    result = reconcile_failed_rerun(
        product,
        source_run=product,
        rerun_receipt=receipt,
        incoming_output=incoming,
    )

    assert result["exit_code"] == 2
    assert result["recovered_case_count"] == 0
    failure_text = (product / "failed_cases.tsv").read_text(encoding="utf-8")
    assert "IGV_TIMEOUT\tretry failed" in failure_text
    state = json.loads(
        (product / ".igv-pipeline" / "rerun" / "failed-only-state.json").read_text(
            encoding="utf-8"
        )
    )
    assert state["status"] == "CASE_FAILURES"
    assert state["active_source_relative_path"] == str(incoming.relative_to(product))


def test_rerun_refuses_to_overwrite_existing_success(tmp_path: Path) -> None:
    product = tmp_path / "output"
    task, receipt = _source_product(product)
    incoming = _incoming_product(product, task, receipt, successful=True)
    reconcile_failed_rerun(
        product,
        source_run=product,
        rerun_receipt=receipt,
        incoming_output=incoming,
    )
    image = incoming / "snapshots/chr11/case_1.png"
    image.write_bytes(b"different")

    with pytest.raises(ValueError, match="checksum"):
        reconcile_failed_rerun(
            product,
            source_run=product,
            rerun_receipt=receipt,
            incoming_output=incoming,
        )
