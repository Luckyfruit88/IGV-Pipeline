from __future__ import annotations

import copy

import pytest

from ssqtl_igv.contracts import (
    ContractValidationError,
    SCHEMA_FILES,
    load_schema,
    validate_case_result_document,
    validate_review_document,
    validate_schema_document,
    validate_task_document,
    validate_unique_task_set,
    validate_v3_task_document,
)


SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def _identity(sha256: str | None = None) -> dict[str, object]:
    return {"size": 10, "mtime_ns": 100, "sha256": sha256}


def _resource(name: str, sha256: str | None = None) -> dict[str, object]:
    return {
        "source_path": f"/inputs/{name}",
        "stage_name": name,
        "identity": _identity(sha256),
    }


def valid_task(*, task_id: str = "case_001", order: int = 1) -> dict[str, object]:
    return {
        "schema_version": "2.0",
        "pipeline_version": "2.0.0",
        "run_id": "run_001",
        "generation_id": "gen_001",
        "task_id": task_id,
        "manifest_order": order,
        "association": {"row": order, "input_sha256": SHA_A},
        "figure_contract_id": "v031_native_igv_pixel_exact",
        "gui_settle_contract_id": "v031_toolbar_locus_settle_v1",
        "ag": {
            "raw": "chr1:100-101",
            "chrom": "chr1",
            "source_start": 100,
            "source_end": 101,
            "start": 100,
            "end": 101,
        },
        "snp": {
            "raw": "chr1.105_A.G",
            "chrom": "chr1",
            "position": 105,
            "ref": "A",
            "alt": "G",
        },
        "strand": "+",
        "regions": {
            "overview": {"chrom": "chr1", "start": 45, "end": 160},
            "detail": {"chrom": "chr1", "start": 88, "end": 117},
        },
        "statistics": {
            "n_total": 100,
            "n_0": 40,
            "n_1": 40,
            "n_2": 20,
            "eligible_n_0": 1,
            "eligible_n_1": 0,
            "eligible_n_2": 0,
            "beta": 0.2,
            "abs_tvalue": 3.5,
        },
        "genotype_groups": {
            "0/0": {"selected_count": 1, "empty": False},
            "0/1": {"selected_count": 0, "empty": True},
            "1/1": {"selected_count": 0, "empty": True},
        },
        "tracks": [
            {
                "sample_id": "sample-1",
                "genotype": "0/0",
                "dosage": 0,
                "ratio": 0.25,
                "selection_label": "all",
                "bam": "/inputs/sample-1.bam",
                "bai": "/inputs/sample-1.bam.bai",
                "stage_bam": "sample-1.bam",
                "stage_bai": "sample-1.bam.bai",
                "bam_identity": _identity(),
                "bai_identity": _identity(),
                "bai_fresh": True,
            }
        ],
        "plot": {
            "state": "PRESENT",
            "pdf": "/inputs/violin.pdf",
            "stage_pdf": "violin.pdf",
            "page": 1,
            "match_key": {"ag_site": "chr1:100-101", "snp": "chr1.105_A.G"},
            "pdf_identity": _identity(SHA_B),
        },
        "reference": {
            "id": "hg38_MANEv1.5",
            "display_name": "Human GRCh38",
            "annotation_version": "MANE v1.5",
            "resource_fingerprint": SHA_C,
            "resources": {
                "definition": _resource("genome.json", SHA_A),
                "fasta": _resource("genome.fa"),
                "fai": _resource("genome.fa.fai"),
                "cytoband": _resource("cytoBand.txt.gz", SHA_B),
                "annotation": _resource("MANE.gff.gz", SHA_C),
            },
        },
        "reference_context": {
            "available": True,
            "chrom": "chr1",
            "start": 100,
            "end": 101,
            "strand": "+",
            "genomic_sequence": "AG",
            "transcript_sequence": "AG",
            "expected_transcript_sequence": "AG",
            "canonical_ag": True,
        },
        "render_contract": {
            "screen_width": 1920,
            "screen_height": 2160,
            "screen_depth": 24,
            "violin_panel_width": 720,
            "igv_version": "2.16.2",
            "overview_padding": 55,
            "detail_padding": 12,
            "policy_fingerprint": SHA_A,
        },
        "preflight_state": "READY",
        "preflight_warnings": [
            {"code": "EMPTY_GENOTYPE_GROUP", "message": "0/1 and 1/1 are empty"}
        ],
        "preflight_errors": [],
        "shard_hint": "chr1_plus",
        "estimated_runtime_seconds": 90,
        "input_fingerprint": SHA_A,
    }


def valid_case_result() -> dict[str, object]:
    return {
        "schema_version": "2.0",
        "pipeline_version": "2.0.0",
        "run_id": "run_001",
        "generation_id": "gen_001",
        "shard_id": "shard_001",
        "session_id": "session_001",
        "task_id": "case_001",
        "manifest_order": 1,
        "attempt": 1,
        "input_fingerprint": SHA_A,
        "render_state": "SUCCEEDED",
        "evidence_state": "EVIDENCE_INCOMPLETE",
        "artifact_review_state": "REVIEW_PENDING",
        "scientific_interpretation": "PENDING",
        "publication_state": "NOT_READY",
        "empty_genotype_groups": ["0/1", "1/1"],
        "artifacts": [
            {
                "role": "combined_png",
                "relative_path": "artifacts/combined.png",
                "sha256": SHA_B,
                "size": 100,
            }
        ],
        "stage_bundles": {},
        "warnings": [{"code": "EMPTY_GENOTYPE_GROUP", "message": "incomplete groups"}],
        "failures": [],
        "rerun_eligible": False,
        "rerun_reason": None,
        "created_at": "2026-07-21T12:00:00Z",
    }


def valid_review() -> dict[str, object]:
    return {
        "schema_version": "2.0",
        "review_record_id": "review_001",
        "run_id": "run_001",
        "generation_id": "gen_001",
        "task_id": "case_001",
        "manifest_order": 1,
        "input_fingerprint": SHA_A,
        "case_result_sha256": SHA_A,
        "combined_sha256": SHA_B,
        "left_pixel_sha256": SHA_C,
        "scientific_qc_sha256": SHA_A,
        "figure_contract_id": "v031_native_igv_pixel_exact",
        "gui_settle_contract_id": "v031_toolbar_locus_settle_v1",
        "artifact_review_state": "APPROVE",
        "scientific_interpretation": "INDETERMINATE",
        "reviewer": "reviewer-1",
        "reviewed_at": "2026-07-21T12:00:00Z",
        "notes": "Evidence incomplete because genotype groups are empty.",
        "manual_assertions": {
            "native_igv_complete_and_readable": True,
            "annotation_track_and_model_visible": True,
            "strand_and_transcript_reviewed": True,
            "ag_site_and_reference_ag_context_reviewed": True,
            "splice_or_junction_presence_absence_judgeable": True,
            "violin_pair_matches": True,
        },
    }


def test_every_schema_is_draft_2020_12_valid() -> None:
    from jsonschema import Draft202012Validator

    for name in SCHEMA_FILES:
        Draft202012Validator.check_schema(load_schema(name))


def test_task_contract_accepts_empty_groups_without_losing_case() -> None:
    task = valid_task()
    validate_task_document(task)
    assert task["preflight_state"] == "READY"
    assert task["genotype_groups"]["0/1"]["empty"] is True


def test_task_rejects_duplicate_staged_names_and_bad_group_counts() -> None:
    task = valid_task()
    second = copy.deepcopy(task["tracks"][0])
    second["sample_id"] = "sample-2"
    second["stage_bai"] = "sample-2.bai"
    task["tracks"].append(second)
    task["genotype_groups"]["0/0"]["selected_count"] = 2
    with pytest.raises(ContractValidationError, match="staged input name"):
        validate_task_document(task)

    task = valid_task()
    task["genotype_groups"]["0/0"]["selected_count"] = 2
    with pytest.raises(ContractValidationError, match="selected_count"):
        validate_task_document(task)


def test_task_cross_field_scientific_identity_is_consistent() -> None:
    task = valid_task()
    task["snp"]["chrom"] = "chr2"
    with pytest.raises(ContractValidationError, match="chromosomes must match"):
        validate_task_document(task)

    task = valid_task()
    task["genotype_groups"]["0/1"]["empty"] = False
    with pytest.raises(ContractValidationError, match="empty"):
        validate_task_document(task)

    task = valid_task()
    task["tracks"][0]["dosage"] = 1
    with pytest.raises(ContractValidationError, match="dosage"):
        validate_task_document(task)


def test_task_preflight_state_is_fail_closed() -> None:
    task = valid_task()
    task["preflight_state"] = "CASE_INPUT_INVALID"
    with pytest.raises(ContractValidationError, match="preflight_errors"):
        validate_task_document(task)
    task["preflight_errors"] = [{"code": "BAM_MISSING", "message": "/inputs/missing.bam"}]
    validate_task_document(task)


def test_manifest_order_and_task_ids_are_exact() -> None:
    tasks = [valid_task(task_id="case_002", order=2), valid_task()]
    assert [task["task_id"] for task in validate_unique_task_set(tasks)] == ["case_001", "case_002"]

    duplicate = [valid_task(), valid_task()]
    with pytest.raises(ContractValidationError, match="duplicate task_id"):
        validate_unique_task_set(duplicate)

    gap = [valid_task(), valid_task(task_id="case_003", order=3)]
    with pytest.raises(ContractValidationError, match="contiguous"):
        validate_unique_task_set(gap)


def test_case_result_keeps_review_and_science_independent() -> None:
    result = valid_case_result()
    validate_case_result_document(result)

    result["scientific_interpretation"] = "SUPPORTED"
    with pytest.raises(ContractValidationError, match="EVIDENCE_INCOMPLETE"):
        validate_case_result_document(result)

    result = valid_case_result()
    result["artifact_review_state"] = "APPROVE"
    result["scientific_interpretation"] = "INDETERMINATE"
    result["publication_state"] = "READY"
    validate_case_result_document(result)


def test_publication_requires_both_human_dimensions() -> None:
    result = valid_case_result()
    result["publication_state"] = "PUBLISHED"
    with pytest.raises(ContractValidationError, match="artifact APPROVE"):
        validate_case_result_document(result)

    result["artifact_review_state"] = "APPROVE"
    with pytest.raises(ContractValidationError, match="scientific interpretation"):
        validate_case_result_document(result)


def test_review_approval_requires_all_visual_assertions() -> None:
    review = valid_review()
    validate_review_document(review)
    review["manual_assertions"]["violin_pair_matches"] = False
    with pytest.raises(ContractValidationError, match="every manual assertion"):
        validate_review_document(review)

    review["artifact_review_state"] = "REJECT"
    validate_review_document(review)


def test_stage_shard_and_run_schema_examples() -> None:
    stage = {
        "schema_version": "2.0",
        "bundle_version": "1.0",
        "run_id": "run_001",
        "generation_id": "gen_001",
        "shard_id": "shard_001",
        "session_id": "session_001",
        "task_id": "case_001",
        "manifest_order": 1,
        "attempt": 1,
        "stage": "RUN_IGV",
        "status": "SUCCEEDED",
        "input_fingerprint": SHA_A,
        "started_at": "2026-07-21T12:00:00Z",
        "finished_at": "2026-07-21T12:01:00Z",
        "artifacts": [],
        "warnings": [],
        "failures": [],
        "telemetry": {"wall_time_seconds": 60, "peak_rss_gb": 2.5, "exit_code": 0},
    }
    validate_schema_document(stage, "stage-result")

    ledger = {
        "schema_version": "2.0",
        "event_id": "event_001",
        "sequence": 1,
        "event_type": "PLANNED",
        "occurred_at": "2026-07-21T12:00:00Z",
        "run_id": "run_001",
        "generation_id": "gen_001",
        "shard_id": "shard_001",
        "expected_task_count": 1,
        "task_set_sha256": SHA_A,
        "pipeline_commit": "d" * 40,
        "schema_identity": "2.0",
        "session_id": None,
        "controller_job_id": None,
        "payload": {},
    }
    validate_schema_document(ledger, "shard-ledger")

    provenance = {
        "schema_version": "2.0",
        "pipeline_version": "2.0.0",
        "pipeline_commit": "d" * 40,
        "run_id": "run_001",
        "generation_id": "gen_001",
        "created_at": "2026-07-21T12:00:00Z",
        "canonical_manifest": {
            "relative_path": "manifests/tasks.jsonl",
            "sha256": SHA_A,
            "task_set_sha256": SHA_B,
            "task_count": 1,
        },
        "parameters": {"relative_path": "parameters.json", "sha256": SHA_C},
        "software": [{"name": "Nextflow", "version": "25.04.7", "executable": "/bin/nextflow", "sha256": None}],
        "references": [{"name": "annotation", "path": "/refs/MANE.gff.gz", "sha256": SHA_A}],
        "shards": [{"shard_id": "shard_001", "task_count": 1, "task_set_sha256": SHA_B, "session_id": None, "status": "PLANNED"}],
        "accounting": {"expected_tasks": 1, "trace_tasks": 0, "terminal_results": 0, "accounted_tasks": 0, "status": "PENDING"},
        "publication": {"state": "NOT_READY", "result_root": "/results/run_001", "checksums_sha256": None},
    }
    validate_schema_document(provenance, "run-provenance")


def test_schema_rejects_invalid_timestamp_and_path_traversal() -> None:
    review = valid_review()
    review["reviewed_at"] = "not-a-timestamp"
    with pytest.raises(ContractValidationError, match="date-time"):
        validate_review_document(review)

    result = valid_case_result()
    result["artifacts"][0]["relative_path"] = "../private.bam"
    with pytest.raises(ContractValidationError, match="relative_path"):
        validate_case_result_document(result)
