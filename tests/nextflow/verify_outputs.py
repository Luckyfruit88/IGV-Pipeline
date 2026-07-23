#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from PIL import Image


EXPECTED_PROCESSES = {
    "VALIDATE_ENVIRONMENT",
    "VALIDATE_CASE_INPUTS",
    "RUN_IGV",
    "COMPOSE_CASE",
    "QC_CASE",
    "SUMMARIZE_SHARD",
}


def _json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise AssertionError(f"expected a JSON object: {path}")
    return value


def _single(root: Path, pattern: str) -> Path:
    matches = list(root.glob(pattern))
    if len(matches) != 1:
        raise AssertionError(f"expected one {pattern} beneath {root}, observed {matches}")
    return matches[0]


def _trace_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _process_name(trace_name: str) -> str:
    return trace_name.split(" (", 1)[0].rsplit(":", 1)[-1]


def verify(root: Path) -> dict[str, Any]:
    output = root / "output"
    raw_before = output / "raw-plan-before-implementation-change"
    raw_after = output / "raw-plan"
    raw_tasks = [
        json.loads(line)
        for line in (raw_after / "normalization_bundle/tasks.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(raw_tasks) == 1
    assert raw_tasks[0]["run_id"] == "test_raw_nextflow"
    assert raw_tasks[0]["preflight_state"] == "READY"
    raw_report_before = _json(raw_before / "normalization_bundle/r_prepare.json")
    raw_report_after = _json(raw_after / "normalization_bundle/r_prepare.json")
    assert raw_report_before["mode"] == "EXECUTED"
    assert raw_report_after["mode"] == "EXECUTED"
    assert raw_report_before["r_wrapper_sha256"] == raw_report_after["r_wrapper_sha256"]
    assert raw_report_before["r_implementation_sha256"] != raw_report_after[
        "r_implementation_sha256"
    ]

    raw_first_trace = _trace_rows(output / "sessions/raw-plan-first/trace.txt")
    raw_resume_trace = _trace_rows(output / "sessions/raw-plan-resume/trace.txt")
    raw_changed_trace = _trace_rows(output / "sessions/raw-plan-changed/trace.txt")
    assert len(raw_first_trace) == 3
    assert {row["status"] for row in raw_first_trace} == {"COMPLETED"}
    assert {row["status"] for row in raw_resume_trace} == {"CACHED"}
    changed_status = {
        _process_name(row["name"]): row["status"] for row in raw_changed_trace
    }
    assert changed_status == {
        "VALIDATE_ENVIRONMENT": "CACHED",
        "VALIDATE_AND_NORMALIZE": "COMPLETED",
        "CREATE_SHARDS": "COMPLETED",
    }

    plan = output / "plan"
    tasks = [
        json.loads(line)
        for line in (plan / "normalization_bundle/tasks.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len(tasks) == 1
    assert tasks[0]["run_id"] == "test_local_nextflow"
    assert tasks[0]["preflight_state"] == "READY"
    shard_plan = _json(plan / "shard_bundle/shard_plan.json")
    assert shard_plan["task_count"] == 1
    assert len(shard_plan["shards"]) == 1

    run_output = output / "run"
    raw_png = _single(run_output, "stages/run_igv/*/raw/igv_client.png")
    combined_png = _single(run_output, "stages/compose_case/*/combined/*.png")
    with Image.open(raw_png) as image:
        assert image.size == (1920, 2160)
        assert image.mode == "RGB"
    with Image.open(combined_png) as image:
        assert image.size == (2640, 2160)
        assert image.mode == "RGB"

    case_result = _json(_single(run_output, "stages/qc_case/*/case_result.json"))
    assert case_result["render_state"] == "SUCCEEDED"
    assert case_result["evidence_state"] == "UNAVAILABLE"
    assert case_result["publication_state"] == "NOT_READY"
    assert [row["code"] for row in case_result["failures"]] == [
        "TEST_DOUBLE_EVIDENCE_NOT_PUBLISHABLE"
    ]

    domain_result = _json(
        _single(output / "domain-failure-run", "stages/qc_case/*/case_result.json")
    )
    assert domain_result["render_state"] == "FAILED"
    assert domain_result["publication_state"] == "NOT_READY"
    assert [row["code"] for row in domain_result["failures"]] == [
        "TEST_DECLARED_INPUT_INVALID"
    ]

    first_trace = _trace_rows(output / "sessions/run/trace.txt")
    resume_trace = _trace_rows(output / "sessions/run-resume/trace.txt")
    assert len(first_trace) == 6
    assert len(resume_trace) == 6
    assert {_process_name(row["name"]) for row in first_trace} == EXPECTED_PROCESSES
    assert {row["status"] for row in first_trace} == {"COMPLETED"}
    assert {row["status"] for row in resume_trace} == {"CACHED"}

    aggregate = output / "aggregate"
    run_summary = _json(aggregate / "aggregate_bundle/run_summary.json")
    reconciliation = _json(aggregate / "aggregate_bundle/reconciliation.json")
    accounting = _json(aggregate / "accounting_bundle/accounting.json")
    assert run_summary["status"] == "RECONCILED_WITH_CASE_FAILURES"
    assert run_summary["terminal_result_count"] == 1
    assert reconciliation["status"] == "PASS"
    assert reconciliation["missing_tasks"] == []
    assert reconciliation["duplicate_tasks"] == []
    assert reconciliation["unexpected_tasks"] == []
    assert accounting["status"] == "SKIPPED_TEST_MODE"
    assert accounting["trace_task_count"] == 6

    review_contract = output / "review/review_package/review_contract.jsonl"
    assert review_contract.read_text(encoding="utf-8") == ""
    publication = _json(output / "published/publication.json")
    assert publication["terminal_case_count"] == 1
    assert publication["published_case_count"] == 0
    assert publication["not_ready_case_count"] == 1
    assert list((output / "published").rglob("*.png")) == []

    required_stub_outputs = [
        "stub/plan/normalization_bundle/validation.json",
        "stub/plan/shard_bundle/shard_plan.json",
        "stub/run/summary/shard_summary_shard_001/shard_summary.json",
        "stub/aggregate/aggregate_bundle/reconciliation.json",
        "stub/aggregate/accounting_bundle/accounting.json",
        "stub/review/review_package/package.json",
        "stub/validated/publication_result.json",
    ]
    for relative in required_stub_outputs:
        assert (output / relative).is_file(), relative

    return {
        "actual_processes": len(first_trace),
        "cached_processes": len(resume_trace),
        "combined_png": {"width": 2640, "height": 2160, "mode": "RGB"},
        "domain_guard": "TEST_DOUBLE_EVIDENCE_NOT_PUBLISHABLE",
        "preflight_domain_failure": "TEST_DECLARED_INPUT_INVALID",
        "publication_png_count": 0,
        "r_implementation_cache_contract": "PASS",
        "raw_r_prepare_mode": "EXECUTED",
        "stub_entries": 4,
        "status": "PASS",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify local Nextflow integration outputs")
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.expanduser().resolve(strict=True)
    print(json.dumps(verify(root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
