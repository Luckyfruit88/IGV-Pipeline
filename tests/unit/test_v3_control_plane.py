from __future__ import annotations

import http.cookiejar
import json
import multiprocessing
import os
import re
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

from ssqtl_igv.accounting import (
    collect_local_accounting,
    collect_scc_accounting,
    finalize_scc_accounting,
    verify_local_accounting_receipt,
    verify_scc_accounting_receipt,
)
from ssqtl_igv.migration_v3 import import_v2_read_only
from ssqtl_igv.orchestrator_v3 import (
    _terminal_execution_state,
    finalize_scc_run_accounting,
)
from ssqtl_igv.publication import (
    assert_public_tree_safe,
    build_publication_promotion_receipt,
    promote_publication,
    verify_checksum_tree,
)
from ssqtl_igv.publication_v3 import build_publication_staging
from ssqtl_igv.rerun_v3 import prepare_rerun_task_set
from ssqtl_igv.review_package_v3 import build_review_package_v3
from ssqtl_igv.review_server import (
    GENERIC_ASSERTIONS,
    SSQTL_ASSERTIONS,
    append_review_decision,
    create_review_server,
    finalize_review,
)
from ssqtl_igv.utils import sha256_file, sha256_json
from ssqtl_igv.v3_cli import _parser as v3_cli_parser
from ssqtl_igv.v3_cli import _publish as v3_cli_publish

def _append_review_from_process(run_dir: str, reviewer: str) -> None:
    append_review_decision(
        run_dir,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer=reviewer,
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )


def _write_trace(path: Path, rows: list[tuple[str, str, str, str, str, str]]) -> None:
    text = "task_id\thash\tnative_id\tname\tstatus\texit\n"
    text += "".join("\t".join(row) + "\n" for row in rows)
    path.write_text(text, encoding="utf-8")


def _qacct_record(
    job_id: str,
    *,
    project: str = "fixture-project",
    qname: str = "fixture.q",
    job_name: str = "fixture-job",
) -> str:
    return "\n".join(
        (
            f"jobnumber    {job_id}",
            "taskid       undefined",
            "owner        fixture-user",
            f"jobname      {job_name}",
            f"project      {project}",
            f"qname        {qname}",
            "hostname     node001",
            "qsub_time    Mon Jul 20 09:59:00 2026",
            "start_time   Mon Jul 20 10:00:00 2026",
            "end_time     Mon Jul 20 10:01:00 2026",
            "ru_wallclock 60.0",
            "failed       0",
            "exit_status  0",
            "",
        )
    )


def _write_terminal_bundle(
    root: Path,
    *,
    task_id: str = "case_1",
    input_fingerprint: str = "f" * 64,
) -> Path:
    root.mkdir()
    artifact = root / "result.txt"
    artifact.write_text("terminal\n", encoding="utf-8")
    marker = {
        "schema_version": "3.0",
        "task_id": task_id,
        "stage": "RUN_IGV",
        "status": "SUCCEEDED",
        "input_fingerprint": input_fingerprint,
        "artifacts": [
            {
                "role": "result",
                "relative_path": "result.txt",
                "size": artifact.stat().st_size,
                "sha256": sha256_file(artifact),
            }
        ],
    }
    (root / "stage_result.json").write_text(json.dumps(marker), encoding="utf-8")
    return root


def _ssqtl_prepare_binding(
    contract: Path,
    trace: Path,
    *,
    task_id: str = "1",
    process: str = "SSQTL_NORMALIZE:NORMALIZE_SSQTL_V3 (run_001:generation_001)",
    task_hash: str = "cc/333333",
) -> dict[str, object]:
    contract.mkdir(parents=True, exist_ok=True)
    canonical = {
        "task_id": "case_1",
        "manifest_order": 1,
        "input_fingerprint": "f" * 64,
    }
    tasks = contract / "tasks.jsonl"
    tasks.write_text(json.dumps(canonical) + "\n", encoding="utf-8")
    preparation = contract / "ssqtl_preparation.json"
    preparation.write_text('{"schema_version":"3.0-ssqtl-preparation","status":"PASS"}\n')
    inventory = contract / "ssqtl_input_inventory.json"
    inventory.write_text('{"schema_version":"3.0-ssqtl-selected-input-inventory"}\n')
    task_set_sha = sha256_json(
        [
            {
                "task_id": canonical["task_id"],
                "manifest_order": canonical["manifest_order"],
                "input_fingerprint": canonical["input_fingerprint"],
            }
        ]
    )
    runtime_manifest_sha = "a" * 64
    runtime_fingerprint_sha = "b" * 64
    execution = {
        "schema_version": "3.0-ssqtl-normalization-execution",
        "status": "SUCCEEDED",
        "process_label": "portable_runtime",
        "normalization_tasks_sha256": sha256_file(tasks),
        "normalization_task_set_sha256": task_set_sha,
        "preparation_receipt_sha256": sha256_file(preparation),
        "input_inventory_sha256": sha256_file(inventory),
        "runtime_manifest_sha256": runtime_manifest_sha,
        "runtime_fingerprint_sha256": runtime_fingerprint_sha,
        "runtime_sif_sha256": None,
        "trace_sha256": sha256_file(trace),
    }
    receipt = contract / "ssqtl_normalization_execution.json"
    receipt.write_text(json.dumps(execution) + "\n", encoding="utf-8")
    return {
        "task_id": task_id,
        "process": process,
        "hash": task_hash,
        "trace_file": str(trace.resolve()),
        "prepare_role": "ssqtl_normalization",
        "prepare_receipt": str(receipt.resolve()),
        "prepare_receipt_sha256": sha256_file(receipt),
        "prepared_tasks_sha256": execution["normalization_tasks_sha256"],
        "prepared_task_set_sha256": task_set_sha,
        "preparation_receipt_sha256": execution["preparation_receipt_sha256"],
        "input_inventory_sha256": execution["input_inventory_sha256"],
        "runtime_manifest_sha256": runtime_manifest_sha,
        "runtime_fingerprint_sha256": runtime_fingerprint_sha,
        "runtime_sif_sha256": None,
        "trace_sha256": execution["trace_sha256"],
    }


def _write_pending_scc_state(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    trace = root / "trace.tsv"
    _write_trace(
        trace,
        [("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0")],
    )
    state = root / "scc-accounting"
    pending = collect_scc_accounting(
        [trace],
        state,
        controller={"native_id": "700"},
        expected_project="fixture-project",
        expected_tasks=[{"task_id": "1"}],
        expected_cases=[{"task_id": "case_1", "input_fingerprint": "f" * 64}],
        terminal_bundles=[_write_terminal_bundle(root / "terminal-scc")],
        raw_qacct_by_native_id={"701": _qacct_record("701")},
    )
    assert pending["status"] == "ACCOUNTING_PENDING"
    return state


def test_local_accounting_closes_trace_cache_and_bundle_lineage(tmp_path: Path) -> None:
    prior_trace = tmp_path / "prior-trace.tsv"
    _write_trace(
        prior_trace,
        [
            ("998", "aa/111111", "-", "PORTABLE_RUN:RUN_IGV (old_case)", "COMPLETED", "0"),
            ("999", "bb/222222", "-", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0"),
        ],
    )
    prior_output = tmp_path / "prior-accounting"
    collect_local_accounting([prior_trace], prior_output)
    trace = tmp_path / "trace.tsv"
    _write_trace(
        trace,
        [
            ("1", "aa/111111", "-", "PORTABLE_RUN:RUN_IGV (old_case)", "COMPLETED", "0"),
            ("2", "bb/222222", "-", "PORTABLE_RUN:RUN_IGV (case_1)", "CACHED", "0"),
        ],
    )
    output = tmp_path / "accounting"
    report = collect_local_accounting(
        [trace],
        output,
        expected_tasks=[
            {
                "task_id": "1",
                "case_id": "old_case",
                "process": "PORTABLE_RUN:RUN_IGV (old_case)",
                "hash": "aa/111111",
            },
            {
                "task_id": "2",
                "process": "PORTABLE_RUN:RUN_IGV (case_1)",
                "hash": "bb/222222",
            },
        ],
        expected_cases=[
            {"task_id": "old_case", "input_fingerprint": "e" * 64, "manifest_order": 1},
            {"task_id": "case_1", "input_fingerprint": "f" * 64, "manifest_order": 2},
        ],
        terminal_bundles=[
            _write_terminal_bundle(
                tmp_path / "terminal-old", task_id="old_case", input_fingerprint="e" * 64
            ),
            _write_terminal_bundle(tmp_path / "terminal-case-1"),
        ],
        cached_lineage=prior_output / "cache_lineage.jsonl",
    )
    assert report["schema_version"] == "3.0-local-accounting"
    assert report["provider"] == "nextflow_trace"
    assert report["site"] == "local"
    assert report["qacct_used"] is False
    assert report["cached_task_count"] == report["cached_lineage_count"] == 1
    assert report["terminal_bundle_count"] == 2
    receipt = json.loads((output / "accounting_receipt.json").read_text())
    assert receipt["provider"] == "nextflow_trace"
    assert receipt["accounting_sha256"] == sha256_file(output / "accounting.json")
    generated_lineage = [json.loads(line) for line in (output / "cache_lineage.jsonl").read_text().splitlines()]
    assert {row["trace_task_id"] for row in generated_lineage} == {"1", "2"}
    assert all(
        row["source_accounting_sha256"] == report["receipt_sha256"]
        for row in generated_lineage
    )
    assert all(
        Path(row["source_accounting_path"]) == output / "accounting_receipt.json"
        for row in generated_lineage
    )


def test_local_accounting_binds_ssqtl_prepare_exact_set_and_detects_tamper(
    tmp_path: Path,
) -> None:
    trace = tmp_path / "normalization.trace.tsv"
    _write_trace(
        trace,
        [
            (
                "1",
                "cc/333333",
                "-",
                "SSQTL_NORMALIZE:NORMALIZE_SSQTL_V3 (run_001:generation_001)",
                "COMPLETED",
                "0",
            )
        ],
    )
    binding = _ssqtl_prepare_binding(tmp_path / "contract", trace)
    output = tmp_path / "accounting"

    report = collect_local_accounting(
        [trace], output, expected_tasks=[binding]
    )

    assert report["preparation_count"] == 1
    assert verify_local_accounting_receipt(output)["preparations"][0][
        "prepared_tasks_sha256"
    ] == binding["prepared_tasks_sha256"]
    (tmp_path / "contract" / "ssqtl_preparation.json").write_text(
        '{"status":"TAMPERED"}\n', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="preparation artifacts drifted"):
        verify_local_accounting_receipt(output)


def test_scc_accounting_allows_session_local_task_id_overlap_by_composite_identity(
    tmp_path: Path,
) -> None:
    normalization_trace = tmp_path / "normalization.trace.tsv"
    render_trace = tmp_path / "render.trace.tsv"
    _write_trace(
        normalization_trace,
        [
            (
                "1",
                "cc/333333",
                "701",
                "SSQTL_NORMALIZE:NORMALIZE_SSQTL_V3 (run_001:generation_001)",
                "COMPLETED",
                "0",
            )
        ],
    )
    _write_trace(
        render_trace,
        [
            (
                "1",
                "dd/444444",
                "702",
                "PORTABLE_RUN:RUN_IGV (case_1)",
                "COMPLETED",
                "0",
            )
        ],
    )
    prepare = _ssqtl_prepare_binding(tmp_path / "contract", normalization_trace)
    state = tmp_path / "scc-accounting"
    report = collect_scc_accounting(
        [normalization_trace, render_trace],
        state,
        controller={"native_id": "700"},
        expected_project="fixture-project",
        expected_qname="fixture.q",
        expected_tasks=[
            prepare,
            {
                "task_id": "1",
                "process": "PORTABLE_RUN:RUN_IGV (case_1)",
                "hash": "dd/444444",
                "case_id": "case_1",
            },
        ],
        expected_cases=[
            {"task_id": "case_1", "input_fingerprint": "f" * 64}
        ],
        terminal_bundles=[_write_terminal_bundle(tmp_path / "terminal-scc-overlap")],
        raw_qacct_by_native_id={
            "700": _qacct_record("700"),
            "701": _qacct_record("701"),
            "702": _qacct_record("702"),
        },
    )

    assert report["status"] == "PASS"
    assert report["preparation_count"] == 1
    assert verify_scc_accounting_receipt(state)["report"]["task_qacct_count"] == 2


def test_local_accounting_rejects_unproven_cache_without_partial_output(tmp_path: Path) -> None:
    trace = tmp_path / "trace.tsv"
    _write_trace(
        trace,
        [("1", "aa/111111", "-", "PORTABLE_RUN:RUN_IGV (case_1)", "CACHED", "0")],
    )
    output = tmp_path / "accounting"
    with pytest.raises(ValueError, match="lacks prior accounting lineage"):
        collect_local_accounting([trace], output)
    assert not output.exists()
    assert not list(tmp_path.glob(".accounting.tmp-*"))


def test_local_accounting_rejects_expected_set_and_failed_trace(tmp_path: Path) -> None:
    trace = tmp_path / "trace.tsv"
    _write_trace(
        trace,
        [("1", "aa/111111", "-", "PORTABLE_RUN:PREPARE", "FAILED", "1")],
    )
    with pytest.raises(ValueError, match="non-successful terminal state"):
        collect_local_accounting([trace], tmp_path / "failed")
    _write_trace(
        trace,
        [("1", "aa/111111", "-", "PORTABLE_RUN:PREPARE", "COMPLETED", "0")],
    )
    with pytest.raises(ValueError, match="outside the expected set"):
        collect_local_accounting(
            [trace],
            tmp_path / "mismatch",
            expected_tasks=[],
        )
    # An explicit non-empty expected set is distinguishable from the trace-derived default.
    with pytest.raises(ValueError, match="matches=0"):
        collect_local_accounting(
            [trace],
            tmp_path / "mismatch-2",
            expected_tasks=[{"task_id": "999"}],
        )


def test_scc_accounting_provider_is_explicit_and_distinct_from_local(tmp_path: Path) -> None:
    trace = tmp_path / "trace.tsv"
    _write_trace(
        trace,
        [("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0")],
    )
    report = collect_scc_accounting(
        [trace],
        tmp_path / "scc-accounting",
        controller={
            "native_id": "700",
            "owner": "fixture-user",
            "job_name": "fixture-controller",
        },
        expected_project="fixture-project",
        expected_qname="fixture.q",
        expected_tasks=[
            {
                "task_id": "1",
                "process": "PORTABLE_RUN:RUN_IGV (case_1)",
                "hash": "aa/111111",
            }
        ],
        expected_cases=[{"task_id": "case_1", "input_fingerprint": "f" * 64}],
        terminal_bundles=[_write_terminal_bundle(tmp_path / "terminal-scc")],
        raw_qacct_by_native_id={
            "700": _qacct_record("700", job_name="fixture-controller"),
            "701": _qacct_record("701"),
        },
    )
    assert report["schema_version"] == "3.0-sge-qacct-accounting"
    assert report["provider"] == "sge_qacct"
    assert report["site"] == "scc"
    assert report["qacct_used"] is True
    assert report["controller_qacct_count"] == report["task_qacct_count"] == 1
    frozen = json.loads((tmp_path / "scc-accounting" / "final" / "accounting.json").read_text())
    assert frozen["provider"] == "sge_qacct"
    assert verify_scc_accounting_receipt(tmp_path / "scc-accounting")["report"] == frozen


def test_scc_accounting_pending_is_recoverable_only_after_controller_qacct(tmp_path: Path) -> None:
    state = _write_pending_scc_state(tmp_path)
    assert not (state / "finalized_accounting.json").exists()

    passed = finalize_scc_accounting(
        state,
        raw_qacct_by_native_id={
            "700": _qacct_record("700", job_name="fixture-controller"),
            "701": _qacct_record("701"),
        },
    )
    assert passed["status"] == "PASS"
    assert verify_scc_accounting_receipt(state)["receipt_sha256"] == passed["receipt_sha256"]
    (state / "finalized_accounting.json").unlink()
    recovered = finalize_scc_accounting(state, qacct_command="must-not-be-invoked")
    assert recovered["status"] == "PASS"
    assert (state / "finalized_accounting.json").is_file()


def test_scc_site_adapter_recognizes_exact_record_not_visible_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _write_pending_scc_state(tmp_path)

    def not_visible(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        job_id = command[command.index("-j") + 1]
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr=f"error: job id {job_id} not found\n",
        )

    monkeypatch.setattr("ssqtl_igv.accounting.subprocess.run", not_visible)
    pending = finalize_scc_accounting(state)

    assert pending["status"] == "ACCOUNTING_PENDING"
    assert pending["review_gate"] is False
    attempt = json.loads((Path(pending["attempt_dir"]) / "attempt.json").read_text())
    assert {row["reason_code"] for row in attempt["pending"]} == {
        "SGE_ACCOUNTING_RECORD_NOT_VISIBLE"
    }
    assert attempt["infrastructure_failures"] == []


@pytest.mark.parametrize(
    ("failure_mode", "reason_code"),
    (
        ("missing", "QACCT_EXECUTION_OS_ERROR"),
        ("permission", "QACCT_EXECUTION_OS_ERROR"),
        ("timeout", "QACCT_TIMEOUT"),
        ("unknown_nonzero", "QACCT_UNRECOGNIZED_NONZERO_EXIT"),
    ),
)
def test_scc_qacct_infrastructure_failure_is_fatal_and_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
    reason_code: str,
) -> None:
    state = _write_pending_scc_state(tmp_path)
    before = set((state / "attempts").iterdir())
    qacct_command = str(tmp_path / "missing-qacct")
    if failure_mode == "permission":
        qacct = tmp_path / "non-executable-qacct"
        qacct.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        qacct.chmod(0o600)
        qacct_command = str(qacct)
    elif failure_mode == "timeout":
        def timeout(command: list[str], **_kwargs: object) -> None:
            raise subprocess.TimeoutExpired(command, 120)

        monkeypatch.setattr("ssqtl_igv.accounting.subprocess.run", timeout)
        qacct_command = "qacct"
    elif failure_mode == "unknown_nonzero":
        def unavailable(
            command: list[str], **_kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess(
                command,
                2,
                stdout="",
                stderr="accounting service connection refused\n",
            )

        monkeypatch.setattr("ssqtl_igv.accounting.subprocess.run", unavailable)
        qacct_command = "qacct"

    with pytest.raises(RuntimeError, match="SCC qacct infrastructure failure retained"):
        finalize_scc_accounting(state, qacct_command=qacct_command)

    created = set((state / "attempts").iterdir()) - before
    assert len(created) == 1
    attempt = json.loads((created.pop() / "attempt.json").read_text())
    assert attempt["status"] == "INFRASTRUCTURE_FATAL"
    assert attempt["review_gate"] is False
    assert attempt["pending"] == []
    assert {row["reason_code"] for row in attempt["infrastructure_failures"]} == {
        reason_code
    }
    assert not (state / "finalized_accounting.json").exists()


def test_scc_accounting_rejects_project_drift_without_opening_review(tmp_path: Path) -> None:
    trace = tmp_path / "trace.tsv"
    _write_trace(
        trace,
        [("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0")],
    )
    with pytest.raises(ValueError, match="project differs"):
        collect_scc_accounting(
            [trace],
            tmp_path / "scc-accounting",
            controller={"native_id": "700"},
            expected_project="fixture-project",
            expected_tasks=[{"task_id": "1"}],
            expected_cases=[{"task_id": "case_1", "input_fingerprint": "f" * 64}],
            terminal_bundles=[_write_terminal_bundle(tmp_path / "terminal-scc")],
            raw_qacct_by_native_id={
                "700": _qacct_record("700", project="wrong-project"),
                "701": _qacct_record("701"),
            },
        )


def test_scc_cached_task_requires_and_retains_prior_qacct_lineage(tmp_path: Path) -> None:
    first_trace = tmp_path / "first.trace.tsv"
    _write_trace(
        first_trace,
        [("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0")],
    )
    task = {"task_id": "case_1", "input_fingerprint": "f" * 64}
    terminal = _write_terminal_bundle(tmp_path / "terminal-scc")
    first = tmp_path / "first-accounting"
    collect_scc_accounting(
        [first_trace],
        first,
        controller={"native_id": "700"},
        expected_project="fixture-project",
        expected_tasks=[{"task_id": "1"}],
        expected_cases=[task],
        terminal_bundles=[terminal],
        raw_qacct_by_native_id={
            "700": _qacct_record("700"),
            "701": _qacct_record("701"),
        },
    )
    cached_trace = tmp_path / "cached.trace.tsv"
    _write_trace(
        cached_trace,
        [("2", "aa/111111", "-", "PORTABLE_RUN:RUN_IGV (case_1)", "CACHED", "0")],
    )
    with pytest.raises(ValueError, match="lacks a verified prior qacct receipt"):
        collect_scc_accounting(
            [cached_trace],
            tmp_path / "unproven-cache",
            controller={"native_id": "800"},
            expected_project="fixture-project",
            expected_tasks=[{"task_id": "2"}],
            expected_cases=[task],
            terminal_bundles=[terminal],
            raw_qacct_by_native_id={"800": _qacct_record("800")},
        )
    second = tmp_path / "second-accounting"
    report = collect_scc_accounting(
        [cached_trace],
        second,
        controller={"native_id": "800"},
        expected_project="fixture-project",
        expected_tasks=[{"task_id": "2"}],
        expected_cases=[task],
        terminal_bundles=[terminal],
        cached_lineage=first / "final" / "cache_lineage.jsonl",
        raw_qacct_by_native_id={"800": _qacct_record("800")},
    )
    assert report["status"] == "PASS"
    assert report["cached_task_count"] == 1
    verified = verify_scc_accounting_receipt(second)
    cached = [row for row in verified["scheduler_rows"] if row["role"] == "task"]
    assert cached[0]["accounting_state"] == "PASS_CACHED_LINEAGE"


def test_v2_import_is_a_read_only_audit_receipt(tmp_path: Path) -> None:
    source = tmp_path / "v2-run"
    source.mkdir()
    marker = source / "run_summary.json"
    marker.write_text('{"schema_version":"2.0","status":"REVIEW_PENDING"}\n', encoding="utf-8")
    before = (marker.read_bytes(), marker.stat().st_mode, marker.stat().st_mtime_ns)
    output = tmp_path / "imported"
    receipt = import_v2_read_only(source, output)
    after = (marker.read_bytes(), marker.stat().st_mode, marker.stat().st_mtime_ns)
    assert before == after
    assert receipt["status"] == "AUDITED_READ_ONLY"
    assert receipt["source_copied"] is False
    assert receipt["permissions"] == {
        "resume_v3": False,
        "review_v3": False,
        "publish_v3": False,
        "write_source": False,
    }
    assert not (output / "run_summary.json").exists()
    assert sha256_file(output / "source_inventory.jsonl") == receipt["inventory_sha256"]
    with pytest.raises(ValueError, match="outside the read-only source"):
        import_v2_read_only(source, source / "audit")


def test_v2_import_rejects_non_v2_and_mixed_sources(tmp_path: Path) -> None:
    source = tmp_path / "not-v2"
    source.mkdir()
    (source / "record.json").write_text('{"schema_version":"3.0"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="beginning with 2"):
        import_v2_read_only(source, tmp_path / "invalid")
    (source / "v2.json").write_text('{"schema_version":"2.0"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="mixes v2 and v3"):
        import_v2_read_only(source, tmp_path / "mixed")


def test_v2_review_package_cannot_cross_the_v3_review_gate(tmp_path: Path) -> None:
    package = tmp_path / "run" / "review" / "review_package"
    package.mkdir(parents=True)
    (package / "package.json").write_text(
        '{"schema_version":"2.0-review-package"}\n', encoding="utf-8"
    )
    (package / "review_contract.jsonl").write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="audit-only"):
        finalize_review(tmp_path / "run")


def _write_review_run(
    tmp_path: Path,
    *,
    adapter: str = "generic",
    profile: str = "standalone",
    evidence_state: str = "COMPLETE",
    eligible: bool = True,
    build_package: bool = True,
) -> Path:
    run = tmp_path / "run"
    case_root = run / "results" / "cases" / "case_1"
    case_root.mkdir(parents=True)
    artifact_names = {
        "review_image": "review.png",
        "scientific_qc": "scientific_qc.json",
        "raw_igv": "raw_igv.png",
        "capture_metadata": "capture_metadata.json",
        "layout": "layout.json",
        "raw_qc": "raw_qc.json",
        "review_qc": "review_qc.json",
        "track_display_contract": "track_display_contract.json",
        "igv_session": "igv_session.xml",
    }
    if adapter == "ssqtl":
        artifact_names.update(
            {
                "scientific_case_evidence": "scientific_case_evidence.json",
                "scientific_qc_evidence": "scientific_qc_evidence.json",
            }
        )
    artifacts = {}
    for role, name in artifact_names.items():
        artifact = case_root / name
        if artifact.suffix == ".json":
            artifact.write_text(json.dumps({"fixture_role": role}) + "\n", encoding="utf-8")
        else:
            artifact.write_bytes(f"synthetic-{role}\n".encode("utf-8"))
        artifacts[role] = {
            "relative_path": str(artifact.relative_to(run)),
            "sha256": sha256_file(artifact),
            "size": artifact.stat().st_size,
        }
    case = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "run_id": "run_001",
        "generation_id": "generation_001",
        "task_id": "case_1",
        "manifest_order": 1,
        "input_fingerprint": "f" * 64,
        "render_state": "SUCCEEDED" if eligible else "FAILED",
        "eligible": eligible,
        "artifact_review_state": "REVIEW_PENDING",
        "publication_state": "NOT_READY",
        "evidence_state": evidence_state if eligible else "UNAVAILABLE",
        "adapter_type": adapter,
        "adapter_evidence": (
            {
                "adapter_schema_version": "3.0-generic",
                "scientific_interpretation": "NOT_APPLICABLE",
            }
            if adapter == "generic"
            else {
                "adapter_schema_version": "3.0-ssqtl",
                "scientific_evidence_available": True,
                "scientific_case_evidence_sha256": artifacts["scientific_case_evidence"]["sha256"],
                "scientific_qc_evidence_sha256": artifacts[
                    "scientific_qc_evidence"
                ]["sha256"],
                "scientific_evidence_state": evidence_state,
                "scientific_result_interpretation": (
                    "INDETERMINATE"
                    if evidence_state == "EVIDENCE_INCOMPLETE"
                    else "PENDING"
                ),
                "scientific_failure_set_sha256": sha256_json([]),
                "empty_genotype_groups": [],
            }
        ),
        "scientific_interpretation": (
            "NOT_APPLICABLE"
            if adapter == "generic"
            else "INDETERMINATE"
            if evidence_state == "EVIDENCE_INCOMPLETE"
            else "PENDING"
        ),
        "debug_only": False,
        "required_manual_assertions": list(
            GENERIC_ASSERTIONS if adapter == "generic" else SSQTL_ASSERTIONS
        ),
        "artifacts": artifacts,
        "pixel_identity": (
            {
                "source_igv_decoded_pixel_sha256": "a" * 64,
                "final_igv_decoded_pixel_sha256": "a" * 64,
                "igv_pixel_identity": True,
            }
            if eligible
            else None
        ),
        "failures": (
            []
            if eligible
            else [
                {
                    "code": "SYNTHETIC_DOMAIN_FAILURE",
                    "message": "synthetic failed case for the SCC accounting gate",
                }
            ]
        ),
        "created_at": "2026-07-21T00:00:00Z",
    }
    case_result = case_root / "case_result.json"
    case_result.write_text(json.dumps(case), encoding="utf-8")
    terminal = case_root / "terminal_bundle.json"
    terminal.write_text(
        json.dumps(
            {
                "schema_version": "3.0",
                "pipeline_version": "3.0.0",
                "run_id": "run_001",
                "generation_id": "generation_001",
                "task_id": "case_1",
                "manifest_order": 1,
                "input_fingerprint": "f" * 64,
                "status": "SUCCEEDED" if eligible else "DOMAIN_FAILED",
                "case_result_sha256": sha256_file(case_result),
                "case_result_size": case_result.stat().st_size,
                "artifact_set_sha256": sha256_json(artifacts),
            }
        ),
        encoding="utf-8",
    )
    contract_root = run / "contract"
    contract_root.mkdir()
    task = {
        "schema_version": "3.0",
        "adapter_id": adapter,
        "run_id": "run_001",
        "generation_id": "generation_001",
        "task_id": "case_1",
        "manifest_order": 1,
        "input_fingerprint": "f" * 64,
    }
    tasks = contract_root / "tasks.jsonl"
    tasks.write_text(json.dumps(task, sort_keys=True) + "\n", encoding="utf-8")
    runtime_oci_digest = "sha256:" + "b" * 64
    runtime_manifest_document = {
        "schema_version": "igv-runtime-manifest-v1",
        "workflow_schema_version": "3.0",
        "pipeline": {"name": "igv-pipeline", "version": "3.0.0"},
        "platform": "linux/amd64",
        "source": {"commit": "c" * 40, "tree": "d" * 40},
        "materials": {
            "path": "containers/runtime-materials.lock.json",
            "sha256": "e" * 64,
        },
        "render_contract": {
            "screen": "1920x2160x24",
            "igv_heap": "6g",
            "locale": "C.UTF-8",
            "font_family": "DejaVu Sans",
            "command_listener_enabled": False,
        },
        "tools": {},
    }
    runtime_manifest_path = contract_root / "runtime-manifest.json"
    runtime_manifest_path.write_text(
        json.dumps(runtime_manifest_document, sort_keys=True) + "\n", encoding="utf-8"
    )
    runtime_manifest_sha = sha256_file(runtime_manifest_path)
    runtime_fingerprint_sha = sha256_json(runtime_manifest_document)
    snapshot = {
        **runtime_manifest_document,
        "runtime_manifest_sha256": runtime_manifest_sha,
        "runtime_fingerprint_sha256": runtime_fingerprint_sha,
        "observed_provenance": {
            "oci": {
                "reference": "ghcr.io/luckyfruit88/igv-pipeline@" + runtime_oci_digest,
                "digest": runtime_oci_digest,
            },
            "sif_sha256": None,
        },
    }
    snapshot_path = contract_root / "runtime_manifest.snapshot.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    (contract_root / "run_identity.json").write_text(
        json.dumps(
            {
                "schema_version": "3.0",
                "run_id": "run_001",
                "generation_id": "generation_001",
                "profile": profile,
                "execution_run_root": str(run.resolve()),
                "runtime_manifest": str(runtime_manifest_path),
                "canonical_tasks_sha256": sha256_file(tasks),
                "runtime_manifest_sha256": runtime_manifest_sha,
                "runtime_manifest_snapshot_sha256": sha256_file(snapshot_path),
                "runtime_fingerprint_sha256": runtime_fingerprint_sha,
            }
        ),
        encoding="utf-8",
    )
    trace = run / "sessions" / "trace.tsv"
    trace.parent.mkdir()
    _write_trace(
        trace,
        [
            (
                "0",
                "cc/000000",
                "-",
                "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "COMPLETED",
                "0",
            ),
            ("1", "aa/111111", "-", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0"),
        ],
    )
    runtime_validation = (
        trace.parent / "runtime_manifest" / "runtime_manifest_validation" / "validation.json"
    )
    runtime_validation.parent.mkdir(parents=True)
    runtime_validation.write_text(
        json.dumps(
            {
                "schema_version": "3.0-runtime-manifest-validation",
                "status": "PASS",
                "runtime_manifest_sha256": runtime_manifest_sha,
                "runtime_fingerprint_sha256": runtime_fingerprint_sha,
            }
        ),
        encoding="utf-8",
    )
    accounting = collect_local_accounting(
        [trace],
        run / "accounting" / "local" / "generation-0001",
        expected_tasks=[
            {
                "task_id": "0",
                "process": "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "hash": "cc/000000",
                "trace_file": str(trace.resolve()),
                "control_role": "runtime_manifest_validation",
                "control_receipt": str(runtime_validation.resolve()),
                "control_receipt_sha256": sha256_file(runtime_validation),
                "runtime_manifest_sha256": runtime_manifest_sha,
                "runtime_fingerprint_sha256": runtime_fingerprint_sha,
            },
            {
                "task_id": "1",
                "case_id": "case_1",
                "process": "PORTABLE_RUN:RUN_IGV (case_1)",
                "hash": "aa/111111",
            }
        ],
        expected_cases=[task],
        terminal_bundles=[terminal],
    )
    accounting["output_relative_path"] = str(
        Path(accounting["output_dir"]).relative_to(run)
    )
    controller_runtime = {
        "schema_version": "3.0-controller-runtime",
        "nextflow": {
            "required_version": "25.04.7",
            "executable": "/usr/local/bin/nextflow",
            "sha256": "3" * 64,
            "version_output_sha256": "4" * 64,
        },
        "java": {
            "required_major": 21,
            "selector": "NXF_JAVA_HOME",
            "executable": "/opt/java-21/bin/java",
            "sha256": "5" * 64,
            "version_output_sha256": "6" * 64,
        },
    }
    controller_runtime["identity_sha256"] = sha256_json(controller_runtime)
    (contract_root / "controller_runtime.json").write_text(
        json.dumps(controller_runtime, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary_path = run / "run_summary.json"
    summary = {
        "schema_version": "3.0",
        "authoritative": False,
        "projection_kind": "UX_ONLY",
        "source_digests": {
            "canonical_tasks": sha256_file(tasks),
            "accounting_receipt": accounting["receipt_sha256"],
        },
        "status": "SNAPSHOTS_READY",
        "review_gate": False,
        "expected_case_count": 1,
        "observed_case_count": 1,
        "failed_case_count": 0,
        "failed_case_ids": [],
        "controller_runtime": controller_runtime,
        "accounting": accounting,
        "shards": [
            {
                "exit_code": 0,
                "trace": str(trace),
                "trace_relative_path": str(trace.relative_to(run)),
            }
        ],
        "publication_state": "NOT_READY",
        "human_review_required": False,
    }
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    if build_package:
        summary["review_package"] = build_review_package_v3(run)
        summary_path.write_text(json.dumps(summary), encoding="utf-8")
    return run


def _freeze_review_package(run: Path) -> None:
    summary_path = run / "run_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    accounting = summary.get("accounting")
    if isinstance(accounting, dict) and accounting.get("receipt_sha256"):
        summary.setdefault("source_digests", {})["accounting_receipt"] = accounting[
            "receipt_sha256"
        ]
    summary.pop("review_package", None)
    summary.update({"status": "SNAPSHOTS_READY", "review_gate": False})
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    summary["review_package"] = build_review_package_v3(run)
    summary_path.write_text(json.dumps(summary), encoding="utf-8")


def test_review_accepts_only_verified_scc_controller_and_task_qacct(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path, profile="scc", build_package=False)
    trace = run / "sessions" / "trace.tsv"
    _write_trace(
        trace,
        [
            (
                "0",
                "cc/000000",
                "702",
                "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "COMPLETED",
                "0",
            ),
            ("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0"),
        ],
    )
    task = json.loads((run / "contract" / "tasks.jsonl").read_text())
    validation = (
        run / "sessions" / "runtime_manifest" / "runtime_manifest_validation" / "validation.json"
    )
    validation_document = json.loads(validation.read_text())
    accounting = collect_scc_accounting(
        [trace],
        run / "accounting" / "scc" / "generation_001",
        controller={"native_id": "700"},
        expected_project="fixture-project",
        expected_qname="fixture.q",
        expected_tasks=[
            {
                "task_id": "0",
                "process": "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "hash": "cc/000000",
                "trace_file": str(trace.resolve()),
                "control_role": "runtime_manifest_validation",
                "control_receipt": str(validation.resolve()),
                "control_receipt_sha256": sha256_file(validation),
                "runtime_manifest_sha256": validation_document[
                    "runtime_manifest_sha256"
                ],
                "runtime_fingerprint_sha256": validation_document[
                    "runtime_fingerprint_sha256"
                ],
            },
            {
                "task_id": "1",
                "process": "PORTABLE_RUN:RUN_IGV (case_1)",
                "hash": "aa/111111",
            }
        ],
        expected_cases=[task],
        terminal_bundles=[run / "results" / "cases" / "case_1" / "terminal_bundle.json"],
        raw_qacct_by_native_id={
            "700": _qacct_record("700", job_name="fixture-controller"),
            "701": _qacct_record("701"),
            "702": _qacct_record("702"),
        },
    )
    summary_path = run / "run_summary.json"
    summary = json.loads(summary_path.read_text())
    summary["accounting"] = accounting
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    _freeze_review_package(run)
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-scc",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    assert finalize_review(run)["status"] == "FINALIZED"


def test_scc_snapshot_output_does_not_wait_for_optional_qacct_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _write_review_run(tmp_path, profile="scc", build_package=False)
    trace = run / "sessions" / "trace.tsv"
    _write_trace(
        trace,
        [
            (
                "0",
                "cc/000000",
                "702",
                "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "COMPLETED",
                "0",
            ),
            ("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0"),
        ],
    )
    task = json.loads((run / "contract" / "tasks.jsonl").read_text())
    validation = (
        run / "sessions" / "runtime_manifest" / "runtime_manifest_validation" / "validation.json"
    )
    validation_document = json.loads(validation.read_text())
    state = run / "accounting" / "scc" / "generation_001"
    pending = collect_scc_accounting(
        [trace],
        state,
        controller={"native_id": "700"},
        expected_project="fixture-project",
        expected_tasks=[
            {
                "task_id": "0",
                "process": "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "hash": "cc/000000",
                "trace_file": str(trace.resolve()),
                "control_role": "runtime_manifest_validation",
                "control_receipt": str(validation.resolve()),
                "control_receipt_sha256": sha256_file(validation),
                "runtime_manifest_sha256": validation_document[
                    "runtime_manifest_sha256"
                ],
                "runtime_fingerprint_sha256": validation_document[
                    "runtime_fingerprint_sha256"
                ],
            },
            {"task_id": "1"},
        ],
        expected_cases=[task],
        terminal_bundles=[run / "results" / "cases" / "case_1" / "terminal_bundle.json"],
        raw_qacct_by_native_id={
            "701": _qacct_record("701"),
            "702": _qacct_record("702"),
        },
    )
    summary_path = run / "run_summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update({"status": "ACCOUNTING_PENDING", "exit_code": 0, "profile": "scc"})
    summary["accounting"] = pending
    summary_path.write_text(json.dumps(summary), encoding="utf-8")

    def not_visible(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        job_id = command[command.index("-j") + 1]
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr=f"error: job id {job_id} not found\n",
        )

    monkeypatch.setattr("ssqtl_igv.accounting.subprocess.run", not_visible)
    still_pending = finalize_scc_run_accounting(run)
    assert still_pending["status"] == "SNAPSHOTS_READY"
    assert still_pending["exit_code"] == 0
    assert still_pending["accounting"]["status"] == "ACCOUNTING_PENDING"
    finalize_scc_accounting(
        state,
        raw_qacct_by_native_id={
            "700": _qacct_record("700"),
            "701": _qacct_record("701"),
            "702": _qacct_record("702"),
        },
    )
    closed = finalize_scc_run_accounting(run, qacct_command="must-not-be-invoked")
    assert closed["status"] == "SNAPSHOTS_READY"
    assert closed["exit_code"] == 0
    assert closed["accounting"]["receipt_sha256"] == sha256_file(
        state / "final" / "accounting_receipt.json"
    )


@pytest.mark.parametrize(
    ("profile", "accounting_pass", "failed_case_ids", "expected"),
    [
        ("scc", False, ["case_1"], ("CASE_FAILURES", 2)),
        ("scc", False, [], ("SNAPSHOTS_READY", 0)),
        ("scc", True, ["case_1"], ("CASE_FAILURES", 2)),
        ("docker", True, [], ("SNAPSHOTS_READY", 0)),
        ("docker", False, [], ("INFRASTRUCTURE_FATAL", 1)),
    ],
)
def test_terminal_execution_state_keeps_scc_accounting_optional(
    profile: str,
    accounting_pass: bool,
    failed_case_ids: list[str],
    expected: tuple[str, int],
) -> None:
    assert _terminal_execution_state(
        profile=profile,
        accounting_pass=accounting_pass,
        failed_case_ids=failed_case_ids,
    ) == expected


def test_scc_case_failure_freezes_rerun_without_waiting_for_qacct(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _write_review_run(
        tmp_path, profile="scc", eligible=False, build_package=False
    )
    trace = run / "sessions" / "trace.tsv"
    _write_trace(
        trace,
        [
            (
                "0",
                "cc/000000",
                "702",
                "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "COMPLETED",
                "0",
            ),
            ("1", "aa/111111", "701", "PORTABLE_RUN:RUN_IGV (case_1)", "COMPLETED", "0"),
        ],
    )
    task = json.loads((run / "contract" / "tasks.jsonl").read_text())
    validation = (
        run / "sessions" / "runtime_manifest" / "runtime_manifest_validation" / "validation.json"
    )
    validation_document = json.loads(validation.read_text())
    state = run / "accounting" / "scc" / "generation_001"
    pending = collect_scc_accounting(
        [trace],
        state,
        controller={"native_id": "700"},
        expected_project="fixture-project",
        expected_tasks=[
            {
                "task_id": "0",
                "process": "PORTABLE_RUN:VALIDATE_RUNTIME_MANIFEST",
                "hash": "cc/000000",
                "trace_file": str(trace.resolve()),
                "control_role": "runtime_manifest_validation",
                "control_receipt": str(validation.resolve()),
                "control_receipt_sha256": sha256_file(validation),
                "runtime_manifest_sha256": validation_document[
                    "runtime_manifest_sha256"
                ],
                "runtime_fingerprint_sha256": validation_document[
                    "runtime_fingerprint_sha256"
                ],
            },
            {"task_id": "1"},
        ],
        expected_cases=[task],
        terminal_bundles=[run / "results" / "cases" / "case_1" / "terminal_bundle.json"],
        raw_qacct_by_native_id={
            "701": _qacct_record("701"),
            "702": _qacct_record("702"),
        },
    )
    summary_path = run / "run_summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(
        {
            "status": "ACCOUNTING_PENDING_WITH_CASE_FAILURES",
            "exit_code": 0,
            "profile": "scc",
            "failed_case_ids": ["case_1"],
            "accounting": pending,
            "review_gate": False,
        }
    )
    summary_path.write_text(json.dumps(summary), encoding="utf-8")
    assert not (run / "rerun").exists()

    def not_visible(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        job_id = command[command.index("-j") + 1]
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr=f"error: job id {job_id} not found\n",
        )

    monkeypatch.setattr("ssqtl_igv.accounting.subprocess.run", not_visible)
    still_pending = finalize_scc_run_accounting(run)
    assert still_pending["status"] == "CASE_FAILURES"
    assert still_pending["exit_code"] == 2
    assert still_pending["failed_case_ids"] == ["case_1"]
    assert (run / "rerun").exists()

    finalize_scc_accounting(
        state,
        raw_qacct_by_native_id={
            "700": _qacct_record("700"),
            "701": _qacct_record("701"),
            "702": _qacct_record("702"),
        },
    )
    closed = finalize_scc_run_accounting(run, qacct_command="must-not-be-invoked")
    assert closed["status"] == "CASE_FAILURES"
    assert closed["exit_code"] == 2
    assert closed["review_gate"] is False
    assert closed["accounting"]["status"] == "PASS"
    assert closed["rerun"]["rerun_case_count"] == 1
    rerun_root = run / closed["rerun"]["relative_path"]
    assert (rerun_root / "rerun_manifest.jsonl").is_file()
    assert (rerun_root / "rerun_receipt.json").is_file()
    assert (rerun_root / "SHA256SUMS").is_file()


def test_review_rejects_runtime_fingerprint_drift_between_snapshot_and_run(
    tmp_path: Path,
) -> None:
    run = _write_review_run(tmp_path)
    snapshot_path = run / "contract" / "runtime_manifest.snapshot.json"
    snapshot = json.loads(snapshot_path.read_text())
    snapshot["runtime_fingerprint_sha256"] = "0" * 64
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    identity_path = run / "contract" / "run_identity.json"
    identity = json.loads(identity_path.read_text())
    identity["runtime_manifest_snapshot_sha256"] = sha256_file(snapshot_path)
    identity_path.write_text(json.dumps(identity), encoding="utf-8")
    with pytest.raises(ValueError, match="runtime manifest snapshot"):
        finalize_review(run)


def test_review_journal_is_append_only_and_finalize_uses_latest_decision(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path)
    with pytest.raises(ValueError, match="do not exactly cover"):
        finalize_review(run)
    first = append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="REJECT",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        notes="needs another look",
    )
    second = append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        notes="approved after recheck",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    assert second["sequence"] == 2
    assert second["previous_event_sha256"] == first["event_sha256"]
    receipt = finalize_review(run)
    assert receipt["status"] == "FINALIZED"
    assert receipt["eligible_count"] == receipt["approved_count"] == 1
    assert receipt["journal_event_count"] == 2
    generation = run / "review" / "generations" / receipt["review_generation_id"]
    verify_checksum_tree(generation)
    record = json.loads((generation / "review_records.jsonl").read_text().strip())
    assert record["artifact_review_state"] == "APPROVE"
    assert record["journal_event_sha256"] == second["event_sha256"]
    assert finalize_review(run) == receipt
    with pytest.raises(RuntimeError, match="already finalized"):
        append_review_decision(
            run,
            task_id="case_1",
            artifact_review_state="REJECT",
            scientific_interpretation="NOT_APPLICABLE",
            reviewer="reviewer-1",
        )


def test_review_journal_is_serialized_across_controller_processes(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path)
    context = multiprocessing.get_context("fork")
    processes = [
        context.Process(target=_append_review_from_process, args=(str(run), f"reviewer-{index}"))
        for index in (1, 2)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=5)
        assert process.exitcode == 0
    rows = [
        json.loads(line)
        for line in (run / "review" / "review_journal.jsonl").read_text().splitlines()
    ]
    assert [row["sequence"] for row in rows] == [1, 2]
    assert rows[1]["previous_event_sha256"] == rows[0]["event_sha256"]
    assert finalize_review(run)["journal_event_count"] == 2


def test_campaign_review_uses_one_ledger_and_freezes_only_its_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = _write_review_run(tmp_path, adapter="ssqtl", build_package=False)
    contract_root = run / "contract"
    campaign_root = tmp_path / "campaign"
    (campaign_root / "ledger").mkdir(parents=True)
    campaign_contract_sha = "1" * 64
    master_tasks_sha = "2" * 64
    master_task_set_sha = "3" * 64
    batch_task_set_sha = "4" * 64
    request = {
        "schema_version": "3.0-batch-request",
        "campaign_id": "run_001",
        "campaign_contract_sha256": campaign_contract_sha,
        "batch_id": "generation_001",
        "purpose": "PILOT_QA",
        "master_tasks_sha256": master_tasks_sha,
        "master_task_set_sha256": master_task_set_sha,
        "tasks_sha256": sha256_file(contract_root / "tasks.jsonl"),
        "task_set_sha256": batch_task_set_sha,
        "task_count": 1,
        "source_tasks": [{"task_id": "case_1"}],
    }
    request_path = contract_root / "batch-request.json"
    request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
    binding = {
        "schema_version": "3.0-batch-admission",
        "campaign_root": str(campaign_root),
        "campaign_id": "run_001",
        "campaign_contract_sha256": campaign_contract_sha,
        "batch_id": "generation_001",
        "purpose": "PILOT_QA",
        "batch_request_sha256": sha256_file(request_path),
        "master_tasks_sha256": master_tasks_sha,
        "master_task_set_sha256": master_task_set_sha,
        "task_count": 1,
        "tasks_sha256": sha256_file(contract_root / "tasks.jsonl"),
        "task_set_sha256": batch_task_set_sha,
    }
    binding_path = contract_root / "campaign_binding.json"
    binding_path.write_text(json.dumps(binding, sort_keys=True) + "\n", encoding="utf-8")
    identity_path = contract_root / "run_identity.json"
    identity = json.loads(identity_path.read_text(encoding="utf-8"))
    identity.update(
        {
            "campaign_binding_sha256": sha256_file(binding_path),
            "campaign_id": "run_001",
            "campaign_contract_sha256": campaign_contract_sha,
            "batch_id": "generation_001",
            "batch_purpose": "PILOT_QA",
            "batch_request_sha256": sha256_file(request_path),
            "master_tasks_sha256": master_tasks_sha,
            "master_task_set_sha256": master_task_set_sha,
            "batch_task_set_sha256": batch_task_set_sha,
        }
    )
    identity_path.write_text(json.dumps(identity, sort_keys=True) + "\n", encoding="utf-8")
    _freeze_review_package(run)

    ledger: list[dict[str, object]] = []

    def append_event(
        *, event_type: str, batch_id: str, actor: str, payload: dict[str, object]
    ) -> dict[str, object]:
        event: dict[str, object] = {
            "schema_version": "3.0-campaign-ledger-event",
            "campaign_id": "run_001",
            "campaign_contract_sha256": campaign_contract_sha,
            "sequence": len(ledger) + 1,
            "recorded_at": f"2026-07-21T00:00:{len(ledger):02d}+00:00",
            "actor": actor,
            "batch_id": batch_id,
            "event_type": event_type,
            "payload": payload,
            "previous_event_sha256": ledger[-1]["event_sha256"] if ledger else None,
        }
        event["event_sha256"] = sha256_json(event)
        ledger.append(event)
        (campaign_root / "ledger" / "campaign-ledger.jsonl").write_text(
            "".join(json.dumps(row, sort_keys=True) + "\n" for row in ledger),
            encoding="utf-8",
        )
        return event

    append_event(
        event_type="SELECTION_FROZEN",
        batch_id="generation_001",
        actor="operator",
        payload={"selected_task_count": 1},
    )

    def fake_append_campaign_event(
        _campaign_dir: str | Path,
        *,
        event_type: str,
        batch_id: str,
        actor: str,
        payload: dict[str, object],
    ) -> dict[str, object]:
        return append_event(
            event_type=event_type,
            batch_id=batch_id,
            actor=actor,
            payload=payload,
        )

    monkeypatch.setattr(
        "ssqtl_igv.review_server.verify_campaign_ledger",
        lambda _campaign_dir: [dict(row) for row in ledger],
    )
    monkeypatch.setattr(
        "ssqtl_igv.review_server.append_campaign_event",
        fake_append_campaign_event,
    )

    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="SUPPORTED",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in SSQTL_ASSERTIONS},
    )
    assert not (run / "review" / "review_journal.jsonl").exists()
    receipt = finalize_review(run)
    generation = run / "review" / "generations" / receipt["review_generation_id"]
    assert receipt["ledger_kind"] == "CAMPAIGN_LEDGER_PREFIX"
    assert receipt["batch_purpose"] == "PILOT_QA"
    assert receipt["campaign_id"] == "run_001"
    assert receipt["batch_task_set_sha256"] == batch_task_set_sha
    assert (generation / "campaign_ledger.jsonl").is_file()
    assert not (generation / "review_journal.jsonl").exists()
    assert [row["event_type"] for row in ledger] == [
        "SELECTION_FROZEN",
        "HUMAN_DECISION",
        "REVIEW_FINALIZED",
    ]

    append_event(
        event_type="NEXT_BATCH_AUTHORIZED",
        batch_id="batch-0002",
        actor="operator",
        payload={"prior_batch_id": "generation_001", "next_batch_id": "batch-0002"},
    )
    assert finalize_review(run) == receipt


def test_review_server_requires_localhost_one_time_token_and_csrf(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path)
    with pytest.raises(ValueError, match="exactly to 127.0.0.1"):
        create_review_server(run, "0.0.0.0", 0, "reviewer-1")
    server, access_url = create_review_server(run, reviewer="reviewer-1")
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    base_url = access_url.split("/?", 1)[0] + "/"
    try:
        with pytest.raises(urllib.error.HTTPError) as unauthenticated:
            urllib.request.urlopen(base_url, timeout=2)
        assert unauthenticated.value.code == 403

        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        body = opener.open(access_url, timeout=2).read().decode("utf-8")
        csrf = re.search(r'name="csrf" value="([^"]+)"', body)
        assert csrf
        source_image = run / "results" / "cases" / "case_1" / "review.png"
        source_original = source_image.read_bytes()
        source_image.write_bytes(b"changed-mutable-source-after-packaging")
        assert opener.open(base_url + "artifact?task_id=case_1", timeout=2).read() == (
            source_original
        )
        source_image.write_bytes(source_original)

        review_image = run / "review" / "review_package" / "cases" / "case_1" / "review.png"
        original_image = review_image.read_bytes()
        review_image.write_bytes(b"changed-after-server-start")
        with pytest.raises(urllib.error.HTTPError) as changed_artifact:
            opener.open(base_url + "artifact?task_id=case_1", timeout=2)
        assert changed_artifact.value.code == 409
        review_image.write_bytes(original_image)
        assert opener.open(base_url + "artifact?task_id=case_1", timeout=2).read() == original_image
        with pytest.raises(urllib.error.HTTPError) as replay:
            urllib.request.urlopen(access_url, timeout=2)
        assert replay.value.code == 403

        invalid = urllib.request.Request(
            base_url + "decision",
            data=urllib.parse.urlencode(
                {
                    "task_id": "case_1",
                    "artifact_review_state": "APPROVE",
                    "scientific_interpretation": "NOT_APPLICABLE",
                }
            ).encode(),
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as bad_csrf:
            opener.open(invalid, timeout=2)
        assert bad_csrf.value.code == 403

        form = {
            "csrf": csrf.group(1),
            "task_id": "case_1",
            "artifact_review_state": "APPROVE",
            "scientific_interpretation": "NOT_APPLICABLE",
            **{f"assertion.{name}": "true" for name in GENERIC_ASSERTIONS},
        }
        valid = urllib.request.Request(
            base_url + "decision",
            data=urllib.parse.urlencode(form).encode(),
            method="POST",
        )
        assert b"Review complete" in opener.open(valid, timeout=2).read()
        thread.join(timeout=2)
        assert not thread.is_alive()
    finally:
        server.shutdown()
        server.server_close()
    receipt = finalize_review(run)
    assert receipt["approved_count"] == 1


@pytest.mark.parametrize("summary_state", ["missing", "stale", "malformed"])
def test_review_package_and_server_ignore_non_authoritative_run_summary(
    tmp_path: Path, summary_state: str
) -> None:
    run = _write_review_run(tmp_path, build_package=False)
    summary_path = run / "run_summary.json"
    if summary_state == "missing":
        summary_path.unlink()
    elif summary_state == "stale":
        summary = json.loads(summary_path.read_text())
        summary.update(
            {
                "authoritative": True,
                "projection_kind": "STALE",
                "status": "CASE_FAILURES",
                "failed_case_ids": ["case_1"],
                "accounting": {"provider": "nextflow_trace", "status": "PASS"},
                "source_digests": {
                    "canonical_tasks": "0" * 64,
                    "accounting_receipt": "1" * 64,
                },
            }
        )
        summary_path.write_text(json.dumps(summary), encoding="utf-8")
    else:
        summary_path.write_text("{not-json", encoding="utf-8")

    package = build_review_package_v3(run)
    assert package["eligible_review_count"] == 1
    server, _url = create_review_server(run, reviewer="reviewer-1")
    server.server_close()


def test_review_never_falls_back_to_mutable_case_tree_without_immutable_package(
    tmp_path: Path,
) -> None:
    run = _write_review_run(tmp_path)
    package = run / "review" / "review_package"
    package.rename(run / "review" / "removed-review-package")
    with pytest.raises(ValueError, match="immutable checksum-bound review package"):
        create_review_server(run, reviewer="reviewer-1")


def test_review_and_publication_staging_survive_run_mount_relocation(
    tmp_path: Path,
) -> None:
    run = _write_review_run(tmp_path / "writer")
    moved_parent = tmp_path / "reader"
    moved_parent.mkdir()
    moved = run.rename(moved_parent / "run")

    append_review_decision(
        moved,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    receipt = finalize_review(moved)
    receipt_path = (
        moved
        / "review"
        / "generations"
        / receipt["review_generation_id"]
        / "review_receipt.json"
    )
    staged = build_publication_staging(
        moved, receipt_path, tmp_path / "relocated-publication-staging"
    )
    assert staged["published_case_count"] == 1


def test_review_exact_set_comes_from_immutable_package_not_mutable_case_tree(
    tmp_path: Path,
) -> None:
    run = _write_review_run(tmp_path)
    (run / "results" / "cases" / "case_1" / "case_result.json").unlink()
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    assert finalize_review(run)["status"] == "FINALIZED"


def test_incomplete_ssqtl_evidence_remains_indeterminate_in_v3_review(
    tmp_path: Path,
) -> None:
    run = _write_review_run(
        tmp_path, adapter="ssqtl", evidence_state="EVIDENCE_INCOMPLETE"
    )
    assertions = {name: True for name in SSQTL_ASSERTIONS}
    with pytest.raises(ValueError, match="requires INDETERMINATE"):
        append_review_decision(
            run,
            task_id="case_1",
            artifact_review_state="APPROVE",
            scientific_interpretation="SUPPORTED",
            reviewer="reviewer-1",
            manual_assertions=assertions,
        )
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="INDETERMINATE",
        reviewer="reviewer-1",
        manual_assertions=assertions,
    )
    assert finalize_review(run)["approved_count"] == 1


def test_review_revalidates_accounting_receipt_bytes(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path)
    bundles = run / "accounting" / "local" / "generation-0001" / "terminal_bundles.jsonl"
    bundles.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="receipt checksum drift"):
        finalize_review(run)


def _write_publication_staging(
    root: Path, review_receipt_path: Path, *, marker: str = "STAGED"
) -> None:
    root.mkdir()
    review = json.loads(review_receipt_path.read_text())
    record = json.loads(
        (review_receipt_path.parent / "review_records.jsonl").read_text()
    )
    case_root = root / "cases" / "case_1"
    case_root.mkdir(parents=True)
    image = case_root / "review.png"
    image.write_bytes(b"fixture-review-image\n")
    assert sha256_file(image) == record["review_image_sha256"]
    image_relative = "cases/case_1/review.png"
    (case_root / "metadata.json").write_text(
        json.dumps(
            {
                "task_id": "case_1",
                "manifest_order": 1,
                "artifact_review_state": "APPROVE",
                "publication_state": "PUBLISHED",
                "review_image": image_relative,
                "review_image_sha256": record["review_image_sha256"],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    status = {
        "manifest_order": 1,
        "task_id": "case_1",
        "artifact_review_state": "APPROVE",
        "scientific_interpretation": "NOT_APPLICABLE",
        "publication_state": "PUBLISHED",
        "review_image": image_relative,
        "review_image_sha256": record["review_image_sha256"],
    }
    (root / "public_case_status.jsonl").write_text(
        json.dumps(status, sort_keys=True) + "\n", encoding="utf-8"
    )
    artifact = root / "publication.json"
    artifact.write_text(
        json.dumps(
            {
                "schema_version": "3.0-publication-staging",
                "status": marker,
                "run_id": "run_001",
                "generation_id": "generation_001",
                "review_generation_id": "review_abc",
                "review_receipt_sha256": sha256_file(review_receipt_path),
                "contract_set_sha256": review["contract_set_sha256"],
                "journal_sha256": review["journal_sha256"],
                "terminal_case_count": 1,
                "runtime_binding_sha256": review["runtime_binding_sha256"],
                "runtime_fingerprint_sha256": review["runtime_binding"][
                    "runtime_fingerprint_sha256"
                ],
                "runtime_manifest_sha256": review["runtime_binding"][
                    "runtime_manifest_sha256"
                ],
                "runtime_oci_digest": review["runtime_binding"]["runtime_oci_digest"],
                "published_case_count": 1,
                "withheld_case_count": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    files = sorted(path for path in root.rglob("*") if path.is_file())
    (root / "SHA256SUMS").write_text(
        "".join(f"{sha256_file(path)}  {path.relative_to(root)}\n" for path in files),
        encoding="utf-8",
    )


def _review_receipt() -> dict[str, object]:
    runtime_binding = {
        "schema_version": "3.0-runtime-review-binding",
        "profile": "standalone",
        "run_identity_sha256": "a" * 64,
        "runtime_manifest_sha256": "b" * 64,
        "runtime_manifest_audit_path": "/restricted/private/runtime-manifest.json",
        "runtime_manifest_snapshot_sha256": "c" * 64,
        "runtime_fingerprint_sha256": "f" * 64,
        "runtime_oci_digest": "sha256:" + "d" * 64,
        "runtime_validation_count": 1,
        "runtime_validation_set_sha256": "e" * 64,
        "runtime_validations": [
            {
                "trace_file": "/restricted/private/trace.txt",
                "control_receipt": "/restricted/private/control.json",
                "validation_sha256": "e" * 64,
            }
        ],
    }
    return {
        "schema_version": "3.0-review-receipt",
        "status": "FINALIZED",
        "publication_gate": "READY_FOR_STAGING",
        "all_eligible_decided": True,
        "review_generation_id": "review_abc",
        "run_id": "run_001",
        "generation_id": "generation_001",
        "contract_set_sha256": "c" * 64,
        "journal_sha256": "j" * 64,
        "runtime_binding": runtime_binding,
        "runtime_binding_sha256": sha256_json(runtime_binding),
        "eligible_count": 1,
        "decision_count": 1,
        "approved_count": 1,
        "rejected_count": 0,
        "rerun_required": False,
        "rerun_case_count": 0,
    }


def _write_frozen_review_receipt(root: Path) -> Path:
    review_root = root / "review"
    generation = review_root / "generations" / "review_abc"
    generation.mkdir(parents=True)
    canonical_tasks_sha256 = "f" * 64
    contract_root = root / "contract"
    contract_root.mkdir()
    (contract_root / "run_identity.json").write_text(
        json.dumps({"canonical_tasks_sha256": canonical_tasks_sha256}) + "\n",
        encoding="utf-8",
    )
    receipt = _review_receipt()
    binding = {
        "run_id": "run_001",
        "generation_id": "generation_001",
        "task_id": "case_1",
        "manifest_order": 1,
        "input_fingerprint": "f" * 64,
        "case_result_sha256": "a" * 64,
        "artifact_set_sha256": "b" * 64,
        "review_image_sha256": (
            "74c0425666fdb61459fc9e918d8489bd1fcb1227b142f17ac1d2223fa2d6b63b"
        ),
        "scientific_qc_sha256": "d" * 64,
        "adapter_type": "generic",
        "evidence_state": "COMPLETE",
    }
    assertions = {name: True for name in GENERIC_ASSERTIONS}
    event_payload = {
        "schema_version": "3.0-review-journal",
        "event_type": "DECISION",
        "sequence": 1,
        "previous_event_sha256": None,
        "recorded_at": "2026-07-21T00:00:00Z",
        "task_id": "case_1",
        "artifact_review_state": "APPROVE",
        "scientific_interpretation": "NOT_APPLICABLE",
        "reviewer": "reviewer-1",
        "notes": "",
        "manual_assertions": assertions,
        "contract_binding": binding,
    }
    event_sha = sha256_json(event_payload)
    event = {
        **event_payload,
        "event_id": f"review_evt_{event_sha}",
        "event_sha256": event_sha,
    }
    journal_path = generation / "review_journal.jsonl"
    journal_path.write_text(
        json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt.update(
        {
            "journal_sha256": sha256_file(journal_path),
            "journal_event_count": 1,
            "review_journal": "review_journal.jsonl",
            "review_journal_sha256": sha256_file(journal_path),
        }
    )
    records_path = generation / "review_records.jsonl"
    records_path.write_text(
        json.dumps(
            {
                "schema_version": "3.0",
                "review_record_id": event["event_id"],
                **binding,
                "artifact_review_state": "APPROVE",
                "scientific_interpretation": "NOT_APPLICABLE",
                "reviewer": "reviewer-1",
                "reviewed_at": event["recorded_at"],
                "notes": "",
                "manual_assertions": assertions,
                "journal_event_sha256": event_sha,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    rerun_manifest = generation / "rerun_manifest.jsonl"
    rerun_manifest.write_text("", encoding="utf-8")
    rerun_request_set_sha256 = sha256_json([])
    receipt.update(
        {
            "review_records_sha256": sha256_file(records_path),
            "rerun_manifest": "rerun_manifest.jsonl",
            "rerun_manifest_sha256": sha256_file(rerun_manifest),
            "rerun_receipt": "rerun_receipt.json",
        }
    )
    rerun_receipt_path = generation / "rerun_receipt.json"
    rerun_receipt_path.write_text(
        json.dumps(
            {
                "schema_version": "3.0-rerun-receipt",
                "rerun_id": "review_rejects_" + rerun_request_set_sha256,
                "state": "NOT_REQUIRED",
                "source_run_id": receipt["run_id"],
                "source_generation_id": receipt["generation_id"],
                "canonical_tasks_sha256": canonical_tasks_sha256,
                "rerun_case_count": 0,
                "rerun_request_set_sha256": rerun_request_set_sha256,
                "rerun_manifest": "rerun_manifest.jsonl",
                "rerun_manifest_sha256": receipt["rerun_manifest_sha256"],
                "target_generation_policy": "MUST_DIFFER_FROM_SOURCE_GENERATION",
                "same_generation_resume_allowed": False,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    receipt["rerun_receipt_sha256"] = sha256_file(rerun_receipt_path)
    receipt_path = generation / "review_receipt.json"
    receipt_path.write_text(json.dumps(receipt) + "\n", encoding="utf-8")
    generation_files = sorted(
        (path for path in generation.iterdir() if path.is_file()),
        key=lambda path: path.name,
    )
    (generation / "SHA256SUMS").write_text(
        "".join(f"{sha256_file(path)}  {path.name}\n" for path in generation_files),
        encoding="utf-8",
    )
    (review_root / "finalized_review.json").write_text(
        json.dumps(
            {
                "schema_version": "3.0-finalized-review-pointer",
                "review_generation_id": "review_abc",
                "receipt_relative_path": "generations/review_abc/review_receipt.json",
                "receipt_sha256": sha256_file(receipt_path),
                "checksums_sha256": sha256_file(generation / "SHA256SUMS"),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return receipt_path


def test_publication_promotion_is_receipt_bound_atomic_and_nonduplicating(tmp_path: Path) -> None:
    review_receipt_path = _write_frozen_review_receipt(tmp_path / "review-state")
    staging = tmp_path / "staging"
    _write_publication_staging(staging, review_receipt_path)
    destination = tmp_path / "published"
    receipt_path = tmp_path / "promotion_receipt.json"
    receipt = build_publication_promotion_receipt(
        staging,
        destination,
        review_receipt_path,
        output=receipt_path,
    )
    result = promote_publication(staging, destination, receipt_path)
    assert result["status"] == "PUBLISHED"
    assert result["review_generation_id"] == "review_abc"
    assert not staging.exists()
    verify_checksum_tree(destination)
    recovered = promote_publication(staging, destination, receipt_path)
    assert recovered["recovered_after_atomic_rename"] is True
    completion = Path(recovered["completion_receipt"])
    assert sha256_file(completion) == recovered["completion_receipt_sha256"]

    second_staging = tmp_path / "staging-2"
    _write_publication_staging(second_staging, review_receipt_path)
    second_receipt_path = tmp_path / "second_promotion_receipt.json"
    build_publication_promotion_receipt(
        second_staging,
        destination,
        review_receipt_path,
        output=second_receipt_path,
    )
    with pytest.raises(FileExistsError, match="already exists"):
        promote_publication(second_staging, destination, second_receipt_path)
    assert second_staging.exists()
    assert receipt["authorized_destination"] == str(destination.resolve())


def test_finalized_review_to_staging_to_atomic_publication_is_closed(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path)
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    review_receipt = finalize_review(run)
    review_receipt_path = (
        run
        / "review"
        / "generations"
        / review_receipt["review_generation_id"]
        / "review_receipt.json"
    )
    staging = tmp_path / "closed-loop-staging"
    staged = build_publication_staging(run, review_receipt_path, staging)
    assert staged["published_case_count"] == 1
    public_metadata = json.loads((staging / "publication.json").read_text())
    public_binding = public_metadata["runtime_binding"]
    assert "runtime_manifest_audit_path" not in public_binding
    assert "runtime_validations" not in public_binding
    assert "/restricted/" not in json.dumps(public_metadata)
    destination = tmp_path / "closed-loop-published"
    promotion_path = tmp_path / "closed-loop-promotion.json"
    build_publication_promotion_receipt(
        staging,
        destination,
        review_receipt_path,
        output=promotion_path,
    )
    published = promote_publication(staging, destination, promotion_path)
    assert published["status"] == "PUBLISHED"
    assert published["review_generation_id"] == review_receipt["review_generation_id"]
    verify_checksum_tree(destination)
    assert (destination / "cases" / "case_1" / "review.png").read_bytes() == (
        b"synthetic-review_image\n"
    )


def test_v300_public_cli_publishes_reviewed_standalone_output(tmp_path: Path) -> None:
    run = _write_review_run(tmp_path)
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    finalize_review(run)
    args = v3_cli_parser().parse_args(
        [
            "publish",
            "--output",
            str(run),
            "--destination",
            str(tmp_path / "published-by-cli"),
        ]
    )
    result = v3_cli_publish(args)
    assert result["publication"]["status"] == "PUBLISHED"


def test_public_tree_safety_rejects_absolute_restricted_paths(tmp_path: Path) -> None:
    root = tmp_path / "public"
    root.mkdir()
    (root / "metadata.json").write_text(
        json.dumps({"audit": "/restricted/private/source.json"}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="absolute source path"):
        assert_public_tree_safe(root)


def test_publication_rejects_rewritten_review_records_even_with_regenerated_tree(
    tmp_path: Path,
) -> None:
    run = _write_review_run(tmp_path)
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    receipt = finalize_review(run)
    generation = run / "review" / "generations" / receipt["review_generation_id"]
    records_path = generation / "review_records.jsonl"
    record = json.loads(records_path.read_text(encoding="utf-8"))
    record["notes"] = "rewritten after finalization"
    records_path.write_text(json.dumps(record, sort_keys=True) + "\n", encoding="utf-8")
    files = sorted(
        (
            path
            for path in generation.rglob("*")
            if path.is_file() and path.name != "SHA256SUMS"
        ),
        key=lambda path: str(path.relative_to(generation)),
    )
    (generation / "SHA256SUMS").write_text(
        "".join(
            f"{sha256_file(path)}  {path.relative_to(generation)}\n" for path in files
        ),
        encoding="utf-8",
    )
    pointer_path = run / "review" / "finalized_review.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    pointer["checksums_sha256"] = sha256_file(generation / "SHA256SUMS")
    pointer_path.write_text(json.dumps(pointer), encoding="utf-8")
    receipt_path = generation / "review_receipt.json"
    with pytest.raises(ValueError, match="review_records.jsonl checksum differs from receipt"):
        build_publication_staging(
            run, receipt_path, tmp_path / "rewritten-record-publication"
        )


def test_publication_promotion_rejects_tamper_and_wrong_destination(tmp_path: Path) -> None:
    review_receipt_path = _write_frozen_review_receipt(tmp_path / "review-state")
    staging = tmp_path / "staging"
    _write_publication_staging(staging, review_receipt_path)
    destination = tmp_path / "published"
    with pytest.raises(ValueError, match="outside the staging tree"):
        build_publication_promotion_receipt(
            staging,
            staging / "nested-destination",
            review_receipt_path,
        )
    receipt_path = tmp_path / "promotion_receipt.json"
    build_publication_promotion_receipt(
        staging, destination, review_receipt_path, output=receipt_path
    )
    with pytest.raises(ValueError, match="does not authorize"):
        promote_publication(staging, tmp_path / "other", receipt_path)
    (staging / "publication.json").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(ValueError, match="drift"):
        promote_publication(staging, destination, receipt_path)
    assert staging.exists()
    assert not destination.exists()


def test_checksum_tree_rejects_symlinked_root(tmp_path: Path) -> None:
    root = tmp_path / "tree"
    root.mkdir()
    artifact = root / "artifact.txt"
    artifact.write_text("bound\n", encoding="utf-8")
    (root / "SHA256SUMS").write_text(
        f"{sha256_file(artifact)}  artifact.txt\n", encoding="utf-8"
    )
    linked = tmp_path / "linked-tree"
    os.symlink(root, linked)
    with pytest.raises(ValueError, match="must not be a symlink"):
        verify_checksum_tree(linked)


def test_existing_publication_without_completion_sidecar_is_not_idempotent(
    tmp_path: Path,
) -> None:
    review_receipt_path = _write_frozen_review_receipt(tmp_path / "review-state")
    staging = tmp_path / "staging"
    _write_publication_staging(staging, review_receipt_path)
    destination = tmp_path / "published"
    promotion_path = tmp_path / "promotion.json"
    build_publication_promotion_receipt(
        staging, destination, review_receipt_path, output=promotion_path
    )
    published = promote_publication(staging, destination, promotion_path)
    Path(published["completion_receipt"]).unlink()
    with pytest.raises(ValueError, match="lacks its sidecar completion receipt"):
        promote_publication(staging, destination, promotion_path)


def test_review_uses_frozen_package_but_publication_rejects_debug_source(
    tmp_path: Path,
) -> None:
    review_run = _write_review_run(tmp_path / "review-case")
    review_case_path = (
        review_run / "results" / "cases" / "case_1" / "case_result.json"
    )
    review_case = json.loads(review_case_path.read_text(encoding="utf-8"))
    review_case["debug_only"] = True
    review_case_path.write_text(json.dumps(review_case), encoding="utf-8")
    append_review_decision(
        review_run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    assert finalize_review(review_run)["status"] == "FINALIZED"

    publication_run = _write_review_run(tmp_path / "publication-case")
    append_review_decision(
        publication_run,
        task_id="case_1",
        artifact_review_state="APPROVE",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        manual_assertions={name: True for name in GENERIC_ASSERTIONS},
    )
    review_receipt = finalize_review(publication_run)
    review_receipt_path = (
        publication_run
        / "review"
        / "generations"
        / review_receipt["review_generation_id"]
        / "review_receipt.json"
    )
    publication_case_path = (
        publication_run / "results" / "cases" / "case_1" / "case_result.json"
    )
    publication_case = json.loads(
        publication_case_path.read_text(encoding="utf-8")
    )
    publication_case["artifact_class"] = "DEBUG_ONLY"
    publication_case_path.write_text(json.dumps(publication_case), encoding="utf-8")
    with pytest.raises(ValueError, match="DEBUG_ONLY"):
        build_publication_staging(
            publication_run,
            review_receipt_path,
            tmp_path / "blocked-publication-staging",
        )


def test_rejected_review_writes_checksum_bound_new_generation_rerun_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = _write_review_run(tmp_path)
    append_review_decision(
        run,
        task_id="case_1",
        artifact_review_state="REJECT",
        scientific_interpretation="NOT_APPLICABLE",
        reviewer="reviewer-1",
        notes="rerender with corrected display policy",
    )
    receipt = finalize_review(run)
    generation = run / "review" / "generations" / receipt["review_generation_id"]
    verify_checksum_tree(generation)
    rerun_manifest = generation / "rerun_manifest.jsonl"
    rerun_receipt_path = generation / "rerun_receipt.json"
    rerun_rows = [json.loads(line) for line in rerun_manifest.read_text().splitlines()]
    rerun_receipt = json.loads(rerun_receipt_path.read_text(encoding="utf-8"))

    assert receipt["rejected_count"] == receipt["rerun_case_count"] == 1
    assert receipt["rerun_required"] is True
    assert receipt["rerun_manifest_sha256"] == sha256_file(rerun_manifest)
    assert receipt["rerun_receipt_sha256"] == sha256_file(rerun_receipt_path)
    assert rerun_rows[0]["source_task_id"] == "case_1"
    assert rerun_rows[0]["control_action"] == "CREATE_NEW_GENERATION"
    assert rerun_rows[0]["same_generation_resume_allowed"] is False
    assert rerun_receipt["state"] == "RERUN_REQUIRED"
    assert rerun_receipt["rerun_manifest_sha256"] == sha256_file(rerun_manifest)
    assert rerun_receipt["target_generation_policy"] == (
        "MUST_DIFFER_FROM_SOURCE_GENERATION"
    )

    # _write_review_run intentionally uses a minimal review-only canonical
    # fixture; the rerun module's own tests cover full task-schema validation.
    monkeypatch.setattr(
        "ssqtl_igv.rerun_v3.validate_v3_task_document", lambda _task: None
    )
    imported = prepare_rerun_task_set(
        run,
        rerun_receipt_path,
        tmp_path / "review-reject-rerun-import",
        run_id="run_001",
        generation_id="generation_review_fix_001",
    )
    assert imported["task_count"] == 1
    assert imported["generation_id"] == "generation_review_fix_001"
    assert imported["source_rerun_id"] == rerun_receipt["rerun_id"]
