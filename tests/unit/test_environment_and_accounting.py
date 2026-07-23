from __future__ import annotations

import json
from pathlib import Path

import pytest

from ssqtl_igv.accounting import collect_nextflow_accounting
from ssqtl_igv.environment import validate_environment


ZERO_COMMIT = "0" * 40
REAL_COMMIT = "1" * 40


def test_environment_missing_command_is_test_only_relaxation(tmp_path: Path) -> None:
    report = validate_environment(
        tmp_path / "environment",
        phase="run_shard",
        pipeline_commit=ZERO_COMMIT,
        nextflow_version="25.04.7",
        required_commands=["command-that-cannot-exist-igv-test"],
        test_mode=True,
    )
    assert report["status"] == "PASS_WITH_TEST_RELAXATIONS"
    assert report["missing_commands"] == ["command-that-cannot-exist-igv-test"]
    with pytest.raises(RuntimeError, match="required runtime commands"):
        validate_environment(
            tmp_path / "environment-production",
            phase="run_shard",
            pipeline_commit=REAL_COMMIT,
            nextflow_version="25.04.7",
            required_commands=["command-that-cannot-exist-igv-test"],
        )


def test_environment_rejects_placeholder_commit_outside_test_mode(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="identify a real commit"):
        validate_environment(
            tmp_path / "environment-production",
            phase="plan",
            pipeline_commit=ZERO_COMMIT,
            nextflow_version="25.04.7",
        )


def test_accounting_accepts_cached_trace_without_qacct(tmp_path: Path) -> None:
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\n"
        "1\taa/bbbbbb\t123\tRUN_SHARD:QC_CASE (case_1)\tCACHED\t0\n",
        encoding="utf-8",
    )
    report = collect_nextflow_accounting(
        [trace],
        tmp_path / "accounting",
        skip_qacct=True,
        test_mode=True,
    )
    assert report == {
        "schema_version": "2.0-nextflow-qacct",
        "created_at": report["created_at"],
        "status": "SKIPPED_TEST_MODE",
        "trace_file_count": 1,
        "trace_task_count": 1,
        "cached_task_count": 1,
        "accounted_task_count": 0,
        "skipped_task_count": 0,
        "output_dir": str((tmp_path / "accounting").resolve()),
    }
    frozen = json.loads((tmp_path / "accounting" / "accounting.json").read_text())
    assert frozen["cached_task_count"] == 1


def test_accounting_skip_is_forbidden_outside_test_mode(tmp_path: Path) -> None:
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="only in explicit test mode"):
        collect_nextflow_accounting(
            [trace],
            tmp_path / "accounting",
            skip_qacct=True,
            test_mode=False,
        )
