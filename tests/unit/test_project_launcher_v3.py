from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from ssqtl_igv import project_launcher


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "project.yaml"
    project.write_text('schema_version: "3.0"\n', encoding="utf-8")
    runtime = tmp_path / "runtime-manifest.json"
    runtime.write_text("{}\n", encoding="utf-8")
    return project, runtime


def test_native_command_maps_cli_options_to_one_project_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, runtime = _inputs(tmp_path)
    monkeypatch.setattr(project_launcher, "_nextflow_executable", lambda _value: "nextflow")
    monkeypatch.setattr(project_launcher, "_project_root", lambda: Path("/pipeline"))

    command, output, work = project_launcher.build_project_run_command(
        project=project,
        batch_request=None,
        output=tmp_path / "output",
        work=None,
        resume=True,
        max_parallel="auto",
        max_cases_per_shard=64,
        runtime_manifest=runtime,
        igv_cpus=2,
        igv_memory="10GiB",
        igv_timeout="45m",
        normalization_cpus=3,
        normalization_memory="20GiB",
        normalization_timeout="12h",
    )

    assert command[:4] == [
        "nextflow",
        "run",
        "/pipeline",
        "-profile",
    ]
    assert command.count("run") == 1
    assert command[command.index("--project") + 1] == str(project.resolve())
    assert command[command.index("--output") + 1] == str(output)
    assert command[command.index("--max_parallel") + 1] == "auto"
    assert command[command.index("--max_cases_per_shard") + 1] == "64"
    assert command[command.index("--igv_memory") + 1] == "10GiB"
    assert command[-1] == "-resume"
    assert work == output / ".work"


def test_native_command_requires_exactly_one_input_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, runtime = _inputs(tmp_path)
    request = tmp_path / "batch-request.json"
    request.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(project_launcher, "_nextflow_executable", lambda _value: "nextflow")

    common = {
        "output": tmp_path / "output",
        "work": None,
        "resume": False,
        "max_parallel": "auto",
        "max_cases_per_shard": 256,
        "runtime_manifest": runtime,
    }
    with pytest.raises(ValueError, match="exactly one"):
        project_launcher.build_project_run_command(
            project=None, batch_request=None, **common
        )
    with pytest.raises(ValueError, match="exactly one"):
        project_launcher.build_project_run_command(
            project=project, batch_request=request, **common
        )


def _completed_output(
    root: Path, *, eligible: bool, duplicate_final_trace: bool = False
) -> None:
    (root / "contract").mkdir(parents=True)
    (root / "results" / "cases" / "case_1").mkdir(parents=True)
    (root / "reports").mkdir(parents=True)
    (root / "contract" / "tasks.jsonl").write_text(
        '{"task_id":"case_1"}\n', encoding="utf-8"
    )
    (root / "results" / "cases" / "case_1" / "case_result.json").write_text(
        json.dumps({"task_id": "case_1", "eligible": eligible}) + "\n",
        encoding="utf-8",
    )
    (root / "results" / "cases" / "case_1" / "terminal_bundle.json").write_text(
        json.dumps({"task_id": "case_1", "status": "SUCCEEDED"}) + "\n",
        encoding="utf-8",
    )
    rows = "1\taa\t-\tPROJECT_RUN:RUN_PORTABLE_CASE (case_1)\tCOMPLETED\t0\n"
    if duplicate_final_trace:
        rows += "2\tbb\t-\tPROJECT_RUN:RUN_PORTABLE_CASE (case_1)\tCACHED\t0\n"
    (root / "reports" / "trace.txt").write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\n" + rows,
        encoding="utf-8",
    )
    status = "SNAPSHOTS_READY" if eligible else "CASE_FAILURES"
    (root / "run_summary.json").write_text(
        json.dumps(
            {
                "authoritative": False,
                "status": status,
                "exit_code": 0 if eligible else 2,
                "expected_case_count": 1,
                "observed_case_count": 1,
            }
        )
        + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize(("eligible", "exit_code"), [(True, 0), (False, 2)])
def test_postflight_maps_terminal_evidence_to_product_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    eligible: bool,
    exit_code: int,
) -> None:
    output = tmp_path / "output"
    _completed_output(output, eligible=eligible)
    monkeypatch.setattr(
        project_launcher,
        "validate_v3_terminal_bundle_document",
        lambda _bundle, _case: None,
    )

    result = project_launcher.validate_project_postflight(output)

    assert result["exit_code"] == exit_code
    assert result["postflight"]["product_exit_code"] == exit_code
    assert result["postflight"]["trace_attempt_counts"] == {"case_1": 1}
    assert (output / "reports" / "postflight.json").is_file()


def test_postflight_rejects_duplicate_terminal_trace_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "output"
    _completed_output(output, eligible=True, duplicate_final_trace=True)
    monkeypatch.setattr(
        project_launcher,
        "validate_v3_terminal_bundle_document",
        lambda _bundle, _case: None,
    )

    with pytest.raises(ValueError, match="exactly one"):
        project_launcher.validate_project_postflight(output)


def test_postflight_accepts_failed_retry_before_one_final_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "output"
    _completed_output(output, eligible=True)
    (output / "reports" / "trace.txt").write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\tattempt\n"
        "1\taa\t-\tPROJECT_RUN:RUN_PORTABLE_CASE (case_1)\tFAILED\t75\t1\n"
        "2\tbb\t-\tPROJECT_RUN:RUN_PORTABLE_CASE (case_1)\tCOMPLETED\t0\t2\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        project_launcher,
        "validate_v3_terminal_bundle_document",
        lambda _bundle, _case: None,
    )

    result = project_launcher.validate_project_postflight(output)

    assert result["exit_code"] == 0
    assert result["postflight"]["trace_attempt_counts"] == {"case_1": 2}


def test_postflight_rejects_deterministic_failure_as_retry_lineage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "output"
    _completed_output(output, eligible=True)
    (output / "reports" / "trace.txt").write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\tattempt\n"
        "1\taa\t-\tPROJECT_RUN:RUN_PORTABLE_CASE (case_1)\tFAILED\t1\t1\n"
        "2\tbb\t-\tPROJECT_RUN:RUN_PORTABLE_CASE (case_1)\tCOMPLETED\t0\t2\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        project_launcher,
        "validate_v3_terminal_bundle_document",
        lambda _bundle, _case: None,
    )

    with pytest.raises(ValueError, match="non-resource retry"):
        project_launcher.validate_project_postflight(output)


def test_launcher_invokes_nextflow_once_then_runs_postflight(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project, runtime = _inputs(tmp_path)
    calls: list[list[str]] = []
    monkeypatch.setattr(project_launcher, "_nextflow_executable", lambda _value: "nextflow")
    monkeypatch.setattr(project_launcher, "_project_root", lambda: Path("/pipeline"))
    monkeypatch.setattr(
        project_launcher.subprocess,
        "run",
        lambda command, **_kwargs: (
            calls.append(list(command)) or SimpleNamespace(returncode=0)
        ),
    )
    monkeypatch.setattr(
        project_launcher,
        "validate_project_postflight",
        lambda _output: {"status": "CASE_FAILURES", "exit_code": 2},
    )

    result, code = project_launcher.run_project_workflow(
        project=project,
        batch_request=None,
        output=tmp_path / "output",
        work=None,
        resume=False,
        max_parallel="auto",
        max_cases_per_shard=256,
        runtime_manifest=runtime,
    )

    assert (result["status"], code) == ("CASE_FAILURES", 2)
    assert len(calls) == 1
    assert calls[0].count("run") == 1
