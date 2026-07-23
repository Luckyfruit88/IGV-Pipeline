from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from ssqtl_igv import orchestrator_v3, v3_cli


def _project(tmp_path: Path) -> Path:
    (tmp_path / "cases.tsv").write_text("header\n", encoding="utf-8")
    (tmp_path / "reference.yaml").write_text("fixture\n", encoding="utf-8")
    project = tmp_path / "project.yaml"
    project.write_text(
        'schema_version: "3.0"\n'
        "adapter: generic\n"
        "inputs: {cases: cases.tsv}\n"
        "reference: reference.yaml\n",
        encoding="utf-8",
    )
    return project


def test_public_run_parser_exposes_only_pull_and_run_options() -> None:
    parser = v3_cli._parser()
    run_parser = next(
        action.choices["run"]
        for action in parser._actions
        if isinstance(action, __import__("argparse")._SubParsersAction)
    )
    options = {
        option
        for action in run_parser._actions
        for option in action.option_strings
        if option not in {"-h", "--help"}
    }
    assert options == {
        "--project",
        "--output",
        "--work",
        "--resume",
        "--max-parallel",
        "--max-cases-per-shard",
    }
    args = parser.parse_args(["run"])
    assert args.project == "/project/project.yaml"
    assert args.output == "/output"
    assert args.work is None
    assert args.max_parallel == "auto"
    assert args.max_cases_per_shard == 256


def test_cli_reports_invalid_project_types_as_structured_fatal(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    project = tmp_path / "project.yaml"
    project.write_text("[]\n", encoding="utf-8")

    assert v3_cli.main(["doctor", "--project", str(project)]) == 1

    error = json.loads(capsys.readouterr().err)
    assert error["status"] == "INFRASTRUCTURE_FATAL"
    assert error["error_type"] == "TypeError"
    assert "one mapping" in error["message"]


def test_generic_run_uses_project_and_embedded_runtime_without_identity_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = _project(tmp_path)
    runtime = tmp_path / "runtime-manifest.json"
    runtime.write_text("{}\n", encoding="utf-8")
    output = tmp_path / "output"
    captured: dict[str, dict] = {}

    monkeypatch.setenv("IGV_RUNTIME_MANIFEST_INTERNAL", str(runtime))

    def prepare(**kwargs: object) -> dict:
        captured["prepare"] = dict(kwargs)
        return {"run_dir": str(output), "identity": {}}

    def execute(_prepared: dict, **kwargs: object) -> dict:
        captured["execute"] = dict(kwargs)
        return {"status": "SNAPSHOTS_READY", "exit_code": 0}

    monkeypatch.setattr(v3_cli, "prepare_portable_run", prepare)
    monkeypatch.setattr(v3_cli, "execute_portable_run", execute)
    args = v3_cli._parser().parse_args(
        [
            "run",
            "--project",
            str(project),
            "--output",
            str(output),
            "--max-parallel",
            "3",
        ]
    )

    result, code = v3_cli._run(args)

    assert (result["status"], code) == ("SNAPSHOTS_READY", 0)
    assert captured["prepare"]["adapter"] == "generic"
    assert captured["prepare"]["manifest"] == str((tmp_path / "cases.tsv").resolve())
    assert captured["prepare"]["runtime_identity_path"] == runtime
    assert captured["prepare"]["project_binding"]["schema_version"] == (
        "3.0-project-source-binding"
    )
    assert len(captured["prepare"]["project_binding"]["binding_sha256"]) == 64
    assert captured["prepare"]["max_parallel"] == 3
    assert captured["execute"]["profile"] == "standalone"
    assert captured["execute"]["work_dir"] == output / ".work"
    assert captured["execute"]["max_parallel"] == 3


def test_resume_rejects_unpublished_runtime_identity_contract(tmp_path: Path) -> None:
    output = tmp_path / "output"
    (output / "contract").mkdir(parents=True)
    (output / "contract" / "run_identity.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "generation_id": "generation-001",
                "adapter": "generic",
                "runtime_identity_sha256": "a" * 64,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="start a new output directory"):
        v3_cli._resume_identity(output)


def test_resume_identity_requires_and_reads_project_source_binding(tmp_path: Path) -> None:
    output = tmp_path / "output"
    (output / "contract").mkdir(parents=True)
    expected = {
        "run_id": "run-1",
        "generation_id": "generation-001",
        "adapter": "generic",
        "runtime_fingerprint_sha256": "a" * 64,
        "project_binding_sha256": "b" * 64,
    }
    (output / "contract" / "run_identity.json").write_text(
        json.dumps(expected), encoding="utf-8"
    )

    assert v3_cli._resume_identity(output) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [("1", 1), ("8", 8), (3, 3)],
)
def test_explicit_max_parallel(value: str | int, expected: int) -> None:
    assert orchestrator_v3.resolve_max_parallel(value) == expected


@pytest.mark.parametrize("value", ["0", "9", "1.5", "many", "01"])
def test_invalid_max_parallel_fails_closed(value: str) -> None:
    with pytest.raises(ValueError, match="max-parallel"):
        orchestrator_v3.resolve_max_parallel(value)


def test_auto_parallelism_uses_cpu_and_eight_gib_memory_slots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orchestrator_v3, "_available_cpu_count", lambda: 6)
    monkeypatch.setattr(
        orchestrator_v3,
        "_available_memory_bytes",
        lambda: 40 * 1024**3,
    )
    assert orchestrator_v3.resolve_max_parallel("auto") == 5
    monkeypatch.setattr(orchestrator_v3, "_available_memory_bytes", lambda: None)
    assert orchestrator_v3.resolve_max_parallel("auto") == 1


def _case_result(task_id: str, order: int, *, eligible: bool) -> dict:
    root = f"results/cases/{task_id}"
    return {
        "task_id": task_id,
        "manifest_order": order,
        "eligible": eligible,
        "adapter_type": "generic",
        "scientific_interpretation": "NOT_APPLICABLE",
        "input_fingerprint": ("a" if eligible else "b") * 64,
        "artifacts": (
            {
                "review_image": {
                    "relative_path": f"{root}/review.png",
                    "sha256": "c" * 64,
                    "size": 10,
                },
                "raw_igv": {
                    "relative_path": f"{root}/raw/igv.png",
                    "sha256": "d" * 64,
                    "size": 10,
                },
            }
            if eligible
            else {}
        ),
        "failures": (
            []
            if eligible
            else [{"code": "CASE_RENDER_FAILED", "message": "fixture failure"}]
        ),
    }


def test_direct_output_tables_cover_ready_and_failed_cases(tmp_path: Path) -> None:
    tasks = [{"task_id": "ready"}, {"task_id": "failed"}]
    results = [
        _case_result("ready", 1, eligible=True),
        _case_result("failed", 2, eligible=False),
    ]

    projection = orchestrator_v3._write_direct_output_tables(tmp_path, tasks, results)

    with (tmp_path / "snapshots.tsv").open(encoding="utf-8", newline="") as handle:
        snapshots = list(csv.DictReader(handle, delimiter="\t"))
    with (tmp_path / "failed_cases.tsv").open(encoding="utf-8", newline="") as handle:
        failures = list(csv.DictReader(handle, delimiter="\t"))
    assert [row["task_id"] for row in snapshots] == ["ready", "failed"]
    assert snapshots[0]["status"] == "SNAPSHOT_READY"
    assert snapshots[0]["review_png"] == "results/cases/ready/review.png"
    assert snapshots[1]["status"] == "CASE_FAILED"
    assert failures == [
        {
            "manifest_order": "2",
            "task_id": "failed",
            "failure_code": "CASE_RENDER_FAILED",
            "message": "fixture failure",
            "case_result_json": "results/cases/failed/case_result.json",
            "input_fingerprint": "b" * 64,
        }
    ]
    assert projection["snapshot_count"] == 1
    assert projection["failed_case_count"] == 1


def test_trace_report_combines_nextflow_sources_and_freezes_digests(tmp_path: Path) -> None:
    header = "task_id\thash\tnative_id\tname\tstatus\texit\n"
    first = tmp_path / "first.trace"
    second = tmp_path / "second.trace"
    first.write_text(header + "1\ta1\t-\tRUN(a)\tCOMPLETED\t0\n", encoding="utf-8")
    second.write_text(header + "2\tb2\t-\tRUN(b)\tCACHED\t0\n", encoding="utf-8")

    report = orchestrator_v3._write_trace_report(tmp_path, [first, second])

    combined = (tmp_path / "reports" / "trace.txt").read_text(encoding="utf-8")
    assert combined.count("task_id\thash") == 1
    assert "RUN(a)" in combined and "RUN(b)" in combined
    sources = json.loads(
        (tmp_path / "reports" / "trace.sources.json").read_text(encoding="utf-8")
    )
    assert sources["authoritative"] is False
    assert len(sources["sources"]) == 2
    assert report["trace_relative_path"] == "reports/trace.txt"


def test_terminal_state_makes_review_optional_and_qacct_nonblocking() -> None:
    assert orchestrator_v3._terminal_execution_state(
        profile="standalone", accounting_pass=True, failed_case_ids=[]
    ) == ("SNAPSHOTS_READY", 0)
    assert orchestrator_v3._terminal_execution_state(
        profile="scc", accounting_pass=False, failed_case_ids=[]
    ) == ("SNAPSHOTS_READY", 0)
    assert orchestrator_v3._terminal_execution_state(
        profile="standalone", accounting_pass=True, failed_case_ids=["failed"]
    ) == ("CASE_FAILURES", 2)
