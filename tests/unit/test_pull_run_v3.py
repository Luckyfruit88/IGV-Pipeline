from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from ssqtl_igv import orchestrator_v3, v3_cli
from ssqtl_igv.project_admission_v3 import merge_snapshot_outputs
from ssqtl_igv.utils import sha256_file


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
        "--igv-cpus",
        "--igv-memory",
        "--igv-timeout",
        "--normalization-cpus",
        "--normalization-memory",
        "--normalization-timeout",
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
    captured: dict[str, object] = {}

    monkeypatch.setenv("IGV_RUNTIME_MANIFEST_INTERNAL", str(runtime))

    def launch(**kwargs: object) -> tuple[dict, int]:
        captured.update(kwargs)
        return {"status": "SNAPSHOTS_READY", "exit_code": 0}, 0

    monkeypatch.setattr(v3_cli, "run_project_workflow", launch)
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
    assert captured == {
        "project": str(project),
        "batch_request": None,
        "output": output,
        "work": None,
        "resume": False,
        "max_parallel": "3",
        "max_cases_per_shard": 256,
        "runtime_manifest": runtime,
        "igv_cpus": 1,
        "igv_memory": "8GiB",
        "igv_timeout": "30m",
        "normalization_cpus": 1,
        "normalization_memory": "12GiB",
        "normalization_timeout": "36h",
    }


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


def _case_result(
    task_id: str, order: int, *, eligible: bool, review_sha256: str = ""
) -> dict:
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
                    "sha256": review_sha256,
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
    tasks = [
        {
            "task_id": task_id,
            "adapter_id": "generic",
            "core": {"locus": {"contig": "chr11"}},
        }
        for task_id in ("ready", "failed")
    ]
    review = tmp_path / "results/cases/ready/review.png"
    review.parent.mkdir(parents=True)
    review.write_bytes(b"0123456789")
    results = [
        _case_result("ready", 1, eligible=True, review_sha256=sha256_file(review)),
        _case_result("failed", 2, eligible=False),
    ]

    projection = orchestrator_v3._write_direct_output_tables(tmp_path, tasks, results)

    with (tmp_path / "snapshots.tsv").open(encoding="utf-8", newline="") as handle:
        snapshots = list(csv.DictReader(handle, delimiter="\t"))
    with (tmp_path / "failed_cases.tsv").open(encoding="utf-8", newline="") as handle:
        failures = list(csv.DictReader(handle, delimiter="\t"))
    assert [row["task_id"] for row in snapshots] == ["ready", "failed"]
    assert snapshots[0]["status"] == "SNAPSHOT_READY"
    assert snapshots[0]["chromosome"] == "chr11"
    assert snapshots[0]["relative_path"] == "snapshots/chr11/ready.png"
    assert snapshots[0]["sha256"] == sha256_file(review)
    assert (tmp_path / snapshots[0]["relative_path"]).read_bytes() == review.read_bytes()
    assert snapshots[1]["status"] == "CASE_FAILED"
    assert failures == [
        {
            "manifest_order": "2",
            "task_id": "failed",
            "chromosome": "chr11",
            "failure_code": "CASE_RENDER_FAILED",
            "message": "fixture failure",
            "input_fingerprint": "b" * 64,
        }
    ]
    assert projection["snapshot_count"] == 1
    assert projection["failed_case_count"] == 1


def test_all_failed_batch_still_publishes_an_empty_snapshot_directory(
    tmp_path: Path,
) -> None:
    tasks = [
        {
            "task_id": "failed",
            "adapter_id": "generic",
            "core": {"locus": {"contig": "chr11"}},
        }
    ]
    projection = orchestrator_v3._write_direct_output_tables(
        tmp_path, tasks, [_case_result("failed", 1, eligible=False)]
    )

    assert (tmp_path / "snapshots").is_dir()
    assert list((tmp_path / "snapshots").iterdir()) == []
    assert projection["snapshot_count"] == 0
    assert projection["failed_case_count"] == 1


def _ssqtl_snapshot_task(
    task_id: str = "AG_chr13_79342833_79342832__SNP_chr13_79342859_T_C",
) -> dict:
    return {
        "task_id": task_id,
        "adapter_id": "ssqtl",
        "core": {"locus": {"contig": "chr13"}},
        "adapter_data": {
            "ag": {
                "chrom": "chr13",
                "source_start": 79342833,
                "source_end": 79342832,
            },
            "snp": {
                "chrom": "chr13",
                "position": 79342859,
                "ref": "T",
                "alt": "C",
            },
        },
    }


def test_ssqtl_snapshot_path_preserves_reverse_strand_source_coordinates() -> None:
    chromosome, relative = orchestrator_v3._snapshot_target(_ssqtl_snapshot_task())
    assert chromosome == "chr13"
    assert relative == (
        "snapshots/chr13/"
        "AG_chr13_79342833_79342832__SNP_chr13_79342859_T_C.png"
    )


def test_ssqtl_snapshot_publication_rejects_cross_chromosome_pair() -> None:
    task = _ssqtl_snapshot_task()
    task["adapter_data"]["snp"]["chrom"] = "chr14"
    with pytest.raises(ValueError, match="AG and SNP chromosomes differ"):
        orchestrator_v3._snapshot_target(task)


def test_ssqtl_snapshot_publication_rejects_task_id_identity_drift() -> None:
    task = _ssqtl_snapshot_task(
        "AG_chr13_79342832_79342833__SNP_chr13_79342859_T_C"
    )
    with pytest.raises(ValueError, match="task_id differs"):
        orchestrator_v3._snapshot_target(task)


def _snapshot_product(root: Path, rows: list[tuple[int, str, str, bytes]]) -> None:
    (root / ".igv-pipeline").mkdir(parents=True)
    snapshot_lines = [
        "manifest_order\ttask_id\tchromosome\trelative_path\tsha256\tstatus"
    ]
    for order, task_id, chromosome, payload in rows:
        image = root / f"snapshots/{chromosome}/{task_id}.png"
        image.parent.mkdir(parents=True, exist_ok=True)
        image.write_bytes(payload)
        snapshot_lines.append(
            f"{order}\t{task_id}\t{chromosome}\t"
            f"snapshots/{chromosome}/{task_id}.png\t{sha256_file(image)}\tSNAPSHOT_READY"
        )
    (root / "snapshots.tsv").write_text(
        "\n".join(snapshot_lines) + "\n", encoding="utf-8"
    )
    (root / "failed_cases.tsv").write_text(
        "manifest_order\ttask_id\tchromosome\tfailure_code\tmessage\tinput_fingerprint\n",
        encoding="utf-8",
    )
    (root / "run_summary.json").write_text(
        '{"authoritative":false,"status":"SNAPSHOTS_READY","exit_code":0}\n',
        encoding="utf-8",
    )


def test_snapshot_batches_append_and_replay_idempotently(tmp_path: Path) -> None:
    destination = tmp_path / "production"
    incoming = tmp_path / "batch-002"
    _snapshot_product(destination, [(1, "case_1", "chr11", b"one")])
    _snapshot_product(incoming, [(2, "case_2", "chr13", b"two")])

    merged = merge_snapshot_outputs(destination, incoming)
    replayed = merge_snapshot_outputs(destination, incoming)

    assert merged["status"] == "PUBLISHED"
    assert merged["commit_mode"] == "LOCKED_POSIX_RENAME_NFS_COMPAT"
    assert merged["added_case_count"] == 1
    assert replayed["status"] == "IDEMPOTENT"
    with (destination / "snapshots.tsv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    assert [row["task_id"] for row in rows] == ["case_1", "case_2"]
    assert (destination / rows[1]["relative_path"]).read_bytes() == b"two"


def test_snapshot_batch_rejects_same_task_with_different_checksum(
    tmp_path: Path,
) -> None:
    destination = tmp_path / "production"
    incoming = tmp_path / "batch-002"
    _snapshot_product(destination, [(1, "case_1", "chr11", b"one")])
    _snapshot_product(incoming, [(1, "case_1", "chr11", b"different")])
    original_index = (destination / "snapshots.tsv").read_bytes()

    with pytest.raises(ValueError, match="different metadata/checksum"):
        merge_snapshot_outputs(destination, incoming)

    assert (destination / "snapshots.tsv").read_bytes() == original_index
    assert (destination / "snapshots/chr11/case_1.png").read_bytes() == b"one"


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
