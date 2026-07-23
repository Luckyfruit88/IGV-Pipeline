from __future__ import annotations

import json
import os
import shutil
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

from .bundles import verify_stage_bundle
from .contracts import (
    PIPELINE_VERSION,
    validate_case_result_document,
    validate_schema_document,
    validate_task_document,
    validate_unique_task_set,
)
from .identity import task_set_fingerprint
from .utils import (
    atomic_write_json,
    read_jsonl,
    sha256_file,
    sha256_json,
    utc_now,
    write_jsonl,
    write_tsv,
)


RERUN_FIELDS = [
    "manifest_order",
    "task_id",
    "shard_id",
    "generation_id",
    "input_fingerprint",
    "failure_codes",
    "rerun_reason",
]


def _new_output_directory(destination: str | Path) -> tuple[Path, Path]:
    final = Path(destination).expanduser().resolve(strict=False)
    final.parent.mkdir(parents=True, exist_ok=True)
    if final.exists() or final.is_symlink():
        raise FileExistsError(f"reconciliation output already exists: {final}")
    staging = final.parent / f".{final.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    return final, staging


def _load_tasks(path: str | Path, *, contiguous: bool) -> list[dict[str, Any]]:
    source = Path(path).expanduser()
    if source.is_symlink():
        raise ValueError(f"task manifest must not be a symlink: {source}")
    source = source.resolve(strict=True)
    tasks = list(read_jsonl(source))
    if not tasks:
        raise ValueError("task manifest is empty")
    for task in tasks:
        validate_task_document(task)
    if contiguous:
        return validate_unique_task_set(tasks)
    task_ids = [task["task_id"] for task in tasks]
    orders = [int(task["manifest_order"]) for task in tasks]
    if len(task_ids) != len(set(task_ids)) or len(orders) != len(set(orders)):
        raise ValueError("shard manifest contains duplicate task_id or manifest_order")
    return sorted(tasks, key=lambda task: int(task["manifest_order"]))


def _failure_codes(case_result: dict[str, Any]) -> str:
    return ",".join(failure["code"] for failure in case_result["failures"])


def _rerun_rows(results: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "manifest_order": result["manifest_order"],
            "task_id": result["task_id"],
            "shard_id": result["shard_id"],
            "generation_id": result["generation_id"],
            "input_fingerprint": result["input_fingerprint"],
            "failure_codes": _failure_codes(result),
            "rerun_reason": result["rerun_reason"] or "",
        }
        for result in results
        if result["rerun_eligible"]
    ]


def summarize_shard(
    shard_manifest: str | Path,
    qc_bundle_dirs: Iterable[str | Path],
    output_dir: str | Path,
    *,
    shard_id: str,
    session_id: str,
    pipeline_commit: str,
    controller_job_id: str | None = None,
    ledger_sequence: int = 1,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Reconcile one immutable shard manifest with explicit QC bundle inputs."""

    if len(pipeline_commit) != 40 or any(c not in "0123456789abcdef" for c in pipeline_commit):
        raise ValueError("pipeline_commit must be a full lowercase Git commit")
    tasks = _load_tasks(shard_manifest, contiguous=False)
    run_ids = {task["run_id"] for task in tasks}
    generation_ids = {task["generation_id"] for task in tasks}
    if len(run_ids) != 1 or len(generation_ids) != 1:
        raise ValueError("shard manifest mixes run or generation identities")
    expected = {task["task_id"]: task for task in tasks}

    observed: dict[str, dict[str, Any]] = {}
    provenance_rows: list[dict[str, Any]] = []
    for directory_value in qc_bundle_dirs:
        directory = Path(directory_value).expanduser().resolve(strict=True)
        stage_result, artifacts = verify_stage_bundle(
            directory,
            expected_stage="QC_CASE",
            required_roles=("case_result",),
            schema_dir=schema_dir,
        )
        task_id = stage_result["task_id"]
        if task_id in observed:
            raise ValueError(f"duplicate QC result for task: {task_id}")
        if task_id not in expected:
            raise ValueError(f"unexpected QC result outside shard manifest: {task_id}")
        task = expected[task_id]
        lineage = {
            "run_id": task["run_id"],
            "generation_id": task["generation_id"],
            "shard_id": shard_id,
            "session_id": session_id,
            "task_id": task_id,
            "manifest_order": task["manifest_order"],
            "input_fingerprint": task["input_fingerprint"],
        }
        mismatched = [key for key, value in lineage.items() if stage_result.get(key) != value]
        if mismatched:
            raise ValueError(
                f"QC stage lineage mismatch for {task_id}: {', '.join(mismatched)}"
            )
        case_result = json.loads(artifacts["case_result"].read_text(encoding="utf-8"))
        validate_case_result_document(case_result, schema_dir=schema_dir)
        mismatched = [key for key, value in lineage.items() if case_result.get(key) != value]
        if mismatched:
            raise ValueError(
                f"case result lineage mismatch for {task_id}: {', '.join(mismatched)}"
            )
        if (stage_result["status"] == "DOMAIN_FAILED") != bool(case_result["failures"]):
            raise ValueError(f"QC stage status and case failure set disagree: {task_id}")
        observed[task_id] = case_result
        provenance_rows.append(
            {
                "manifest_order": task["manifest_order"],
                "task_id": task_id,
                "qc_bundle": str(directory),
                "qc_stage_result_sha256": sha256_file(directory / "stage_result.json"),
                "case_result_sha256": sha256_file(artifacts["case_result"]),
                "qc_status": stage_result["status"],
            }
        )

    missing = sorted(set(expected) - set(observed))
    if missing:
        raise ValueError(f"shard is missing terminal QC results: {missing[:10]}")
    ordered_results = [observed[task["task_id"]] for task in tasks]
    task_set_sha = task_set_fingerprint(tasks)
    result_set_sha = sha256_json(
        [
            {
                "task_id": result["task_id"],
                "manifest_order": result["manifest_order"],
                "input_fingerprint": result["input_fingerprint"],
                "render_state": result["render_state"],
                "evidence_state": result["evidence_state"],
                "failure_codes": _failure_codes(result),
            }
            for result in ordered_results
        ]
    )
    counts = {
        "render": dict(sorted(Counter(result["render_state"] for result in ordered_results).items())),
        "evidence": dict(
            sorted(Counter(result["evidence_state"] for result in ordered_results).items())
        ),
        "artifact_review": dict(
            sorted(
                Counter(result["artifact_review_state"] for result in ordered_results).items()
            )
        ),
        "scientific": dict(
            sorted(
                Counter(result["scientific_interpretation"] for result in ordered_results).items()
            )
        ),
    }
    failed_count = sum(bool(result["failures"]) for result in ordered_results)
    rerun_rows = _rerun_rows(ordered_results)
    summary = {
        "schema_version": "2.0",
        "pipeline_version": PIPELINE_VERSION,
        "run_id": next(iter(run_ids)),
        "generation_id": next(iter(generation_ids)),
        "shard_id": shard_id,
        "session_id": session_id,
        "pipeline_commit": pipeline_commit,
        "created_at": utc_now(),
        "status": "COMPLETE_WITH_CASE_FAILURES" if failed_count else "COMPLETED",
        "expected_task_count": len(tasks),
        "terminal_result_count": len(ordered_results),
        "domain_failed_count": failed_count,
        "rerun_eligible_count": len(rerun_rows),
        "task_set_sha256": task_set_sha,
        "result_set_sha256": result_set_sha,
        "counts": counts,
    }
    ledger_identity = {
        "run_id": summary["run_id"],
        "generation_id": summary["generation_id"],
        "shard_id": shard_id,
        "sequence": ledger_sequence,
        "event_type": "COMPLETED",
        "task_set_sha256": task_set_sha,
        "result_set_sha256": result_set_sha,
    }
    ledger = {
        "schema_version": "2.0",
        "event_id": f"evt_{sha256_json(ledger_identity)}",
        "sequence": ledger_sequence,
        "event_type": "COMPLETED",
        "occurred_at": utc_now(),
        "run_id": summary["run_id"],
        "generation_id": summary["generation_id"],
        "shard_id": shard_id,
        "expected_task_count": len(tasks),
        "task_set_sha256": task_set_sha,
        "pipeline_commit": pipeline_commit,
        "schema_identity": "2.0",
        "session_id": session_id,
        "controller_job_id": controller_job_id,
        "payload": {
            "status": summary["status"],
            "domain_failed_count": failed_count,
            "rerun_eligible_count": len(rerun_rows),
            "result_set_sha256": result_set_sha,
        },
    }
    validate_schema_document(ledger, "shard-ledger", schema_dir=schema_dir)

    destination, staging = _new_output_directory(output_dir)
    try:
        write_jsonl(staging / "case_results.jsonl", ordered_results)
        atomic_write_json(staging / "shard_summary.json", summary)
        write_jsonl(staging / "shard_ledger.jsonl", [ledger])
        write_tsv(staging / "rerun_manifest.tsv", RERUN_FIELDS, rerun_rows)
        write_tsv(
            staging / "provenance_index.tsv",
            [
                "manifest_order",
                "task_id",
                "qc_bundle",
                "qc_stage_result_sha256",
                "case_result_sha256",
                "qc_status",
            ],
            sorted(provenance_rows, key=lambda row: int(row["manifest_order"])),
        )
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**summary, "output_dir": str(destination)}


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot load JSON object {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"JSON value must be an object: {path}")
    return value


def aggregate_run(
    canonical_tasks: str | Path,
    shard_plan_path: str | Path,
    shard_summary_dirs: Iterable[str | Path],
    output_dir: str | Path,
    *,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Fail closed unless shard summaries exactly cover the canonical task set."""

    tasks = _load_tasks(canonical_tasks, contiguous=True)
    expected_tasks = {task["task_id"]: task for task in tasks}
    plan_path = Path(shard_plan_path).expanduser().resolve(strict=True)
    plan = _load_json_object(plan_path)
    required_plan = {
        "schema_version": "2.0",
        "run_id": tasks[0]["run_id"],
        "generation_id": tasks[0]["generation_id"],
        "task_count": len(tasks),
        "task_set_sha256": task_set_fingerprint(tasks),
    }
    mismatched = [key for key, value in required_plan.items() if plan.get(key) != value]
    if mismatched:
        raise ValueError("shard plan differs from canonical tasks: " + ", ".join(mismatched))
    expected_shards = {row["shard_id"]: row for row in plan["shards"]}

    observed_shards: dict[str, dict[str, Any]] = {}
    results_by_task: dict[str, dict[str, Any]] = {}
    ledger_rows: list[dict[str, Any]] = []
    shard_inventory: list[dict[str, Any]] = []
    for directory_value in shard_summary_dirs:
        root = Path(directory_value).expanduser().resolve(strict=True)
        if not root.is_dir():
            raise ValueError(f"shard summary input is not a directory: {root}")
        summary_path = root / "shard_summary.json"
        results_path = root / "case_results.jsonl"
        ledger_path = root / "shard_ledger.jsonl"
        if any(path.is_symlink() or not path.is_file() for path in (summary_path, results_path, ledger_path)):
            raise ValueError(f"shard summary bundle is incomplete or symlinked: {root}")
        summary = _load_json_object(summary_path)
        shard_id = summary.get("shard_id")
        if shard_id not in expected_shards:
            raise ValueError(f"unexpected shard summary: {shard_id}")
        if shard_id in observed_shards:
            raise ValueError(f"duplicate shard summary: {shard_id}")
        plan_row = expected_shards[shard_id]
        expected_summary = {
            "run_id": plan["run_id"],
            "generation_id": plan["generation_id"],
            "expected_task_count": plan_row["task_count"],
            "terminal_result_count": plan_row["task_count"],
            "task_set_sha256": plan_row["task_set_sha256"],
        }
        mismatched = [key for key, value in expected_summary.items() if summary.get(key) != value]
        if mismatched:
            raise ValueError(
                f"shard summary identity mismatch for {shard_id}: {', '.join(mismatched)}"
            )
        shard_results = list(read_jsonl(results_path))
        if len(shard_results) != int(plan_row["task_count"]):
            raise ValueError(f"shard result count mismatch: {shard_id}")
        for result in shard_results:
            validate_case_result_document(result, schema_dir=schema_dir)
            task_id = result["task_id"]
            if task_id in results_by_task:
                raise ValueError(f"case result occurs in multiple shards: {task_id}")
            if task_id not in expected_tasks:
                raise ValueError(f"case result is outside canonical manifest: {task_id}")
            task = expected_tasks[task_id]
            if (
                result["manifest_order"] != task["manifest_order"]
                or result["input_fingerprint"] != task["input_fingerprint"]
                or result["shard_id"] != shard_id
            ):
                raise ValueError(f"case result differs from canonical task: {task_id}")
            results_by_task[task_id] = result
        shard_tasks = [expected_tasks[result["task_id"]] for result in shard_results]
        if task_set_fingerprint(shard_tasks) != plan_row["task_set_sha256"]:
            raise ValueError(f"shard case result task set mismatch: {shard_id}")
        for event in read_jsonl(ledger_path):
            validate_schema_document(event, "shard-ledger", schema_dir=schema_dir)
            if event["shard_id"] != shard_id:
                raise ValueError(f"shard ledger event identity mismatch: {shard_id}")
            ledger_rows.append(event)
        observed_shards[shard_id] = summary
        shard_inventory.append(
            {
                "shard_id": shard_id,
                "task_count": summary["terminal_result_count"],
                "task_set_sha256": summary["task_set_sha256"],
                "result_set_sha256": summary["result_set_sha256"],
                "session_id": summary["session_id"],
                "summary_sha256": sha256_file(summary_path),
                "case_results_sha256": sha256_file(results_path),
                "ledger_sha256": sha256_file(ledger_path),
            }
        )

    missing_shards = sorted(set(expected_shards) - set(observed_shards))
    if missing_shards:
        raise ValueError(f"run is missing shard summaries: {missing_shards}")
    missing_tasks = sorted(set(expected_tasks) - set(results_by_task))
    if missing_tasks:
        raise ValueError(f"run is missing terminal case results: {missing_tasks[:10]}")
    ordered_results = [results_by_task[task["task_id"]] for task in tasks]
    failed_count = sum(bool(result["failures"]) for result in ordered_results)
    rerun_rows = _rerun_rows(ordered_results)
    run_summary = {
        "schema_version": "2.0",
        "pipeline_version": PIPELINE_VERSION,
        "run_id": plan["run_id"],
        "generation_id": plan["generation_id"],
        "created_at": utc_now(),
        "status": "RECONCILED_WITH_CASE_FAILURES" if failed_count else "RECONCILED",
        "expected_task_count": len(tasks),
        "terminal_result_count": len(ordered_results),
        "shard_count": len(expected_shards),
        "domain_failed_count": failed_count,
        "rerun_eligible_count": len(rerun_rows),
        "task_set_sha256": task_set_fingerprint(tasks),
        "case_result_set_sha256": sha256_json(ordered_results),
        "evidence_counts": dict(
            sorted(Counter(result["evidence_state"] for result in ordered_results).items())
        ),
        "artifact_review_counts": dict(
            sorted(Counter(result["artifact_review_state"] for result in ordered_results).items())
        ),
    }
    reconciliation = {
        "schema_version": "2.0",
        "status": "PASS",
        "expected_tasks": len(tasks),
        "observed_terminal_results": len(ordered_results),
        "expected_shards": len(expected_shards),
        "observed_shards": len(observed_shards),
        "missing_tasks": [],
        "unexpected_tasks": [],
        "duplicate_tasks": [],
        "canonical_task_set_sha256": task_set_fingerprint(tasks),
        "plan_task_set_sha256": plan["task_set_sha256"],
    }
    destination, staging = _new_output_directory(output_dir)
    try:
        write_jsonl(staging / "case_results.jsonl", ordered_results)
        atomic_write_json(staging / "run_summary.json", run_summary)
        atomic_write_json(staging / "reconciliation.json", reconciliation)
        write_jsonl(
            staging / "run_ledger.jsonl",
            sorted(
                ledger_rows,
                key=lambda event: (event["shard_id"], int(event["sequence"])),
            ),
        )
        write_tsv(staging / "rerun_manifest.tsv", RERUN_FIELDS, rerun_rows)
        write_tsv(
            staging / "shard_inventory.tsv",
            [
                "shard_id",
                "task_count",
                "task_set_sha256",
                "result_set_sha256",
                "session_id",
                "summary_sha256",
                "case_results_sha256",
                "ledger_sha256",
            ],
            sorted(shard_inventory, key=lambda row: row["shard_id"]),
        )
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**run_summary, "output_dir": str(destination)}
