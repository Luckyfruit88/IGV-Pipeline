from __future__ import annotations

import copy
import fcntl
import json
import os
import shutil
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .contracts import (
    validate_v3_case_result_document,
    validate_v3_terminal_bundle_document,
)
from .identity import task_set_fingerprint
from .orchestrator_v3 import _validated_terminal_case_results
from .product_paths_v3 import cases_root, contract_root
from .project_admission_v3 import (
    _FAILURE_FIELDS,
    _SNAPSHOT_FIELDS,
    _fsync_directory,
    _validate_snapshot_product,
)
from .project_v3 import build_project_source_binding, load_project_config
from .publication import verify_checksum_tree
from .rerun_v3 import freeze_case_failure_rerun
from .snapshot_store import snapshot_view
from .utils import (
    atomic_write_json,
    read_jsonl,
    sha256_file,
    sha256_json,
    utc_now,
    write_tsv,
)

_STATE_SCHEMA = "3.0-failed-only-rerun-state"


def _object(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}: {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def _product_root(value: str | Path) -> Path:
    root_value = Path(value).expanduser()
    if root_value.is_symlink() or not root_value.resolve(strict=True).is_dir():
        raise ValueError(
            f"output must be a regular non-symlink directory: {root_value}"
        )
    root = root_value.resolve(strict=True)
    if contract_root(root).is_symlink() or not contract_root(root).is_dir():
        raise ValueError("output lacks its immutable run contract")
    return root


def _control_root(product: Path) -> Path:
    control = product / ".igv-pipeline" / "rerun"
    if control.is_symlink():
        raise ValueError("failed-only rerun control root must not be a symlink")
    control.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not control.is_dir():
        raise ValueError("failed-only rerun control root is not a directory")
    return control


@contextmanager
def failed_only_rerun_lock(product_root: str | Path) -> Iterator[Path]:
    """Own the ordinary-user rerun controller for one public product."""

    product = _product_root(product_root)
    control = _control_root(product)
    lock_path = control / "failed-only.lock"
    if lock_path.is_symlink():
        raise ValueError("failed-only rerun lock must not be a symlink")
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"another failed-only rerun owns this output: {product}"
            ) from exc
        yield product


def _safe_relative_child(root: Path, relative_value: object, *, label: str) -> Path:
    relative = Path(str(relative_value or ""))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError(f"{label} path is unsafe")
    candidate = (root / relative).resolve(strict=True)
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} escapes the output root") from exc
    if candidate.is_symlink() or not candidate.is_dir():
        raise ValueError(f"{label} is not a regular directory: {candidate}")
    return candidate


def _state(product: Path) -> dict[str, Any] | None:
    path = snapshot_view(product) / ".igv-pipeline/rerun/failed-only-state.json"
    if not path.exists():
        return None
    value = _object(path, label="failed-only rerun state")
    if value.get("schema_version") != _STATE_SCHEMA:
        raise ValueError("failed-only rerun state schema differs")
    if value.get("status") not in {"CASE_FAILURES", "SNAPSHOTS_READY"}:
        raise ValueError("failed-only rerun state status is invalid")
    generations = value.get("generations")
    if not isinstance(generations, list):
        raise ValueError("failed-only rerun generation history is invalid")
    return value


def current_failed_source(
    product_root: str | Path,
) -> tuple[Path | None, dict[str, Any] | None]:
    """Return the immutable generation that currently owns unresolved failures."""

    product = _product_root(product_root)
    state = _state(product)
    if state is None:
        return product, None
    if state["status"] == "SNAPSHOTS_READY":
        return None, state
    return (
        _safe_relative_child(
            product,
            state.get("active_source_relative_path"),
            label="active rerun source",
        ),
        state,
    )


def validate_live_project_binding(source_run: str | Path, project: str | Path) -> None:
    """Authorize rerun input from a direct project or an immutable batch source."""

    source = _product_root(source_run)
    contract = contract_root(source)
    frozen_path = contract / "project_binding.json"
    if frozen_path.exists() or frozen_path.is_symlink():
        frozen = _object(frozen_path, label="source project binding")
        live = build_project_source_binding(load_project_config(project))
        if live != frozen:
            raise ValueError(
                "project metadata or input fingerprints differ from the failed generation"
            )
        return

    request_path = contract / "batch-request.json"
    binding_path = contract / "campaign_binding.json"
    if not request_path.exists() and not binding_path.exists():
        raise ValueError(
            "rerun source lacks project_binding.json or immutable campaign binding"
        )
    request = _object(request_path, label="source batch request")
    binding = _object(binding_path, label="source campaign binding")
    identity = _object(contract / "run_identity.json", label="rerun source identity")
    tasks_path = contract / "tasks.jsonl"
    if tasks_path.is_symlink() or not tasks_path.is_file():
        raise ValueError("campaign rerun source tasks must be a regular file")
    tasks = list(read_jsonl(tasks_path))

    if request.get("schema_version") != "3.0-batch-request":
        raise ValueError("campaign rerun batch request schema differs")
    if binding.get("schema_version") != "3.0-batch-admission":
        raise ValueError("campaign rerun binding schema differs")
    if identity.get("adapter") != "ssqtl":
        raise ValueError("campaign rerun source adapter must be ssqtl")

    request_sha = sha256_file(request_path)
    binding_sha = sha256_file(binding_path)
    tasks_sha = sha256_file(tasks_path)
    tasks_set_sha = task_set_fingerprint(tasks)
    if identity.get("batch_request_sha256") != request_sha:
        raise ValueError("campaign rerun batch request differs from run identity")
    if identity.get("campaign_binding_sha256") != binding_sha:
        raise ValueError("campaign rerun binding differs from run identity")
    if identity.get("canonical_tasks_sha256") != tasks_sha:
        raise ValueError("campaign rerun tasks differ from run identity")
    if identity.get("canonical_task_set_sha256") != tasks_set_sha:
        raise ValueError("campaign rerun task set differs from run identity")

    request_without_self_hash = {
        key: value for key, value in request.items() if key != "request_sha256"
    }
    if request.get("request_sha256") != sha256_json(request_without_self_hash):
        raise ValueError("campaign rerun batch request self-hash differs")
    expected_request = {
        "execution_run_id": identity.get("run_id"),
        "execution_generation_id": identity.get("generation_id"),
        "task_count": len(tasks),
        "tasks_sha256": tasks_sha,
        "task_set_sha256": tasks_set_sha,
    }
    for key, expected in expected_request.items():
        if request.get(key) != expected:
            raise ValueError(f"campaign rerun batch request {key} differs")

    expected_binding = {
        "batch_request_sha256": request_sha,
        "batch_id": request.get("batch_id"),
        "campaign_id": request.get("campaign_id"),
        "purpose": request.get("purpose"),
        "task_count": len(tasks),
        "tasks_sha256": tasks_sha,
        "task_set_sha256": tasks_set_sha,
    }
    for key, expected in expected_binding.items():
        if binding.get(key) != expected:
            raise ValueError(f"campaign rerun binding {key} differs")

    source_tasks = request.get("source_tasks")
    if not isinstance(source_tasks, list) or len(source_tasks) != len(tasks):
        raise ValueError("campaign rerun source task mapping differs")
    for index, (source_task, task) in enumerate(zip(source_tasks, tasks), 1):
        if not isinstance(source_task, Mapping):
            raise ValueError("campaign rerun source task mapping is invalid")
        expected_source = {
            "task_id": task.get("task_id"),
            "batch_manifest_order": index,
            "batch_input_fingerprint": task.get("input_fingerprint"),
        }
        for key, expected in expected_source.items():
            if source_task.get(key) != expected:
                raise ValueError(f"campaign rerun source task {key} differs")


def build_failed_rerun_plan(
    product_root: str | Path,
    *,
    runtime_manifest: str | Path,
) -> dict[str, Any] | None:
    """Freeze the current failure set and derive one deterministic target generation."""

    product = _product_root(product_root)
    source, state = current_failed_source(product)
    if source is None:
        return None
    immutable_contract = contract_root(source)
    tasks = list(read_jsonl(immutable_contract / "tasks.jsonl"))
    case_results, failures = _validated_terminal_case_results(source, tasks)
    if not failures:
        return None
    pointer = freeze_case_failure_rerun(source, tasks, case_results)
    if pointer is None:
        raise ValueError("failed generation did not produce a rerun request")
    receipt_path = source / str(pointer["relative_path"]) / "rerun_receipt.json"
    if receipt_path.is_symlink() or not receipt_path.is_file():
        raise ValueError("frozen rerun receipt is unavailable")
    runtime_value = Path(runtime_manifest).expanduser()
    if runtime_value.is_symlink() or not runtime_value.resolve(strict=True).is_file():
        raise ValueError("embedded runtime manifest is unavailable")
    runtime_path = runtime_value.resolve(strict=True)
    generation_digest = sha256_json(
        {
            "source_rerun_receipt_sha256": sha256_file(receipt_path),
            "runtime_manifest_sha256": sha256_file(runtime_path),
        }
    )
    generation_id = "rerun-" + generation_digest
    identity = _object(
        immutable_contract / "run_identity.json", label="rerun source identity"
    )
    control = _control_root(product)
    generation_output = control / "executions" / generation_id
    work = product / ".work" / "rerun" / generation_id
    return {
        "schema_version": "3.0-failed-only-rerun-plan",
        "product_root": str(product),
        "source_run": str(source),
        "source_relative_path": (
            "." if source == product else str(source.relative_to(product))
        ),
        "source_run_id": identity["run_id"],
        "source_generation_id": identity["generation_id"],
        "source_canonical_tasks_sha256": sha256_file(
            immutable_contract / "tasks.jsonl"
        ),
        "rerun_id": pointer["rerun_id"],
        "rerun_case_count": len(failures),
        "rerun_receipt": str(receipt_path),
        "rerun_receipt_sha256": sha256_file(receipt_path),
        "generation_id": generation_id,
        "generation_output": str(generation_output),
        "generation_output_relative_path": str(generation_output.relative_to(product)),
        "default_work": str(work),
        "runtime_manifest_sha256": sha256_file(runtime_path),
        "prior_generation_count": len((state or {}).get("generations", [])),
    }


def _rerun_request(
    source: Path, receipt_path: Path
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if receipt_path.is_symlink() or not receipt_path.is_file():
        raise ValueError("rerun receipt must be a regular non-symlink file")
    verify_checksum_tree(receipt_path.parent)
    receipt = _object(receipt_path, label="rerun receipt")
    if (
        receipt.get("schema_version") != "3.0-rerun-receipt"
        or receipt.get("state") != "RERUN_REQUIRED"
        or receipt.get("target_generation_policy")
        != "MUST_DIFFER_FROM_SOURCE_GENERATION"
        or receipt.get("same_generation_resume_allowed") is not False
    ):
        raise ValueError("rerun receipt does not authorize a different generation")
    manifest_path = receipt_path.parent / str(receipt.get("rerun_manifest", ""))
    if manifest_path.name != "rerun_manifest.jsonl" or manifest_path.is_symlink():
        raise ValueError("rerun manifest path is invalid")
    requests = list(read_jsonl(manifest_path))
    if (
        not requests
        or len(requests) != int(receipt.get("rerun_case_count", -1))
        or sha256_file(manifest_path) != receipt.get("rerun_manifest_sha256")
        or sha256_json(requests) != receipt.get("rerun_request_set_sha256")
    ):
        raise ValueError("rerun request set differs from its receipt")
    identity = _object(
        contract_root(source) / "run_identity.json", label="source run identity"
    )
    tasks_path = contract_root(source) / "tasks.jsonl"
    if (
        identity.get("run_id") != receipt.get("source_run_id")
        or identity.get("generation_id") != receipt.get("source_generation_id")
        or sha256_file(tasks_path) != receipt.get("canonical_tasks_sha256")
    ):
        raise ValueError("rerun receipt differs from its source generation")
    return receipt, requests


def _project_case_evidence(
    incoming: Path,
    staging_cases: Path,
    canonical_task: Mapping[str, Any],
    *,
    rerun_receipt_sha256: str,
) -> None:
    """Project immutable rerun evidence onto the canonical product identity.

    The original terminal files remain untouched in ``incoming``.  The public
    product keeps a compact, explicitly non-authoritative projection so later
    review and publication commands continue to observe one canonical task
    identity per case.
    """

    task_id = str(canonical_task["task_id"])
    source_case = cases_root(incoming) / task_id
    if source_case.is_symlink() or not source_case.is_dir():
        raise ValueError(f"rerun compact case evidence is unavailable: {task_id}")
    for child in source_case.rglob("*"):
        if child.is_symlink() or not (child.is_file() or child.is_dir()):
            raise ValueError(f"rerun compact case evidence is unsafe: {task_id}")
    source_result_path = source_case / "case_result.json"
    source_bundle_path = source_case / "terminal_bundle.json"
    source_result = _object(source_result_path, label=f"rerun case result {task_id}")
    source_bundle = _object(
        source_bundle_path, label=f"rerun terminal bundle {task_id}"
    )

    target_case = staging_cases / task_id
    if target_case.exists() or target_case.is_symlink():
        if target_case.is_symlink() or not target_case.is_dir():
            raise ValueError(f"destination compact case evidence is unsafe: {task_id}")
        shutil.rmtree(target_case)
    shutil.copytree(source_case, target_case, symlinks=False)

    identity_fields = (
        "run_id",
        "generation_id",
        "task_id",
        "manifest_order",
        "input_fingerprint",
    )
    projected_result = copy.deepcopy(source_result)
    for field in identity_fields:
        projected_result[field] = canonical_task[field]
    validate_v3_case_result_document(projected_result)
    projected_result_path = target_case / "case_result.json"
    atomic_write_json(projected_result_path, projected_result)

    projected_bundle = copy.deepcopy(source_bundle)
    for field in identity_fields:
        projected_bundle[field] = canonical_task[field]
    projected_bundle["case_result_sha256"] = sha256_file(projected_result_path)
    projected_bundle["case_result_size"] = projected_result_path.stat().st_size
    projected_bundle["artifact_set_sha256"] = sha256_json(projected_result["artifacts"])
    validate_v3_terminal_bundle_document(projected_bundle, projected_result)
    projected_bundle_path = target_case / "terminal_bundle.json"
    atomic_write_json(projected_bundle_path, projected_bundle)

    atomic_write_json(
        target_case / "rerun_projection.json",
        {
            "schema_version": "3.0-rerun-case-projection",
            "authoritative": False,
            "task_id": task_id,
            "canonical_identity": {
                field: canonical_task[field] for field in identity_fields
            },
            "source_identity": {
                field: source_result[field] for field in identity_fields
            },
            "source_case_result_sha256": sha256_file(source_result_path),
            "source_terminal_bundle_sha256": sha256_file(source_bundle_path),
            "projected_case_result_sha256": sha256_file(projected_result_path),
            "projected_terminal_bundle_sha256": sha256_file(projected_bundle_path),
            "rerun_receipt_sha256": rerun_receipt_sha256,
        },
    )


def _generation_record(
    product: Path,
    source: Path,
    incoming: Path,
    receipt_path: Path,
    failed_ids: list[str],
) -> dict[str, Any]:
    identity = _object(
        contract_root(incoming) / "run_identity.json", label="rerun generation identity"
    )
    tasks = list(read_jsonl(contract_root(incoming) / "tasks.jsonl"))
    terminal_bundles = [
        {
            "task_id": str(task["task_id"]),
            "sha256": sha256_file(
                cases_root(incoming) / str(task["task_id"]) / "terminal_bundle.json"
            ),
        }
        for task in tasks
    ]
    return {
        "generation_id": identity["generation_id"],
        "source_relative_path": (
            "." if source == product else str(source.relative_to(product))
        ),
        "output_relative_path": str(incoming.relative_to(product)),
        "rerun_receipt_sha256": sha256_file(receipt_path),
        "snapshots_sha256": sha256_file(incoming / "snapshots.tsv"),
        "failed_cases_sha256": sha256_file(incoming / "failed_cases.tsv"),
        "terminal_bundle_set_sha256": sha256_json(terminal_bundles),
        "status": "CASE_FAILURES" if failed_ids else "SNAPSHOTS_READY",
        "failed_task_ids": failed_ids,
    }


def validate_projected_rerun_replacements(product_root: str | Path, expected_fingerprints: Mapping[str, str]) -> None:
    """Authorize collection updates from the same receipts used by failed-only reruns."""
    product = _product_root(product_root)
    state = _state(product)
    if state is None:
        raise ValueError("batch replacement lacks a failed-only rerun state")
    checked = {}
    for tid, expected_fp in expected_fingerprints.items():
        case = cases_root(product) / tid
        projection = _object(case / "rerun_projection.json", label="rerun projection")
        result = _object(case / "case_result.json", label="projected case result")
        fields = ("run_id", "generation_id", "task_id", "manifest_order", "input_fingerprint")
        if (projection.get("schema_version") != "3.0-rerun-case-projection"
                or projection.get("task_id") != tid or result.get("input_fingerprint") != expected_fp
                or projection.get("canonical_identity") != {key: result[key] for key in fields}
                or projection.get("projected_case_result_sha256") != sha256_file(case / "case_result.json")
                or projection.get("projected_terminal_bundle_sha256") != sha256_file(case / "terminal_bundle.json")):
            raise ValueError("rerun projection does not bind the canonical case")
        source_identity = projection.get("source_identity", {})
        generation_id = source_identity.get("generation_id")
        if generation_id not in checked:
            records = [r for r in state["generations"] if isinstance(r, Mapping) and r.get("generation_id") == generation_id]
            if len(records) != 1:
                raise ValueError("rerun projection generation is not uniquely registered")
            record = records[0]
            incoming = _safe_relative_child(product, record["output_relative_path"], label="rerun output")
            source = product if record["source_relative_path"] == "." else _safe_relative_child(product, record["source_relative_path"], label="rerun source")
            control = source / "rerun" if contract_root(source) == source / "contract" else source / ".igv-pipeline/rerun"
            receipts = []
            for path in control.glob("generations/*/rerun_receipt.json"):
                path.resolve(strict=True).relative_to(source)
                if not path.is_symlink() and sha256_file(path) == record["rerun_receipt_sha256"]:
                    receipts.append(path)
            if len(receipts) != 1:
                raise ValueError("rerun projection receipt is unavailable or ambiguous")
            _, requests = _rerun_request(source, receipts[0])
            tasks = list(read_jsonl(contract_root(incoming) / "tasks.jsonl"))
            validated, _ = _validated_terminal_case_results(incoming, tasks)
            if (sha256_file(snapshot_view(incoming) / "snapshots.tsv") != record["snapshots_sha256"]
                    or sha256_json([{"task_id": t["task_id"], "sha256": sha256_file(cases_root(incoming) / t["task_id"] / "terminal_bundle.json")} for t in tasks]) != record["terminal_bundle_set_sha256"]):
                raise ValueError("registered rerun output has changed")
            checked[generation_id] = (incoming, record, {r["source_task_id"] for r in requests}, {r["task_id"]: r for r in validated})
        incoming, record, requested, validated = checked[generation_id]
        original = cases_root(incoming) / tid
        if (tid not in requested or projection.get("rerun_receipt_sha256") != record["rerun_receipt_sha256"]
                or projection.get("source_case_result_sha256") != sha256_file(original / "case_result.json")
                or projection.get("source_terminal_bundle_sha256") != sha256_file(original / "terminal_bundle.json")
                or source_identity != {key: validated[tid][key] for key in fields}):
            raise ValueError("case replacement is outside its authorized rerun")


def reconcile_failed_rerun(
    product_root: str | Path,
    *,
    source_run: str | Path,
    rerun_receipt: str | Path,
    incoming_output: str | Path,
) -> dict[str, Any]:
    """Atomically project one authorized failed-only generation into the product."""

    product = _product_root(product_root)
    source = _product_root(source_run)
    incoming = _product_root(incoming_output)
    for child, label in ((source, "source run"), (incoming, "incoming output")):
        try:
            child.relative_to(product)
        except ValueError as exc:
            if child != product:
                raise ValueError(
                    f"{label} is outside the product control root"
                ) from exc
    receipt_path = Path(rerun_receipt).expanduser().resolve(strict=True)
    receipt, requests = _rerun_request(source, receipt_path)
    requested_ids = [str(row.get("source_task_id", "")) for row in requests]
    if not all(requested_ids) or len(requested_ids) != len(set(requested_ids)):
        raise ValueError("rerun request task identities are empty or duplicate")

    destination_rows, destination_failures = _validate_snapshot_product(product)
    incoming_rows, incoming_failures = _validate_snapshot_product(incoming)
    destination_summary = _object(snapshot_view(product) / "run_summary.json", label="run summary")
    if [row["task_id"] for row in incoming_rows] != requested_ids:
        raise ValueError(
            "incoming generation differs from the frozen failed-only task set"
        )

    incoming_contract = contract_root(incoming)
    incoming_identity = _object(
        incoming_contract / "run_identity.json", label="incoming run identity"
    )
    binding = _object(
        incoming_contract / "rerun_binding.json", label="incoming rerun binding"
    )
    incoming_tasks = list(read_jsonl(incoming_contract / "tasks.jsonl"))
    if (
        binding.get("schema_version") != "3.0-rerun-binding"
        or binding.get("source_run_id") != receipt.get("source_run_id")
        or binding.get("source_generation_id") != receipt.get("source_generation_id")
        or binding.get("source_rerun_id") != receipt.get("rerun_id")
        or binding.get("source_rerun_receipt_sha256") != sha256_file(receipt_path)
        or binding.get("source_rerun_manifest_sha256")
        != receipt.get("rerun_manifest_sha256")
        or binding.get("target_run_id") != incoming_identity.get("run_id")
        or binding.get("target_generation_id") != incoming_identity.get("generation_id")
        or binding.get("target_tasks_sha256")
        != sha256_file(incoming_contract / "tasks.jsonl")
        or binding.get("target_task_set_sha256") != task_set_fingerprint(incoming_tasks)
        or incoming_identity.get("rerun_binding_sha256")
        != sha256_file(incoming_contract / "rerun_binding.json")
    ):
        raise ValueError("incoming generation is not bound to the rerun receipt")

    destination_tasks = list(read_jsonl(contract_root(product) / "tasks.jsonl"))
    if [str(task["task_id"]) for task in destination_tasks] != [
        row["task_id"] for row in destination_rows
    ]:
        raise ValueError("destination snapshot index differs from canonical tasks")
    destination_task_by_id = {str(task["task_id"]): task for task in destination_tasks}
    incoming_case_results, _incoming_failures = _validated_terminal_case_results(
        incoming, incoming_tasks
    )
    incoming_result_by_task = {
        str(result["task_id"]): result for result in incoming_case_results
    }
    for row in incoming_rows:
        eligible = bool(incoming_result_by_task[row["task_id"]]["eligible"])
        expected_status = "SNAPSHOT_READY" if eligible else "CASE_FAILED"
        if row["status"] != expected_status:
            raise ValueError(
                "rerun snapshot status differs from terminal evidence: "
                f"{row['task_id']}"
            )

    destination_by_task = {row["task_id"]: row for row in destination_rows}
    incoming_failure_by_task: dict[str, list[dict[str, str]]] = {}
    for row in incoming_failures:
        incoming_failure_by_task.setdefault(row["task_id"], []).append(row)
    failure_by_task: dict[str, list[dict[str, str]]] = {}
    for row in destination_failures:
        failure_by_task.setdefault(row["task_id"], []).append(row)

    transitioned = 0
    for incoming_row in incoming_rows:
        task_id = incoming_row["task_id"]
        current = destination_by_task.get(task_id)
        if current is None:
            raise ValueError(
                f"rerun task is absent from the destination product: {task_id}"
            )
        if incoming_row["chromosome"] != current["chromosome"]:
            raise ValueError(
                f"rerun chromosome differs from the source task: {task_id}"
            )
        expected_relative = f"snapshots/{current['chromosome']}/{task_id}.png"
        normalized = {
            **incoming_row,
            "manifest_order": current["manifest_order"],
        }
        if normalized["status"] == "SNAPSHOT_READY":
            if normalized["relative_path"] != expected_relative:
                raise ValueError(
                    f"rerun snapshot path differs from task identity: {task_id}"
                )
            if current["status"] == "SNAPSHOT_READY":
                if current != normalized:
                    raise ValueError(
                        "rerun would overwrite a successful task with different "
                        f"output: {task_id}"
                    )
            elif current["status"] == "CASE_FAILED":
                destination_by_task[task_id] = normalized
                transitioned += 1
            else:
                raise ValueError(f"destination task status is invalid: {task_id}")
            failure_by_task.pop(task_id, None)
        elif normalized["status"] == "CASE_FAILED":
            if current["status"] != "CASE_FAILED":
                raise ValueError(f"rerun cannot demote a successful task: {task_id}")
            latest_failures = incoming_failure_by_task.get(task_id, [])
            if not latest_failures:
                raise ValueError(f"failed rerun lacks failure evidence: {task_id}")
            failure_by_task[task_id] = [
                {
                    **row,
                    "manifest_order": current["manifest_order"],
                    "chromosome": current["chromosome"],
                }
                for row in latest_failures
            ]
        else:
            raise ValueError(f"incoming rerun status is invalid: {task_id}")

    merged_rows = sorted(
        destination_by_task.values(), key=lambda row: int(row["manifest_order"])
    )
    merged_failures = sorted(
        (row for rows in failure_by_task.values() for row in rows),
        key=lambda row: (
            int(row["manifest_order"]),
            row["failure_code"],
            row["message"],
        ),
    )
    failed_ids = sorted(
        failure_by_task,
        key=lambda task_id: int(destination_by_task[task_id]["manifest_order"]),
    )
    generation_record = _generation_record(
        product, source, incoming, receipt_path, failed_ids
    )

    control = _control_root(product)
    state_path = control / "failed-only-state.json"
    previous_state = _state(product)
    generations = list((previous_state or {}).get("generations", []))
    existing = next(
        (
            row
            for row in generations
            if row.get("generation_id") == generation_record["generation_id"]
        ),
        None,
    )
    if (
        existing is not None
        and {key: value for key, value in existing.items() if key != "applied_at"}
        != generation_record
    ):
        raise ValueError(
            "existing rerun generation record differs from incoming evidence"
        )
    if existing is None:
        generations.append({**generation_record, "applied_at": utc_now()})

    state = {
        "schema_version": _STATE_SCHEMA,
        "status": "CASE_FAILURES" if merged_failures else "SNAPSHOTS_READY",
        "active_source_relative_path": (
            str(incoming.relative_to(product)) if merged_failures else None
        ),
        "failed_task_ids": failed_ids,
        "generations": generations,
        "updated_at": utc_now(),
    }
    if (
        existing is not None
        and merged_rows == destination_rows
        and merged_failures == destination_failures
        and previous_state is not None
        and previous_state.get("status") == state["status"]
        and previous_state.get("active_source_relative_path")
        == state["active_source_relative_path"]
        and previous_state.get("failed_task_ids") == state["failed_task_ids"]
    ):
        return {
            "schema_version": "3.0-failed-only-rerun-merge",
            "status": "IDEMPOTENT",
            "generation_id": generation_record["generation_id"],
            "recovered_case_count": 0,
            "remaining_failed_case_count": len(failed_ids),
            "exit_code": 2 if merged_failures else 0,
        }

    from .snapshot_store import SnapshotTransaction
    import tempfile

    with SnapshotTransaction(product) as transaction:
        if transaction.rows != destination_rows or transaction.failures != destination_failures:
            raise RuntimeError("snapshot product changed while rerun reconciliation waited")
        with tempfile.TemporaryDirectory(prefix=".case-projection-", dir=control) as temporary:
            projected_cases = Path(temporary)
            receipt_sha256 = sha256_file(receipt_path)
            for row in incoming_rows:
                _project_case_evidence(incoming, projected_cases,
                    destination_task_by_id[row["task_id"]], rerun_receipt_sha256=receipt_sha256)
            source_digests = dict(destination_summary.get("source_digests") or {})
            source_digests.update({"rerun_receipt": receipt_sha256,
                                  "rerun_generation_snapshots": sha256_file(snapshot_view(incoming) / "snapshots.tsv")})
            summary = {**destination_summary, "rerun_required": bool(failed_ids),
                       "publication_state": "NOT_READY" if merged_failures else "SNAPSHOTS_READY",
                       "rerun_generation_count": len(generations), "updated_at": utc_now(),
                       "source_digests": source_digests}
            def validate_projection(stage):
                _validate_snapshot_product(stage)
                _, observed_failures = _validated_terminal_case_results(stage, destination_tasks)
                if observed_failures != failed_ids:
                    raise ValueError("projected terminal evidence differs from rerun failure coverage")
            publication = transaction.publish(merged_rows, merged_failures,
                images={row["task_id"]: snapshot_view(incoming) / row["relative_path"]
                        for row in incoming_rows if row["status"] == "SNAPSHOT_READY"},
                summary=summary,
                case_updates={row["task_id"]: projected_cases / row["task_id"] for row in incoming_rows},
                extra_json={".igv-pipeline/rerun/failed-only-state.json": state},
                validate=validate_projection)

    return {
        "schema_version": "3.0-failed-only-rerun-merge",
        "status": "PUBLISHED",
        "commit_mode": publication["commit_mode"],
        "generation_id": generation_record["generation_id"],
        "recovered_case_count": transitioned,
        "remaining_failed_case_count": len(failed_ids),
        "exit_code": 2 if merged_failures else 0,
        "snapshots_sha256": sha256_file(product / "snapshots.tsv"),
        "failed_cases_sha256": sha256_file(product / "failed_cases.tsv"),
    }
