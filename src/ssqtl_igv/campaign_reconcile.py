"""Recover completed campaign batches without submitting or rendering work."""
from __future__ import annotations

import csv
import json
from pathlib import Path

from .campaign_v3 import load_and_validate_batch_request, read_campaign_master_index
from .product_paths_v3 import contract_root
from .project_launcher import observed_run_output, validate_project_postflight
from .snapshot_store import SnapshotTransaction, merge_failure_rows, snapshot_view
from .utils import atomic_write_json, reject_symlink_path_components, sha256_file, sha256_json


def _rows(path):
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream, delimiter="\t"))


def _failure_groups(rows):
    groups = {}
    for row in rows:
        groups.setdefault(row["task_id"], []).append(row)
    return groups


def reconcile_campaign(campaign_dir, runs_dir, output):
    campaign = reject_symlink_path_components(campaign_dir, label="campaign").resolve(strict=True)
    runs = reject_symlink_path_components(runs_dir, label="batch runs").resolve(strict=True)
    destination = reject_symlink_path_components(output, label="campaign output").resolve(strict=False)
    if any(destination == source or destination in source.parents or source in destination.parents for source in (runs, campaign)):
        raise ValueError("reconciliation output must not overlap its source runs or campaign")
    master = read_campaign_master_index(campaign)
    expected = [r["task_id"] for r in sorted(master.by_id.values(), key=lambda r: int(r["manifest_order"]))]
    bindings = []
    for path in sorted((campaign / "batches").glob("*/batch-request.json")):
        binding = load_and_validate_batch_request(path, master_index=master)
        binding.pop("tasks")  # Keep the identity mapping, not every BAM payload.
        bindings.append(binding)
    claimed = [row["task_id"] for binding in bindings for row in binding["request"]["source_tasks"]]
    if len(claimed) != len(set(claimed)):
        raise ValueError("campaign batch requests overlap")
    accepted, blocked, new_receipts = [], [], []
    with SnapshotTransaction(destination, expected_task_ids=expected, master_sha256=master.master_sha256) as transaction:
        receipt_dir = reject_symlink_path_components(destination / "reports" / "batch-reconciliation", label="batch receipts")
        receipt_dir.mkdir(parents=True, exist_ok=True)
        merged = {r["task_id"]: r for r in transaction.rows}
        failures = list(transaction.failures)
        images = {}
        for binding in bindings:
            request = binding["request"]
            batch = request["batch_id"]
            source = runs / batch
            try:
                with observed_run_output(source):
                    if source.is_symlink() or not source.is_dir():
                        raise ValueError("batch output has not been produced")
                    view = snapshot_view(source)
                    contract = contract_root(source)
                    identity = json.loads((contract / "run_identity.json").read_text())
                    if identity.get("batch_request_sha256") != binding["request_sha256"]:
                        raise ValueError("batch output belongs to a different request")
                    if sha256_file(contract / "tasks.jsonl") != binding["tasks_sha256"]:
                        raise ValueError("batch output canonical tasks differ from request")
                    signature = {"master_sha256": master.master_sha256, "batch_request_sha256": binding["request_sha256"],
                                 "tasks_sha256": binding["tasks_sha256"], "snapshots_sha256": sha256_file(view / "snapshots.tsv"),
                                 "failed_cases_sha256": sha256_file(view / "failed_cases.tsv"),
                                 "run_identity_sha256": sha256_file(contract / "run_identity.json"),
                                 "trace_sha256": sha256_file(source / "reports/trace.txt"),
                                 "publication_generation": view.name if view != source else None}
                    receipt_path = receipt_dir / (batch + ".json")
                    if receipt_path.is_symlink():
                        raise ValueError("batch receipt must not be a symlink")
                    cached = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
                    previous_failures = _failure_groups(failures)
                    if (cached is not None and cached.get("source_signature") == signature
                            and all(merged.get(row["task_id"]) == row for row in cached["rows"])
                            and all(previous_failures.get(tid) == group for tid, group in _failure_groups(cached["failures"]).items())):
                        accepted.append({"batch_id": batch, "status": "CACHED", "task_count": len(cached["rows"])})
                        continue
                    verified = validate_project_postflight(source, repair_projection=False)
                    if (verified["source_digests"]["snapshots"] != signature["snapshots_sha256"]
                            or verified["source_digests"]["failed_cases"] != signature["failed_cases_sha256"]
                            or verified["postflight"]["canonical_tasks_sha256"] != binding["tasks_sha256"]):
                        raise ValueError("batch output changed during reconciliation")
                    mapping = {r["task_id"]: r for r in request["source_tasks"]}
                    incoming = _rows(view / "snapshots.tsv")
                    failed = _rows(view / "failed_cases.tsv")
                    if [r["task_id"] for r in incoming] != list(mapping):
                        raise ValueError("batch snapshot index differs from request order")
                    for row in incoming + failed:
                        row["manifest_order"] = str(mapping[row["task_id"]]["source_manifest_order"])
                    incoming_failures = _failure_groups(failed)
                    changed = {r["task_id"] for r in incoming if r["task_id"] in merged and
                               (merged[r["task_id"]] != r or previous_failures.get(r["task_id"], []) != incoming_failures.get(r["task_id"], []))}
                    if any(merged[tid]["status"] != "CASE_FAILED" for tid in changed):
                        raise ValueError("an accepted successful snapshot cannot be replaced")
                    if changed:
                        from .public_rerun_v3 import validate_projected_rerun_replacements
                        validate_projected_rerun_replacements(source, {tid: mapping[tid]["batch_input_fingerprint"] for tid in changed})
                    for row in incoming:
                        if (row["task_id"] not in merged or row["task_id"] in changed) and row["status"] == "SNAPSHOT_READY":
                            images[row["task_id"]] = view / row["relative_path"]
                        merged[row["task_id"]] = row
                    failures = merge_failure_rows([r for r in failures if r["task_id"] not in changed], failed)
                    receipt = {"schema_version": "3.1-batch-reconciliation", "batch_id": batch,
                               "source_signature": signature, "rows": incoming, "failures": failed,
                               "postflight_sha256": sha256_json(verified["postflight"])}
                    new_receipts.append((receipt_path, receipt))
                    accepted.append({"batch_id": batch, "status": "VERIFIED", "task_count": len(incoming)})
            except (OSError, ValueError, RuntimeError) as exc:
                blocked.append({"batch_id": batch, "reason": f"{type(exc).__name__}: {exc}"})
        master.assert_unchanged(verify_digest=True)
        rows = sorted(merged.values(), key=lambda r: int(r["manifest_order"]))
        failed = sorted(failures, key=lambda r: int(r["manifest_order"]))
        if rows != transaction.rows or failed != transaction.failures:
            from .project_admission_v3 import _validate_snapshot_product
            transaction.publish(rows, failed, images=images, summary=transaction.summary,
                                validate=_validate_snapshot_product if len(rows) == len(expected) else None)
        for path, receipt in new_receipts:
            atomic_write_json(path, receipt)
        missing = [tid for tid in expected if tid not in merged]
        result = {"schema_version": "3.1-campaign-reconciliation", "master_sha256": master.master_sha256,
                  "expected_case_count": len(expected), "observed_case_count": len(rows),
                  "ready_case_count": sum(r["status"] == "SNAPSHOT_READY" for r in rows),
                  "failed_case_count": len({r["task_id"] for r in failed}), "missing_task_ids": missing,
                  "accepted_batches": accepted, "blocked_batches": blocked,
                  "status": "INCOMPLETE" if missing or blocked else "CASE_FAILURES" if failed else "SNAPSHOTS_READY",
                  "exit_code": 1 if missing or blocked else 2 if failed else 0,
                  "publication_generation": transaction.view.name}
        atomic_write_json(destination / "reports/campaign-reconciliation.json", result)
        return result
