from __future__ import annotations

import json
import shutil

from ssqtl_igv import campaign_reconcile, campaign_v3, project_launcher
from ssqtl_igv.snapshot_store import snapshot_view
from ssqtl_igv.utils import sha256_file, write_tsv
from test_campaign_v3 import _master_tasks, _prepared_campaign


def _completed_pilot(root, binding):
    contract = root / ".igv-pipeline/contract"
    contract.mkdir(parents=True)
    shutil.copyfile(binding["tasks_path"], contract / "tasks.jsonl")
    (contract / "run_identity.json").write_text(json.dumps({"batch_request_sha256": binding["request_sha256"]}))
    (root / "reports").mkdir()
    rows, trace = [], ["task_id\thash\tnative_id\tname\tstatus\texit"]
    for i, task in enumerate(binding["tasks"], 1):
        tid = task["task_id"]
        path = root / "snapshots" / task["core"]["locus"]["contig"] / (tid + ".png")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(tid.encode())
        rows.append({"manifest_order": str(i), "task_id": tid, "chromosome": task["core"]["locus"]["contig"],
                     "relative_path": str(path.relative_to(root)), "sha256": sha256_file(path), "status": "SNAPSHOT_READY"})
        case = root / ".igv-pipeline/cases" / tid
        case.mkdir(parents=True)
        result = {key: task[key] for key in ("task_id", "run_id", "generation_id", "manifest_order", "input_fingerprint")}
        result.update(eligible=True, artifacts={"review_image": {"sha256": sha256_file(path)}})
        (case / "case_result.json").write_text(json.dumps(result))
        (case / "terminal_bundle.json").write_text(json.dumps({"task_id": tid, "status": "SUCCEEDED"}))
        trace.append(f"{i}\taa\t-\tPROJECT_RUN:RUN_PORTABLE_CASE ({tid})\tCOMPLETED\t0")
    write_tsv(root / "snapshots.tsv", list(rows[0]), rows)
    write_tsv(root / "failed_cases.tsv", ["manifest_order", "task_id", "chromosome", "failure_code", "message", "input_fingerprint"], [])
    (root / "reports/trace.txt").write_text("\n".join(trace) + "\n")
    (root / "run_summary.json").write_text('{"authoritative":false,"status":"INFRASTRUCTURE_FATAL","exit_code":1}\n')


def test_campaign_recovers_completed_batches_without_changing_sources_or_shrinking_master(tmp_path, monkeypatch):
    master = _master_tasks()
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master)
    binding = campaign_v3.load_and_validate_batch_request(campaign / "batches/pilot-001/batch-request.json")
    runs = tmp_path / "runs"
    runs.mkdir()
    source = runs / "pilot-001"
    _completed_pilot(source, binding)
    monkeypatch.setattr(project_launcher, "validate_v3_terminal_bundle_document", lambda *a: None)
    before = (source / "run_summary.json").read_bytes()
    output = tmp_path / "collection"
    first = campaign_reconcile.reconcile_campaign(campaign, runs, output)
    assert first["expected_case_count"] == 8973
    assert first["observed_case_count"] == 100
    assert len(first["missing_task_ids"]) == 8873
    assert first["status"] == "INCOMPLETE" and first["exit_code"] == 1
    assert (source / "run_summary.json").read_bytes() == before
    assert not (source / "reports/postflight.json").exists()
    generation = snapshot_view(output)
    original = campaign_reconcile.validate_project_postflight
    monkeypatch.setattr(campaign_reconcile, "validate_project_postflight", lambda *a, **k: (_ for _ in ()).throw(AssertionError("accepted payload was revalidated")))
    second = campaign_reconcile.reconcile_campaign(campaign, runs, output)
    assert second["accepted_batches"][0]["status"] == "CACHED"
    assert snapshot_view(output) == generation
    with project_launcher.exclusive_run_output(source):
        active = campaign_reconcile.reconcile_campaign(campaign, runs, output)
        assert active["status"] == "INCOMPLETE"
        assert "producer is still active" in active["blocked_batches"][0]["reason"]
        assert snapshot_view(output) == generation
    monkeypatch.setattr(campaign_reconcile, "validate_project_postflight", original)
    (output / "reports/batch-reconciliation/pilot-001.json").unlink()
    third = campaign_reconcile.reconcile_campaign(campaign, runs, output)
    assert third["accepted_batches"][0]["status"] == "VERIFIED"
    assert snapshot_view(output) == generation


def test_cached_master_detects_drift(tmp_path, monkeypatch):
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, _master_tasks())
    master = campaign_v3.read_campaign_master_index(campaign)
    (campaign / "contract/master_tasks.jsonl").write_text("{}\n")
    import pytest
    with pytest.raises(ValueError, match="master changed"):
        master.assert_unchanged()


def test_collection_updates_only_a_verified_failed_only_replacement(tmp_path, monkeypatch):
    from ssqtl_igv import public_rerun_v3
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, _master_tasks())
    binding = campaign_v3.load_and_validate_batch_request(campaign / "batches/pilot-001/batch-request.json")
    runs = tmp_path / "runs"
    runs.mkdir()
    source = runs / "pilot-001"
    _completed_pilot(source, binding)
    monkeypatch.setattr(project_launcher, "validate_v3_terminal_bundle_document", lambda *a: None)
    rows = campaign_reconcile._rows(source / "snapshots.tsv")
    original = dict(rows[0])
    tid = original["task_id"]
    image = source / original["relative_path"]
    payload = image.read_bytes()
    image.unlink()
    rows[0].update(relative_path="", sha256="", status="CASE_FAILED")
    write_tsv(source / "snapshots.tsv", list(rows[0]), rows)
    task = binding["tasks"][0]
    failures = [{"manifest_order": "1", "task_id": tid, "chromosome": original["chromosome"],
                 "failure_code": "CASE_RENDER_FAILED", "message": "fixture", "input_fingerprint": task["input_fingerprint"]}]
    write_tsv(source / "failed_cases.tsv", list(failures[0]), failures)
    case = source / ".igv-pipeline/cases" / tid / "case_result.json"
    result = json.loads(case.read_text())
    result["eligible"] = False
    case.write_text(json.dumps(result))
    output = tmp_path / "collection"
    first = campaign_reconcile.reconcile_campaign(campaign, runs, output)
    assert first["ready_case_count"] == 99 and first["failed_case_count"] == 1
    image.write_bytes(payload)
    rows[0] = original
    write_tsv(source / "snapshots.tsv", list(rows[0]), rows)
    write_tsv(source / "failed_cases.tsv", list(failures[0]), [])
    result["eligible"] = True
    case.write_text(json.dumps(result))
    refused = campaign_reconcile.reconcile_campaign(campaign, runs, output)
    assert refused["ready_case_count"] == 99
    assert "rerun state" in refused["blocked_batches"][0]["reason"]
    calls = []
    monkeypatch.setattr(public_rerun_v3, "validate_projected_rerun_replacements", lambda *args: calls.append(args))
    accepted = campaign_reconcile.reconcile_campaign(campaign, runs, output)
    assert accepted["ready_case_count"] == 100 and accepted["failed_case_count"] == 0
    assert calls == [(source, {tid: task["input_fingerprint"]})]
