from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from ssqtl_igv import campaign_v3
from ssqtl_igv import v3_cli
from ssqtl_igv.campaign_v3 import (
    CampaignLockError,
    EXPECTED_MASTER_TASK_COUNT,
    EXPECTED_STRATA,
    append_campaign_event,
    campaign_lock,
    create_next_batch,
    load_and_validate_batch_request,
    materialize_batch_tasks,
    prepare_campaign,
    reduce_campaign_status,
    select_pilot_tasks,
    verify_campaign_ledger,
)
from ssqtl_igv.contracts import validate_schema_document
from ssqtl_igv.utils import sha256_file, sha256_json


SELECTION_FIELDS = {
    "task_id",
    "stratum",
    "track_count",
    "track_input_bytes",
    "overview_span_bp",
    "reason",
    "input_fingerprint",
}


def _master_tasks() -> list[dict]:
    strata = sorted(EXPECTED_STRATA)
    rows: list[dict] = []
    for order in range(1, EXPECTED_MASTER_TASK_COUNT + 1):
        stratum = strata[(order - 1) % len(strata)]
        contig, strand = stratum.split("|")
        track_count = 1 + (order % 4)
        tracks = [
            {
                "bam": {"identity": {"size": order + index}},
                "bai": {"identity": {"size": order // 2 + index}},
            }
            for index in range(track_count)
        ]
        task_id = f"case-{order:05d}"
        rows.append(
            {
                "schema_version": "3.0",
                "pipeline_version": "3.0.0",
                "run_id": "master-source",
                "generation_id": "master-generation",
                "task_id": task_id,
                "manifest_order": order,
                "adapter_id": "ssqtl",
                "core": {
                    "locus": {"contig": contig},
                    "strand": strand,
                    "tracks": tracks,
                    "preflight": {"state": "READY"},
                },
                "adapter_data": {
                    "adapter_schema_version": "3.0-ssqtl",
                    "regions": {
                        "overview": {"start": 100, "end": 100 + (order % 997)}
                    },
                },
                "estimated_runtime_seconds": 90.0,
                "input_fingerprint": hashlib.sha256(task_id.encode()).hexdigest(),
            }
        )
    return rows


@pytest.fixture(scope="module")
def master_tasks() -> list[dict]:
    return _master_tasks()


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _prepared_campaign(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> tuple[Path, dict]:
    monkeypatch.setattr(campaign_v3, "validate_v3_task_document", lambda _task: None)
    source = tmp_path / "master.jsonl"
    _write_jsonl(source, master_tasks)
    campaign = tmp_path / "campaign"
    result = prepare_campaign(
        source,
        campaign,
        campaign_id="fhs-igv-v3",
        actor="operator",
    )
    return campaign, result


def _append_approved_pilot(campaign: Path) -> tuple[list[dict], dict]:
    binding = load_and_validate_batch_request(
        campaign / "batches" / "pilot-001" / "batch-request.json"
    )
    for task in binding["tasks"]:
        append_campaign_event(
            campaign,
            event_type="HUMAN_DECISION",
            batch_id="pilot-001",
            actor="reviewer",
            payload={
                "task_id": task["task_id"],
                "artifact_review_state": "APPROVE",
                "scientific_interpretation": "INDETERMINATE",
            },
        )
    finalized = append_campaign_event(
        campaign,
        event_type="REVIEW_FINALIZED",
        batch_id="pilot-001",
        actor="reviewer",
        payload={
            "review_generation_id": "review-001",
            "decision_count": 100,
            "approved_count": 100,
            "rejected_count": 0,
            "all_eligible_decided": True,
            "review_receipt_sha256": "a" * 64,
        },
    )
    return binding["tasks"], finalized


def _publication_completion(
    tmp_path: Path,
    campaign: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    batch_id: str = "pilot-001",
    review_generation_id: str = "review-001",
    review_receipt_sha256: str = "a" * 64,
    runtime_fingerprint_sha256: str = "e" * 64,
) -> Path:
    del monkeypatch
    published = tmp_path / f"published-{batch_id}"
    published.mkdir(parents=True)
    (published / "result.txt").write_text("approved\n", encoding="utf-8")
    (published / "SHA256SUMS").write_text(
        f"{sha256_file(published / 'result.txt')}  result.txt\n",
        encoding="utf-8",
    )
    request = json.loads(
        (campaign / "batches" / batch_id / "batch-request.json").read_text(
            encoding="utf-8"
        )
    )
    contract = json.loads(
        (campaign / "contract" / "campaign.json").read_text(encoding="utf-8")
    )
    completion = {
        "schema_version": "3.0-publication-completion-receipt",
        "status": "ATOMIC_RENAME_COMMIT_RECORD",
        "authorized_destination": str(published.resolve()),
        "promotion_receipt_sha256": "b" * 64,
        "staging_tree_sha256": campaign_v3._tree_identity(published),
        "checksums_sha256": sha256_file(published / "SHA256SUMS"),
        "review_receipt_sha256": review_receipt_sha256,
        "review_generation_id": review_generation_id,
        "runtime_binding_sha256": "c" * 64,
        "runtime_fingerprint_sha256": runtime_fingerprint_sha256,
        "batch_purpose": request["purpose"],
        "master_task_set_sha256": contract["master_task_set_sha256"],
        "batch_task_set_sha256": request["task_set_sha256"],
    }
    path = tmp_path / f".{published.name}.publication-completion.json"
    path.write_text(json.dumps(completion, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_pilot_selection_is_deterministic_exact_and_science_neutral(
    master_tasks: list[dict],
) -> None:
    master_sha = sha256_json(
        [
            {
                "task_id": task["task_id"],
                "manifest_order": task["manifest_order"],
                "input_fingerprint": task["input_fingerprint"],
            }
            for task in master_tasks
        ]
    )
    first = select_pilot_tasks(
        master_tasks, master_task_set_sha256=master_sha, validate_documents=False
    )
    second = select_pilot_tasks(
        master_tasks, master_task_set_sha256=master_sha, validate_documents=False
    )
    assert first == second
    assert len(first) == len({row["task_id"] for row in first}) == 100
    assert all(set(row) == SELECTION_FIELDS for row in first)
    assert [row["reason"] for row in first].count("STRATUM_MIN") == 46
    assert [row["reason"] for row in first].count("STRATUM_MAX") == 46
    assert [row["reason"] for row in first].count("HASH_COMPLETION") == 8
    order = {task["task_id"]: task["manifest_order"] for task in master_tasks}
    assert [order[row["task_id"]] for row in first] == sorted(
        order[row["task_id"]] for row in first
    )
    assert not any(
        forbidden in json.dumps(first)
        for forbidden in ("abs_tvalue", "beta", "qacct", "runtime", "task_status")
    )


def test_pilot_selection_fails_closed_on_master_or_stratum_drift(
    master_tasks: list[dict],
) -> None:
    with pytest.raises(ValueError, match="exactly 8973"):
        select_pilot_tasks(master_tasks[:-1], validate_documents=False)
    changed = [dict(task) for task in master_tasks]
    changed[0] = {
        **changed[0],
        "core": {**changed[0]["core"], "strand": "?"},
    }
    with pytest.raises(ValueError, match="unsupported pilot stratum"):
        select_pilot_tasks(changed, validate_documents=False)


def test_prepare_freezes_atomic_contract_request_and_exact_subset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, result = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    assert result["status"] == "PREPARED"
    assert (campaign / "contract" / "campaign.json").is_file()
    assert not (campaign / ".contract.prepare").exists()
    assert not (campaign / "batches" / ".pilot-001.prepare").exists()
    selection = json.loads(
        (campaign / "contract" / "pilot_selection.json").read_text(encoding="utf-8")
    )
    contract = json.loads(
        (campaign / "contract" / "campaign.json").read_text(encoding="utf-8")
    )
    validate_schema_document(contract, "campaign-v3")
    validate_schema_document(selection, "pilot-selection-v3")
    assert all(set(row) == SELECTION_FIELDS for row in selection["selection"])
    tasks, binding = materialize_batch_tasks(result["batch_request"])
    validate_schema_document(binding["request"], "batch-request-v3")
    assert binding["request"]["purpose"] == "PILOT_QA"
    assert binding["request"]["execution_profile"] == "scc"
    assert len(tasks) == 100
    assert [task["manifest_order"] for task in tasks] == list(range(1, 101))
    assert {task["run_id"] for task in tasks} == {"fhs-igv-v3"}
    assert {task["generation_id"] for task in tasks} == {"pilot-001"}
    ledger = verify_campaign_ledger(campaign)
    validate_schema_document(ledger[0], "campaign-ledger-event-v3")
    assert [event["event_type"] for event in ledger] == ["SELECTION_FROZEN"]
    assert ledger[0]["payload"]["batch_request_sha256"] == sha256_file(
        result["batch_request"]
    )
    with pytest.raises(FileExistsError, match="already prepared"):
        prepare_campaign(
            tmp_path / "master.jsonl",
            campaign,
            campaign_id="fhs-igv-v3",
            actor="operator",
        )


def test_ledger_is_single_scientific_chain_and_rejects_execution_truth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    request = load_and_validate_batch_request(
        campaign / "batches" / "pilot-001" / "batch-request.json"
    )["request"]
    task_id = request["source_tasks"][0]["task_id"]
    event = append_campaign_event(
        campaign,
        event_type="HUMAN_DECISION",
        batch_id="pilot-001",
        actor="reviewer",
        payload={
            "task_id": task_id,
            "artifact_review_state": "APPROVE",
            "scientific_interpretation": "SUPPORTED",
        },
    )
    assert event["sequence"] == 2
    before = (campaign / "ledger" / "campaign-ledger.jsonl").read_bytes()
    with pytest.raises(ValueError, match="cannot record execution"):
        append_campaign_event(
            campaign,
            event_type="HUMAN_DECISION",
            batch_id="pilot-001",
            actor="reviewer",
            payload={
                "task_id": task_id,
                "artifact_review_state": "APPROVE",
                "scientific_interpretation": "SUPPORTED",
                "qacct": {"exit_code": 0},
            },
        )
    assert (campaign / "ledger" / "campaign-ledger.jsonl").read_bytes() == before


def test_campaign_lock_is_nonblocking_and_short(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    with campaign_lock(campaign):
        with pytest.raises(CampaignLockError, match="already active"):
            with campaign_lock(campaign):
                pass
    with campaign_lock(campaign):
        pass


def test_status_is_a_read_only_live_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    trace = tmp_path / "trace.txt"
    trace.write_text("task_id\tstatus\n", encoding="utf-8")
    qacct = tmp_path / "qacct.txt"
    qacct.write_text("jobnumber 123\n", encoding="utf-8")
    accounting = tmp_path / "accounting.json"
    accounting.write_text(
        json.dumps({"schema_version": "3.0-accounting", "status": "PASS"}) + "\n",
        encoding="utf-8",
    )
    before = {
        str(path.relative_to(campaign)): (path.stat().st_mtime_ns, sha256_file(path))
        for path in campaign.rglob("*")
        if path.is_file()
    }
    status = reduce_campaign_status(
        campaign,
        nextflow_trace=trace,
        raw_qacct=qacct,
        accounting_attestation=accounting,
    )
    after = {
        str(path.relative_to(campaign)): (path.stat().st_mtime_ns, sha256_file(path))
        for path in campaign.rglob("*")
        if path.is_file()
    }
    assert before == after
    assert status["authoritative"] is False
    assert status["projection_mode"] == "READ_ONLY_LIVE_REDUCER"
    assert status["live_sources"]["nextflow_trace"]["sha256"] == sha256_file(trace)
    assert status["live_sources"]["raw_qacct"]["sha256"] == sha256_file(qacct)
    assert status["live_sources"]["accounting_attestation"]["reported_status"] == "PASS"
    assert status["source_consistency"]["state"] == "UNBOUND"

    accounting.write_text(
        json.dumps(
            {
                "schema_version": "3.0-accounting",
                "provider": "sge_qacct",
                "status": "PASS",
                "qacct_used": True,
                "trace_sha256": "0" * 64,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    inconsistent = reduce_campaign_status(
        campaign,
        nextflow_trace=trace,
        raw_qacct=qacct,
        accounting_attestation=accounting,
    )
    assert inconsistent["status"] == "INCONSISTENT"
    assert inconsistent["source_consistency"]["state"] == "INCONSISTENT"
    assert inconsistent["source_consistency"]["comparisons"][0]["state"] == "MISMATCH"

    state = tmp_path / "scc-accounting"
    (state / "final").mkdir(parents=True)
    (state / "request").mkdir()
    scc_receipt = {
        "schema_version": "3.0-sge-qacct-accounting-receipt",
        "provider": "sge_qacct",
        "status": "PASS",
        "qacct_used": True,
    }
    scc_path = state / "final" / "accounting_receipt.json"
    scc_path.write_text(json.dumps(scc_receipt) + "\n", encoding="utf-8")
    (state / "request" / "request.json").write_text(
        json.dumps(
            {
                "schema_version": "3.0-sge-qacct-request",
                "trace_inputs": [{"sha256": sha256_file(trace)}],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    raw_sha = sha256_file(qacct)
    monkeypatch.setattr(
        "ssqtl_igv.accounting.verify_scc_accounting_receipt",
        lambda _root: {
            "receipt_sha256": sha256_file(scc_path),
            "receipt": scc_receipt,
            "scheduler_rows": [{"raw_sha256": raw_sha}],
        },
    )
    exact = reduce_campaign_status(
        campaign,
        nextflow_trace=trace,
        raw_qacct=qacct,
        accounting_attestation=scc_path,
    )
    assert exact["source_consistency"]["state"] == "CONSISTENT"
    qacct.write_text("tampered\n", encoding="utf-8")
    conflict = reduce_campaign_status(
        campaign,
        nextflow_trace=trace,
        raw_qacct=qacct,
        accounting_attestation=scc_path,
    )
    assert conflict["source_consistency"]["state"] == "INCONSISTENT"
    assert any("raw_qacct" in issue for issue in conflict["source_consistency"]["conflicts"])


def test_next_requires_pilot_approval_and_atomically_freezes_256_cases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    _append_approved_pilot(campaign)
    completion = _publication_completion(tmp_path, campaign, monkeypatch)
    result = create_next_batch(campaign, completion, actor="operator")
    assert result["status"] == "AUTHORIZED"
    assert result["batch_id"] == "batch-0002"
    assert result["task_count"] == 256
    assert not (campaign / "batches" / ".batch-0002.prepare").exists()
    binding = load_and_validate_batch_request(result["batch_request"])
    assert len(binding["tasks"]) == 256
    requests = [
        load_and_validate_batch_request(path)["request"]
        for path in sorted(campaign.glob("batches/*/batch-request.json"))
    ]
    ids = [row["task_id"] for request in requests for row in request["source_tasks"]]
    assert len(ids) == len(set(ids)) == 356
    with pytest.raises((FileExistsError, ValueError)):
        create_next_batch(campaign, completion, actor="operator")


def test_next_rejects_runtime_fingerprint_drift_between_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    _append_approved_pilot(campaign)
    pilot_completion = _publication_completion(tmp_path, campaign, monkeypatch)
    first = create_next_batch(campaign, pilot_completion, actor="operator")
    binding = load_and_validate_batch_request(first["batch_request"])
    for task in binding["tasks"]:
        append_campaign_event(
            campaign,
            event_type="HUMAN_DECISION",
            batch_id="batch-0002",
            actor="reviewer",
            payload={
                "task_id": task["task_id"],
                "artifact_review_state": "APPROVE",
                "scientific_interpretation": "INDETERMINATE",
            },
        )
    append_campaign_event(
        campaign,
        event_type="REVIEW_FINALIZED",
        batch_id="batch-0002",
        actor="reviewer",
        payload={
            "review_generation_id": "review-002",
            "decision_count": 256,
            "approved_count": 256,
            "rejected_count": 0,
            "all_eligible_decided": True,
            "review_receipt_sha256": "b" * 64,
        },
    )
    completion = _publication_completion(
        tmp_path,
        campaign,
        monkeypatch,
        batch_id="batch-0002",
        review_generation_id="review-002",
        review_receipt_sha256="b" * 64,
        runtime_fingerprint_sha256="f" * 64,
    )

    with pytest.raises(ValueError, match="runtime fingerprint changed"):
        create_next_batch(campaign, completion, actor="operator")


def test_next_leaves_partial_staging_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    _append_approved_pilot(campaign)
    completion = _publication_completion(tmp_path, campaign, monkeypatch)
    original = campaign_v3._write_exclusive_json

    def fail_request(path: Path, value: dict) -> None:
        if path.name == "batch-request.json" and ".batch-0002.prepare" in str(path):
            raise RuntimeError("injected batch freeze interruption")
        original(path, value)

    monkeypatch.setattr(campaign_v3, "_write_exclusive_json", fail_request)
    with pytest.raises(RuntimeError, match="injected"):
        create_next_batch(campaign, completion, actor="operator")
    orphan = campaign / "batches" / ".batch-0002.prepare"
    assert orphan.is_dir()
    assert not (campaign / "batches" / "batch-0002").exists()
    with pytest.raises(FileExistsError, match="requires audit"):
        create_next_batch(campaign, completion, actor="operator")


def test_next_recovers_object_published_before_ledger_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, master_tasks: list[dict]
) -> None:
    campaign, _ = _prepared_campaign(tmp_path, monkeypatch, master_tasks)
    _append_approved_pilot(campaign)
    completion = _publication_completion(tmp_path, campaign, monkeypatch)
    original = campaign_v3._append_event_locked

    def interrupt_after_publish(*args: object, **kwargs: object) -> dict:
        if kwargs.get("event_type") == "NEXT_BATCH_AUTHORIZED":
            raise RuntimeError("injected ledger interruption")
        return original(*args, **kwargs)

    monkeypatch.setattr(campaign_v3, "_append_event_locked", interrupt_after_publish)
    with pytest.raises(RuntimeError, match="ledger interruption"):
        create_next_batch(campaign, completion, actor="operator")
    orphan = campaign / "batches" / "batch-0002" / "batch-request.json"
    assert orphan.is_file()
    assert [row["event_type"] for row in verify_campaign_ledger(campaign)] == [
        "SELECTION_FROZEN",
        *(["HUMAN_DECISION"] * 100),
        "REVIEW_FINALIZED",
    ]

    monkeypatch.setattr(campaign_v3, "_append_event_locked", original)
    recovered = create_next_batch(campaign, completion, actor="operator")
    assert recovered["status"] == "AUTHORIZED"
    assert recovered["batch_id"] == "batch-0002"
    assert recovered["recovered_after_object_publish"] is True
    assert verify_campaign_ledger(campaign)[-1]["event_type"] == (
        "NEXT_BATCH_AUTHORIZED"
    )


def test_campaign_status_cli_fails_closed_on_inconsistent_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("IGV_SCC_APPROVED_ROOT", str(tmp_path))
    monkeypatch.setattr(
        v3_cli,
        "reduce_campaign_status",
        lambda *_args, **_kwargs: {
            "schema_version": "3.0-campaign-status-projection",
            "authoritative": False,
            "status": "INCONSISTENT",
        },
    )
    code = v3_cli.main(
        ["campaign", "status", "--campaign-dir", str(tmp_path)]
    )
    assert code == 2
    assert json.loads(capsys.readouterr().out)["status"] == "INCONSISTENT"
