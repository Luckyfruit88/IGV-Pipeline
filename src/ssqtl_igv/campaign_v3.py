from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import re
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

from .contracts import (
    SCIENTIFIC_INTERPRETATIONS,
    validate_unique_task_set,
    validate_v3_task_document,
    v3_task_fingerprint,
)
from .identity import task_set_fingerprint
from .publication import atomic_rename_noreplace, verify_checksum_tree
from .utils import (
    read_jsonl,
    read_regular_file_bytes,
    sha256_file,
    sha256_json,
    utc_now,
)


PILOT_POLICY_ID = "scc-pilot100-v1"
EXPECTED_MASTER_TASK_COUNT = 8_973
PILOT_TASK_COUNT = 100
FOLLOWUP_BATCH_LIMIT = 256
EXPECTED_STRATA = frozenset(
    f"chr{chrom}|{strand}"
    for chrom in [str(value) for value in range(1, 23)] + ["X"]
    for strand in ("+", "-")
)
LEDGER_EVENT_TYPES = frozenset(
    {
        "SELECTION_FROZEN",
        "HUMAN_DECISION",
        "REVIEW_FINALIZED",
        "NEXT_BATCH_AUTHORIZED",
    }
)
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
_SAFE_TASK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_EXECUTION_STATE_KEYS = frozenset(
    {
        "nextflow",
        "process",
        "process_name",
        "task_state",
        "task_status",
        "attempt",
        "attempts",
        "cache",
        "cached",
        "native_id",
        "native_ids",
        "qacct",
        "qacct_exit",
        "scheduler",
        "executor",
        "exit_code",
        "resource_usage",
        "cpu",
        "memory",
        "walltime",
        "failed_cases",
    }
)


class CampaignLockError(RuntimeError):
    """Raised when another short campaign control transaction owns the lock."""


@dataclass(frozen=True)
class CampaignLedgerContext:
    campaign_root: Path
    campaign_id: str
    campaign_contract_sha256: str
    ledger_path: Path
    lock_path: Path


def _safe_id(value: object, *, label: str) -> str:
    token = str(value).strip()
    if not _SAFE_ID.fullmatch(token):
        raise ValueError(f"{label} must match {_SAFE_ID.pattern}")
    return token


def _safe_task_id(value: object, *, label: str) -> str:
    token = str(value).strip()
    if not _SAFE_TASK_ID.fullmatch(token):
        raise ValueError(f"{label} must match {_SAFE_TASK_ID.pattern}")
    return token


def _sha(value: object, *, label: str) -> str:
    token = str(value).strip().lower()
    if not _SHA256.fullmatch(token):
        raise ValueError(f"{label} must be a lowercase SHA-256")
    return token


def _regular_directory(path: str | Path, *, label: str, create: bool = False) -> Path:
    value = Path(path).expanduser()
    if value.is_symlink():
        raise ValueError(f"{label} must not be a symlink: {value}")
    resolved = value.resolve(strict=False)
    if create:
        resolved.mkdir(parents=True, exist_ok=True)
    if not resolved.is_dir() or resolved.is_symlink():
        raise ValueError(f"{label} must be a regular directory: {resolved}")
    return resolved


def _json_object(path: str | Path, *, label: str) -> dict[str, Any]:
    source = Path(path).expanduser()
    if source.is_symlink() or not source.resolve(strict=True).is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {source}")
    try:
        value = json.loads(read_regular_file_bytes(source, label=label).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label}: {source}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {source}")
    return value


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_exclusive_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise ValueError(f"immutable parent must not be a symlink: {path.parent}")
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        offset = 0
        while offset < len(payload):
            written = os.write(descriptor, payload[offset:])
            if written < 1:  # pragma: no cover - defensive POSIX write guard
                raise OSError(f"zero-length write while freezing {path}")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _write_exclusive_json(path: Path, value: Mapping[str, Any]) -> None:
    _write_exclusive_bytes(path, _json_bytes(value))


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _json_document_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _jsonl_objects(payload: bytes, *, label: str) -> list[dict[str, Any]]:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} is not UTF-8: {exc}") from exc
    rows: list[dict[str, Any]] = []
    for line_number, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        try:
            row = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSONL in {label} at line {line_number}: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"non-object JSONL row in {label} at line {line_number}")
        rows.append(row)
    return rows


def _create_exclusive_staging_directory(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        raise FileExistsError(
            f"orphan or concurrent campaign staging directory requires audit: {path}"
        )
    os.mkdir(path, mode=0o700)
    _fsync_directory(path.parent)
    return path


def _publish_staged_directory(staging: Path, destination: Path) -> None:
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"immutable campaign destination already exists: {destination}")
    if staging.is_symlink() or not staging.is_dir():
        raise ValueError(f"campaign staging path is not a regular directory: {staging}")
    _fsync_directory(staging)
    atomic_rename_noreplace(staging, destination)
    _fsync_directory(destination.parent)


@contextmanager
def campaign_lock(campaign_dir: str | Path) -> Iterator[Path]:
    """Own the sole short, non-blocking control lock for one campaign.

    The caller must never hold this context while running Nextflow, qsub,
    interactive review, or file transfer.
    """

    root = _regular_directory(campaign_dir, label="campaign directory", create=True)
    control = root / "control"
    if control.is_symlink():
        raise ValueError(f"campaign control directory must not be a symlink: {control}")
    control.mkdir(mode=0o700, exist_ok=True)
    lock_path = control / "campaign.lock"
    if lock_path.is_symlink():
        raise ValueError(f"campaign lock must not be a symlink: {lock_path}")
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"campaign lock is not a regular file: {lock_path}")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CampaignLockError(f"campaign control transaction is already active: {root}") from exc
        yield lock_path
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _contract_path(root: Path) -> Path:
    return root / "contract" / "campaign.json"


def campaign_ledger_path(campaign_dir: str | Path) -> Path:
    root = Path(campaign_dir).expanduser().resolve(strict=False)
    return root / "ledger" / "campaign-ledger.jsonl"


def _validate_campaign_contract(contract: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(contract)
    required = {
        "schema_version",
        "campaign_id",
        "created_at",
        "adapter_id",
        "pilot_policy_id",
        "master_task_count",
        "master_tasks_relative_path",
        "master_tasks_sha256",
        "master_task_set_sha256",
        "pilot_selection_relative_path",
        "pilot_task_count",
        "followup_batch_limit",
        "ledger_relative_path",
        "contract_sha256",
    }
    if set(value) != required:
        raise ValueError("campaign contract field set differs from schema 3.0")
    if value["schema_version"] != "3.0-scientific-campaign":
        raise ValueError("campaign contract schema_version is invalid")
    _safe_id(value["campaign_id"], label="campaign_id")
    if value["adapter_id"] != "ssqtl" or value["pilot_policy_id"] != PILOT_POLICY_ID:
        raise ValueError("campaign contract is not the BU SCC ssQTL pilot policy")
    if int(value["master_task_count"]) != EXPECTED_MASTER_TASK_COUNT:
        raise ValueError("campaign contract master count is not 8,973")
    if int(value["pilot_task_count"]) != PILOT_TASK_COUNT:
        raise ValueError("campaign contract pilot count is not 100")
    if int(value["followup_batch_limit"]) != FOLLOWUP_BATCH_LIMIT:
        raise ValueError("campaign follow-up batch limit is not 256")
    for key in ("master_tasks_sha256", "master_task_set_sha256"):
        _sha(value[key], label=key)
    if value["master_tasks_relative_path"] != "contract/master_tasks.jsonl":
        raise ValueError("campaign master task path is invalid")
    if value["pilot_selection_relative_path"] != "contract/pilot_selection.json":
        raise ValueError("campaign pilot selection path is invalid")
    if value["ledger_relative_path"] != "ledger/campaign-ledger.jsonl":
        raise ValueError("campaign ledger path is invalid")
    claim = {key: item for key, item in value.items() if key != "contract_sha256"}
    if value["contract_sha256"] != sha256_json(claim):
        raise ValueError("campaign contract checksum is invalid")
    return value


def campaign_ledger_context(campaign_dir: str | Path) -> CampaignLedgerContext:
    root = _regular_directory(campaign_dir, label="campaign directory")
    contract_path = _contract_path(root)
    contract = _validate_campaign_contract(_json_object(contract_path, label="campaign contract"))
    return CampaignLedgerContext(
        campaign_root=root,
        campaign_id=str(contract["campaign_id"]),
        campaign_contract_sha256=sha256_file(contract_path),
        ledger_path=campaign_ledger_path(root),
        lock_path=root / "control" / "campaign.lock",
    )


def _complexity(task: Mapping[str, Any]) -> tuple[int, int, int]:
    try:
        tracks = task["core"]["tracks"]
        track_count = len(tracks)
        track_input_bytes = sum(
            int(track[role]["identity"]["size"])
            for track in tracks
            for role in ("bam", "bai")
        )
        overview = task["adapter_data"]["regions"]["overview"]
        overview_span_bp = int(overview["end"]) - int(overview["start"]) + 1
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"task lacks the fixed pilot complexity vector: {task.get('task_id')}") from exc
    if track_count < 0 or track_input_bytes < 0 or overview_span_bp < 1:
        raise ValueError(f"task has an invalid pilot complexity vector: {task.get('task_id')}")
    return track_count, track_input_bytes, overview_span_bp


def _stratum(task: Mapping[str, Any]) -> str:
    try:
        contig = str(task["core"]["locus"]["contig"])
        strand = str(task["core"]["strand"])
    except (KeyError, TypeError) as exc:
        raise ValueError(f"task lacks chromosome/strand pilot strata: {task.get('task_id')}") from exc
    value = f"{contig}|{strand}"
    if value not in EXPECTED_STRATA:
        raise ValueError(f"task has unsupported pilot stratum {value}: {task.get('task_id')}")
    return value


def _validate_master_tasks(
    tasks: Sequence[Mapping[str, Any]], *, validate_documents: bool
) -> list[dict[str, Any]]:
    rows = [dict(task) for task in tasks]
    if len(rows) != EXPECTED_MASTER_TASK_COUNT:
        raise ValueError(
            f"BU SCC campaign requires exactly {EXPECTED_MASTER_TASK_COUNT} master tasks; "
            f"observed {len(rows)}"
        )
    if [int(row.get("manifest_order", 0)) for row in rows] != list(
        range(1, EXPECTED_MASTER_TASK_COUNT + 1)
    ):
        raise ValueError("master tasks must be stored in contiguous manifest_order")
    if len({str(row.get("task_id", "")) for row in rows}) != len(rows):
        raise ValueError("master tasks contain duplicate task_id")
    for row in rows:
        if validate_documents:
            validate_v3_task_document(row)
        if (
            row.get("adapter_id") != "ssqtl"
            or row.get("adapter_data", {}).get("adapter_schema_version") != "3.0-ssqtl"
            or row.get("core", {}).get("preflight", {}).get("state") != "READY"
        ):
            raise ValueError(
                "campaign master must contain only READY native schema 3.0 ssQTL tasks"
            )
        _sha(row.get("input_fingerprint"), label="task input_fingerprint")
        _complexity(row)
        _stratum(row)
    return rows


def select_pilot_tasks(
    tasks: Sequence[Mapping[str, Any]],
    *,
    master_task_set_sha256: str | None = None,
    validate_documents: bool = True,
) -> list[dict[str, Any]]:
    """Select the deterministic 100-case BU SCC maintenance QA set."""

    rows = _validate_master_tasks(tasks, validate_documents=validate_documents)
    master_sha = master_task_set_sha256 or task_set_fingerprint(rows)
    _sha(master_sha, label="master_task_set_sha256")
    grouped: dict[str, list[dict[str, Any]]] = {value: [] for value in EXPECTED_STRATA}
    for task in rows:
        grouped[_stratum(task)].append(task)
    observed = {key for key, values in grouped.items() if values}
    if observed != EXPECTED_STRATA:
        raise ValueError("master task set does not contain exactly 46 chromosome×strand strata")

    selected: dict[str, dict[str, Any]] = {}
    for stratum in sorted(EXPECTED_STRATA):
        candidates = grouped[stratum]
        if len(candidates) < 2:
            raise ValueError(f"pilot stratum has fewer than two distinct tasks: {stratum}")
        low = min(candidates, key=lambda task: (_complexity(task), str(task["task_id"])))
        maximum = max(_complexity(task) for task in candidates)
        high = min(
            (
                task
                for task in candidates
                if task["task_id"] != low["task_id"] and _complexity(task) == maximum
            ),
            key=lambda task: str(task["task_id"]),
            default=None,
        )
        if high is None:
            high = min(
                (task for task in candidates if task["task_id"] != low["task_id"]),
                key=lambda task: (_complexity(task), str(task["task_id"])),
            )
        for task, reason in ((low, "STRATUM_MIN"), (high, "STRATUM_MAX")):
            track_count, track_bytes, span = _complexity(task)
            selected[str(task["task_id"])] = {
                "task_id": str(task["task_id"]),
                "stratum": stratum,
                "track_count": track_count,
                "track_input_bytes": track_bytes,
                "overview_span_bp": span,
                "reason": reason,
                "input_fingerprint": str(task["input_fingerprint"]),
            }
    if len(selected) != 92:
        raise ValueError("stratified pilot selection did not produce 92 distinct tasks")

    remaining = [task for task in rows if str(task["task_id"]) not in selected]
    completion = sorted(
        remaining,
        key=lambda task: (
            hashlib.sha256(
                (
                    PILOT_POLICY_ID + master_sha + str(task["task_id"])
                ).encode("utf-8")
            )
            .hexdigest(),
            str(task["task_id"]),
        ),
    )[:8]
    if len(completion) != 8:
        raise ValueError("master task set cannot supply eight hash-completion pilot tasks")
    for task in completion:
        track_count, track_bytes, span = _complexity(task)
        selected[str(task["task_id"])] = {
            "task_id": str(task["task_id"]),
            "stratum": _stratum(task),
            "track_count": track_count,
            "track_input_bytes": track_bytes,
            "overview_span_bp": span,
            "reason": "HASH_COMPLETION",
            "input_fingerprint": str(task["input_fingerprint"]),
        }
    master_order = {str(task["task_id"]): int(task["manifest_order"]) for task in rows}
    result = sorted(selected.values(), key=lambda row: master_order[str(row["task_id"])])
    if len(result) != PILOT_TASK_COUNT:
        raise ValueError("pilot selection did not produce exactly 100 distinct tasks")
    return result


def _materialized_tasks(
    source_tasks: Sequence[Mapping[str, Any]], *, run_id: str, generation_id: str
) -> list[dict[str, Any]]:
    materialized: list[dict[str, Any]] = []
    for order, source in enumerate(source_tasks, 1):
        task = copy.deepcopy(dict(source))
        task["run_id"] = run_id
        task["generation_id"] = generation_id
        task["manifest_order"] = order
        task["input_fingerprint"] = v3_task_fingerprint(task)
        validate_v3_task_document(task)
        materialized.append(task)
    return validate_unique_task_set(materialized)


def _batch_paths(root: Path, batch_id: str) -> tuple[Path, Path]:
    batch_root = root / "batches" / batch_id
    return batch_root / "batch-request.json", batch_root / "tasks.jsonl"


def _jsonl_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    return "".join(
        json.dumps(dict(row), sort_keys=True, ensure_ascii=False) + "\n" for row in rows
    ).encode("utf-8")


def _build_batch_request(
    *,
    root: Path,
    contract: Mapping[str, Any],
    batch_id: str,
    batch_index: int,
    purpose: str,
    source_tasks: Sequence[Mapping[str, Any]],
    selection_sha256: str | None,
    created_at: str | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], bytes]:
    run_id = str(contract["campaign_id"])
    generation_id = batch_id
    tasks = _materialized_tasks(source_tasks, run_id=run_id, generation_id=generation_id)
    task_bytes = _jsonl_bytes(tasks)
    task_sha = hashlib.sha256(task_bytes).hexdigest()
    relative_root = Path("batches") / batch_id
    request: dict[str, Any] = {
        "schema_version": "3.0-batch-request",
        "created_at": created_at or utc_now(),
        "campaign_id": contract["campaign_id"],
        "campaign_contract_sha256": _json_document_sha256(contract),
        "batch_id": batch_id,
        "batch_index": batch_index,
        "purpose": purpose,
        "execution_profile": "scc",
        "execution_run_id": run_id,
        "execution_generation_id": generation_id,
        "master_task_count": contract["master_task_count"],
        "master_tasks_sha256": contract["master_tasks_sha256"],
        "master_task_set_sha256": contract["master_task_set_sha256"],
        "pilot_selection_sha256": selection_sha256,
        "tasks_relative_path": str(relative_root / "tasks.jsonl"),
        "task_count": len(tasks),
        "tasks_sha256": task_sha,
        "task_set_sha256": task_set_fingerprint(tasks),
        "source_tasks": [
            {
                "task_id": str(source["task_id"]),
                "source_manifest_order": int(source["manifest_order"]),
                "source_input_fingerprint": str(source["input_fingerprint"]),
                "batch_manifest_order": index,
                "batch_input_fingerprint": str(task["input_fingerprint"]),
            }
            for index, (source, task) in enumerate(zip(source_tasks, tasks), 1)
        ],
    }
    request["request_sha256"] = sha256_json(request)
    return request, tasks, task_bytes


def _validate_batch_request_document(
    request: Mapping[str, Any], *, contract: Mapping[str, Any]
) -> dict[str, Any]:
    value = dict(request)
    required = {
        "schema_version",
        "created_at",
        "campaign_id",
        "campaign_contract_sha256",
        "batch_id",
        "batch_index",
        "purpose",
        "execution_profile",
        "execution_run_id",
        "execution_generation_id",
        "master_task_count",
        "master_tasks_sha256",
        "master_task_set_sha256",
        "pilot_selection_sha256",
        "tasks_relative_path",
        "task_count",
        "tasks_sha256",
        "task_set_sha256",
        "source_tasks",
        "request_sha256",
    }
    if set(value) != required:
        raise ValueError("batch-request field set differs from schema 3.0")
    if value["schema_version"] != "3.0-batch-request":
        raise ValueError("batch-request schema_version is invalid")
    if value["execution_profile"] != "scc":
        raise ValueError("campaign batch-request execution profile must be scc")
    if value["campaign_id"] != contract["campaign_id"]:
        raise ValueError("batch-request campaign identity differs from campaign contract")
    if value["campaign_contract_sha256"] != _json_document_sha256(contract):
        raise ValueError("batch-request campaign contract checksum differs")
    batch_id = _safe_id(value["batch_id"], label="batch_id")
    if value["execution_run_id"] != contract["campaign_id"]:
        raise ValueError("batch-request execution run ID differs from campaign")
    if value["execution_generation_id"] != batch_id:
        raise ValueError("batch-request generation ID differs from batch ID")
    if int(value["batch_index"]) < 1:
        raise ValueError("batch-request index must be positive")
    purpose = value["purpose"]
    if purpose not in {"PILOT_QA", "PRODUCTION_CONTINUATION"}:
        raise ValueError("batch-request purpose is invalid")
    count = int(value["task_count"])
    if count < 1 or count > FOLLOWUP_BATCH_LIMIT:
        raise ValueError("batch-request task count is outside 1..256")
    if purpose == "PILOT_QA" and (
        int(value["batch_index"]) != 1 or count != PILOT_TASK_COUNT
    ):
        raise ValueError("pilot batch-request must be index 1 with exactly 100 tasks")
    if value["master_task_count"] != contract["master_task_count"]:
        raise ValueError("batch-request master task count differs")
    for key in ("master_tasks_sha256", "master_task_set_sha256"):
        if value[key] != contract[key]:
            raise ValueError(f"batch-request {key} differs from campaign contract")
    for key in ("tasks_sha256", "task_set_sha256"):
        _sha(value[key], label=key)
    if value["pilot_selection_sha256"] is not None:
        _sha(value["pilot_selection_sha256"], label="pilot_selection_sha256")
    expected_relative = str(Path("batches") / batch_id / "tasks.jsonl")
    if value["tasks_relative_path"] != expected_relative:
        raise ValueError("batch-request task path is invalid")
    sources = value["source_tasks"]
    if not isinstance(sources, list) or len(sources) != count:
        raise ValueError("batch-request source task mapping count differs")
    source_fields = {
        "task_id",
        "source_manifest_order",
        "source_input_fingerprint",
        "batch_manifest_order",
        "batch_input_fingerprint",
    }
    if any(not isinstance(row, dict) or set(row) != source_fields for row in sources):
        raise ValueError("batch-request source task mapping field set is invalid")
    if len({row["task_id"] for row in sources}) != count:
        raise ValueError("batch-request source task mapping contains duplicate task IDs")
    if [int(row.get("batch_manifest_order", 0)) for row in sources] != list(
        range(1, count + 1)
    ):
        raise ValueError("batch-request manifest order mapping is not contiguous")
    source_orders = [int(row.get("source_manifest_order", 0)) for row in sources]
    if source_orders != sorted(source_orders) or len(source_orders) != len(set(source_orders)):
        raise ValueError("batch-request does not preserve immutable master order")
    for row in sources:
        _safe_task_id(row["task_id"], label="batch source task_id")
        _sha(row["source_input_fingerprint"], label="source_input_fingerprint")
        _sha(row["batch_input_fingerprint"], label="batch_input_fingerprint")
    claim = {key: item for key, item in value.items() if key != "request_sha256"}
    if value["request_sha256"] != sha256_json(claim):
        raise ValueError("batch-request checksum is invalid")
    return value


def load_and_validate_batch_request(batch_request: str | Path) -> dict[str, Any]:
    """Return the exact request-bound canonical subset for Nextflow admission."""

    request_path = Path(batch_request).expanduser()
    request = _json_object(request_path, label="batch-request")
    try:
        root = request_path.resolve(strict=True).parents[2]
    except IndexError as exc:
        raise ValueError("batch-request is outside a campaign batches tree") from exc
    if request_path.resolve(strict=True).parent.parent.name != "batches":
        raise ValueError("batch-request is outside a campaign batches tree")
    contract_path = _contract_path(root)
    contract = _validate_campaign_contract(_json_object(contract_path, label="campaign contract"))
    request = _validate_batch_request_document(request, contract=contract)
    if request["campaign_contract_sha256"] != _json_document_sha256(contract):
        raise ValueError("batch-request no longer binds the campaign contract")
    tasks_path = root / str(request["tasks_relative_path"])
    if tasks_path.is_symlink() or not tasks_path.resolve(strict=True).is_file():
        raise ValueError("batch-request canonical task subset is unavailable")
    resolved_tasks = tasks_path.resolve(strict=True)
    try:
        resolved_tasks.relative_to((root / "batches").resolve(strict=True))
    except ValueError as exc:
        raise ValueError("batch-request canonical task subset escapes campaign root") from exc
    task_bytes = read_regular_file_bytes(
        resolved_tasks,
        expected_sha256=request["tasks_sha256"],
        label="batch canonical task subset",
    )
    tasks = _jsonl_objects(task_bytes, label="batch canonical task subset")
    if len(tasks) != int(request["task_count"]):
        raise ValueError("batch-request canonical task count differs")
    tasks = validate_unique_task_set(tasks)
    source_map = request["source_tasks"]
    for task, source in zip(tasks, source_map):
        validate_v3_task_document(task)
        if (
            task["adapter_id"] != "ssqtl"
            or task["run_id"] != request["execution_run_id"]
            or task["generation_id"] != request["execution_generation_id"]
            or task["task_id"] != source["task_id"]
            or task["manifest_order"] != source["batch_manifest_order"]
            or task["input_fingerprint"] != source["batch_input_fingerprint"]
        ):
            raise ValueError("batch-request canonical task identity differs from source mapping")
    if task_set_fingerprint(tasks) != request["task_set_sha256"]:
        raise ValueError("batch-request canonical task-set checksum differs")
    master_path = root / str(contract["master_tasks_relative_path"])
    master_bytes = read_regular_file_bytes(
        master_path,
        expected_sha256=contract["master_tasks_sha256"],
        label="campaign master canonical tasks",
    )
    master = _jsonl_objects(master_bytes, label="campaign master canonical tasks")
    master_by_id = {str(task["task_id"]): task for task in master}
    if task_set_fingerprint(master) != contract["master_task_set_sha256"]:
        raise ValueError("campaign master task set changed")
    for source in source_map:
        master_task = master_by_id.get(str(source["task_id"]))
        if master_task is None or (
            int(master_task["manifest_order"]) != int(source["source_manifest_order"])
            or master_task["input_fingerprint"] != source["source_input_fingerprint"]
        ):
            raise ValueError("batch-request source mapping differs from immutable master")
    return {
        "schema_version": "3.0-validated-batch-binding",
        "campaign_root": str(root),
        "campaign_id": contract["campaign_id"],
        "campaign_contract_sha256": _json_document_sha256(contract),
        "request_path": str(request_path.resolve(strict=True)),
        "request_sha256": sha256_file(request_path),
        "request": request,
        "tasks_path": str(resolved_tasks),
        "tasks": tasks,
        "tasks_sha256": request["tasks_sha256"],
        "task_set_sha256": request["task_set_sha256"],
    }


def materialize_batch_tasks(batch_request: str | Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Convenience admission API: exact tasks plus their immutable binding."""

    binding = load_and_validate_batch_request(batch_request)
    return list(binding["tasks"]), {key: value for key, value in binding.items() if key != "tasks"}


def _payload_key_scan(value: object, *, location: str = "payload") -> None:
    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key).strip().lower().replace("-", "_")
            if key in _EXECUTION_STATE_KEYS:
                raise ValueError(
                    f"campaign ledger cannot record execution/scheduler state: {location}.{raw_key}"
                )
            _payload_key_scan(child, location=f"{location}.{raw_key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _payload_key_scan(child, location=f"{location}[{index}]")


def _validate_event_payload(event_type: str, payload: Mapping[str, Any]) -> None:
    _payload_key_scan(payload)
    if event_type == "SELECTION_FROZEN":
        required = {
            "pilot_selection_relative_path",
            "pilot_selection_sha256",
            "batch_request_sha256",
            "selected_task_count",
            "master_task_set_sha256",
        }
        if not required.issubset(payload) or int(payload["selected_task_count"]) != 100:
            raise ValueError("selection ledger event is incomplete")
        _sha(payload["pilot_selection_sha256"], label="pilot_selection_sha256")
        _sha(payload["batch_request_sha256"], label="batch_request_sha256")
        _sha(payload["master_task_set_sha256"], label="master_task_set_sha256")
    elif event_type == "HUMAN_DECISION":
        required = {"task_id", "artifact_review_state", "scientific_interpretation"}
        if not required.issubset(payload):
            raise ValueError("human-decision ledger event is incomplete")
        _safe_task_id(payload["task_id"], label="decision task_id")
        if payload["artifact_review_state"] not in {"APPROVE", "REJECT"}:
            raise ValueError("human decision must be APPROVE or REJECT")
        if payload["scientific_interpretation"] not in SCIENTIFIC_INTERPRETATIONS - {"PENDING"}:
            raise ValueError("human decision requires a final scientific interpretation")
    elif event_type == "REVIEW_FINALIZED":
        required = {
            "review_generation_id",
            "decision_count",
            "approved_count",
            "rejected_count",
            "all_eligible_decided",
            "review_receipt_sha256",
        }
        if not required.issubset(payload) or payload["all_eligible_decided"] is not True:
            raise ValueError("review-finalized ledger event is incomplete")
        _sha(payload["review_receipt_sha256"], label="review_receipt_sha256")
        if int(payload["approved_count"]) + int(payload["rejected_count"]) != int(
            payload["decision_count"]
        ):
            raise ValueError("review-finalized decision counts are inconsistent")
    elif event_type == "NEXT_BATCH_AUTHORIZED":
        required = {
            "prior_batch_id",
            "next_batch_id",
            "publication_completion_sha256",
            "published_tree_sha256",
            "runtime_fingerprint_sha256",
            "next_batch_request_sha256",
        }
        if not required.issubset(payload):
            raise ValueError("next-batch authorization ledger event is incomplete")
        _safe_id(payload["prior_batch_id"], label="prior_batch_id")
        _safe_id(payload["next_batch_id"], label="next_batch_id")
        _sha(payload["publication_completion_sha256"], label="publication_completion_sha256")
        _sha(payload["published_tree_sha256"], label="published_tree_sha256")
        _sha(payload["runtime_fingerprint_sha256"], label="runtime_fingerprint_sha256")
        _sha(payload["next_batch_request_sha256"], label="next_batch_request_sha256")


def _authorized_request_sha256(
    rows: Sequence[Mapping[str, Any]], batch_id: str
) -> str | None:
    for event in reversed(rows):
        if event["event_type"] == "SELECTION_FROZEN" and event["batch_id"] == batch_id:
            return str(event["payload"]["batch_request_sha256"])
        if (
            event["event_type"] == "NEXT_BATCH_AUTHORIZED"
            and event["payload"]["next_batch_id"] == batch_id
        ):
            return str(event["payload"]["next_batch_request_sha256"])
    return None


def _load_authorized_request(
    context: CampaignLedgerContext, batch_id: str, expected_sha256: str
) -> dict[str, Any]:
    request_path, _ = _batch_paths(context.campaign_root, batch_id)
    if sha256_file(request_path) != _sha(expected_sha256, label="authorized batch-request"):
        raise ValueError(f"authorized batch-request bytes changed: {batch_id}")
    contract = _validate_campaign_contract(
        _json_object(_contract_path(context.campaign_root), label="campaign contract")
    )
    return _validate_batch_request_document(
        _json_object(request_path, label="batch-request"), contract=contract
    )


def _event_claim(event: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in event.items() if key != "event_sha256"}


def _verify_ledger_rows(context: CampaignLedgerContext) -> list[dict[str, Any]]:
    path = context.ledger_path
    if path.is_symlink():
        raise ValueError(f"campaign ledger must not be a symlink: {path}")
    if not path.exists():
        return []
    if not path.is_file():
        raise ValueError(f"campaign ledger is not a regular file: {path}")
    rows = list(read_jsonl(path))
    previous: str | None = None
    for sequence, event in enumerate(rows, 1):
        required = {
            "schema_version",
            "campaign_id",
            "campaign_contract_sha256",
            "sequence",
            "recorded_at",
            "actor",
            "batch_id",
            "event_type",
            "payload",
            "previous_event_sha256",
            "event_sha256",
        }
        if set(event) != required:
            raise ValueError(f"campaign ledger event {sequence} field set is invalid")
        if (
            event["schema_version"] != "3.0-campaign-ledger-event"
            or event["campaign_id"] != context.campaign_id
            or event["campaign_contract_sha256"] != context.campaign_contract_sha256
            or int(event["sequence"]) != sequence
            or event["previous_event_sha256"] != previous
            or event["event_type"] not in LEDGER_EVENT_TYPES
            or not isinstance(event["payload"], dict)
        ):
            raise ValueError(f"campaign ledger event {sequence} identity/chain is invalid")
        _safe_id(event["batch_id"], label="ledger batch_id")
        if not str(event["actor"]).strip():
            raise ValueError(f"campaign ledger event {sequence} actor is empty")
        _validate_event_payload(str(event["event_type"]), event["payload"])
        if event["event_sha256"] != sha256_json(_event_claim(event)):
            raise ValueError(f"campaign ledger event {sequence} checksum is invalid")
        previous = str(event["event_sha256"])
    for batch_id in sorted({str(event["batch_id"]) for event in rows}):
        expected = _authorized_request_sha256(rows, batch_id)
        if expected is None:
            raise ValueError(f"campaign ledger batch lacks immutable request authorization: {batch_id}")
        _load_authorized_request(context, batch_id, expected)
    return rows


def verify_campaign_ledger(campaign_dir: str | Path) -> list[dict[str, Any]]:
    return _verify_ledger_rows(campaign_ledger_context(campaign_dir))


def _append_event_locked(
    context: CampaignLedgerContext,
    *,
    event_type: str,
    batch_id: str,
    actor: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if event_type not in LEDGER_EVENT_TYPES:
        raise ValueError(f"unsupported campaign ledger event: {event_type}")
    batch_id = _safe_id(batch_id, label="batch_id")
    actor_value = str(actor).strip()
    if not actor_value or len(actor_value) > 200:
        raise ValueError("campaign ledger actor must be 1..200 characters")
    payload_value = copy.deepcopy(dict(payload))
    _validate_event_payload(event_type, payload_value)
    rows = _verify_ledger_rows(context)
    expected_request_sha = _authorized_request_sha256(rows, batch_id)
    if event_type == "SELECTION_FROZEN":
        expected_request_sha = str(payload_value["batch_request_sha256"])
    elif event_type == "NEXT_BATCH_AUTHORIZED":
        expected_request_sha = str(payload_value["next_batch_request_sha256"])
    if expected_request_sha is None:
        raise ValueError(f"campaign event batch lacks immutable request authorization: {batch_id}")
    request = _load_authorized_request(context, batch_id, expected_request_sha)
    if event_type == "HUMAN_DECISION":
        allowed = {row["task_id"] for row in request["source_tasks"]}
        if payload_value["task_id"] not in allowed:
            raise ValueError("human decision task is outside its immutable batch-request")
        if any(
            event["batch_id"] == batch_id
            and event["event_type"] == "REVIEW_FINALIZED"
            for event in rows
        ):
            raise ValueError("finalized campaign batch cannot accept more human decisions")
    elif event_type == "REVIEW_FINALIZED":
        if any(
            event["batch_id"] == batch_id
            and event["event_type"] == "REVIEW_FINALIZED"
            for event in rows
        ):
            raise ValueError("campaign batch review is already finalized")
        decisions: dict[str, Mapping[str, Any]] = {}
        for prior in rows:
            if (
                prior["batch_id"] == batch_id
                and prior["event_type"] == "HUMAN_DECISION"
            ):
                decisions[str(prior["payload"]["task_id"])] = prior
        expected = {row["task_id"] for row in request["source_tasks"]}
        approved = sum(
            event["payload"]["artifact_review_state"] == "APPROVE"
            for event in decisions.values()
        )
        rejected = len(decisions) - approved
        if (
            set(decisions) != expected
            or int(payload_value["decision_count"]) != len(expected)
            or int(payload_value["approved_count"]) != approved
            or int(payload_value["rejected_count"]) != rejected
        ):
            raise ValueError("review finalize does not exactly cover batch human decisions")
    event: dict[str, Any] = {
        "schema_version": "3.0-campaign-ledger-event",
        "campaign_id": context.campaign_id,
        "campaign_contract_sha256": context.campaign_contract_sha256,
        "sequence": len(rows) + 1,
        "recorded_at": utc_now(),
        "actor": actor_value,
        "batch_id": batch_id,
        "event_type": event_type,
        "payload": payload_value,
        "previous_event_sha256": rows[-1]["event_sha256"] if rows else None,
    }
    event["event_sha256"] = sha256_json(event)
    path = context.ledger_path
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        payload_bytes = (
            json.dumps(event, sort_keys=True, ensure_ascii=False) + "\n"
        ).encode("utf-8")
        offset = 0
        while offset < len(payload_bytes):
            offset += os.write(descriptor, payload_bytes[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)
    return event


def append_campaign_event(
    campaign_dir: str | Path,
    *,
    event_type: str,
    batch_id: str,
    actor: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    with campaign_lock(campaign_dir):
        context = campaign_ledger_context(campaign_dir)
        return _append_event_locked(
            context,
            event_type=event_type,
            batch_id=batch_id,
            actor=actor,
            payload=payload,
        )


def prepare_campaign(
    master_tasks: str | Path,
    campaign_dir: str | Path,
    *,
    campaign_id: str,
    actor: str,
) -> dict[str, Any]:
    campaign_id = _safe_id(campaign_id, label="campaign_id")
    source = Path(master_tasks).expanduser()
    if source.is_symlink() or not source.resolve(strict=True).is_file():
        raise ValueError(f"master tasks must be a regular non-symlink file: {source}")
    source = source.resolve(strict=True)
    source_bytes = read_regular_file_bytes(source, label="master canonical tasks")
    rows = _jsonl_objects(source_bytes, label="master canonical tasks")
    rows = _validate_master_tasks(rows, validate_documents=True)
    master_set_sha = task_set_fingerprint(rows)
    selection_rows = select_pilot_tasks(
        rows, master_task_set_sha256=master_set_sha, validate_documents=False
    )
    master_by_id = {str(task["task_id"]): task for task in rows}
    pilot_source = sorted(
        (master_by_id[row["task_id"]] for row in selection_rows),
        key=lambda task: int(task["manifest_order"]),
    )
    root = _regular_directory(campaign_dir, label="campaign directory", create=True)
    contract_claim: dict[str, Any] = {
        "schema_version": "3.0-scientific-campaign",
        "campaign_id": campaign_id,
        "created_at": utc_now(),
        "adapter_id": "ssqtl",
        "pilot_policy_id": PILOT_POLICY_ID,
        "master_task_count": EXPECTED_MASTER_TASK_COUNT,
        "master_tasks_relative_path": "contract/master_tasks.jsonl",
        "master_tasks_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "master_task_set_sha256": master_set_sha,
        "pilot_selection_relative_path": "contract/pilot_selection.json",
        "pilot_task_count": PILOT_TASK_COUNT,
        "followup_batch_limit": FOLLOWUP_BATCH_LIMIT,
        "ledger_relative_path": "ledger/campaign-ledger.jsonl",
    }
    contract = {**contract_claim, "contract_sha256": sha256_json(contract_claim)}
    selection_claim: dict[str, Any] = {
        "schema_version": "3.0-pilot-selection",
        "campaign_id": campaign_id,
        "policy_id": PILOT_POLICY_ID,
        "created_at": utc_now(),
        "master_task_count": EXPECTED_MASTER_TASK_COUNT,
        "master_tasks_sha256": contract["master_tasks_sha256"],
        "master_task_set_sha256": master_set_sha,
        "stratum_count": len(EXPECTED_STRATA),
        "strata": sorted(EXPECTED_STRATA),
        "selected_task_count": PILOT_TASK_COUNT,
        "selection": selection_rows,
    }
    selection = {
        **selection_claim,
        "selection_sha256": sha256_json(selection_claim),
    }
    selection_file_sha256 = _json_document_sha256(selection)
    request, _tasks, task_bytes = _build_batch_request(
        root=root,
        contract=contract,
        batch_id="pilot-001",
        batch_index=1,
        purpose="PILOT_QA",
        source_tasks=pilot_source,
        selection_sha256=selection_file_sha256,
    )
    request_path, task_path = _batch_paths(root, "pilot-001")
    with campaign_lock(root):
        visible_entries = {path.name for path in root.iterdir()} - {"control"}
        if visible_entries:
            raise FileExistsError(f"campaign is already prepared: {root}")
        contract_stage = _create_exclusive_staging_directory(root / ".contract.prepare")
        batches_root = root / "batches"
        batches_root.mkdir(mode=0o700)
        _fsync_directory(root)
        batch_stage = _create_exclusive_staging_directory(
            batches_root / ".pilot-001.prepare"
        )
        _write_exclusive_bytes(contract_stage / "master_tasks.jsonl", source_bytes)
        _write_exclusive_json(contract_stage / "campaign.json", contract)
        _write_exclusive_json(contract_stage / "pilot_selection.json", selection)
        _write_exclusive_bytes(batch_stage / "tasks.jsonl", task_bytes)
        _write_exclusive_json(batch_stage / "batch-request.json", request)
        _publish_staged_directory(contract_stage, root / "contract")
        _publish_staged_directory(batch_stage, request_path.parent)
        context = campaign_ledger_context(root)
        selection_event = _append_event_locked(
            context,
            event_type="SELECTION_FROZEN",
            batch_id="pilot-001",
            actor=actor,
            payload={
                "pilot_selection_relative_path": "contract/pilot_selection.json",
                "pilot_selection_sha256": selection_file_sha256,
                "batch_request_sha256": sha256_file(request_path),
                "selected_task_count": PILOT_TASK_COUNT,
                "master_task_set_sha256": master_set_sha,
            },
        )
    return {
        "schema_version": "3.0-campaign-prepare-result",
        "status": "PREPARED",
        "campaign_id": campaign_id,
        "campaign_dir": str(root),
        "master_task_count": EXPECTED_MASTER_TASK_COUNT,
        "pilot_task_count": PILOT_TASK_COUNT,
        "campaign_contract": str(_contract_path(root)),
        "campaign_contract_sha256": sha256_file(_contract_path(root)),
        "pilot_selection": str(root / "contract" / "pilot_selection.json"),
        "pilot_selection_sha256": sha256_file(root / "contract" / "pilot_selection.json"),
        "batch_request": str(request_path),
        "batch_request_sha256": sha256_file(request_path),
        "ledger_head_sha256": selection_event["event_sha256"],
    }


def _batch_requests(root: Path) -> list[tuple[Path, dict[str, Any]]]:
    paths = sorted((root / "batches").glob("*/batch-request.json"))
    requests = [(path, load_and_validate_batch_request(path)["request"]) for path in paths]
    requests.sort(key=lambda item: int(item[1]["batch_index"]))
    if [int(value["batch_index"]) for _path, value in requests] != list(
        range(1, len(requests) + 1)
    ):
        raise ValueError("campaign batch-request indices are not contiguous")
    ids = [source["task_id"] for _path, value in requests for source in value["source_tasks"]]
    if len(ids) != len(set(ids)):
        raise ValueError("campaign batch-requests assign a master task more than once")
    return requests


def _tree_identity(root: Path) -> str:
    rows = [
        {
            "relative_path": str(path.relative_to(root)),
            "size": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(
            (candidate for candidate in root.rglob("*") if candidate.is_file()),
            key=lambda candidate: str(candidate.relative_to(root)),
        )
    ]
    return sha256_json(rows)


def _verify_publication_completion(
    completion_path: str | Path,
    *,
    finalized_event: Mapping[str, Any],
    request: Mapping[str, Any],
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], str, str]:
    path = Path(completion_path).expanduser()
    completion = _json_object(path, label="publication completion receipt")
    required = {
        "schema_version",
        "status",
        "authorized_destination",
        "promotion_receipt_sha256",
        "staging_tree_sha256",
        "checksums_sha256",
        "review_receipt_sha256",
        "review_generation_id",
        "runtime_binding_sha256",
        "runtime_fingerprint_sha256",
        "batch_purpose",
        "master_task_set_sha256",
        "batch_task_set_sha256",
    }
    optional = {"runtime_manifest_sha256", "runtime_oci_digest"}
    if not required.issubset(completion) or set(completion) - required - optional or (
        completion["schema_version"] != "3.0-publication-completion-receipt"
        or completion["status"] != "ATOMIC_RENAME_COMMIT_RECORD"
    ):
        raise ValueError("publication completion receipt is invalid")
    payload = finalized_event["payload"]
    if (
        completion["review_receipt_sha256"] != payload["review_receipt_sha256"]
        or completion["review_generation_id"] != payload["review_generation_id"]
    ):
        raise ValueError("publication completion differs from finalized human review")
    if (
        completion["batch_purpose"] != request["purpose"]
        or completion["master_task_set_sha256"] != contract["master_task_set_sha256"]
        or completion["batch_task_set_sha256"] != request["task_set_sha256"]
    ):
        raise ValueError("publication completion differs from campaign batch")
    for field in (
        "promotion_receipt_sha256",
        "staging_tree_sha256",
        "checksums_sha256",
        "review_receipt_sha256",
        "runtime_binding_sha256",
        "runtime_fingerprint_sha256",
        "master_task_set_sha256",
        "batch_task_set_sha256",
    ):
        _sha(completion[field], label=f"completion {field}")
    if "runtime_manifest_sha256" in completion:
        _sha(
            completion["runtime_manifest_sha256"],
            label="completion runtime_manifest_sha256",
        )
    destination = Path(str(completion["authorized_destination"])).expanduser()
    if destination.is_symlink() or not destination.resolve(strict=True).is_dir():
        raise ValueError("published destination is unavailable or symlinked")
    destination = destination.resolve(strict=True)
    expected_sidecar = destination.parent / f".{destination.name}.publication-completion.json"
    if path.resolve(strict=True) != expected_sidecar.resolve(strict=False):
        raise ValueError("publication completion is not the destination sidecar")
    verify_checksum_tree(destination)
    if sha256_file(destination / "SHA256SUMS") != completion["checksums_sha256"]:
        raise ValueError("published checksum manifest differs from completion receipt")
    tree_sha = _tree_identity(destination)
    if tree_sha != completion["staging_tree_sha256"]:
        raise ValueError("published tree differs from completion receipt")
    return completion, sha256_file(path), tree_sha


def _latest_finalized_event(
    rows: Sequence[Mapping[str, Any]], *, batch_id: str, request: Mapping[str, Any]
) -> dict[str, Any]:
    decisions: dict[str, Mapping[str, Any]] = {}
    finalized: dict[str, Any] | None = None
    for event in rows:
        if event["batch_id"] != batch_id:
            continue
        if event["event_type"] == "HUMAN_DECISION":
            decisions[str(event["payload"]["task_id"])] = event
        elif event["event_type"] == "REVIEW_FINALIZED":
            finalized = dict(event)
    if finalized is None:
        raise ValueError(f"batch lacks a finalized human review: {batch_id}")
    expected = {row["task_id"] for row in request["source_tasks"]}
    if set(decisions) != expected:
        raise ValueError("finalized campaign decisions do not exactly cover the batch-request")
    payload = finalized["payload"]
    approved = sum(
        event["payload"]["artifact_review_state"] == "APPROVE"
        for event in decisions.values()
    )
    rejected = len(decisions) - approved
    if (
        int(payload["decision_count"]) != len(expected)
        or int(payload["approved_count"]) != approved
        or int(payload["rejected_count"]) != rejected
    ):
        raise ValueError("finalized review counts differ from campaign human decisions")
    if request["purpose"] == "PILOT_QA" and (
        len(expected) != PILOT_TASK_COUNT or approved != PILOT_TASK_COUNT or rejected != 0
    ):
        raise ValueError("BU SCC pilot QA requires 100/100 APPROVE and zero REJECT")
    return finalized


def create_next_batch(
    campaign_dir: str | Path,
    publication_completion: str | Path,
    *,
    actor: str,
) -> dict[str, Any]:
    root = _regular_directory(campaign_dir, label="campaign directory")
    actor_value = str(actor).strip()
    if not actor_value or len(actor_value) > 200:
        raise ValueError("campaign ledger actor must be 1..200 characters")
    contract_path = _contract_path(root)
    contract = _validate_campaign_contract(
        _json_object(contract_path, label="campaign contract")
    )
    contract_file_sha = sha256_file(contract_path)
    requests = _batch_requests(root)
    if not requests:
        raise ValueError("campaign has no pilot batch-request")
    context = campaign_ledger_context(root)
    ledger = _verify_ledger_rows(context)
    ledger_head = ledger[-1]["event_sha256"] if ledger else None
    authorized_batch_ids = {
        str(event["batch_id"])
        for event in ledger
        if event["event_type"] in {"SELECTION_FROZEN", "NEXT_BATCH_AUTHORIZED"}
    }
    unauthorized_requests = [
        (path, request)
        for path, request in requests
        if str(request["batch_id"]) not in authorized_batch_ids
    ]
    recovering_orphan = bool(unauthorized_requests)
    if recovering_orphan:
        if (
            len(unauthorized_requests) != 1
            or unauthorized_requests[0] != requests[-1]
            or len(requests) < 2
        ):
            raise ValueError(
                "campaign contains an ambiguous unauthorized batch-request"
            )
        base_requests = requests[:-1]
        orphan_path, orphan_request = unauthorized_requests[0]
    else:
        base_requests = requests
        orphan_path = None
        orphan_request = None
    _prior_path, prior = base_requests[-1]
    request_inventory = tuple(
        sorted(
            (str(path.relative_to(root)), sha256_file(path))
            for path, _request in requests
        )
    )
    finalized = _latest_finalized_event(
        ledger, batch_id=str(prior["batch_id"]), request=prior
    )
    completion, completion_sha, published_tree_sha = _verify_publication_completion(
        publication_completion,
        finalized_event=finalized,
        request=prior,
        contract=contract,
    )
    runtime_fingerprint = str(completion["runtime_fingerprint_sha256"])
    continuity_events = [
        event
        for event in ledger
        if event["event_type"] == "NEXT_BATCH_AUTHORIZED"
        and event["payload"]["next_batch_id"] == prior["batch_id"]
    ]
    if int(prior["batch_index"]) == 1:
        if continuity_events:
            raise ValueError("pilot batch has an unexpected prior runtime authorization")
    elif (
        len(continuity_events) != 1
        or continuity_events[0]["payload"]["runtime_fingerprint_sha256"]
        != runtime_fingerprint
    ):
        raise ValueError("campaign runtime fingerprint changed between published batches")
    published_manifest = (
        Path(str(completion["authorized_destination"])).resolve(strict=True) / "SHA256SUMS"
    )
    published_manifest_sha = sha256_file(published_manifest)
    already = [
        event
        for event in ledger
        if event["event_type"] == "NEXT_BATCH_AUTHORIZED"
        and event["payload"]["prior_batch_id"] == prior["batch_id"]
    ]
    if already:
        raise FileExistsError(f"next batch is already authorized after {prior['batch_id']}")

    master_path = root / str(contract["master_tasks_relative_path"])
    master_bytes = read_regular_file_bytes(
        master_path,
        expected_sha256=contract["master_tasks_sha256"],
        label="campaign master canonical tasks",
    )
    master = _jsonl_objects(master_bytes, label="campaign master canonical tasks")
    if task_set_fingerprint(master) != contract["master_task_set_sha256"]:
        raise ValueError("campaign master task set changed")
    assigned = {
        source["task_id"]
        for _path, request in base_requests
        for source in request["source_tasks"]
    }
    remaining = [task for task in master if task["task_id"] not in assigned]
    if not remaining:
        raise ValueError("campaign task set is already fully assigned")
    source_tasks = remaining[:FOLLOWUP_BATCH_LIMIT]
    batch_index = len(base_requests) + 1
    batch_id = f"batch-{batch_index:04d}"
    request, _tasks, task_bytes = _build_batch_request(
        root=root,
        contract=contract,
        batch_id=batch_id,
        batch_index=batch_index,
        purpose="PRODUCTION_CONTINUATION",
        source_tasks=source_tasks,
        selection_sha256=None,
        created_at=(
            str(orphan_request["created_at"])
            if orphan_request is not None
            else None
        ),
    )
    request_path, _task_path = _batch_paths(root, batch_id)

    with campaign_lock(root):
        if sha256_file(contract_path) != contract_file_sha:
            raise ValueError("campaign contract changed before next-batch commit")
        current_inventory = tuple(
            (str(path.relative_to(root)), sha256_file(path))
            for path in sorted((root / "batches").glob("*/batch-request.json"))
        )
        if current_inventory != request_inventory:
            raise ValueError("campaign batch-request set changed before next-batch commit")
        current_ledger = _verify_ledger_rows(context)
        current_head = current_ledger[-1]["event_sha256"] if current_ledger else None
        if current_head != ledger_head:
            raise ValueError("campaign ledger changed before next-batch commit")
        if (
            sha256_file(master_path) != contract["master_tasks_sha256"]
            or sha256_file(publication_completion) != completion_sha
            or sha256_file(published_manifest) != published_manifest_sha
        ):
            raise ValueError("next-batch source binding changed before commit")
        if recovering_orphan:
            if (
                orphan_path != request_path
                or orphan_request != request
                or sha256_file(request_path) != _json_document_sha256(request)
                or read_regular_file_bytes(
                    request_path.parent / "tasks.jsonl",
                    expected_sha256=request["tasks_sha256"],
                    label="orphan batch canonical tasks",
                )
                != task_bytes
            ):
                raise ValueError(
                    "unauthorized batch-request cannot be deterministically reconciled"
                )
        else:
            batch_stage = _create_exclusive_staging_directory(
                request_path.parent.parent / f".{batch_id}.prepare"
            )
            _write_exclusive_bytes(batch_stage / "tasks.jsonl", task_bytes)
            _write_exclusive_json(batch_stage / "batch-request.json", request)
            _publish_staged_directory(batch_stage, request_path.parent)
        event = _append_event_locked(
            context,
            event_type="NEXT_BATCH_AUTHORIZED",
            batch_id=batch_id,
            actor=actor_value,
            payload={
                "prior_batch_id": prior["batch_id"],
                "next_batch_id": batch_id,
                "publication_completion_sha256": completion_sha,
                "published_tree_sha256": published_tree_sha,
                "runtime_fingerprint_sha256": runtime_fingerprint,
                "next_batch_request_sha256": sha256_file(request_path),
            },
        )
    return {
        "schema_version": "3.0-campaign-next-result",
        "status": "AUTHORIZED",
        "campaign_id": contract["campaign_id"],
        "prior_batch_id": prior["batch_id"],
        "batch_id": batch_id,
        "batch_index": batch_index,
        "task_count": len(source_tasks),
        "batch_request": str(request_path),
        "batch_request_sha256": sha256_file(request_path),
        "publication_completion_sha256": completion_sha,
        "runtime_fingerprint_sha256": runtime_fingerprint,
        "ledger_head_sha256": event["event_sha256"],
        "recovered_after_object_publish": recovering_orphan,
    }


def _observe_file(value: str | Path | None, *, label: str, json_object: bool) -> dict[str, Any]:
    if value is None:
        return {"state": "NOT_SUPPLIED"}
    path = Path(value).expanduser()
    if path.is_symlink() or not path.resolve(strict=True).is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    path = path.resolve(strict=True)
    result: dict[str, Any] = {
        "state": "OBSERVED",
        "path": str(path),
        "size": path.stat().st_size,
        "sha256": sha256_file(path),
    }
    if json_object:
        document = _json_object(path, label=label)
        result["schema_version"] = document.get("schema_version")
        result["reported_status"] = document.get("status")
    return result


def _declared_digests(value: object, field_names: set[str]) -> set[str]:
    observed: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in field_names and isinstance(child, str) and _SHA256.fullmatch(child):
                observed.add(child)
            observed.update(_declared_digests(child, field_names))
    elif isinstance(value, list):
        for child in value:
            observed.update(_declared_digests(child, field_names))
    return observed


def _accounting_source_consistency(
    accounting_path: str | Path | None,
    observations: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if accounting_path is None:
        return {"state": "NOT_EVALUATED", "comparisons": [], "conflicts": []}
    document = _json_object(accounting_path, label="accounting attestation")
    comparisons: list[dict[str, Any]] = []
    conflicts: list[str] = []
    if document.get("schema_version") == "3.0-sge-qacct-accounting-receipt":
        path = Path(accounting_path).expanduser().resolve(strict=True)
        try:
            from .accounting import verify_scc_accounting_receipt

            verified = verify_scc_accounting_receipt(path.parent)
            if (
                verified.get("receipt_sha256") != sha256_file(path)
                or verified.get("receipt") != document
            ):
                raise ValueError("verified SCC accounting receipt identity differs")
            state_root = path.parent.parent
            request = _json_object(
                state_root / "request" / "request.json",
                label="SCC accounting request",
            )
            trace_set = {
                str(row["sha256"])
                for row in request.get("trace_inputs", [])
                if isinstance(row, Mapping) and _SHA256.fullmatch(str(row.get("sha256", "")))
            }
            raw_set = {
                str(row["raw_sha256"])
                for row in verified.get("scheduler_rows", [])
                if isinstance(row, Mapping)
                and _SHA256.fullmatch(str(row.get("raw_sha256", "")))
            }
            for source_name, declared in (
                ("nextflow_trace", trace_set),
                ("raw_qacct", raw_set),
            ):
                observation = observations[source_name]
                if observation.get("state") != "OBSERVED":
                    continue
                observed_sha = str(observation["sha256"])
                matched = observed_sha in declared
                comparisons.append(
                    {
                        "source": source_name,
                        "observed_sha256": observed_sha,
                        "declared_sha256": sorted(declared),
                        "state": "MATCH" if matched else "MISMATCH",
                    }
                )
                if not matched:
                    conflicts.append(
                        f"{source_name} is outside the verified SCC accounting exact set"
                    )
            comparisons.insert(
                0,
                {
                    "source": "accounting_attestation",
                    "observed_sha256": sha256_file(path),
                    "declared_sha256": [verified["receipt_sha256"]],
                    "state": "MATCH",
                },
            )
        except (OSError, ValueError, RuntimeError) as exc:
            conflicts.append(f"SCC accounting verification failed: {exc}")
        return {
            "state": "INCONSISTENT" if conflicts else "CONSISTENT",
            "comparisons": comparisons,
            "conflicts": conflicts,
        }
    provider = document.get("provider")
    if provider is not None and provider != "sge_qacct":
        conflicts.append(f"provider is {provider!r}, expected 'sge_qacct' for BU SCC")
    if document.get("status") == "PASS" and document.get("qacct_used") is False:
        conflicts.append("passing SCC accounting explicitly reports qacct_used=false")
    bindings = (
        (
            "nextflow_trace",
            {"trace_sha256", "nextflow_trace_sha256"},
        ),
        (
            "raw_qacct",
            {"raw_qacct_sha256", "qacct_sha256"},
        ),
    )
    for source_name, fields in bindings:
        observation = observations[source_name]
        if observation.get("state") != "OBSERVED":
            continue
        declared = _declared_digests(document, fields)
        if not declared:
            continue
        observed_sha = str(observation["sha256"])
        matched = declared == {observed_sha}
        comparisons.append(
            {
                "source": source_name,
                "observed_sha256": observed_sha,
                "declared_sha256": sorted(declared),
                "state": "MATCH" if matched else "MISMATCH",
            }
        )
        if not matched:
            conflicts.append(f"{source_name} differs from accounting attestation")
    return {
        "state": (
            "INCONSISTENT"
            if conflicts
            else "CONSISTENT"
            if comparisons
            else "UNBOUND"
        ),
        "comparisons": comparisons,
        "conflicts": conflicts,
    }


def reduce_campaign_status(
    campaign_dir: str | Path,
    *,
    batch_id: str | None = None,
    nextflow_trace: str | Path | None = None,
    raw_qacct: str | Path | None = None,
    accounting_attestation: str | Path | None = None,
    publication_completion: str | Path | None = None,
) -> dict[str, Any]:
    """Build a read-only, non-authoritative projection from live source files."""

    root = _regular_directory(campaign_dir, label="campaign directory")
    contract = _validate_campaign_contract(
        _json_object(_contract_path(root), label="campaign contract")
    )
    requests = _batch_requests(root)
    if not requests:
        raise ValueError("campaign has no batch-request")
    if batch_id is None:
        request_path, request = requests[-1]
    else:
        matches = [(path, value) for path, value in requests if value["batch_id"] == batch_id]
        if len(matches) != 1:
            raise ValueError(f"campaign batch-request is unavailable: {batch_id}")
        request_path, request = matches[0]
    ledger = verify_campaign_ledger(root)
    decisions: dict[str, Mapping[str, Any]] = {}
    finalized: Mapping[str, Any] | None = None
    next_authorization: Mapping[str, Any] | None = None
    for event in ledger:
        if event["batch_id"] == request["batch_id"]:
            if event["event_type"] == "HUMAN_DECISION":
                decisions[str(event["payload"]["task_id"])] = event
            elif event["event_type"] == "REVIEW_FINALIZED":
                finalized = event
            elif event["event_type"] == "NEXT_BATCH_AUTHORIZED":
                next_authorization = event
    observations = {
        "nextflow_trace": _observe_file(nextflow_trace, label="Nextflow trace", json_object=False),
        "raw_qacct": _observe_file(raw_qacct, label="raw qacct", json_object=False),
        "accounting_attestation": _observe_file(
            accounting_attestation, label="accounting attestation", json_object=True
        ),
        "publication_completion": _observe_file(
            publication_completion, label="publication completion receipt", json_object=True
        ),
    }
    source_consistency = _accounting_source_consistency(
        accounting_attestation, observations
    )
    return {
        "schema_version": "3.0-campaign-status-projection",
        "authoritative": False,
        "projection_mode": "READ_ONLY_LIVE_REDUCER",
        "status": (
            "INCONSISTENT"
            if source_consistency["state"] == "INCONSISTENT"
            else "OBSERVED"
        ),
        "campaign_id": contract["campaign_id"],
        "campaign_contract_sha256": contract["contract_sha256"],
        "master_task_count": contract["master_task_count"],
        "batch": {
            "batch_id": request["batch_id"],
            "batch_index": request["batch_index"],
            "purpose": request["purpose"],
            "task_count": request["task_count"],
            "request_path": str(request_path.resolve(strict=True)),
            "request_sha256": sha256_file(request_path),
            "task_set_sha256": request["task_set_sha256"],
        },
        "scientific_campaign": {
            "decision_count": len(decisions),
            "approved_count": sum(
                event["payload"]["artifact_review_state"] == "APPROVE"
                for event in decisions.values()
            ),
            "rejected_count": sum(
                event["payload"]["artifact_review_state"] == "REJECT"
                for event in decisions.values()
            ),
            "review_finalized": finalized is not None,
            "review_receipt_sha256": (
                finalized["payload"]["review_receipt_sha256"] if finalized else None
            ),
            "next_batch_authorized": next_authorization is not None,
            "ledger_event_count": len(ledger),
            "ledger_head_sha256": ledger[-1]["event_sha256"] if ledger else None,
        },
        "live_sources": observations,
        "source_consistency": source_consistency,
    }
