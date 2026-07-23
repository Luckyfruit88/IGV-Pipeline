from __future__ import annotations

import csv
import fcntl
import json
import os
import re
import shlex
import shutil
import subprocess
import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .contracts import (
    validate_v3_case_result_document,
    validate_v3_terminal_bundle_document,
)
from .validation_lineage import observed_peak_concurrency, parse_qacct_output
from .identity import task_set_fingerprint

from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_jsonl,
    sha256_file,
    sha256_json,
    utc_now,
    write_jsonl,
    write_tsv,
)


ACCOUNTING_FIELDS = (
    "trace_file",
    "task_id",
    "process",
    "hash",
    "status",
    "native_id",
    "accounting_state",
    "failed",
    "exit_status",
    "raw_sha256",
)

LOCAL_ACCOUNTING_FIELDS = (
    "trace_file",
    "trace_sha256",
    "task_id",
    "process",
    "hash",
    "status",
    "exit",
    "accounting_state",
    "source_accounting_path",
    "source_accounting_sha256",
)

SCC_ACCOUNTING_FIELDS = (
    "role",
    "trace_task_id",
    "process",
    "hash",
    "status",
    "native_id",
    "job_id",
    "task_id",
    "owner",
    "job_name",
    "project",
    "qname",
    "hostname",
    "qsub_time",
    "start_time",
    "end_time",
    "start_epoch",
    "end_epoch",
    "ru_wallclock_seconds",
    "failed",
    "exit_status",
    "accounting_state",
    "raw_relative_path",
    "raw_sha256",
    "source_accounting_path",
    "source_accounting_sha256",
)


def _trace_rows(paths: Iterable[str | Path]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for value in paths:
        path = Path(value).expanduser()
        if path.is_symlink():
            raise ValueError(f"trace input must not be a symlink: {path}")
        path = path.resolve(strict=True)
        if not path.is_file():
            raise ValueError(f"trace input is not a regular file: {path}")
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            required = {"task_id", "hash", "status", "native_id"}
            if (
                not reader.fieldnames
                or not required.issubset(reader.fieldnames)
                or not ({"name", "process"} & set(reader.fieldnames))
            ):
                raise ValueError(f"Nextflow trace lacks required fields: {path}")
            for row in reader:
                normalized = {key: str(value or "") for key, value in row.items()}
                normalized["process"] = normalized.get("process") or normalized.get("name", "")
                rows.append({**normalized, "trace_file": str(path)})
    identities = [(row["trace_file"], row["task_id"], row["process"], row["hash"]) for row in rows]
    if len(identities) != len(set(identities)):
        raise ValueError("Nextflow trace contains duplicate task identities")
    return rows


def _qacct_fields(text: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for line in text.splitlines():
        if not line.strip() or line.startswith("===="):
            continue
        match = re.match(r"^(\S+)\s+(.*)$", line.strip())
        if match:
            fields[match.group(1)] = match.group(2).strip()
    return fields


def collect_nextflow_accounting(
    trace_paths: Iterable[str | Path],
    output_dir: str | Path,
    *,
    qacct_command: str = "qacct",
    skip_qacct: bool = False,
    test_mode: bool = False,
    _report_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Freeze qacct output for every non-cached SGE task in completed trace files."""

    if skip_qacct and not test_mode:
        raise ValueError("qacct may be skipped only in explicit test mode")
    traces = _trace_rows(trace_paths)
    destination = Path(output_dir).expanduser().resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"accounting output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    raw_root = staging / "raw"
    raw_root.mkdir(parents=True, mode=0o700)
    records: list[dict[str, Any]] = []
    try:
        for index, trace in enumerate(traces, 1):
            status = trace["status"].upper()
            native_id = trace["native_id"].strip()
            base = {
                "trace_file": trace["trace_file"],
                "task_id": trace["task_id"],
                "process": trace["process"],
                "hash": trace["hash"],
                "status": status,
                "native_id": native_id,
            }
            if status == "CACHED":
                records.append(
                    {
                        **base,
                        "accounting_state": "NOT_APPLICABLE_CACHED",
                        "failed": "",
                        "exit_status": "",
                        "raw_sha256": "",
                    }
                )
                continue
            if status != "COMPLETED":
                raise ValueError(
                    f"trace contains a nonterminal or failed task: {trace['process']}:{status}"
                )
            if not native_id or native_id == "-":
                if skip_qacct:
                    records.append(
                        {
                            **base,
                            "accounting_state": "SKIPPED_TEST_MODE",
                            "failed": "",
                            "exit_status": "",
                            "raw_sha256": "",
                        }
                    )
                    continue
                raise ValueError(f"completed SGE task lacks native_id: {trace['process']}")
            if skip_qacct:
                records.append(
                    {
                        **base,
                        "accounting_state": "SKIPPED_TEST_MODE",
                        "failed": "",
                        "exit_status": "",
                        "raw_sha256": "",
                    }
                )
                continue
            completed = subprocess.run(
                [qacct_command, "-j", native_id],
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=120,
            )
            if completed.returncode != 0:
                raise RuntimeError(
                    f"qacct failed for native_id {native_id}: {completed.stderr.strip()}"
                )
            raw_path = raw_root / f"{index:06d}_{native_id.replace('/', '_')}.txt"
            atomic_write_text(raw_path, completed.stdout)
            fields = _qacct_fields(completed.stdout)
            failed = fields.get("failed")
            exit_status = fields.get("exit_status")
            if failed is None or exit_status is None:
                raise ValueError(f"qacct output lacks failed/exit_status for {native_id}")
            if failed != "0" or exit_status != "0":
                raise ValueError(
                    f"qacct reports unsuccessful task {native_id}: failed={failed}, exit_status={exit_status}"
                )
            records.append(
                {
                    **base,
                    "accounting_state": "PASS",
                    "failed": failed,
                    "exit_status": exit_status,
                    "raw_sha256": sha256_file(raw_path),
                }
            )
        write_tsv(staging / "scheduler.tsv", ACCOUNTING_FIELDS, records)
        report = {
            "schema_version": "2.0-nextflow-qacct",
            "created_at": utc_now(),
            "status": "SKIPPED_TEST_MODE" if skip_qacct else "PASS",
            "trace_file_count": len({row["trace_file"] for row in records}),
            "trace_task_count": len(records),
            "cached_task_count": sum(row["status"] == "CACHED" for row in records),
            "accounted_task_count": sum(row["accounting_state"] == "PASS" for row in records),
            "skipped_task_count": sum(
                row["accounting_state"] == "SKIPPED_TEST_MODE" for row in records
            ),
            **dict(_report_metadata or {}),
        }
        atomic_write_json(staging / "accounting.json", report)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**report, "output_dir": str(destination)}


def collect_scc_accounting(
    trace_paths: Iterable[str | Path],
    output_dir: str | Path,
    *,
    controller: Mapping[str, Any] | str | Path | None = None,
    expected_project: str | None = None,
    expected_qname: str | None = None,
    expected_tasks: Iterable[str | Mapping[str, Any]] | str | Path | None = None,
    expected_cases: Iterable[Mapping[str, Any]] | str | Path | None = None,
    terminal_bundles: Iterable[str | Path] | None = None,
    cached_lineage: Iterable[Mapping[str, Any]] | str | Path | None = None,
    qacct_command: str = "qacct",
    raw_qacct_by_native_id: Mapping[str, str] | None = None,
    skip_qacct: bool = False,
    test_mode: bool = False,
) -> dict[str, Any]:
    """Prepare and attempt to finalize one recoverable v3 SCC accounting gate.

    Unlike the legacy v2 collector, this never treats skipped qacct as success.
    If any scheduler record (most commonly the still-running controller) is not
    yet visible according to the explicit SGE response adapter, the immutable
    request and attempt evidence are retained and an ``ACCOUNTING_PENDING``
    result is returned for a later finalizer invocation. Command execution and
    unrecognized scheduler failures are retained as infrastructure-fatal
    attempts and raised to the CLI.
    """

    if skip_qacct or test_mode:
        raise ValueError("v3 SCC accounting cannot skip or simulate qacct as PASS")
    request = prepare_scc_accounting_request(
        trace_paths,
        output_dir,
        controller=controller,
        expected_project=expected_project,
        expected_qname=expected_qname,
        expected_tasks=expected_tasks,
        expected_cases=expected_cases,
        terminal_bundles=terminal_bundles,
        cached_lineage=cached_lineage,
    )
    result = finalize_scc_accounting(
        output_dir,
        qacct_command=qacct_command,
        raw_qacct_by_native_id=raw_qacct_by_native_id,
    )
    return {**result, "request": request}


def _rows_from_value(
    value: Iterable[str | Mapping[str, Any]] | str | Path | None,
    *,
    label: str,
) -> list[str | dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, (str, Path)):
        path = Path(value).expanduser()
        if path.is_symlink() or not path.resolve(strict=True).is_file():
            raise ValueError(f"{label} must be a regular non-symlink file: {path}")
        path = path.resolve(strict=True)
        if path.suffix.lower() == ".jsonl":
            return [dict(row) for row in read_jsonl(path)]
        if path.suffix.lower() == ".json":
            try:
                document = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(f"cannot read {label}: {path}: {exc}") from exc
            if isinstance(document, list):
                rows = document
            elif isinstance(document, dict):
                rows = next(
                    (
                        document[key]
                        for key in ("tasks", "records", "lineage")
                        if isinstance(document.get(key), list)
                    ),
                    None,
                )
                if rows is None:
                    rows = [document]
            else:
                raise ValueError(f"{label} JSON must contain an object or array: {path}")
            if not all(isinstance(row, (str, dict)) for row in rows):
                raise ValueError(f"{label} contains a non-object/non-string row: {path}")
            return [dict(row) if isinstance(row, dict) else row for row in rows]
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if not reader.fieldnames:
                raise ValueError(f"{label} table lacks a header: {path}")
            return [{key: str(item or "") for key, item in row.items()} for row in reader]
    rows = list(value)
    if not all(isinstance(row, (str, Mapping)) for row in rows):
        raise ValueError(f"{label} rows must be strings or mappings")
    return [dict(row) if isinstance(row, Mapping) else row for row in rows]


def _matches_expected(trace: Mapping[str, str], expected: str | Mapping[str, Any]) -> bool:
    if isinstance(expected, str):
        return trace["task_id"] == expected
    expected_task = str(expected.get("trace_task_id", expected.get("task_id", "")))
    if not expected_task or trace["task_id"] != expected_task:
        return False
    comparisons = {
        "process": trace["process"],
        "hash": trace["hash"],
        "trace_file": trace["trace_file"],
    }
    return all(
        key not in expected or str(expected[key]) == observed
        for key, observed in comparisons.items()
    )


def _validate_expected_trace_set(
    traces: list[dict[str, str]],
    expected_rows: list[str | dict[str, Any]],
    *,
    explicit: bool,
) -> None:
    if not explicit:
        return
    unmatched_trace = set(range(len(traces)))
    for expected in expected_rows:
        matches = [index for index in unmatched_trace if _matches_expected(traces[index], expected)]
        if len(matches) != 1:
            raise ValueError(
                "expected trace task must match exactly one observed task: "
                f"{expected!r}; matches={len(matches)}"
            )
        unmatched_trace.remove(matches[0])
    if unmatched_trace:
        unexpected = [
            f"{traces[index]['task_id']}:{traces[index]['process']}:{traces[index]['hash']}"
            for index in sorted(unmatched_trace)
        ]
        raise ValueError(f"trace contains tasks outside the expected set: {unexpected[:10]}")


def _validated_control_receipts(
    expected_rows: Iterable[str | Mapping[str, Any]],
    *,
    relocation_root: Path | None = None,
    frozen_run_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Revalidate control receipts frozen beside expected trace identities."""

    bindings: list[dict[str, Any]] = []
    for raw in expected_rows:
        if isinstance(raw, str) or not raw.get("control_role"):
            continue
        if raw.get("control_role") != "runtime_manifest_validation":
            raise ValueError(f"unsupported accounting control role: {raw.get('control_role')}")
        logical_path = Path(str(raw.get("control_receipt", ""))).expanduser()
        path = _relocated_run_path(
            logical_path,
            relocation_root=relocation_root,
            frozen_run_root=frozen_run_root,
        )
        if (
            path.is_symlink()
            or not path.is_file()
            or sha256_file(path) != raw.get("control_receipt_sha256")
        ):
            raise ValueError("accounting runtime validation control receipt checksum drift")
        validation = _json_object(path, label="accounting runtime validation receipt")
        if (
            validation.get("schema_version") != "3.0-runtime-manifest-validation"
            or validation.get("status") != "PASS"
            or validation.get("runtime_manifest_sha256")
            != raw.get("runtime_manifest_sha256")
            or validation.get("runtime_fingerprint_sha256")
            != raw.get("runtime_fingerprint_sha256")
        ):
            raise ValueError("accounting runtime manifest validation receipt drift")
        bindings.append(
            {
                "trace_file": str(raw.get("trace_file", "")),
                # Preserve the frozen logical path in the identity hash.  The
                # resolved path may be a host relocation of a container /run.
                "control_receipt": str(logical_path),
                "control_receipt_sha256": sha256_file(path),
                "runtime_manifest_sha256": str(
                    raw.get("runtime_manifest_sha256", "")
                ),
                "runtime_fingerprint_sha256": str(
                    raw.get("runtime_fingerprint_sha256", "")
                ),
            }
        )
    identities = [row["trace_file"] for row in bindings]
    if len(identities) != len(set(identities)):
        raise ValueError("accounting has duplicate runtime validation controls for a trace")
    return sorted(bindings, key=lambda row: row["trace_file"])


def _validate_expected_roles(
    expected_rows: Iterable[str | Mapping[str, Any]],
) -> None:
    for raw in expected_rows:
        if isinstance(raw, str):
            continue
        control_role = str(raw.get("control_role") or "")
        prepare_role = str(raw.get("prepare_role") or "")
        case_id = str(raw.get("case_id") or "")
        if sum(bool(value) for value in (control_role, prepare_role, case_id)) > 1:
            raise ValueError("accounting expected role/case bindings must be mutually exclusive")
        if control_role and control_role != "runtime_manifest_validation":
            raise ValueError(f"unsupported accounting control role: {control_role}")
        if prepare_role and prepare_role != "ssqtl_normalization":
            raise ValueError(f"unsupported accounting prepare role: {prepare_role}")


def _validated_prepare_receipts(
    expected_rows: Iterable[str | Mapping[str, Any]],
    *,
    relocation_root: Path | None = None,
    frozen_run_root: Path | None = None,
) -> list[dict[str, Any]]:
    """Revalidate one-time portable scientific-preparation receipts."""

    bindings: list[dict[str, Any]] = []
    for raw in expected_rows:
        if isinstance(raw, str) or not raw.get("prepare_role"):
            continue
        if raw.get("prepare_role") != "ssqtl_normalization":
            raise ValueError(f"unsupported accounting prepare role: {raw.get('prepare_role')}")
        logical_receipt = Path(str(raw.get("prepare_receipt", ""))).expanduser()
        receipt_path = _relocated_run_path(
            logical_receipt,
            relocation_root=relocation_root,
            frozen_run_root=frozen_run_root,
        )
        if (
            receipt_path.is_symlink()
            or not receipt_path.is_file()
            or sha256_file(receipt_path) != raw.get("prepare_receipt_sha256")
        ):
            raise ValueError("accounting ssQTL preparation receipt checksum drift")
        receipt = _json_object(receipt_path, label="accounting ssQTL preparation receipt")
        if (
            receipt.get("schema_version") != "3.0-ssqtl-normalization-execution"
            or receipt.get("status") != "SUCCEEDED"
            or receipt.get("process_label") != "portable_runtime"
            or receipt.get("normalization_tasks_sha256")
            != raw.get("prepared_tasks_sha256")
            or receipt.get("normalization_task_set_sha256")
            != raw.get("prepared_task_set_sha256")
            or receipt.get("preparation_receipt_sha256")
            != raw.get("preparation_receipt_sha256")
            or receipt.get("input_inventory_sha256")
            != raw.get("input_inventory_sha256")
            or receipt.get("runtime_manifest_sha256")
            != raw.get("runtime_manifest_sha256")
            or receipt.get("runtime_fingerprint_sha256")
            != raw.get("runtime_fingerprint_sha256")
            or receipt.get("runtime_sif_sha256") != raw.get("runtime_sif_sha256")
            or receipt.get("trace_sha256") != raw.get("trace_sha256")
        ):
            raise ValueError("accounting ssQTL preparation identity drift")
        trace_path = _relocated_run_path(
            str(raw.get("trace_file", "")),
            relocation_root=relocation_root,
            frozen_run_root=frozen_run_root,
        )
        contract_root = receipt_path.parent
        tasks_path = contract_root / "tasks.jsonl"
        preparation_path = contract_root / "ssqtl_preparation.json"
        inventory_path = contract_root / "ssqtl_input_inventory.json"
        if (
            trace_path.is_symlink()
            or not trace_path.is_file()
            or sha256_file(trace_path) != receipt["trace_sha256"]
            or tasks_path.is_symlink()
            or sha256_file(tasks_path) != receipt["normalization_tasks_sha256"]
            or task_set_fingerprint(list(read_jsonl(tasks_path)))
            != receipt["normalization_task_set_sha256"]
            or preparation_path.is_symlink()
            or sha256_file(preparation_path) != receipt["preparation_receipt_sha256"]
            or inventory_path.is_symlink()
            or sha256_file(inventory_path) != receipt["input_inventory_sha256"]
        ):
            raise ValueError("accounting ssQTL preparation artifacts drifted")
        bindings.append(
            {
                "trace_file": str(raw.get("trace_file", "")),
                "prepare_receipt": str(logical_receipt),
                "prepare_receipt_sha256": sha256_file(receipt_path),
                "prepared_tasks_sha256": receipt["normalization_tasks_sha256"],
                "prepared_task_set_sha256": receipt[
                    "normalization_task_set_sha256"
                ],
                "preparation_receipt_sha256": receipt[
                    "preparation_receipt_sha256"
                ],
                "input_inventory_sha256": receipt["input_inventory_sha256"],
                "runtime_manifest_sha256": receipt["runtime_manifest_sha256"],
                "runtime_fingerprint_sha256": receipt[
                    "runtime_fingerprint_sha256"
                ],
                "runtime_sif_sha256": receipt.get("runtime_sif_sha256"),
            }
        )
    identities = [row["trace_file"] for row in bindings]
    if len(identities) != len(set(identities)):
        raise ValueError("accounting has duplicate ssQTL preparation controls for a trace")
    return sorted(bindings, key=lambda row: row["trace_file"])


def _relocated_run_path(
    value: str | Path,
    *,
    relocation_root: Path | None,
    frozen_run_root: Path | None,
) -> Path:
    """Resolve one frozen run-internal path after its mount namespace changes."""

    logical = Path(value).expanduser()
    if logical.exists() and not logical.is_symlink():
        return logical.resolve(strict=True)
    if relocation_root is None or frozen_run_root is None or not logical.is_absolute():
        return logical
    try:
        relative = logical.relative_to(frozen_run_root)
    except ValueError:
        return logical
    if not relative.parts or ".." in relative.parts:
        return logical
    candidate = relocation_root / relative
    if candidate.is_symlink():
        return candidate
    return candidate.resolve(strict=False)


def _sha256_digest(value: Any, *, label: str) -> str:
    digest = str(value or "").strip().lower()
    if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ValueError(f"{label} must be a lowercase SHA-256 digest")
    return digest


def _cached_lineage_map(
    value: Iterable[Mapping[str, Any]] | str | Path | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    rows = _rows_from_value(value, label="cached lineage")
    result: dict[tuple[str, str], dict[str, Any]] = {}
    verified_sources: dict[Path, dict[str, Any]] = {}
    for raw in rows:
        if isinstance(raw, str):
            raise ValueError("cached lineage rows must be objects")
        source_task_id = str(raw.get("trace_task_id", raw.get("task_id", "")))
        key = (str(raw.get("process", "")), str(raw.get("hash", "")))
        if not all(key):
            raise ValueError("cached lineage requires process and hash")
        if key in result:
            raise ValueError(f"duplicate cached lineage identity: {key}")
        source_digest = _sha256_digest(
            raw.get("source_accounting_sha256"),
            label=f"cached lineage source for {source_task_id or key[0]}",
        )
        source_path = raw.get("source_accounting_path")
        if not source_path:
            raise ValueError("cached lineage requires source_accounting_path")
        path = Path(str(source_path)).expanduser()
        if path.is_symlink() or not path.resolve(strict=True).is_file():
            raise ValueError(f"cached lineage source is unavailable: {path}")
        path = path.resolve(strict=True)
        if path.name != "accounting_receipt.json" or sha256_file(path) != source_digest:
            raise ValueError(f"cached lineage source receipt checksum drift: {path}")
        if path not in verified_sources:
            verified_sources[path] = verify_local_accounting_receipt(path.parent)
        source_rows = verified_sources[path]["scheduler_rows"]
        matches = [
            row for row in source_rows if row["process"] == key[0] and row["hash"] == key[1]
        ]
        if len(matches) != 1 or matches[0]["accounting_state"] not in {
            "PASS_TRACE",
            "PASS_CACHED_LINEAGE",
        }:
            raise ValueError(
                f"cached lineage task is absent from its source receipt: {key[0]}:{key[1]}"
            )
        result[key] = {
            **raw,
            "source_accounting_path": str(path),
            "source_accounting_sha256": source_digest,
        }
    return result


def _terminal_bundle_inventory(paths: Iterable[str | Path] | None) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for value in paths or ():
        root = Path(value).expanduser()
        if root.is_symlink():
            raise ValueError(f"terminal bundle must not be a symlink: {root}")
        root = root.resolve(strict=True)
        marker = root / "stage_result.json" if root.is_dir() else root
        if marker.is_symlink() or not marker.is_file():
            raise ValueError(f"terminal bundle lacks a regular completion marker: {root}")
        try:
            document = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"cannot read terminal bundle marker {marker}: {exc}") from exc
        if not isinstance(document, dict):
            raise ValueError(f"terminal bundle marker must be a JSON object: {marker}")
        for field in ("task_id", "status", "input_fingerprint"):
            if document.get(field) in (None, ""):
                raise ValueError(f"terminal bundle marker lacks {field}: {marker}")
        if root.is_dir():
            for path in root.rglob("*"):
                if path.is_symlink():
                    raise ValueError(f"terminal bundle contains a symlink: {path}")
            for artifact in document.get("artifacts", []):
                if not isinstance(artifact, dict):
                    raise ValueError(f"terminal bundle artifact entry is invalid: {marker}")
                relative = Path(str(artifact.get("relative_path", "")))
                if relative.is_absolute() or not relative.parts or ".." in relative.parts:
                    raise ValueError(f"unsafe terminal artifact path: {relative}")
                path = root / relative
                if path.is_symlink() or not path.is_file():
                    raise ValueError(f"terminal artifact is unavailable: {path}")
                if int(artifact.get("size", -1)) != path.stat().st_size:
                    raise ValueError(f"terminal artifact size drift: {path}")
                if _sha256_digest(artifact.get("sha256"), label=str(path)) != sha256_file(path):
                    raise ValueError(f"terminal artifact checksum drift: {path}")
        elif document.get("case_result_sha256") is not None:
            case_result = marker.parent / "case_result.json"
            if case_result.is_symlink() or not case_result.is_file():
                raise ValueError(f"terminal bundle lacks its case_result.json: {marker}")
            expected = _sha256_digest(
                document.get("case_result_sha256"), label=f"case result for {marker}"
            )
            if sha256_file(case_result) != expected:
                raise ValueError(f"terminal bundle case result checksum drift: {case_result}")
            if marker.name == "terminal_bundle.json":
                if document.get("schema_version") != "3.0":
                    raise ValueError("v3 terminal bundle schema_version must be exactly 3.0")
                try:
                    case_document = json.loads(case_result.read_text(encoding="utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise ValueError(f"cannot read v3 case result {case_result}: {exc}") from exc
                if not isinstance(case_document, dict):
                    raise ValueError(f"v3 case result must be a JSON object: {case_result}")
                validate_v3_case_result_document(case_document)
                validate_v3_terminal_bundle_document(document, case_document)
                if int(document.get("case_result_size", -1)) != case_result.stat().st_size:
                    raise ValueError(f"terminal bundle case result size drift: {case_result}")
                run_root = marker.parents[3]
                for role, record in case_document["artifacts"].items():
                    relative = Path(str(record["relative_path"]))
                    artifact = run_root / relative
                    if artifact.is_symlink() or not artifact.is_file():
                        raise ValueError(f"v3 case artifact is unavailable: {role}:{artifact}")
                    resolved_artifact = artifact.resolve(strict=True)
                    try:
                        resolved_artifact.relative_to(run_root.resolve(strict=True))
                    except ValueError as exc:
                        raise ValueError(f"v3 case artifact escapes run root: {artifact}") from exc
                    if int(record["size"]) != resolved_artifact.stat().st_size:
                        raise ValueError(f"v3 case artifact size drift: {artifact}")
                    if str(record["sha256"]) != sha256_file(resolved_artifact):
                        raise ValueError(f"v3 case artifact checksum drift: {artifact}")
        inventory.append(
            {
                "bundle": str(root),
                "task_id": str(document.get("task_id", "")),
                "stage": str(document.get("stage", "")),
                "status": str(document.get("status", "")),
                "input_fingerprint": str(document.get("input_fingerprint", "")),
                "completion_marker_sha256": sha256_file(marker),
            }
        )
    identities = [
        (row["task_id"], row["stage"], row["completion_marker_sha256"])
        for row in inventory
    ]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate terminal bundle identity")
    return inventory


def _expected_case_map(
    value: Iterable[Mapping[str, Any]] | str | Path | None,
) -> dict[str, dict[str, Any]]:
    rows = _rows_from_value(value, label="expected cases")
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        if isinstance(raw, str):
            raise ValueError("expected case rows must be objects")
        task_id = str(raw.get("task_id", ""))
        fingerprint = str(raw.get("input_fingerprint", ""))
        if not task_id or not fingerprint:
            raise ValueError("expected cases require task_id and input_fingerprint")
        if task_id in result:
            raise ValueError(f"duplicate expected case task_id: {task_id}")
        result[task_id] = {
            "task_id": task_id,
            "input_fingerprint": fingerprint,
            "manifest_order": raw.get("manifest_order"),
        }
    return result


def _trace_case_id(trace: Mapping[str, str], expected: Mapping[str, Any] | None = None) -> str:
    if expected and expected.get("case_id"):
        return str(expected["case_id"])
    match = re.search(r"\(([^()]*)\)\s*$", trace["process"])
    return match.group(1) if match else ""


def _validate_case_coverage(
    traces: list[dict[str, str]],
    expected_trace_rows: list[str | dict[str, Any]],
    bundles: list[dict[str, Any]],
    expected_cases: dict[str, dict[str, Any]],
) -> None:
    bundle_map = {str(row["task_id"]): row for row in bundles}
    if len(bundle_map) != len(bundles):
        raise ValueError("terminal bundles contain duplicate task_id values")
    if expected_cases:
        if set(bundle_map) != set(expected_cases):
            raise ValueError(
                "terminal bundle task set differs from canonical cases; "
                f"missing={sorted(set(expected_cases) - set(bundle_map))[:10]} "
                f"unexpected={sorted(set(bundle_map) - set(expected_cases))[:10]}"
            )
        drift = [
            task_id
            for task_id, expected in expected_cases.items()
            if bundle_map[task_id]["input_fingerprint"] != expected["input_fingerprint"]
        ]
        if drift:
            raise ValueError(f"terminal bundle input fingerprint drift: {drift[:10]}")
    if bundles and traces:
        expected_by_trace: dict[tuple[str, str, str], Mapping[str, Any]] = {}
        for raw in expected_trace_rows:
            if isinstance(raw, dict):
                key = (
                    str(raw.get("task_id", "")),
                    str(raw.get("process", "")),
                    str(raw.get("hash", "")),
                )
                expected_by_trace[key] = raw
        role_aware = any(
            isinstance(raw, dict)
            and (raw.get("case_id") or raw.get("control_role") or raw.get("prepare_role"))
            for raw in expected_trace_rows
        )
        trace_cases: list[str] = []
        for trace in traces:
            expected = expected_by_trace.get(
                (trace["task_id"], trace["process"], trace["hash"])
            )
            if role_aware and expected and (
                expected.get("control_role") or expected.get("prepare_role")
            ):
                continue
            case_id = _trace_case_id(trace, expected)
            if not case_id:
                raise ValueError("trace process names cannot be bound to terminal case IDs")
            trace_cases.append(case_id)
        if any(not task_id for task_id in trace_cases):
            raise ValueError("trace process names cannot be bound to terminal case IDs")
        if len(trace_cases) != len(set(trace_cases)) or set(trace_cases) != set(bundle_map):
            raise ValueError(
                "trace case tags differ from terminal bundles; "
                f"trace={sorted(trace_cases)[:10]} bundles={sorted(bundle_map)[:10]}"
            )


def _read_tsv(path: Path) -> list[dict[str, str]]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"accounting table must be a regular non-symlink file: {path}")
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not reader.fieldnames:
            raise ValueError(f"accounting table lacks a header: {path}")
        return [{key: str(value or "") for key, value in row.items()} for row in reader]


def _json_object(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def verify_local_accounting_receipt(
    output_dir: str | Path,
    *,
    expected_cases: Iterable[Mapping[str, Any]] | str | Path | None = None,
    run_root: str | Path | None = None,
    frozen_run_root: str | Path | None = None,
) -> dict[str, Any]:
    """Re-verify a complete local accounting generation and its source inputs."""

    relocation_root = (
        Path(run_root).expanduser().resolve(strict=True) if run_root is not None else None
    )
    frozen_root = (
        Path(frozen_run_root).expanduser() if frozen_run_root is not None else None
    )
    root_value = Path(output_dir).expanduser()
    if root_value.is_symlink():
        raise ValueError(f"accounting generation must not be a symlink: {root_value}")
    root = root_value.resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"accounting generation must be a directory: {root}")
    receipt_path = root / "accounting_receipt.json"
    receipt = _json_object(receipt_path, label="local accounting receipt")
    if (
        receipt.get("schema_version") != "3.0-accounting-receipt"
        or receipt.get("provider") != "nextflow_trace"
        or receipt.get("status") != "PASS"
        or receipt.get("qacct_used") is not False
    ):
        raise ValueError("local accounting receipt has an invalid provider/status contract")
    files = {
        "accounting_sha256": root / "accounting.json",
        "scheduler_sha256": root / "scheduler.tsv",
        "terminal_bundles_sha256": root / "terminal_bundles.jsonl",
        "expected_tasks_sha256": root / "expected_tasks.jsonl",
    }
    for field, path in files.items():
        if path.is_symlink() or not path.is_file() or sha256_file(path) != receipt.get(field):
            raise ValueError(f"local accounting receipt checksum drift: {field}")
    report = _json_object(files["accounting_sha256"], label="local accounting report")
    if (
        report.get("provider") != "nextflow_trace"
        or report.get("status") != "PASS"
        or report.get("site") != "local"
    ):
        raise ValueError("local accounting report is not a passing local provider")
    scheduler = _read_tsv(files["scheduler_sha256"])
    if len(scheduler) != int(report.get("trace_task_count", -1)):
        raise ValueError("local scheduler row count differs from accounting report")
    task_set = sha256_json(
        [
            {
                key: row[key]
                for key in ("task_id", "process", "hash", "status", "accounting_state")
            }
            for row in scheduler
        ]
    )
    if task_set != receipt.get("task_set_sha256"):
        raise ValueError("local accounting task-set checksum drift")
    expected_rows = list(read_jsonl(files["expected_tasks_sha256"]))
    if sha256_json(expected_rows) != receipt.get("expected_task_set_sha256"):
        raise ValueError("local accounting expected-task contract checksum drift")
    _validate_expected_roles(expected_rows)
    _validate_expected_trace_set(scheduler, expected_rows, explicit=True)
    control_bindings = _validated_control_receipts(
        expected_rows,
        relocation_root=relocation_root,
        frozen_run_root=frozen_root,
    )
    if (
        int(report.get("runtime_validation_count", -1)) != len(control_bindings)
        or receipt.get("runtime_validation_set_sha256") != sha256_json(control_bindings)
    ):
        raise ValueError("local accounting runtime validation exact-set drift")
    prepare_bindings = _validated_prepare_receipts(
        expected_rows,
        relocation_root=relocation_root,
        frozen_run_root=frozen_root,
    )
    if (
        int(report.get("preparation_count", -1)) != len(prepare_bindings)
        or receipt.get("preparation_set_sha256") != sha256_json(prepare_bindings)
    ):
        raise ValueError("local accounting preparation exact-set drift")
    bundles = list(read_jsonl(files["terminal_bundles_sha256"]))
    if sha256_json(bundles) != receipt.get("terminal_bundle_set_sha256"):
        raise ValueError("local accounting terminal-bundle set checksum drift")
    expected_map = _expected_case_map(expected_cases)
    if expected_map:
        _validate_case_coverage([], [], bundles, expected_map)
        if receipt.get("canonical_case_set_sha256") != sha256_json(
            [expected_map[key] for key in sorted(expected_map)]
        ):
            raise ValueError("local accounting canonical case-set checksum drift")
    for item in receipt.get("trace_inputs", []):
        if not isinstance(item, dict):
            raise ValueError("local accounting trace input receipt is malformed")
        path = _relocated_run_path(
            str(item.get("path", "")),
            relocation_root=relocation_root,
            frozen_run_root=frozen_root,
        )
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"local accounting trace input is unavailable: {path}")
        if sha256_file(path.resolve(strict=True)) != item.get("sha256"):
            raise ValueError(f"local accounting trace input checksum drift: {path}")
    lineage_path = root / "cache_lineage.jsonl"
    lineage_receipt_path = root / "cache_lineage_receipt.json"
    if lineage_path.is_symlink() or not lineage_path.is_file():
        raise ValueError(f"cache lineage must be a regular non-symlink file: {lineage_path}")
    lineage_receipt = _json_object(lineage_receipt_path, label="cache lineage receipt")
    if (
        lineage_receipt.get("schema_version") != "3.0-cache-lineage-receipt"
        or lineage_receipt.get("accounting_receipt_sha256") != sha256_file(receipt_path)
        or lineage_receipt.get("cache_lineage_sha256") != sha256_file(lineage_path)
    ):
        raise ValueError("cache lineage receipt checksum drift")
    source_receipt_sha = sha256_file(receipt_path)
    scheduler_keys = {(row["process"], row["hash"]) for row in scheduler}
    lineage_rows = list(read_jsonl(lineage_path))
    if len(lineage_rows) != len(scheduler):
        raise ValueError("cache lineage row count differs from scheduler")
    for row in lineage_rows:
        source_path = _relocated_run_path(
            str(row.get("source_accounting_path", "")),
            relocation_root=relocation_root,
            frozen_run_root=frozen_root,
        )
        if (
            source_path.resolve(strict=False) != receipt_path
            or row.get("source_accounting_sha256") != source_receipt_sha
            or (str(row.get("process", "")), str(row.get("hash", ""))) not in scheduler_keys
        ):
            raise ValueError("generated cache lineage is not bound to this accounting receipt")
    return {
        "output_dir": str(root),
        "report": report,
        "receipt": receipt,
        "receipt_sha256": source_receipt_sha,
        "scheduler_rows": scheduler,
        "terminal_bundles": bundles,
        "expected_tasks": expected_rows,
        "runtime_validations": control_bindings,
        "runtime_validation_set_sha256": sha256_json(control_bindings),
        "preparations": prepare_bindings,
        "preparation_set_sha256": sha256_json(prepare_bindings),
        "cache_lineage_rows": lineage_rows,
    }


def collect_local_accounting(
    trace_paths: Iterable[str | Path],
    output_dir: str | Path,
    *,
    expected_tasks: Iterable[str | Mapping[str, Any]] | str | Path | None = None,
    expected_cases: Iterable[Mapping[str, Any]] | str | Path | None = None,
    terminal_bundles: Iterable[str | Path] | None = None,
    cached_lineage: Iterable[Mapping[str, Any]] | str | Path | None = None,
) -> dict[str, Any]:
    """Freeze local Nextflow trace evidence without claiming SGE/qacct coverage.

    Completed tasks are proven by a successful terminal trace row.  Every
    ``CACHED`` row additionally requires a content-bound record from a prior
    accounting generation.  Optional terminal stage bundles are re-hashed and
    included in the immutable receipt.  The output directory is created through
    a sibling staging directory and is never merged into an existing result.
    """

    traces = _trace_rows(trace_paths)
    expected_rows = _rows_from_value(expected_tasks, label="expected tasks")
    _validate_expected_roles(expected_rows)
    _validate_expected_trace_set(traces, expected_rows, explicit=expected_tasks is not None)
    expected_case_map = _expected_case_map(expected_cases)
    lineage = _cached_lineage_map(cached_lineage)
    bundles = _terminal_bundle_inventory(terminal_bundles)
    _validate_case_coverage(traces, expected_rows, bundles, expected_case_map)
    destination_value = Path(output_dir).expanduser()
    if destination_value.is_symlink():
        raise ValueError(f"local accounting output must not be a symlink: {destination_value}")
    destination = destination_value.resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"local accounting output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        expected_contract = [
            dict(row) if isinstance(row, dict) else {"task_id": str(row)}
            for row in (
                expected_rows
                if expected_tasks is not None
                else [
                    {
                        "task_id": row["task_id"],
                        "process": row["process"],
                        "hash": row["hash"],
                        "trace_file": row["trace_file"],
                    }
                    for row in traces
                ]
            )
        ]
        control_bindings = _validated_control_receipts(expected_contract)
        prepare_bindings = _validated_prepare_receipts(expected_contract)
        records: list[dict[str, Any]] = []
        consumed_lineage: set[tuple[str, str]] = set()
        for trace in traces:
            status = trace["status"].strip().upper()
            if status not in {"COMPLETED", "CACHED"}:
                raise ValueError(
                    f"local trace contains a non-successful terminal state: "
                    f"{trace['process']}:{status}"
                )
            exit_status = trace.get("exit", "").strip()
            if exit_status not in {"", "0", "-"}:
                raise ValueError(
                    f"local trace reports a non-zero exit: {trace['process']}:{exit_status}"
                )
            # Nextflow task_id is session-local; process+hash is the stable cache
            # identity that survives a new controller session.
            key = (trace["process"], trace["hash"])
            source_digest = ""
            source_path = ""
            accounting_state = "PASS_TRACE"
            if status == "CACHED":
                if key not in lineage:
                    raise ValueError(
                        "cached trace task lacks prior accounting lineage: "
                        f"{trace['task_id']}:{trace['process']}:{trace['hash']}"
                    )
                source_digest = lineage[key]["source_accounting_sha256"]
                source_path = lineage[key]["source_accounting_path"]
                consumed_lineage.add(key)
                accounting_state = "PASS_CACHED_LINEAGE"
            records.append(
                {
                    "trace_file": trace["trace_file"],
                    "trace_sha256": sha256_file(trace["trace_file"]),
                    "task_id": trace["task_id"],
                    "process": trace["process"],
                    "hash": trace["hash"],
                    "status": status,
                    "exit": exit_status,
                    "accounting_state": accounting_state,
                    "source_accounting_path": source_path,
                    "source_accounting_sha256": source_digest,
                }
            )
        write_tsv(staging / "scheduler.tsv", list(LOCAL_ACCOUNTING_FIELDS), records)
        write_jsonl(staging / "terminal_bundles.jsonl", bundles)
        write_jsonl(staging / "expected_tasks.jsonl", expected_contract)
        report = {
            "schema_version": "3.0-local-accounting",
            "created_at": utc_now(),
            "provider": "nextflow_trace",
            "site": "local",
            "qacct_used": False,
            "status": "PASS",
            "expected_set_source": "explicit" if expected_tasks is not None else "trace",
            "expected_task_count": len(expected_rows) if expected_tasks is not None else len(traces),
            "trace_file_count": len({row["trace_file"] for row in records}),
            "trace_task_count": len(records),
            "completed_task_count": sum(row["status"] == "COMPLETED" for row in records),
            "cached_task_count": sum(row["status"] == "CACHED" for row in records),
            "cached_lineage_count": len(consumed_lineage),
            "terminal_bundle_count": len(bundles),
            "canonical_case_count": len(expected_case_map) if expected_cases is not None else len(bundles),
            "runtime_validation_count": len(control_bindings),
            "preparation_count": len(prepare_bindings),
        }
        atomic_write_json(staging / "accounting.json", report)
        accounting_sha = sha256_file(staging / "accounting.json")
        receipt = {
            "schema_version": "3.0-accounting-receipt",
            "created_at": utc_now(),
            "provider": "nextflow_trace",
            "site": "local",
            "status": "PASS",
            "qacct_used": False,
            "accounting_sha256": accounting_sha,
            "scheduler_sha256": sha256_file(staging / "scheduler.tsv"),
            "terminal_bundles_sha256": sha256_file(staging / "terminal_bundles.jsonl"),
            "expected_tasks_sha256": sha256_file(staging / "expected_tasks.jsonl"),
            "trace_inputs": [
                {"path": path, "sha256": sha256_file(path)}
                for path in sorted({row["trace_file"] for row in records})
            ],
            "task_set_sha256": sha256_json(
                [
                    {
                        key: row[key]
                        for key in ("task_id", "process", "hash", "status", "accounting_state")
                    }
                    for row in records
                ]
            ),
            "expected_task_set_sha256": sha256_json(expected_contract),
            "runtime_validation_set_sha256": sha256_json(control_bindings),
            "preparation_set_sha256": sha256_json(prepare_bindings),
            "terminal_bundle_set_sha256": sha256_json(bundles),
            "canonical_case_set_sha256": sha256_json(
                [expected_case_map[key] for key in sorted(expected_case_map)]
            ),
        }
        atomic_write_json(staging / "accounting_receipt.json", receipt)
        receipt_sha = sha256_file(staging / "accounting_receipt.json")
        future_receipt_path = destination / "accounting_receipt.json"
        cache_rows = [
            {
                "schema_version": "3.0-cache-lineage",
                "trace_task_id": row["task_id"],
                "process": row["process"],
                "hash": row["hash"],
                "source_accounting_path": str(future_receipt_path),
                "source_accounting_sha256": receipt_sha,
                "accounting_state": row["accounting_state"],
            }
            for row in records
        ]
        write_jsonl(staging / "cache_lineage.jsonl", cache_rows)
        lineage_receipt = {
            "schema_version": "3.0-cache-lineage-receipt",
            "created_at": utc_now(),
            "accounting_receipt_sha256": receipt_sha,
            "cache_lineage_sha256": sha256_file(staging / "cache_lineage.jsonl"),
        }
        atomic_write_json(staging / "cache_lineage_receipt.json", lineage_receipt)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    verified = verify_local_accounting_receipt(destination, expected_cases=expected_cases)
    return {
        **report,
        "output_dir": str(destination),
        "receipt": receipt,
        "receipt_sha256": verified["receipt_sha256"],
        "cache_lineage_receipt": lineage_receipt,
    }


_SCC_QACCT_TIME_FORMAT = "%a %b %d %H:%M:%S %Y"
_SCC_NATIVE_ID = re.compile(r"^(?P<job>[1-9][0-9]*)(?:[._](?P<task>[1-9][0-9]*))?$")


def _write_checksum_manifest(root: Path) -> None:
    paths = sorted(
        (path for path in root.rglob("*") if path.is_file() and path.name != "SHA256SUMS"),
        key=lambda path: str(path.relative_to(root)),
    )
    atomic_write_text(
        root / "SHA256SUMS",
        "".join(f"{sha256_file(path)}  {path.relative_to(root)}\n" for path in paths),
    )


def _verify_checksum_manifest(root_value: str | Path) -> dict[str, str]:
    value = Path(root_value).expanduser()
    if value.is_symlink():
        raise ValueError(f"immutable evidence root must not be a symlink: {value}")
    root = value.resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"immutable evidence root is not a directory: {root}")
    manifest = root / "SHA256SUMS"
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError(f"immutable evidence lacks SHA256SUMS: {manifest}")
    recorded: dict[str, str] = {}
    for line_number, line in enumerate(manifest.read_text(encoding="utf-8").splitlines(), 1):
        try:
            digest, relative_text = line.split("  ", 1)
        except ValueError as exc:
            raise ValueError(f"malformed SHA256SUMS line {line_number}: {manifest}") from exc
        relative = Path(relative_text)
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise ValueError(f"unsafe immutable-evidence path: {relative_text}")
        if relative_text in recorded:
            raise ValueError(f"duplicate immutable-evidence path: {relative_text}")
        artifact = root / relative
        if artifact.is_symlink() or not artifact.is_file():
            raise ValueError(f"immutable evidence artifact is missing or symlinked: {artifact}")
        if sha256_file(artifact) != digest:
            raise ValueError(f"immutable evidence checksum drift: {artifact}")
        recorded[relative_text] = digest
    paths = list(root.rglob("*"))
    if any(path.is_symlink() for path in paths):
        raise ValueError(f"immutable evidence tree contains a symlink: {root}")
    actual = {
        str(path.relative_to(root))
        for path in paths
        if path.is_file() and path.name != "SHA256SUMS"
    }
    if set(recorded) != actual:
        raise ValueError("immutable evidence SHA256SUMS does not exactly cover its file set")
    return recorded


def _mapping_object(
    value: Mapping[str, Any] | str | Path | None, *, label: str
) -> dict[str, Any]:
    if value is None:
        raise ValueError(f"{label} is required")
    if isinstance(value, Mapping):
        return dict(value)
    path = Path(value).expanduser()
    if path.is_symlink() or not path.resolve(strict=True).is_file():
        raise ValueError(f"{label} must be a regular non-symlink JSON file: {path}")
    return _json_object(path.resolve(strict=True), label=label)


def _native_id_parts(value: Any, *, label: str) -> tuple[str, str | None, str]:
    native_id = str(value or "").strip()
    match = _SCC_NATIVE_ID.fullmatch(native_id)
    if not match:
        raise ValueError(f"{label} must be a numeric SGE job ID with optional task ID")
    return match.group("job"), match.group("task"), native_id


def _scc_source_lineage(
    value: Iterable[Mapping[str, Any]] | str | Path | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    rows = _rows_from_value(value, label="SCC cached lineage")
    result: dict[tuple[str, str], dict[str, Any]] = {}
    verified: dict[Path, dict[str, Any]] = {}
    for raw in rows:
        if isinstance(raw, str):
            raise ValueError("SCC cached lineage rows must be objects")
        key = (str(raw.get("process", "")), str(raw.get("hash", "")))
        if not all(key) or key in result:
            raise ValueError(f"invalid or duplicate SCC cached lineage identity: {key}")
        source_path = Path(str(raw.get("source_accounting_path", ""))).expanduser()
        source_digest = _sha256_digest(
            raw.get("source_accounting_sha256"), label=f"SCC cached lineage {key}"
        )
        if source_path.is_symlink() or not source_path.resolve(strict=True).is_file():
            raise ValueError(f"SCC cached source receipt is unavailable: {source_path}")
        source_path = source_path.resolve(strict=True)
        if source_path.name != "accounting_receipt.json" or sha256_file(source_path) != source_digest:
            raise ValueError(f"SCC cached source receipt checksum drift: {source_path}")
        state_root = source_path.parent.parent
        if state_root not in verified:
            verified[state_root] = verify_scc_accounting_receipt(state_root)
        matches = [
            row
            for row in verified[state_root]["scheduler_rows"]
            if row["role"] == "task" and row["process"] == key[0] and row["hash"] == key[1]
        ]
        if len(matches) != 1:
            raise ValueError(f"SCC cached task is absent from source accounting: {key}")
        result[key] = {
            **raw,
            "source_accounting_path": str(source_path),
            "source_accounting_sha256": source_digest,
        }
    return result


def _request_document(root: Path) -> dict[str, Any]:
    request_root = root / "request"
    _verify_checksum_manifest(request_root)
    request = _json_object(request_root / "request.json", label="SCC accounting request")
    if (
        request.get("schema_version") != "3.0-sge-qacct-request"
        or request.get("provider") != "sge_qacct"
        or request.get("status") != "AWAITING_QACCT"
    ):
        raise ValueError("SCC accounting request has an invalid contract")
    for field, name in (
        ("trace_rows_sha256", "trace_rows.jsonl"),
        ("expected_tasks_sha256", "expected_tasks.jsonl"),
        ("canonical_cases_sha256", "canonical_cases.jsonl"),
        ("terminal_bundles_sha256", "terminal_bundles.jsonl"),
        ("cached_sources_sha256", "cached_sources.jsonl"),
    ):
        path = request_root / name
        if request.get(field) != sha256_file(path):
            raise ValueError(f"SCC accounting request checksum drift: {field}")
    return request


def prepare_scc_accounting_request(
    trace_paths: Iterable[str | Path],
    output_dir: str | Path,
    *,
    controller: Mapping[str, Any] | str | Path | None,
    expected_project: str | None,
    expected_qname: str | None = None,
    expected_tasks: Iterable[str | Mapping[str, Any]] | str | Path | None = None,
    expected_cases: Iterable[Mapping[str, Any]] | str | Path | None = None,
    terminal_bundles: Iterable[str | Path] | None = None,
    cached_lineage: Iterable[Mapping[str, Any]] | str | Path | None = None,
) -> dict[str, Any]:
    """Freeze the controller/task accounting request before qacct finalization."""

    project = str(expected_project or "").strip()
    if not project:
        raise ValueError("SCC accounting requires an explicit expected project")
    qname = str(expected_qname or "").strip() or None
    controller_row = _mapping_object(controller, label="SCC controller identity")
    controller_job, controller_task, controller_native = _native_id_parts(
        controller_row.get("native_id"), label="SCC controller native_id"
    )
    controller_contract = {
        "native_id": controller_native,
        "job_id": controller_job,
        "task_id": controller_task,
        "owner": str(controller_row.get("owner", "")).strip() or None,
        "job_name": str(controller_row.get("job_name", "")).strip() or None,
    }
    traces = _trace_rows(trace_paths)
    expected_rows = _rows_from_value(expected_tasks, label="expected SCC trace tasks")
    _validate_expected_roles(expected_rows)
    _validate_expected_trace_set(traces, expected_rows, explicit=expected_tasks is not None)
    expected_contract = [
        dict(row) if isinstance(row, dict) else {"task_id": str(row)}
        for row in (
            expected_rows
            if expected_tasks is not None
            else [
                {
                    "task_id": row["task_id"],
                    "process": row["process"],
                    "hash": row["hash"],
                    "trace_file": row["trace_file"],
                }
                for row in traces
            ]
        )
    ]
    control_bindings = _validated_control_receipts(expected_contract)
    prepare_bindings = _validated_prepare_receipts(expected_contract)
    for trace in traces:
        status = trace["status"].upper()
        if status not in {"COMPLETED", "CACHED"} or str(trace.get("exit", "")) not in {
            "",
            "-",
            "0",
        }:
            raise ValueError(
                f"SCC trace contains a non-successful terminal state: {trace['process']}:{status}"
            )
        if status == "COMPLETED":
            _native_id_parts(trace.get("native_id"), label=f"native_id for {trace['process']}")
    completed_native_ids = [
        str(row["native_id"]).strip() for row in traces if row["status"].upper() == "COMPLETED"
    ]
    if len(completed_native_ids) != len(set(completed_native_ids)):
        raise ValueError("SCC trace contains duplicate task native IDs")
    if controller_native in completed_native_ids:
        raise ValueError("SCC controller native ID collides with a task native ID")
    cases = _expected_case_map(expected_cases)
    bundles = _terminal_bundle_inventory(terminal_bundles)
    _validate_case_coverage(traces, expected_rows, bundles, cases)
    source_lineage = _scc_source_lineage(cached_lineage)
    consumed_sources: list[dict[str, Any]] = []
    for trace in traces:
        if trace["status"].upper() != "CACHED":
            continue
        key = (trace["process"], trace["hash"])
        if key not in source_lineage:
            raise ValueError(
                f"cached SCC task lacks a verified prior qacct receipt: {trace['process']}:{trace['hash']}"
            )
        consumed_sources.append(
            {
                **source_lineage[key],
                "trace_task_id": trace["task_id"],
                "process": trace["process"],
                "hash": trace["hash"],
            }
        )
    canonical_rows = [
        cases[key]
        for key in sorted(
            cases,
            key=lambda key: (
                int(cases[key]["manifest_order"])
                if str(cases[key].get("manifest_order", "")).isdigit()
                else 2**31,
                key,
            ),
        )
    ]
    trace_inputs = []
    for path_text in sorted({str(row["trace_file"]) for row in traces}):
        path = Path(path_text)
        trace_inputs.append(
            {"source_path": str(path), "size": path.stat().st_size, "sha256": sha256_file(path)}
        )
    stable_identity = {
        "controller": controller_contract,
        "expected_project": project,
        "expected_qname": qname,
        "traces": [
            {
                key: str(row.get(key, ""))
                for key in ("task_id", "process", "hash", "status", "native_id", "trace_file")
            }
            for row in traces
        ],
        "expected_tasks": expected_contract,
        "runtime_validations": control_bindings,
        "preparations": prepare_bindings,
        "trace_inputs": trace_inputs,
        "canonical_cases": canonical_rows,
        "terminal_bundles": bundles,
        "cached_sources": consumed_sources,
    }
    request_id = f"sccacct_{sha256_json(stable_identity)}"
    destination_value = Path(output_dir).expanduser()
    if destination_value.is_symlink():
        raise ValueError(f"SCC accounting state must not be a symlink: {destination_value}")
    destination = destination_value.resolve(strict=False)
    if destination.exists():
        request = _request_document(destination)
        if request.get("request_id") != request_id:
            raise ValueError("existing SCC accounting request belongs to different immutable inputs")
        return {**request, "output_dir": str(destination)}
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.parent.is_symlink() or not destination.parent.is_dir():
        raise ValueError(f"SCC accounting parent must be a regular directory: {destination.parent}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    request_root = staging / "request"
    (request_root / "traces").mkdir(parents=True, mode=0o700)
    try:
        for index, item in enumerate(trace_inputs, 1):
            source = Path(item["source_path"])
            target = request_root / "traces" / f"{index:04d}.trace.tsv"
            shutil.copyfile(source, target)
            if sha256_file(target) != item["sha256"]:
                raise RuntimeError(f"trace changed while freezing SCC request: {source}")
            item["frozen_relative_path"] = str(target.relative_to(request_root))
        write_jsonl(request_root / "trace_rows.jsonl", traces)
        write_jsonl(request_root / "expected_tasks.jsonl", expected_contract)
        write_jsonl(request_root / "canonical_cases.jsonl", canonical_rows)
        write_jsonl(request_root / "terminal_bundles.jsonl", bundles)
        write_jsonl(request_root / "cached_sources.jsonl", consumed_sources)
        request = {
            "schema_version": "3.0-sge-qacct-request",
            "request_id": request_id,
            "created_at": utc_now(),
            "provider": "sge_qacct",
            "site": "scc",
            "status": "AWAITING_QACCT",
            "review_gate": False,
            "controller": controller_contract,
            "expected_project": project,
            "expected_qname": qname,
            "trace_inputs": trace_inputs,
            "trace_task_count": len(traces),
            "expected_task_count": len(expected_contract),
            "completed_task_count": sum(row["status"].upper() == "COMPLETED" for row in traces),
            "cached_task_count": sum(row["status"].upper() == "CACHED" for row in traces),
            "canonical_case_count": len(canonical_rows),
            "terminal_bundle_count": len(bundles),
            "runtime_validation_count": len(control_bindings),
            "runtime_validation_set_sha256": sha256_json(control_bindings),
            "preparation_count": len(prepare_bindings),
            "preparation_set_sha256": sha256_json(prepare_bindings),
            "trace_rows_sha256": sha256_file(request_root / "trace_rows.jsonl"),
            "expected_tasks_sha256": sha256_file(request_root / "expected_tasks.jsonl"),
            "canonical_cases_sha256": sha256_file(request_root / "canonical_cases.jsonl"),
            "terminal_bundles_sha256": sha256_file(request_root / "terminal_bundles.jsonl"),
            "cached_sources_sha256": sha256_file(request_root / "cached_sources.jsonl"),
            "meaning": "immutable request; review remains closed until controller and task qacct are finalized",
        }
        atomic_write_json(request_root / "request.json", request)
        _write_checksum_manifest(request_root)
        _verify_checksum_manifest(request_root)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {**request, "output_dir": str(destination)}


def _qacct_int(value: Any, *, field: str) -> int:
    token = str(value or "").strip().split(None, 1)[0]
    if not token.isdigit():
        raise ValueError(f"qacct {field} is missing or nonnumeric")
    return int(token)


def _qacct_epoch(value: Any, *, field: str) -> float:
    try:
        return datetime.strptime(str(value).strip(), _SCC_QACCT_TIME_FORMAT).astimezone().timestamp()
    except ValueError as exc:
        raise ValueError(f"qacct {field} has an invalid timestamp") from exc


def _validated_scc_qacct_record(
    text: str,
    expected: Mapping[str, Any],
    *,
    expected_project: str,
    expected_qname: str | None,
) -> dict[str, Any]:
    job_id, task_id, native_id = _native_id_parts(
        expected.get("native_id"), label=f"qacct native_id for {expected.get('role')}"
    )
    records = [row for row in parse_qacct_output(text) if str(row.get("jobnumber", "")) == job_id]
    if task_id is not None:
        records = [row for row in records if str(row.get("taskid", "")) == task_id]
    if len(records) != 1:
        raise ValueError(
            f"qacct does not contain exactly one record for native ID {native_id}: {len(records)}"
        )
    raw = records[0]
    for field in ("owner", "jobname", "project", "qname", "hostname"):
        if not str(raw.get(field, "")).strip():
            raise ValueError(f"qacct record for {native_id} lacks {field}")
    if raw["project"] != expected_project:
        raise ValueError(f"qacct project differs for {native_id}")
    if expected_qname and raw["qname"] != expected_qname:
        raise ValueError(f"qacct qname differs for {native_id}")
    if expected.get("owner") and raw["owner"] != expected["owner"]:
        raise ValueError(f"qacct owner differs for {native_id}")
    if expected.get("job_name") and raw["jobname"] != expected["job_name"]:
        raise ValueError(f"qacct job name differs for {native_id}")
    failed = _qacct_int(raw.get("failed"), field="failed")
    exit_status = _qacct_int(raw.get("exit_status"), field="exit_status")
    if failed != 0 or exit_status != 0:
        raise ValueError(
            f"qacct reports unsuccessful native ID {native_id}: failed={failed} exit_status={exit_status}"
        )
    start_epoch = _qacct_epoch(raw.get("start_time"), field="start_time")
    end_epoch = _qacct_epoch(raw.get("end_time"), field="end_time")
    try:
        wallclock = float(raw.get("ru_wallclock", ""))
    except ValueError as exc:
        raise ValueError(f"qacct ru_wallclock is nonnumeric for {native_id}") from exc
    if end_epoch <= start_epoch or wallclock <= 0:
        raise ValueError(f"qacct interval is not positive for successful native ID {native_id}")
    return {
        "role": str(expected["role"]),
        "trace_task_id": str(expected.get("trace_task_id", "")),
        "process": str(expected.get("process", "")),
        "hash": str(expected.get("hash", "")),
        "status": str(expected.get("status", "COMPLETED")),
        "native_id": native_id,
        "job_id": job_id,
        "task_id": task_id or str(raw.get("taskid", "")),
        "owner": raw["owner"],
        "job_name": raw["jobname"],
        "project": raw["project"],
        "qname": raw["qname"],
        "hostname": raw["hostname"],
        "qsub_time": str(raw.get("qsub_time", "")),
        "start_time": raw["start_time"],
        "end_time": raw["end_time"],
        "start_epoch": start_epoch,
        "end_epoch": end_epoch,
        "reported_end_epoch": end_epoch,
        "ru_wallclock_seconds": wallclock,
        "failed": failed,
        "exit_status": exit_status,
        "accounting_state": "PASS_QACCT",
        "raw_relative_path": "",
        "raw_sha256": "",
        "source_accounting_path": "",
        "source_accounting_sha256": "",
    }


def _qacct_text(
    native_id: str,
    *,
    qacct_command: str,
    raw_qacct_by_native_id: Mapping[str, str] | None,
) -> tuple[str | None, dict[str, Any]]:
    if raw_qacct_by_native_id is not None:
        if native_id not in raw_qacct_by_native_id:
            return None, {
                "native_id": native_id,
                "returncode": None,
                "classification": "RECORD_NOT_YET_VISIBLE",
                "reason_code": "FIXTURE_RECORD_NOT_VISIBLE",
                "reason": "fixture_missing",
            }
        return str(raw_qacct_by_native_id[native_id]), {
            "native_id": native_id,
            "returncode": 0,
            "source": "provided_raw_qacct",
        }
    job_id, task_id, _native = _native_id_parts(native_id, label="qacct native_id")
    prefix = shlex.split(qacct_command)
    if not prefix:
        raise ValueError("qacct command is empty")
    command = [*prefix, "-j", job_id]
    if task_id:
        command.extend(["-t", task_id])
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    except subprocess.TimeoutExpired as exc:
        return None, {
            "native_id": native_id,
            "returncode": None,
            "classification": "INFRASTRUCTURE_FATAL",
            "reason_code": "QACCT_TIMEOUT",
            "error_type": type(exc).__name__,
            "reason": str(exc),
            "command": command,
        }
    except OSError as exc:
        return None, {
            "native_id": native_id,
            "returncode": None,
            "classification": "INFRASTRUCTURE_FATAL",
            "reason_code": "QACCT_EXECUTION_OS_ERROR",
            "error_type": type(exc).__name__,
            "errno": exc.errno,
            "reason": str(exc),
            "command": command,
        }
    metadata = {
        "native_id": native_id,
        "returncode": completed.returncode,
        "stderr": completed.stderr.strip(),
        "command": command,
    }
    if completed.returncode != 0:
        normalized_error = " ".join(completed.stderr.strip().split())
        expected_error_id = re.escape(job_id)
        if task_id:
            expected_error_id += rf"(?:\.{re.escape(task_id)})?"
        record_not_visible_patterns = (
            rf"(?:error:\s*)?job(?:\s+id)?\s+{expected_error_id}\s+not\s+found"
            rf"(?:\s+in\s+(?:the\s+)?accounting(?:\s+(?:file|database))?)?\.?",
            rf"(?:error:\s*)?no\s+accounting\s+(?:information|record)\s+"
            rf"(?:is\s+)?(?:available|found)\s+for\s+job(?:\s+id)?\s+"
            rf"{expected_error_id}\.?",
        )
        if not completed.stdout.strip() and any(
            re.fullmatch(pattern, normalized_error, flags=re.IGNORECASE)
            for pattern in record_not_visible_patterns
        ):
            metadata.update(
                classification="RECORD_NOT_YET_VISIBLE",
                reason_code="SGE_ACCOUNTING_RECORD_NOT_VISIBLE",
            )
        else:
            metadata.update(
                classification="INFRASTRUCTURE_FATAL",
                reason_code="QACCT_UNRECOGNIZED_NONZERO_EXIT",
            )
        return None, metadata
    if not completed.stdout.strip():
        metadata.update(
            classification="INFRASTRUCTURE_FATAL",
            reason_code="QACCT_EMPTY_SUCCESS_OUTPUT",
        )
        return None, metadata
    return completed.stdout, metadata


def _recover_scc_final_pointer(root: Path, request: Mapping[str, Any]) -> None:
    """Complete the tiny crash window after final-tree rename and before pointer write."""

    final_root = root / "final"
    if final_root.is_symlink():
        raise ValueError("SCC accounting final generation must not be a symlink")
    _verify_checksum_manifest(final_root)
    receipt_path = final_root / "accounting_receipt.json"
    receipt = _json_object(receipt_path, label="recoverable SCC accounting receipt")
    if (
        receipt.get("schema_version") != "3.0-sge-qacct-accounting-receipt"
        or receipt.get("provider") != "sge_qacct"
        or receipt.get("status") != "PASS"
        or receipt.get("review_gate") is not True
        or receipt.get("request_id") != request.get("request_id")
        or receipt.get("request_manifest_sha256")
        != sha256_file(root / "request" / "SHA256SUMS")
    ):
        raise ValueError("orphaned SCC final tree cannot be bound to its immutable request")
    pointer_path = root / "finalized_accounting.json"
    if pointer_path.is_symlink():
        raise ValueError("SCC finalized accounting pointer must not be a symlink")
    if pointer_path.exists():
        return
    pointer = {
        "schema_version": "3.0-finalized-sge-qacct-pointer",
        "status": "PASS",
        "request_id": request["request_id"],
        "receipt_relative_path": "final/accounting_receipt.json",
        "receipt_sha256": sha256_file(receipt_path),
    }
    descriptor = os.open(
        pointer_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.write(
            descriptor,
            (json.dumps(pointer, indent=2, sort_keys=True) + "\n").encode("utf-8"),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _finalize_scc_accounting_unlocked(
    output_dir: str | Path,
    *,
    qacct_command: str = "qacct",
    raw_qacct_by_native_id: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Finalize a frozen SCC request, or retain an immutable pending attempt."""

    root_value = Path(output_dir).expanduser()
    if root_value.is_symlink():
        raise ValueError(f"SCC accounting state must not be a symlink: {root_value}")
    root = root_value.resolve(strict=True)
    request = _request_document(root)
    if (root / "final").exists() or (root / "finalized_accounting.json").exists():
        _recover_scc_final_pointer(root, request)
        verified = verify_scc_accounting_receipt(root)
        return {
            **verified["report"],
            "output_dir": str(root),
            "receipt": verified["receipt"],
            "receipt_sha256": verified["receipt_sha256"],
        }
    request_root = root / "request"
    traces = [dict(row) for row in read_jsonl(request_root / "trace_rows.jsonl")]
    expected_contract = [
        dict(row) for row in read_jsonl(request_root / "expected_tasks.jsonl")
    ]
    _validate_expected_roles(expected_contract)
    _validate_expected_trace_set(traces, expected_contract, explicit=True)
    control_bindings = _validated_control_receipts(expected_contract)
    if request.get("runtime_validation_set_sha256") != sha256_json(control_bindings):
        raise ValueError("SCC accounting request runtime validation set drift")
    prepare_bindings = _validated_prepare_receipts(expected_contract)
    if request.get("preparation_set_sha256") != sha256_json(prepare_bindings):
        raise ValueError("SCC accounting request preparation set drift")
    cached_sources = {
        (str(row.get("process", "")), str(row.get("hash", ""))): dict(row)
        for row in read_jsonl(request_root / "cached_sources.jsonl")
    }
    expected_records: list[dict[str, Any]] = [
        {"role": "controller", **dict(request["controller"]), "status": "COMPLETED"}
    ]
    for trace in traces:
        if str(trace["status"]).upper() == "COMPLETED":
            expected_records.append(
                {
                    "role": "task",
                    "trace_task_id": trace["task_id"],
                    "process": trace["process"],
                    "hash": trace["hash"],
                    "status": "COMPLETED",
                    "native_id": trace["native_id"],
                }
            )
    attempts_root = root / "attempts"
    attempts_root.mkdir(mode=0o700, exist_ok=True)
    attempt_id = f"attempt_{uuid.uuid4().hex}"
    attempt_staging = attempts_root / f".{attempt_id}.tmp"
    attempt_raw = attempt_staging / "raw"
    attempt_raw.mkdir(parents=True, mode=0o700)
    pending: list[dict[str, Any]] = []
    infrastructure_failures: list[dict[str, Any]] = []
    parsed_rows: list[dict[str, Any]] = []
    fatal: Exception | None = None
    try:
        for index, expected in enumerate(expected_records, 1):
            native_id = str(expected["native_id"])
            raw, metadata = _qacct_text(
                native_id,
                qacct_command=qacct_command,
                raw_qacct_by_native_id=raw_qacct_by_native_id,
            )
            if raw is None:
                if metadata.get("classification") == "RECORD_NOT_YET_VISIBLE":
                    pending.append(metadata)
                else:
                    infrastructure_failures.append(metadata)
                continue
            raw_path = attempt_raw / f"{index:04d}_{expected['role']}_{native_id.replace('.', '_')}.txt"
            atomic_write_text(raw_path, raw if raw.endswith("\n") else raw + "\n")
            try:
                row = _validated_scc_qacct_record(
                    raw_path.read_text(encoding="utf-8"),
                    expected,
                    expected_project=str(request["expected_project"]),
                    expected_qname=request.get("expected_qname"),
                )
            except (OSError, ValueError) as exc:
                fatal = exc
                break
            row["raw_relative_path"] = str(raw_path.relative_to(attempt_staging))
            row["raw_sha256"] = sha256_file(raw_path)
            parsed_rows.append(row)
        if fatal is not None:
            status = "INCONSISTENT"
        elif infrastructure_failures:
            status = "INFRASTRUCTURE_FATAL"
        elif pending:
            status = "ACCOUNTING_PENDING"
        else:
            status = "COMPLETE"
        attempt = {
            "schema_version": "3.0-sge-qacct-attempt",
            "attempt_id": attempt_id,
            "created_at": utc_now(),
            "request_id": request["request_id"],
            "status": status,
            "review_gate": False,
            "queried_native_id_count": len(expected_records),
            "observed_qacct_count": len(parsed_rows),
            "pending": pending,
            "infrastructure_failures": infrastructure_failures,
            "error": f"{type(fatal).__name__}: {fatal}" if fatal else None,
        }
        atomic_write_json(attempt_staging / "attempt.json", attempt)
        write_jsonl(attempt_staging / "parsed_qacct.jsonl", parsed_rows)
        _write_checksum_manifest(attempt_staging)
        _verify_checksum_manifest(attempt_staging)
        attempt_root = attempts_root / attempt_id
        os.replace(attempt_staging, attempt_root)
    except BaseException:
        shutil.rmtree(attempt_staging, ignore_errors=True)
        raise
    if fatal is not None:
        raise ValueError(
            f"INCONSISTENT SCC trace/qacct evidence retained in {attempt_root}: {fatal}"
        ) from fatal
    if infrastructure_failures:
        reasons = "; ".join(
            f"{row.get('native_id')}:{row.get('reason_code')}"
            for row in infrastructure_failures
        )
        raise RuntimeError(
            f"SCC qacct infrastructure failure retained in {attempt_root}: {reasons}"
        )
    if pending:
        return {
            "schema_version": "3.0-sge-qacct-accounting",
            "provider": "sge_qacct",
            "site": "scc",
            "status": "ACCOUNTING_PENDING",
            "review_gate": False,
            "qacct_used": True,
            "request_id": request["request_id"],
            "pending_native_ids": [str(row["native_id"]) for row in pending],
            "attempt_dir": str(attempt_root),
            "output_dir": str(root),
            "reason": "qacct records are not all visible; rerun the accounting finalizer after the controller exits",
        }

    cached_rows: list[dict[str, Any]] = []
    for trace in traces:
        if str(trace["status"]).upper() != "CACHED":
            continue
        source = cached_sources[(str(trace["process"]), str(trace["hash"]))]
        cached_rows.append(
            {
                "role": "task",
                "trace_task_id": str(trace["task_id"]),
                "process": str(trace["process"]),
                "hash": str(trace["hash"]),
                "status": "CACHED",
                "native_id": "",
                "job_id": "",
                "task_id": "",
                "owner": "",
                "job_name": "",
                "project": str(request["expected_project"]),
                "qname": "",
                "hostname": "",
                "qsub_time": "",
                "start_time": "",
                "end_time": "",
                "start_epoch": "",
                "end_epoch": "",
                "ru_wallclock_seconds": "",
                "failed": "",
                "exit_status": "",
                "accounting_state": "PASS_CACHED_LINEAGE",
                "raw_relative_path": "",
                "raw_sha256": "",
                "source_accounting_path": source["source_accounting_path"],
                "source_accounting_sha256": source["source_accounting_sha256"],
            }
        )
    scheduler_rows = [
        parsed_rows[0],
        *sorted(
            parsed_rows[1:] + cached_rows,
            key=lambda row: (
                row["trace_task_id"],
                row["process"],
                row["hash"],
            ),
        ),
    ]
    final_root = root / "final"
    if final_root.exists() or final_root.is_symlink():
        raise FileExistsError(f"SCC accounting final generation already exists without pointer: {final_root}")
    final_staging = root / f".final.tmp-{uuid.uuid4().hex}"
    raw_root = final_staging / "raw"
    raw_root.mkdir(parents=True, mode=0o700)
    try:
        for row in scheduler_rows:
            if row["accounting_state"] != "PASS_QACCT":
                continue
            source = attempt_root / row["raw_relative_path"]
            target = raw_root / source.name
            shutil.copyfile(source, target)
            row["raw_relative_path"] = str(target.relative_to(final_staging))
            row["raw_sha256"] = sha256_file(target)
        write_tsv(final_staging / "scheduler.tsv", list(SCC_ACCOUNTING_FIELDS), scheduler_rows)
        shutil.copyfile(request_root / "trace_rows.jsonl", final_staging / "trace_rows.jsonl")
        shutil.copyfile(request_root / "expected_tasks.jsonl", final_staging / "expected_tasks.jsonl")
        shutil.copyfile(request_root / "canonical_cases.jsonl", final_staging / "canonical_cases.jsonl")
        shutil.copyfile(request_root / "terminal_bundles.jsonl", final_staging / "terminal_bundles.jsonl")
        task_qacct = [
            row for row in scheduler_rows if row["role"] == "task" and row["accounting_state"] == "PASS_QACCT"
        ]
        report = {
            "schema_version": "3.0-sge-qacct-accounting",
            "created_at": utc_now(),
            "provider": "sge_qacct",
            "site": "scc",
            "status": "PASS",
            "review_gate": True,
            "qacct_used": True,
            "request_id": request["request_id"],
            "project": request["expected_project"],
            "qnames": sorted({row["qname"] for row in scheduler_rows if row["qname"]}),
            "controller_native_id": request["controller"]["native_id"],
            "controller_qacct_count": 1,
            "trace_task_count": len(traces),
            "task_qacct_count": len(task_qacct),
            "cached_task_count": len(cached_rows),
            "terminal_bundle_count": int(request["terminal_bundle_count"]),
            "canonical_case_count": int(request["canonical_case_count"]),
            "runtime_validation_count": len(control_bindings),
            "runtime_validation_set_sha256": sha256_json(control_bindings),
            "preparation_count": len(prepare_bindings),
            "preparation_set_sha256": sha256_json(prepare_bindings),
            "observed_task_peak_concurrency": observed_peak_concurrency(task_qacct) if task_qacct else 0,
        }
        atomic_write_json(final_staging / "accounting.json", report)
        receipt = {
            "schema_version": "3.0-sge-qacct-accounting-receipt",
            "created_at": utc_now(),
            "provider": "sge_qacct",
            "site": "scc",
            "status": "PASS",
            "review_gate": True,
            "qacct_used": True,
            "request_id": request["request_id"],
            "request_manifest_sha256": sha256_file(request_root / "SHA256SUMS"),
            "accounting_sha256": sha256_file(final_staging / "accounting.json"),
            "scheduler_sha256": sha256_file(final_staging / "scheduler.tsv"),
            "trace_rows_sha256": sha256_file(final_staging / "trace_rows.jsonl"),
            "expected_tasks_sha256": sha256_file(final_staging / "expected_tasks.jsonl"),
            "canonical_cases_sha256": sha256_file(final_staging / "canonical_cases.jsonl"),
            "terminal_bundles_sha256": sha256_file(final_staging / "terminal_bundles.jsonl"),
            "raw_qacct_set_sha256": sha256_json(
                [
                    {
                        "native_id": row["native_id"],
                        "relative_path": row["raw_relative_path"],
                        "sha256": row["raw_sha256"],
                    }
                    for row in scheduler_rows
                    if row["accounting_state"] == "PASS_QACCT"
                ]
            ),
            "runtime_validation_set_sha256": sha256_json(control_bindings),
            "preparation_set_sha256": sha256_json(prepare_bindings),
            "scheduler_set_sha256": sha256_json(
                [
                    {
                        key: str(row[key])
                        for key in (
                            "role",
                            "trace_task_id",
                            "process",
                            "hash",
                            "status",
                            "native_id",
                            "project",
                            "qname",
                            "failed",
                            "exit_status",
                            "accounting_state",
                            "raw_sha256",
                            "source_accounting_sha256",
                        )
                    }
                    for row in scheduler_rows
                ]
            ),
        }
        atomic_write_json(final_staging / "accounting_receipt.json", receipt)
        receipt_sha = sha256_file(final_staging / "accounting_receipt.json")
        future_receipt = final_root / "accounting_receipt.json"
        cache_rows = [
            {
                "schema_version": "3.0-scc-cache-lineage",
                "trace_task_id": row["trace_task_id"],
                "process": row["process"],
                "hash": row["hash"],
                "source_accounting_path": str(future_receipt),
                "source_accounting_sha256": receipt_sha,
                "accounting_state": row["accounting_state"],
            }
            for row in scheduler_rows
            if row["role"] == "task"
        ]
        write_jsonl(final_staging / "cache_lineage.jsonl", cache_rows)
        atomic_write_json(
            final_staging / "cache_lineage_receipt.json",
            {
                "schema_version": "3.0-scc-cache-lineage-receipt",
                "accounting_receipt_sha256": receipt_sha,
                "cache_lineage_sha256": sha256_file(final_staging / "cache_lineage.jsonl"),
            },
        )
        _write_checksum_manifest(final_staging)
        _verify_checksum_manifest(final_staging)
        os.replace(final_staging, final_root)
        pointer = {
            "schema_version": "3.0-finalized-sge-qacct-pointer",
            "status": "PASS",
            "request_id": request["request_id"],
            "receipt_relative_path": "final/accounting_receipt.json",
            "receipt_sha256": sha256_file(final_root / "accounting_receipt.json"),
        }
        descriptor = os.open(
            root / "finalized_accounting.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(
                descriptor,
                (json.dumps(pointer, indent=2, sort_keys=True) + "\n").encode("utf-8"),
            )
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException:
        shutil.rmtree(final_staging, ignore_errors=True)
        raise
    verified = verify_scc_accounting_receipt(root)
    return {
        **verified["report"],
        "output_dir": str(root),
        "receipt": verified["receipt"],
        "receipt_sha256": verified["receipt_sha256"],
    }


def finalize_scc_accounting(
    output_dir: str | Path,
    *,
    qacct_command: str = "qacct",
    raw_qacct_by_native_id: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Serialize SCC qacct attempts and final-tree/pointer promotion across controllers."""

    root_value = Path(output_dir).expanduser()
    if root_value.is_symlink() or not root_value.resolve(strict=True).is_dir():
        raise ValueError(f"SCC accounting state must be a regular non-symlink directory: {root_value}")
    root = root_value.resolve(strict=True)
    lock_path = root / ".finalize.lock"
    if lock_path.is_symlink():
        raise ValueError(f"SCC accounting finalizer lock must not be a symlink: {lock_path}")
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return _finalize_scc_accounting_unlocked(
            root,
            qacct_command=qacct_command,
            raw_qacct_by_native_id=raw_qacct_by_native_id,
        )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def verify_scc_accounting_receipt(
    output_dir: str | Path,
    *,
    expected_cases: Iterable[Mapping[str, Any]] | str | Path | None = None,
) -> dict[str, Any]:
    """Re-verify the frozen request, final qacct set, and current terminal bundles."""

    value = Path(output_dir).expanduser()
    if value.is_symlink():
        raise ValueError(f"SCC accounting state must not be a symlink: {value}")
    resolved = value.resolve(strict=True)
    root = resolved.parent if resolved.name == "final" else resolved
    request = _request_document(root)
    pointer_path = root / "finalized_accounting.json"
    pointer = _json_object(pointer_path, label="finalized SCC accounting pointer")
    receipt_relative = Path(str(pointer.get("receipt_relative_path", "")))
    if receipt_relative != Path("final/accounting_receipt.json"):
        raise ValueError("finalized SCC accounting pointer has an unsafe receipt path")
    final_root = root / "final"
    if final_root.is_symlink():
        raise ValueError("SCC accounting final generation must not be a symlink")
    _verify_checksum_manifest(final_root)
    receipt_path = final_root / "accounting_receipt.json"
    receipt = _json_object(receipt_path, label="SCC accounting receipt")
    receipt_sha = sha256_file(receipt_path)
    if (
        pointer.get("schema_version") != "3.0-finalized-sge-qacct-pointer"
        or pointer.get("status") != "PASS"
        or pointer.get("request_id") != request.get("request_id")
        or pointer.get("receipt_sha256") != receipt_sha
        or receipt.get("schema_version") != "3.0-sge-qacct-accounting-receipt"
        or receipt.get("provider") != "sge_qacct"
        or receipt.get("status") != "PASS"
        or receipt.get("review_gate") is not True
        or receipt.get("qacct_used") is not True
        or receipt.get("request_id") != request.get("request_id")
        or receipt.get("request_manifest_sha256") != sha256_file(root / "request" / "SHA256SUMS")
    ):
        raise ValueError("SCC accounting pointer/receipt contract is invalid")
    files = {
        "accounting_sha256": final_root / "accounting.json",
        "scheduler_sha256": final_root / "scheduler.tsv",
        "trace_rows_sha256": final_root / "trace_rows.jsonl",
        "expected_tasks_sha256": final_root / "expected_tasks.jsonl",
        "canonical_cases_sha256": final_root / "canonical_cases.jsonl",
        "terminal_bundles_sha256": final_root / "terminal_bundles.jsonl",
    }
    for field, path in files.items():
        if path.is_symlink() or not path.is_file() or receipt.get(field) != sha256_file(path):
            raise ValueError(f"SCC accounting receipt checksum drift: {field}")
    report = _json_object(files["accounting_sha256"], label="SCC accounting report")
    if (
        report.get("provider") != "sge_qacct"
        or report.get("status") != "PASS"
        or report.get("review_gate") is not True
        or report.get("qacct_used") is not True
        or report.get("request_id") != request.get("request_id")
    ):
        raise ValueError("SCC accounting report is not a passing qacct provider")
    scheduler = _read_tsv(files["scheduler_sha256"])
    if len([row for row in scheduler if row["role"] == "controller"]) != 1:
        raise ValueError("SCC accounting must contain exactly one controller qacct row")
    controller_row = next(row for row in scheduler if row["role"] == "controller")
    if controller_row["native_id"] != request["controller"]["native_id"]:
        raise ValueError("SCC controller native ID differs from its request")
    traces = [dict(row) for row in read_jsonl(files["trace_rows_sha256"])]
    expected_contract = [dict(row) for row in read_jsonl(files["expected_tasks_sha256"])]
    _validate_expected_roles(expected_contract)
    _validate_expected_trace_set(traces, expected_contract, explicit=True)
    control_bindings = _validated_control_receipts(expected_contract)
    if (
        int(report.get("runtime_validation_count", -1)) != len(control_bindings)
        or report.get("runtime_validation_set_sha256") != sha256_json(control_bindings)
        or receipt.get("runtime_validation_set_sha256") != sha256_json(control_bindings)
    ):
        raise ValueError("SCC accounting runtime validation exact-set drift")
    prepare_bindings = _validated_prepare_receipts(expected_contract)
    if (
        int(report.get("preparation_count", -1)) != len(prepare_bindings)
        or report.get("preparation_set_sha256") != sha256_json(prepare_bindings)
        or receipt.get("preparation_set_sha256") != sha256_json(prepare_bindings)
    ):
        raise ValueError("SCC accounting preparation exact-set drift")
    task_rows = [row for row in scheduler if row["role"] == "task"]
    if len(task_rows) != len(traces):
        raise ValueError("SCC accounting task rows do not exactly cover the trace")
    trace_keys = {
        (
            str(row["task_id"]),
            str(row["process"]),
            str(row["hash"]),
            str(row["status"]).upper(),
            "" if str(row["status"]).upper() == "CACHED" else str(row["native_id"]),
        )
        for row in traces
    }
    scheduler_keys = {
        (
            row["trace_task_id"],
            row["process"],
            row["hash"],
            row["status"],
            row["native_id"],
        )
        for row in task_rows
    }
    if trace_keys != scheduler_keys or len(scheduler_keys) != len(task_rows):
        raise ValueError("SCC accounting scheduler rows differ from exact trace identities")
    trace_by_identity = {
        (str(row["task_id"]), str(row["process"]), str(row["hash"])): row
        for row in traces
    }
    if len(trace_by_identity) != len(traces):
        raise ValueError("SCC trace contains duplicate composite task identities")
    verified_cached_sources: dict[Path, dict[str, Any]] = {}
    for row in scheduler:
        if row["project"] != request["expected_project"]:
            raise ValueError("SCC qacct project differs from its request")
        if row["accounting_state"] == "PASS_QACCT":
            if row["failed"] != "0" or row["exit_status"] != "0" or not row["qname"]:
                raise ValueError("SCC qacct row is not a successful project/qname-bound record")
            if request.get("expected_qname") and row["qname"] != request["expected_qname"]:
                raise ValueError("SCC qacct qname differs from its request")
            raw_path = final_root / row["raw_relative_path"]
            if raw_path.is_symlink() or not raw_path.is_file() or sha256_file(raw_path) != row["raw_sha256"]:
                raise ValueError(f"SCC raw qacct checksum drift: {raw_path}")
            expected = (
                {"role": "controller", **dict(request["controller"]), "status": "COMPLETED"}
                if row["role"] == "controller"
                else {
                    "role": "task",
                    "trace_task_id": row["trace_task_id"],
                    "process": row["process"],
                    "hash": row["hash"],
                    "status": "COMPLETED",
                    "native_id": trace_by_identity[
                        (row["trace_task_id"], row["process"], row["hash"])
                    ]["native_id"],
                }
            )
            reparsed = _validated_scc_qacct_record(
                raw_path.read_text(encoding="utf-8"),
                expected,
                expected_project=str(request["expected_project"]),
                expected_qname=request.get("expected_qname"),
            )
            comparable = (
                "role",
                "trace_task_id",
                "process",
                "hash",
                "status",
                "native_id",
                "job_id",
                "task_id",
                "owner",
                "job_name",
                "project",
                "qname",
                "hostname",
                "qsub_time",
                "start_time",
                "end_time",
                "failed",
                "exit_status",
            )
            if any(str(reparsed[key]) != row[key] for key in comparable):
                raise ValueError(f"SCC parsed scheduler row differs from raw qacct: {row['native_id']}")
        elif row["accounting_state"] == "PASS_CACHED_LINEAGE":
            source = Path(row["source_accounting_path"])
            if source.is_symlink() or not source.is_file() or sha256_file(source) != row["source_accounting_sha256"]:
                raise ValueError("SCC cached lineage source receipt drift")
            state_root = source.resolve(strict=True).parent.parent
            if state_root not in verified_cached_sources:
                verified_cached_sources[state_root] = verify_scc_accounting_receipt(state_root)
            matches = [
                source_row
                for source_row in verified_cached_sources[state_root]["scheduler_rows"]
                if source_row["role"] == "task"
                and source_row["process"] == row["process"]
                and source_row["hash"] == row["hash"]
            ]
            if len(matches) != 1:
                raise ValueError("SCC cached lineage task is absent from its source receipt")
        else:
            raise ValueError(f"SCC accounting row has an invalid state: {row['accounting_state']}")
    raw_set = [
        {
            "native_id": row["native_id"],
            "relative_path": row["raw_relative_path"],
            "sha256": row["raw_sha256"],
        }
        for row in scheduler
        if row["accounting_state"] == "PASS_QACCT"
    ]
    if receipt.get("raw_qacct_set_sha256") != sha256_json(raw_set):
        raise ValueError("SCC raw qacct set checksum drift")
    scheduler_set = [
        {
            key: str(row[key])
            for key in (
                "role",
                "trace_task_id",
                "process",
                "hash",
                "status",
                "native_id",
                "project",
                "qname",
                "failed",
                "exit_status",
                "accounting_state",
                "raw_sha256",
                "source_accounting_sha256",
            )
        }
        for row in scheduler
    ]
    if receipt.get("scheduler_set_sha256") != sha256_json(scheduler_set):
        raise ValueError("SCC scheduler exact-set checksum drift")
    bundles = [dict(row) for row in read_jsonl(files["terminal_bundles_sha256"])]
    observed_bundles = _terminal_bundle_inventory([row["bundle"] for row in bundles])
    if observed_bundles != bundles:
        raise ValueError("SCC terminal bundle/checksum evidence drift")
    canonical = [dict(row) for row in read_jsonl(files["canonical_cases_sha256"])]
    expected_map = _expected_case_map(expected_cases)
    if expected_map and canonical != [
        expected_map[key]
        for key in sorted(
            expected_map,
            key=lambda key: (
                int(expected_map[key]["manifest_order"])
                if str(expected_map[key].get("manifest_order", "")).isdigit()
                else 2**31,
                key,
            ),
        )
    ]:
        raise ValueError("SCC accounting canonical case set drift")
    _validate_case_coverage(
        traces,
        expected_contract,
        bundles,
        {row["task_id"]: row for row in canonical},
    )
    lineage_path = final_root / "cache_lineage.jsonl"
    lineage_receipt = _json_object(
        final_root / "cache_lineage_receipt.json", label="SCC cache lineage receipt"
    )
    if (
        lineage_receipt.get("schema_version") != "3.0-scc-cache-lineage-receipt"
        or lineage_receipt.get("accounting_receipt_sha256") != receipt_sha
        or lineage_receipt.get("cache_lineage_sha256") != sha256_file(lineage_path)
    ):
        raise ValueError("SCC cache lineage receipt checksum drift")
    return {
        "output_dir": str(root),
        "report": report,
        "receipt": receipt,
        "receipt_sha256": receipt_sha,
        "scheduler_rows": scheduler,
        "terminal_bundles": bundles,
        "canonical_cases": canonical,
        "expected_tasks": expected_contract,
        "runtime_validations": control_bindings,
        "runtime_validation_set_sha256": sha256_json(control_bindings),
        "preparations": prepare_bindings,
        "preparation_set_sha256": sha256_json(prepare_bindings),
        "cache_lineage_rows": list(read_jsonl(lineage_path)),
    }
