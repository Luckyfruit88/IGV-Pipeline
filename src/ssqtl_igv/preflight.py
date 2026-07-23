from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .storage import collect_storage_evidence
from .utils import (
    atomic_write_json,
    command_prefix,
    nearest_existing_parent,
    read_jsonl,
    resource_contains_remote_url,
    sha256_file,
    optional_text,
    sha256_json,
    utc_now,
)


def _resolve_command(value: Any) -> str | None:
    try:
        command = command_prefix(value)
    except ValueError:
        return None
    executable = command[0]
    if "/" in executable:
        path = Path(executable).expanduser()
        return str(path) if path.is_file() and os.access(path, os.X_OK) else None
    return shutil.which(executable)


def _issue(
    code: str,
    message: str,
    *,
    fatal: bool = True,
    scope: str = "environment",
) -> dict[str, Any]:
    return {"code": code, "message": message, "fatal": fatal, "scope": scope}


def run_preflight(
    config: WorkflowConfig,
    *,
    run_root: str | Path | None = None,
    manifest: str | Path | None = None,
) -> dict[str, Any]:
    root = config.validate_run_root(run_root) if run_root else None
    issues: list[dict[str, Any]] = []
    mode = optional_text(config.get("execution.mode", "local")).lower()
    command_keys = {
        "Rscript": "binaries.rscript",
        "IGV": "binaries.igv",
        "Xvfb": "binaries.xvfb",
        "xwininfo": "binaries.xwininfo",
        "xprop": "binaries.xprop",
        "ImageMagick import": "binaries.import",
        "pdftotext": "binaries.pdftotext",
        "pdftoppm": "binaries.pdftoppm",
        "tesseract": "binaries.tesseract",
    }
    if mode == "grid_engine":
        command_keys.update({"qsub": "binaries.qsub", "qacct": "binaries.qacct"})
    if optional_text(config.get("storage.provider", "filesystem")).lower() == "pquota":
        command_keys["pquota"] = "binaries.pquota"
    command_report: dict[str, Any] = {}
    for label, key in command_keys.items():
        configured = config.get(key)
        resolved = _resolve_command(configured)
        command_report[label] = {"configured": configured, "resolved": resolved}
        if not resolved:
            issues.append(_issue("TOOL_MISSING", f"{label}: {configured}"))

    try:
        import PIL

        pillow_version: str | None = PIL.__version__
    except ModuleNotFoundError:
        pillow_version = None
        issues.append(_issue("PYTHON_DEPENDENCY_MISSING", "Pillow is not installed"))
    try:
        import yaml

        pyyaml_version: str | None = yaml.__version__
    except ModuleNotFoundError:
        pyyaml_version = None
        issues.append(_issue("PYTHON_DEPENDENCY_MISSING", "PyYAML is not installed"))

    resources = {
        "associations": (
            config.path_value("paths.associations"),
            "paths.associations_sha256",
        ),
        "bam_lookup": (config.path_value("paths.bam_lookup"), None),
        "genome_definition": (
            config.path_value("genome.definition"),
            "genome.definition_sha256",
        ),
        "genome_fasta": (
            config.path_value("genome.fasta"),
            "genome.fasta_sha256",
        ),
        "genome_fai": (config.path_value("genome.fai"), "genome.fai_sha256"),
        "cytoband": (
            config.path_value("genome.cytoband"),
            "genome.cytoband_sha256",
        ),
        "annotation": (
            config.path_value("genome.annotation"),
            "genome.annotation_sha256",
        ),
    }
    resource_report: dict[str, Any] = {}
    for label, (path, hash_key) in resources.items():
        if not path.is_file():
            issues.append(_issue("RESOURCE_MISSING", f"{label}: {path}"))
            continue
        digest = sha256_file(path)
        resource_report[label] = {
            "path": str(path),
            "size": path.stat().st_size,
            "mtime_ns": path.stat().st_mtime_ns,
            "sha256": digest,
        }
        expected = str(config.get(hash_key, "") or "").lower() if hash_key else ""
        if expected and digest != expected:
            issues.append(
                _issue(
                    "RESOURCE_SHA256_MISMATCH",
                    f"{label}: expected {expected}, observed {digest}",
                )
            )
        if path.suffix.lower() in {".json", ".genome"} and resource_contains_remote_url(path):
            issues.append(_issue("REMOTE_GENOME_RESOURCE", str(path)))

    for label, path in {
        "rds_dir": config.path_value("paths.rds_dir"),
        "violin_dir": config.path_value("paths.violin_dir"),
    }.items():
        if not path.is_dir():
            issues.append(_issue("INPUT_DIRECTORY_MISSING", f"{label}: {path}"))

    for label, path in {
        "output_root": config.output_root,
        "publish_root": config.publish_root,
    }.items():
        parent = nearest_existing_parent(path)
        if not parent.exists() or not os.access(parent, os.W_OK | os.X_OK):
            issues.append(_issue("OUTPUT_NOT_WRITABLE", f"{label}: {path}"))

    manifest_path = Path(manifest) if manifest else None
    if manifest_path is None and root:
        candidate = root / ".work" / "manifests" / "case_manifest.jsonl"
        if candidate.is_file():
            manifest_path = candidate
    cases: list[dict[str, Any]] = []
    manifest_sha: str | None = None
    if manifest_path:
        if not manifest_path.is_file():
            issues.append(_issue("MANIFEST_MISSING", str(manifest_path)))
        else:
            cases = list(read_jsonl(manifest_path))
            manifest_sha = sha256_file(manifest_path)
    expected_count = config.get("inputs.expected_case_count")
    if expected_count not in (None, "") and cases and len(cases) != int(expected_count):
        issues.append(
            _issue(
                "MANIFEST_CASE_COUNT_MISMATCH",
                f"expected {expected_count}, observed {len(cases)}",
            )
        )
    case_failures = sum(bool(case.get("preflight_errors")) for case in cases)
    if case_failures:
        issues.append(
            _issue(
                "MANIFEST_CASE_ERRORS",
                str(case_failures),
                fatal=False,
                scope="case",
            )
        )

    completed = 0
    if root:
        for case in cases:
            state_path = root / ".work" / "state" / f"{case['case_id']}.json"
            if not state_path.is_file():
                continue
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if state.get("status") in {"REVIEW_PENDING", "PUBLISHED"}:
                completed += 1
    remaining_cases = max(0, len(cases) - completed) if cases else int(expected_count or 0)
    storage: dict[str, Any] | None = None
    if remaining_cases or cases:
        try:
            storage = collect_storage_evidence(
                config,
                remaining_cases=remaining_cases,
                total_cases=len(cases) if cases else int(expected_count or remaining_cases),
            )
        except Exception as exc:
            issues.append(_issue("STORAGE_GATE_FAILED", f"{type(exc).__name__}: {exc}"))

    fatal_count = sum(bool(issue["fatal"]) for issue in issues)
    status = (
        "FAIL"
        if fatal_count
        else "PASS_WITH_CASE_FAILURES"
        if case_failures
        else "PASS"
    )
    result = {
        "created_at": utc_now(),
        "status": status,
        "fatal_count": fatal_count,
        "case_failure_count": case_failures,
        "issues": issues,
        "commands": command_report,
        "resources": resource_report,
        "manifest": {
            "path": str(manifest_path) if manifest_path else None,
            "sha256": manifest_sha,
            "cases": len(cases),
            "case_failure_count": case_failures,
        },
        "remaining_cases": remaining_cases,
        "storage": storage,
        "config_fingerprint": sha256_json(config.data),
        "python": {"version": sys.version, "executable": sys.executable},
        "pillow_version": pillow_version,
        "pyyaml_version": pyyaml_version,
    }
    if root:
        report_path = root / ".work" / "preflight.json"
    else:
        report_path = config.output_root / ".work" / "preflight.json"
    atomic_write_json(report_path, result)
    result["report_path"] = str(report_path)
    return result
