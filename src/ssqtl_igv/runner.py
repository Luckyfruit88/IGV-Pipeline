from __future__ import annotations

import csv
import json
import os
import re
import resource
import shutil
import subprocess
import fcntl
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

from .compose import compose_desktop_case
from .config import WorkflowConfig
from .contracts import (
    ATTEMPT_RSS_SAMPLE_INTERVAL_SECONDS,
    DEFAULT_CLIENT_HEIGHT,
    DEFAULT_CLIENT_WIDTH,
    DESKTOP_LAYOUT_SCHEMA,
    FIGURE_CONTRACT_ID,
    GUI_SETTLE_CONTRACT_ID,
    WHOLE_CASE_RSS_CONTRACT_ID,
    WHOLE_CASE_RSS_SOURCE,
)
from .desktop import DesktopFailure, run_desktop_session
from .igv import (
    GENOTYPE_TOKENS,
    build_desktop_batch,
)
from .qc import inspect_png
from .parsing import locus
from .scientific_qc import scientific_qc
from .selection import representative_order_key
from .storage import cached_storage_low_watermark
from .state import (
    COMPOSED,
    FAILED,
    GUI_SETTLED,
    IGV_STARTING,
    PREPARED,
    PUBLISHED,
    QC_PASS,
    RENDER_STABLE,
    RERUN,
    REVIEW_PENDING,
    WINDOW_CAPTURED,
    CaseState,
)
from .utils import (
    atomic_write_json,
    atomic_write_text,
    optional_text,
    read_jsonl,
    resource_contains_remote_url,
    safe_name,
    sha256_file,
    sha256_json,
    utc_now,
    write_tsv,
)
from .violin import render_pdf_page


class CaseFailure(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _process_tree_rss_kb(root_pid: int) -> int:
    """Return simultaneous RSS for a process and every live descendant.

    The sampler's own ``ps`` child is deliberately included.  That small
    overhead makes the observation conservative instead of hiding the cost of
    measuring the attempt.
    """

    completed = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,rss="],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=5,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"ps process-tree sample failed with exit {completed.returncode}: "
            f"{completed.stderr.strip()}"
        )
    rows: dict[int, tuple[int, int]] = {}
    for line in completed.stdout.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            pid, parent, rss = map(int, parts)
        except ValueError:
            continue
        rows[pid] = (parent, rss)
    if root_pid not in rows:
        raise RuntimeError(f"runner PID {root_pid} is absent from ps process-tree sample")
    selected = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, (parent, _rss) in rows.items():
            if parent in selected and pid not in selected:
                selected.add(pid)
                changed = True
    total = sum(rows[pid][1] for pid in selected)
    if total <= 0:
        raise RuntimeError(f"process-tree RSS sample is not positive for runner PID {root_pid}")
    return total


class _AttemptRSSMonitor:
    """Continuously observe simultaneous RSS throughout one case attempt."""

    def __init__(
        self,
        *,
        root_pid: int | None = None,
        interval_seconds: float = ATTEMPT_RSS_SAMPLE_INTERVAL_SECONDS,
        sampler: Any | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("attempt RSS sample interval must be positive")
        self.root_pid = os.getpid() if root_pid is None else int(root_pid)
        self.interval_seconds = float(interval_seconds)
        self._sampler = sampler or _process_tree_rss_kb
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._sample_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._started = False
        self._stopped = False
        self._peak_kb = 0
        self._sample_count = 0
        self._error_count = 0
        self._errors: list[str] = []
        self._snapshot: dict[str, Any] | None = None

    def _record_error(self, exc: BaseException) -> None:
        with self._lock:
            self._error_count += 1
            if len(self._errors) < 10:
                self._errors.append(f"{type(exc).__name__}: {exc}")

    def sample(self) -> None:
        with self._sample_lock:
            try:
                value = int(self._sampler(self.root_pid))
                if value <= 0:
                    raise RuntimeError(f"non-positive process-tree RSS sample: {value}")
            except Exception as exc:
                self._record_error(exc)
                return
            with self._lock:
                self._sample_count += 1
                self._peak_kb = max(self._peak_kb, value)

    def _run(self) -> None:
        try:
            while not self._stop.wait(self.interval_seconds):
                self.sample()
        except BaseException as exc:
            # A dead sampler thread must never look like valid capacity
            # evidence, even for failures outside the ordinary ps call.
            self._record_error(exc)

    def start(self) -> None:
        if self._started:
            raise RuntimeError("attempt RSS monitor was already started")
        self._started = True
        # Synchronous boundary sample closes the gap before the background
        # thread gets its first scheduling opportunity.
        self.sample()
        self._thread = threading.Thread(
            target=self._run,
            name=f"attempt-rss-{self.root_pid}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        if self._snapshot is not None:
            return dict(self._snapshot)
        if not self._started:
            self._record_error(RuntimeError("attempt RSS monitor was never started"))
        else:
            # The pre-stop sample closes the gap between the last periodic
            # sample and the terminal state transition.
            self.sample()
            self._stop.set()
            if self._thread is not None:
                self._thread.join(timeout=max(2.0, self.interval_seconds * 4.0))
                if self._thread.is_alive():
                    self._record_error(RuntimeError("attempt RSS monitor thread did not stop"))
        self._stopped = True
        with self._lock:
            status = (
                "PASS"
                if self._started
                and self._stopped
                and self._thread is not None
                and not self._thread.is_alive()
                and self._sample_count >= 2
                and self._peak_kb > 0
                and self._error_count == 0
                else "FAIL"
            )
            self._snapshot = {
                "status": status,
                "root_pid": self.root_pid,
                "interval_seconds": self.interval_seconds,
                "sample_count": self._sample_count,
                "error_count": self._error_count,
                "peak_kb": self._peak_kb,
                "errors": self._errors.copy(),
            }
        return dict(self._snapshot)


def _attempt_telemetry(
    *,
    started_monotonic: float,
    started_at_epoch: float,
    attempt: int,
    rss_monitor: _AttemptRSSMonitor,
    desktop_result: Any | None,
    result: str,
    validation_lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    sampler = rss_monitor.stop()
    ended_at_epoch = time.time()
    self_maxrss = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    children_maxrss = float(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
    # Linux reports KiB and macOS bytes; normalize both for portable telemetry.
    unit = 1.0 if sys.platform != "darwin" else 1.0 / 1024.0
    self_gb = self_maxrss * unit / (1024.0 * 1024.0)
    children_gb = children_maxrss * unit / (1024.0 * 1024.0)
    desktop_gb = float(desktop_result.peak_rss_gb) if desktop_result is not None else 0.0
    process_tree_gb = float(sampler["peak_kb"]) / (1024.0 * 1024.0)
    rusage_bound_gb = self_gb + children_gb
    conservative_peak_gb = max(process_tree_gb, rusage_bound_gb, desktop_gb)
    telemetry = {
        "source": WHOLE_CASE_RSS_SOURCE,
        "rss_contract_id": WHOLE_CASE_RSS_CONTRACT_ID,
        "started_at_epoch": round(started_at_epoch, 6),
        "ended_at_epoch": round(ended_at_epoch, 6),
        "wall_time_seconds": round(time.monotonic() - started_monotonic, 3),
        "peak_rss_gb": round(conservative_peak_gb, 4),
        "process_tree_peak_rss_gb": round(process_tree_gb, 4),
        "process_tree_sampler_status": sampler["status"],
        "process_tree_sample_interval_seconds": sampler["interval_seconds"],
        "process_tree_sample_count": sampler["sample_count"],
        "process_tree_sample_errors": sampler["error_count"],
        "process_tree_sample_error_messages": sampler["errors"],
        "runner_self_peak_rss_gb": round(self_gb, 4),
        "children_peak_rss_gb": round(children_gb, 4),
        "rusage_conservative_bound_gb": round(rusage_bound_gb, 4),
        "desktop_tree_peak_rss_gb": round(desktop_gb, 4),
        "attempt": attempt,
        "result": result,
        "scheduler_job_id": os.environ.get("JOB_ID", ""),
        "scheduler_task_id": os.environ.get("SGE_TASK_ID", ""),
    }
    if validation_lineage is not None:
        telemetry["validation_scheduler_lineage"] = validation_lineage
    return telemetry


def manifest_path(run_root: str | Path) -> Path:
    return Path(run_root) / ".work" / "manifests" / "case_manifest.jsonl"


def load_cases(run_root: str | Path) -> list[dict[str, Any]]:
    path = manifest_path(run_root)
    if not path.is_file():
        raise FileNotFoundError(f"manifest missing: {path}")
    cases = list(read_jsonl(path))
    ids = [case["case_id"] for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("manifest contains duplicate case_id values")
    return cases


def assert_manifest_config(
    cases: list[dict[str, Any]], config: WorkflowConfig, run_root: str | Path
) -> None:
    root = config.validate_run_root(run_root, must_exist=True)
    if not cases:
        raise ValueError("manifest is empty")
    current = sha256_json(config.data)
    if {case.get("schema_version") for case in cases} != {"1.0"}:
        raise ValueError("manifest schema identity is missing or mixed")
    if {case.get("run_id") for case in cases} != {root.name}:
        raise ValueError("manifest run identity is missing, mixed, or incorrect")
    if {case.get("workflow_config_fingerprint") for case in cases} != {current}:
        raise ValueError(
            "manifest configuration identity is missing, mixed, or differs from the "
            "configuration used to prepare it; create a new run"
        )
    for case in cases:
        declared_fingerprint = optional_text(case.get("input_fingerprint")).lower()
        calculated_fingerprint = sha256_json(
            {key: value for key, value in case.items() if key != "input_fingerprint"}
        )
        if declared_fingerprint != calculated_fingerprint:
            raise ValueError(
                f"manifest input fingerprint mismatch: {case.get('case_id', '<unknown>')}"
            )

    expected_association_sha = optional_text(
        config.get("paths.associations_sha256")
    ).lower()
    declared_association_shas = {
        optional_text(case.get("associations_sha256")).lower() for case in cases
    }
    if (
        len(declared_association_shas) != 1
        or not all(re.fullmatch(r"[0-9a-f]{64}", value) for value in declared_association_shas)
    ):
        raise ValueError("manifest association identity is missing, malformed, or mixed")
    association_sha = next(iter(declared_association_shas))
    if expected_association_sha and association_sha != expected_association_sha:
        raise ValueError(
            "manifest association identity differs from paths.associations_sha256"
        )

    manifest_source = manifest_path(root)
    if manifest_source.is_symlink() or not manifest_source.is_file():
        raise ValueError(f"manifest missing or symlinked: {manifest_source}")
    manifest = manifest_source.resolve(strict=False)
    report_path = root / ".work" / "prepare_report.json"
    if report_path.is_symlink() or not report_path.is_file():
        raise ValueError(f"prepare report missing or symlinked: {report_path}")
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid prepare report: {report_path}") from exc
    if not isinstance(report, dict):
        raise ValueError("prepare report must be a JSON object")
    snapshot_value = optional_text(report.get("associations_snapshot"))
    if not snapshot_value:
        raise ValueError("prepare report lacks an association snapshot path")
    inputs_source = root / ".work" / "inputs"
    if inputs_source.is_symlink() or not inputs_source.is_dir():
        raise ValueError("association snapshot directory is missing or symlinked")
    snapshot = Path(snapshot_value).expanduser().resolve(strict=False)
    inputs_root = inputs_source.resolve(strict=False)
    shards_source = root / ".work" / "manifests" / "shards.tsv"
    if shards_source.is_symlink() or not shards_source.is_file():
        raise ValueError(f"shard manifest missing or symlinked: {shards_source}")
    expected_report = {
        "run_root": str(root),
        "manifest": str(manifest),
        "manifest_sha256": sha256_file(manifest),
        "shards": str(shards_source.resolve(strict=False)),
        "case_count": len(cases),
        "config_fingerprint": current,
        "associations_sha256": association_sha,
        "associations_snapshot_sha256": association_sha,
    }
    mismatched = [
        key for key, value in expected_report.items() if report.get(key) != value
    ]
    if mismatched:
        raise ValueError(
            "prepare report identity differs from the manifest/run: "
            + ", ".join(mismatched)
        )
    if (
        snapshot.parent != inputs_root
        or snapshot.is_symlink()
        or not snapshot.is_file()
        or snapshot.stat().st_mode & 0o777 != 0o444
        or sha256_file(snapshot) != association_sha
    ):
        raise ValueError("association snapshot is missing, mutable, or differs from prepare")


def _load_state(path: Path, case: dict[str, Any]) -> CaseState:
    if path.is_file():
        return CaseState.load(path)
    return CaseState.fresh(case["case_id"], case["input_fingerprint"])


def _validate_case_inputs(case: dict[str, Any], config: WorkflowConfig | None = None) -> list[str]:
    warnings: list[str] = [
        f"{item.get('code', 'WARNING')}: {item.get('message', '')}"
        for item in case.get("preflight_warnings", [])
    ]
    errors = case.get("preflight_errors", [])
    if errors:
        raise CaseFailure("MANIFEST_PREFLIGHT_ERROR", json.dumps(errors, ensure_ascii=False))
    for genotype in GENOTYPE_TOKENS:
        samples = case.get("genotypes", {}).get(genotype, [])
        if len(samples) > 6:
            raise CaseFailure("TOO_MANY_REPRESENTATIVES", f"{genotype}: {len(samples)}")
        for sample in samples:
            bam = Path(sample["bam"])
            bai = Path(sample["bai"])
            if not bam.is_file():
                raise CaseFailure("BAM_MISSING", str(bam))
            if not bai.is_file():
                raise CaseFailure("BAI_MISSING", str(bai))
            if bai.stat().st_mtime_ns < bam.stat().st_mtime_ns:
                if config and config.get("inputs.stale_bai_policy", "warn") == "fail":
                    raise CaseFailure("BAI_OLDER_THAN_BAM", str(bai))
                warnings.append(f"BAI older than BAM: {bai}")
            for label, path, expected in (
                ("BAM", bam, sample.get("bam_identity")),
                ("BAI", bai, sample.get("bai_identity")),
            ):
                if expected and (
                    path.stat().st_size != expected.get("size")
                    or path.stat().st_mtime_ns != expected.get("mtime_ns")
                ):
                    raise CaseFailure("INPUT_CHANGED_SINCE_PREPARE", f"{label}: {path}")
            for path in (bam, bai):
                if any(char.isspace() for char in str(path.resolve(strict=False))):
                    raise CaseFailure("PATH_WHITESPACE", str(path))
    if not any(case.get("genotypes", {}).get(genotype, []) for genotype in GENOTYPE_TOKENS):
        raise CaseFailure("NO_ELIGIBLE_SAMPLES", str(case.get("case_id", "")))
    for key in ("definition", "fasta", "fai", "cytoband", "annotation"):
        resource = Path(case["genome"][key])
        if not resource.is_file():
            raise CaseFailure("GENOME_RESOURCE_MISSING", f"{key}: {resource}")
        if any(char.isspace() for char in str(resource.resolve(strict=False))):
            raise CaseFailure("PATH_WHITESPACE", str(resource))
        expected = case["genome"].get("resource_identity", {}).get(key)
        if expected and (
            resource.stat().st_size != expected.get("size")
            or resource.stat().st_mtime_ns != expected.get("mtime_ns")
        ):
            raise CaseFailure("RESOURCE_CHANGED_SINCE_PREPARE", f"{key}: {resource}")
        configured_sha = (
            optional_text(config.get(f"genome.{key}_sha256")).lower()
            if config
            else ""
        )
        if configured_sha and sha256_file(resource) != configured_sha:
            raise CaseFailure("RESOURCE_SHA256_MISMATCH", f"{key}: {resource}")
        if (
            key == "annotation"
            and config
            and bool(config.get("inputs.require_read_only_annotation", False))
            and (resource.stat().st_mode & 0o222)
        ):
            raise CaseFailure("ANNOTATION_NOT_READ_ONLY", str(resource))
    definition = Path(case["genome"]["definition"])
    if definition.suffix.lower() in {".json", ".genome"} and resource_contains_remote_url(definition):
        raise CaseFailure("REMOTE_GENOME_RESOURCE", str(definition))
    pdf = Path(case["violin"]["pdf"])
    if not pdf.is_file() or case["violin"].get("page") is None:
        raise CaseFailure("VIOLIN_MAPPING_INVALID", str(case["violin"]))
    pdf_identity = case["violin"].get("pdf_identity") or {}
    if (
        pdf.stat().st_size != pdf_identity.get("size")
        or pdf.stat().st_mtime_ns != pdf_identity.get("mtime_ns")
        or sha256_file(pdf) != pdf_identity.get("sha256")
    ):
        raise CaseFailure("VIOLIN_PDF_CHANGED_SINCE_PREPARE", str(pdf))
    return warnings


def _state_path(root: Path, case_id: str) -> Path:
    return root / ".work" / "state" / f"{safe_name(case_id)}.json"


def _case_attempt(state: CaseState) -> int:
    return 1 + sum(event.get("status") == RERUN for event in state.history)


def _runtime_input_fingerprint(case: dict[str, Any], config: WorkflowConfig) -> str:
    paths: list[Path] = []
    for samples in case.get("genotypes", {}).values():
        for sample in samples:
            paths.extend((Path(sample["bam"]), Path(sample["bai"])))
    for key in ("definition", "fasta", "fai", "cytoband", "annotation"):
        value = case.get("genome", {}).get(key)
        if value:
            paths.append(Path(value))
    violin = case.get("violin", {}).get("pdf")
    if violin:
        paths.append(Path(violin))
    identities = []
    for path in paths:
        resolved = path.resolve(strict=False)
        identities.append(
            {
                "path": str(resolved),
                "size": resolved.stat().st_size if resolved.is_file() else None,
                "mtime_ns": resolved.stat().st_mtime_ns if resolved.is_file() else None,
            }
        )
    return sha256_json(
        {
            "case_input_fingerprint": case["input_fingerprint"],
            "workflow_config_fingerprint": sha256_json(config.data),
            "files": identities,
        }
    )


@contextmanager
def _case_lock(root: Path, case_id: str):
    lock_path = root / ".work" / "locks" / f"{safe_name(case_id)}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise CaseFailure("CASE_LOCKED", str(lock_path)) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps({"pid": os.getpid(), "at": utc_now()}) + "\n")
        handle.flush()
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _artifact_is_current(state: CaseState, case: dict[str, Any], config: WorkflowConfig) -> bool:
    if state.status not in {REVIEW_PENDING, PUBLISHED} or state.input_fingerprint != case["input_fingerprint"]:
        return False
    for path_key, digest_key in (
        ("combined_png", "combined_sha256"),
        ("sample_table", "sample_table_sha256"),
        ("layout_json", "layout_sha256"),
    ):
        output = state.artifacts.get(path_key)
        digest = state.artifacts.get(digest_key)
        if not (output and digest and Path(output).is_file() and sha256_file(output) == digest):
            return False
    if (
        state.artifacts.get("gui_settle_contract_id") != GUI_SETTLE_CONTRACT_ID
        or state.artifacts.get("gui_settle", {}).get("status") != "PASS"
        or state.artifacts.get("pixel_stability", {})
        .get("toolbar_locus_guard", {})
        .get("status")
        != "PASS"
    ):
        return False
    try:
        _validate_case_inputs(case, config)
    except CaseFailure:
        return False
    return state.artifacts.get("runtime_input_fingerprint") == _runtime_input_fingerprint(case, config)


def _prepare_rerun(state: CaseState, case: dict[str, Any]) -> None:
    if state.status in {REVIEW_PENDING, PUBLISHED, FAILED}:
        state.transition(RERUN, detail={"reason": "explicit rerun or stale artifact"})
    elif state.status not in {PREPARED, RERUN}:
        state.fail("recovery", "INTERRUPTED_ATTEMPT", f"recovered from {state.status}")
    if state.status == RERUN:
        state.transition(PREPARED, detail={"reason": "begin isolated rerun attempt"})
    state.input_fingerprint = case["input_fingerprint"]


SAMPLE_TABLE_FIELDS = [
    "case_id",
    "genotype",
    "sample_id",
    "dosage",
    "ratio",
    "selection_label",
    "bam",
    "bai",
]

def _write_sample_table(case: dict[str, Any], path: str | Path) -> Path:
    rows: list[dict[str, Any]] = []
    for genotype in GENOTYPE_TOKENS:
        for sample in sorted(
            case.get("genotypes", {}).get(genotype, []),
            key=representative_order_key,
        ):
            rows.append(
                {
                    "case_id": case["case_id"],
                    "genotype": genotype,
                    "sample_id": sample.get("sample_id", ""),
                    "dosage": sample.get("dosage", ""),
                    "ratio": sample.get("ratio", ""),
                    "selection_label": sample.get("selection_label", ""),
                    "bam": sample.get("bam", ""),
                    "bai": sample.get("bai", ""),
                }
            )
    target = Path(path)
    write_tsv(target, SAMPLE_TABLE_FIELDS, rows)
    return target


def _validate_desktop_layout(layout: dict[str, Any], config: WorkflowConfig) -> dict[str, Any]:
    evidence = layout.get("evidence", {})
    panel = layout.get("panels", {}).get("igv_client", {})
    if layout.get("schema_version") != DESKTOP_LAYOUT_SCHEMA:
        raise CaseFailure("DESKTOP_LAYOUT_SCHEMA_INVALID", str(layout.get("schema_version")))
    if layout.get("figure_contract_id") != FIGURE_CONTRACT_ID:
        raise CaseFailure("FIGURE_CONTRACT_INVALID", str(layout.get("figure_contract_id")))
    if panel.get("full_client") is not True or panel.get("crop_applied") is not False:
        raise CaseFailure("IGV_CLIENT_NOT_PRESERVED", json.dumps(panel, ensure_ascii=False))
    if panel.get("resize_count") != 0 or evidence.get("client_resize_count") != 0:
        raise CaseFailure("IGV_CLIENT_RESIZE_COUNT_INVALID", json.dumps(evidence, ensure_ascii=False))
    if evidence.get("root_screenshot_publishable") is not False:
        raise CaseFailure("ROOT_SCREENSHOT_PUBLISHABLE", json.dumps(evidence, ensure_ascii=False))
    forbidden = {
        "client_post_capture_overlays": 0,
        "external_header_present": False,
        "divider_present": False,
        "dosage_badges_present": False,
    }
    if any(evidence.get(key) != expected for key, expected in forbidden.items()):
        raise CaseFailure("POST_CAPTURE_DECORATION_PRESENT", json.dumps(evidence, ensure_ascii=False))
    client_width = int(config.get("desktop.screen_width", DEFAULT_CLIENT_WIDTH))
    client_height = int(config.get("desktop.screen_height", DEFAULT_CLIENT_HEIGHT))
    expected_left = [client_width, client_height]
    if evidence.get("left_origin") != [0, 0] or evidence.get("left_size") != expected_left:
        raise CaseFailure("IGV_CLIENT_GEOMETRY_INVALID", json.dumps(evidence, ensure_ascii=False))
    if (
        evidence.get("left_pixel_identity") is not True
        or not evidence.get("source_client_pixel_sha256")
        or evidence.get("source_client_pixel_sha256") != evidence.get("final_left_pixel_sha256")
    ):
        raise CaseFailure("IGV_CLIENT_PIXEL_IDENTITY_FAILED", json.dumps(evidence, ensure_ascii=False))
    expected_width = client_width + int(config.get("compose.violin_panel_width", 720))
    final_width = int(evidence.get("final_png_width", 0))
    final_height = int(evidence.get("final_png_height", 0))
    if final_width != expected_width or final_height != client_height:
        raise CaseFailure("FINAL_CANVAS_GEOMETRY_INVALID", f"{final_width}x{final_height}")
    return {
        "status": "PASS",
        "full_client": True,
        "resize_count": 0,
        "crop_applied": False,
        "root_screenshot_publishable": False,
        "post_capture_overlays": 0,
        "left_pixel_identity": True,
        "left_pixel_sha256": evidence["final_left_pixel_sha256"],
        "final_png_width": final_width,
        "final_png_height": final_height,
    }


def _desktop_log_errors(stdout: Path, stderr: Path, igv_home: Path) -> list[str]:
    pattern = re.compile(
        r"UNKNOWN COMMAND|UNKOWN COMMAND|\bSEVERE\b|Could not load|Error loading|FileNotFoundException|SAMException|Connection refused|Loading (?:genome|resource):\s*(?:https?|ftp|s3|gs)://|genome server.*(?:https?|ftp|s3|gs)://",
        re.IGNORECASE,
    )
    paths = [stdout, stderr]
    if igv_home.is_dir():
        paths.extend(path for path in igv_home.rglob("*.log") if path not in paths)
    errors: list[str] = []
    for path in paths:
        if not path.is_file():
            continue
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if pattern.search(line):
                errors.append(f"{path.name}: {line.strip()}")
                if len(errors) >= 20:
                    return errors
    return errors


def _run_case_impl(
    case: dict[str, Any],
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    force: bool = False,
    validation_lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = config.validate_run_root(run_root, must_exist=True)
    state_file = _state_path(root, case["case_id"])
    state = _load_state(state_file, case)
    if state.status == PUBLISHED and force:
        return {"case_id": case["case_id"], "status": PUBLISHED, "action": "SKIP_PUBLISHED_IMMUTABLE"}
    if not force and _artifact_is_current(state, case, config):
        return {"case_id": case["case_id"], "status": state.status, "action": "SKIP_CURRENT"}
    is_fresh = (
        len(state.history) == 1
        and state.history[0].get("status") == PREPARED
        and state.status == PREPARED
    )
    if not is_fresh:
        _prepare_rerun(state, case)
    attempt = _case_attempt(state)
    case_root = (
        root
        / ".work"
        / "cases"
        / safe_name(case["ag"]["chrom"])
        / safe_name(case["case_id"])
        / f"attempt_{attempt:03d}"
    )
    raw_root = case_root / "raw"
    batch_root = case_root / "batch"
    logs_root = case_root / "logs"
    state_root = state_file.parent
    attempt_started_monotonic = time.monotonic()
    attempt_started_epoch = time.time()
    attempt_rss_monitor = _AttemptRSSMonitor()
    attempt_rss_monitor.start()
    # Monitoring starts while the case is PREPARED and before its attempt
    # checkpoint is written.  It stops only after REVIEW_PENDING or RERUN has
    # become the terminal state below.
    desktop_result = None
    stage = "attempt_setup"
    try:
        state.save(state_root)
        for directory in (raw_root, batch_root, logs_root, state_root):
            directory.mkdir(parents=True, exist_ok=True)
        stage = "preflight"
        warnings = _validate_case_inputs(case, config)
        state.artifacts["preflight"] = {"warnings": warnings, "attempt": attempt}
        state.transition(IGV_STARTING, detail={"warnings": warnings, "attempt": attempt})
        state.save(state_root)

        stage = "desktop_render"
        control_root = raw_root / "control"
        batch_text, batch_spec = build_desktop_batch(
            case,
            control_directory=control_root,
            config=config,
        )
        batch_path = batch_root / "igv_desktop_batch.txt"
        atomic_write_text(batch_path, batch_text)
        client_png = raw_root / "igv_client.png"
        capture_metadata_path = case_root / "capture_metadata.json"

        def capture_checkpoint(value: dict[str, Any]) -> None:
            state.artifacts["capture_checkpoint"] = value
            state.transition(WINDOW_CAPTURED, detail={"attempt": attempt, "capture": value})
            state.save(state_root)

        def stability_checkpoint(value: dict[str, Any]) -> None:
            state.artifacts["pixel_stability"] = value
            state.transition(RENDER_STABLE, detail={"attempt": attempt, "stability": value})
            state.save(state_root)

        def settle_checkpoint(value: dict[str, Any]) -> None:
            state.artifacts["gui_settle"] = value
            state.artifacts["gui_settle_contract_id"] = GUI_SETTLE_CONTRACT_ID
            state.transition(GUI_SETTLED, detail={"attempt": attempt, "gui_settle": value})
            state.save(state_root)

        desktop_result = run_desktop_session(
            config,
            batch_path=batch_path,
            ready_marker=batch_spec["ready_marker"],
            expected_locus=locus(case["windows"]["overview"]),
            igv_directory=case_root / "igv_home",
            log_directory=logs_root / "desktop",
            capture_directory=raw_root / "capture",
            output_png=client_png,
            metadata_path=capture_metadata_path,
            on_capture=capture_checkpoint,
            on_settle=settle_checkpoint,
            on_stable=stability_checkpoint,
        )
        log_errors = _desktop_log_errors(
            desktop_result.stdout_path,
            desktop_result.stderr_path,
            case_root / "igv_home",
        )
        if log_errors:
            raise CaseFailure("IGV_LOG_ERROR", " | ".join(log_errors))
        client_qc = inspect_png(
            client_png,
            min_width=int(config.get("desktop.minimum_window_width", 1700)),
            min_height=int(config.get("desktop.minimum_window_height", 1800)),
            min_stddev=float(config.get("qc.min_stddev", 0.5)),
        )
        atomic_write_json(case_root / "raw_qc.json", client_qc)
        if client_qc["status"] != "PASS":
            raise CaseFailure("RAW_CLIENT_QC_FAILED", json.dumps(client_qc, ensure_ascii=False))
        state.artifacts.update(
            {
                "attempt_root": str(case_root),
                "batch": str(batch_path),
                "batch_sha256": sha256_file(batch_path),
                "capture_metadata": str(capture_metadata_path),
                "capture_metadata_sha256": sha256_file(capture_metadata_path),
                "client_png": str(client_png),
                "client_sha256": client_qc["sha256"],
                "client_qc": client_qc,
                "desktop_telemetry": {
                    "started_at_epoch": round(desktop_result.started_at_epoch, 6),
                    "ended_at_epoch": round(desktop_result.ended_at_epoch, 6),
                    "wall_time_seconds": round(desktop_result.wall_time_seconds, 3),
                    "peak_rss_gb": round(desktop_result.peak_rss_gb, 4),
                    "attempt": attempt,
                    "scheduler_job_id": os.environ.get("JOB_ID", ""),
                    "scheduler_task_id": os.environ.get("SGE_TASK_ID", ""),
                },
            }
        )
        state.save(state_root)

        stage = "violin"
        violin_png = case_root / "violin.png"
        render_pdf_page(
            case["violin"]["pdf"],
            int(case["violin"]["page"]),
            violin_png,
            pdftoppm=config.get("binaries.pdftoppm", "pdftoppm"),
            dpi=int(config.get("render.violin_dpi", 180)),
            timeout=int(config.get("timeouts.pdftoppm_seconds", 300)),
        )
        violin_qc = inspect_png(
            violin_png,
            min_width=300,
            min_height=300,
            min_stddev=float(config.get("qc.min_stddev", 0.5)),
        )
        if violin_qc["status"] != "PASS":
            raise CaseFailure(violin_qc["code"], f"violin: {violin_qc}")

        stage = "compose"
        combined_root = case_root / "combined"
        combined = combined_root / f"{case['case_id']}.png"
        sample_table = combined_root / f"{case['case_id']}.samples.tsv"
        layout = case_root / "combined" / "layout.json"
        layout_value = compose_desktop_case(
            case,
            client_png,
            violin_png,
            combined,
            layout,
            desktop_result.metadata,
            config,
        )
        _write_sample_table(case, sample_table)
        state.transition(
            COMPOSED,
            detail={"png": str(combined), "sample_table": str(sample_table)},
        )
        state.save(state_root)

        stage = "qc"
        final_qc = inspect_png(
            combined,
            min_width=int(config.get("qc.final_min_width", 3600)),
            min_height=int(config.get("qc.final_min_height", 1200)),
            min_stddev=float(config.get("qc.min_stddev", 0.5)),
        )
        layout_qc = _validate_desktop_layout(layout_value, config)
        final_qc_bundle = {"status": "PASS", "png": final_qc, "layout": layout_qc}
        if final_qc["status"] != "PASS":
            final_qc_bundle["status"] = "FAIL"
        atomic_write_json(case_root / "final_qc.json", final_qc_bundle)
        if final_qc["status"] != "PASS":
            raise CaseFailure(final_qc["code"], json.dumps(final_qc, ensure_ascii=False))
        scientific = scientific_qc(
            case,
            batch_text=batch_text,
            capture=desktop_result.metadata,
            layout=layout_value,
            violin_qc=violin_qc,
            final_png_qc=final_qc,
            config=config,
        )
        scientific_path = case_root / "scientific_qc.json"
        atomic_write_json(scientific_path, scientific)
        if scientific["status"] != "PASS":
            raise CaseFailure("SCIENTIFIC_QC_FAILED", json.dumps(scientific["failed_codes"], ensure_ascii=False))
        state.artifacts.update(
            {
                "combined_png": str(combined),
                "combined_sha256": final_qc["sha256"],
                "sample_table": str(sample_table),
                "sample_table_sha256": sha256_file(sample_table),
                "layout_json": str(layout),
                "layout_sha256": sha256_file(layout),
                "figure_contract_id": FIGURE_CONTRACT_ID,
                "gui_settle_contract_id": GUI_SETTLE_CONTRACT_ID,
                "left_pixel_sha256": layout_value["evidence"]["final_left_pixel_sha256"],
                "left_pixel_identity": layout_value["evidence"]["left_pixel_identity"],
                "final_qc": final_qc_bundle,
                "scientific_qc": str(scientific_path),
                "scientific_qc_sha256": sha256_file(scientific_path),
                "violin_qc": violin_qc,
                "runtime_input_fingerprint": _runtime_input_fingerprint(case, config),
            }
        )
        state.transition(
            QC_PASS,
            detail={
                "png_sha256": final_qc["sha256"],
                "sample_table_sha256": state.artifacts["sample_table_sha256"],
            },
        )
        state.save(state_root)
        state.transition(
            REVIEW_PENDING,
            detail={
                "manual_checks": scientific["manual_review_required"],
                "combined_png": str(combined),
            },
        )
        # Persist the terminal state while the background sampler is still
        # running; the following atomic rewrite binds its final telemetry.
        state.save(state_root)
        whole_case_telemetry = _attempt_telemetry(
            started_monotonic=attempt_started_monotonic,
            started_at_epoch=attempt_started_epoch,
            attempt=attempt,
            rss_monitor=attempt_rss_monitor,
            desktop_result=desktop_result,
            result="REVIEW_PENDING",
            validation_lineage=validation_lineage,
        )
        state.artifacts["telemetry"] = whole_case_telemetry
        state.artifacts.setdefault("telemetry_attempts", []).append(whole_case_telemetry)
        state.save(state_root)
        return {"case_id": case["case_id"], "status": REVIEW_PENDING, "action": "MANUAL_REVIEW"}
    except DesktopFailure as exc:
        state.fail(stage, exc.code, str(exc))
    except CaseFailure as exc:
        state.fail(stage, exc.code, str(exc))
    except Exception as exc:
        state.fail(stage, "UNEXPECTED_ERROR", f"{type(exc).__name__}: {exc}")
    failed_telemetry = _attempt_telemetry(
        started_monotonic=attempt_started_monotonic,
        started_at_epoch=attempt_started_epoch,
        attempt=attempt,
        rss_monitor=attempt_rss_monitor,
        desktop_result=desktop_result,
        result="RERUN",
        validation_lineage=validation_lineage,
    )
    state.artifacts["telemetry"] = failed_telemetry
    state.artifacts.setdefault("telemetry_attempts", []).append(failed_telemetry)
    state.save(state_root)
    return {"case_id": case["case_id"], "status": RERUN, "action": "RERUN", "failure": state.failure}


def run_case(
    case: dict[str, Any],
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    force: bool = False,
    validation_lineage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        root = config.validate_run_root(run_root, must_exist=True)
        with _case_lock(root, case["case_id"]):
            return _run_case_impl(
                case,
                config,
                root,
                force=force,
                validation_lineage=validation_lineage,
            )
    except CaseFailure as exc:
        return {
            "case_id": case["case_id"],
            "status": RERUN,
            "action": "RERUN",
            "failure": {"stage": "lock", "code": exc.code, "message": str(exc)},
        }
    except Exception as exc:
        root = config.validate_run_root(run_root, must_exist=True)
        state_path = _state_path(root, case["case_id"])
        try:
            if state_path.is_file():
                archive = root / ".work" / "state_corrupt"
                archive.mkdir(parents=True, exist_ok=True)
                shutil.copy2(state_path, archive / f"{safe_name(case['case_id'])}.{utc_now().replace(':', '')}.json")
            state = CaseState.fresh(case["case_id"], case["input_fingerprint"])
            state.fail("state", "STATE_OR_SETUP_CORRUPT", f"{type(exc).__name__}: {exc}")
            state.save(state_path.parent)
            failure = state.failure
        except Exception as state_exc:
            failure = {
                "stage": "state",
                "code": "STATE_OR_SETUP_CORRUPT",
                "message": f"{type(exc).__name__}: {exc}; checkpoint failed: {type(state_exc).__name__}: {state_exc}",
            }
        return {"case_id": case["case_id"], "status": RERUN, "action": "RERUN", "failure": failure}


def _task_from_index(
    run_root: Path,
    task_id: int,
    shard_map: str | Path | None = None,
) -> tuple[str, set[str] | None, str]:
    import csv

    path = Path(shard_map) if shard_map else run_root / ".work" / "manifests" / "shards.tsv"
    with path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    matches = [row for row in rows if int(row["task_id"]) == task_id]
    if len(matches) != 1:
        raise ValueError(f"SGE task ID does not map to exactly one shard: {task_id}")
    row = matches[0]
    case_ids: set[str] | None = None
    case_list = row.get("case_list", "").strip()
    if case_list:
        case_list_path = Path(case_list).resolve(strict=False)
        expected_case_list_sha = row.get("case_list_sha256", "").strip().lower()
        if case_list_path.is_symlink() or not case_list_path.is_file():
            raise ValueError(f"chunk case list is missing or symlinked: {case_list_path}")
        if expected_case_list_sha and sha256_file(case_list_path) != expected_case_list_sha:
            raise ValueError(f"chunk case-list SHA-256 mismatch: {case_list_path}")
        with case_list_path.open(encoding="utf-8", newline="") as handle:
            case_ids = {
                item["case_id"]
                for item in csv.DictReader(handle, delimiter="\t")
                if item.get("case_id")
            }
        if len(case_ids) != int(row.get("case_count", len(case_ids))):
            raise ValueError(f"chunk case list count mismatch: {case_list}")
    report_id = str(row.get("report_id", "")).strip() or str(row.get("chunk", "")).strip()
    if not report_id:
        report_id = f"task_{task_id:05d}__{row['shard']}"
    return row["shard"], case_ids, report_id


def _deterministic_report_id(shard: str, case_ids: set[str] | None) -> str:
    """Return a collision-resistant report key for non-array/direct execution."""

    scope = sorted(case_ids) if case_ids is not None else ["ALL_CASES_IN_SHARD"]
    return f"direct__{safe_name(shard)}__{sha256_json(scope)[:16]}"


def _write_shard_report(run_root: Path, report_id: str, report: dict[str, Any]) -> Path:
    path = run_root / ".work" / "shard_reports" / f"{safe_name(report_id)}.json"
    atomic_write_json(path, report)
    return path


def run_shard(
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    shard: str | None = None,
    shard_index: int | None = None,
    shard_map: str | Path | None = None,
    case_ids: set[str] | None = None,
    report_id: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    root = config.validate_run_root(run_root, must_exist=True)
    if shard is None and shard_index is None and shard_map:
        cases = load_cases(root)
        assert_manifest_config(cases, config, root)
        map_path = Path(shard_map)
        with map_path.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        reports = [
            run_shard(
                config,
                root,
                shard_index=int(row["task_id"]),
                shard_map=map_path,
                force=force,
            )
            for row in rows
        ]
        failed = sum(report["exit_code"] != 0 for report in reports)
        return {
            "status": "REVIEW_PENDING" if not failed else "RERUN",
            "report_count": len(reports),
            "reports": reports,
            "exit_code": 2 if failed else 0,
        }
    if shard is None:
        if shard_index is None:
            raise ValueError("shard or shard_index is required")
        shard, mapped_case_ids, mapped_report_id = _task_from_index(root, shard_index, shard_map)
        if report_id is not None and report_id != mapped_report_id:
            raise ValueError("explicit report_id conflicts with the task-map report identity")
        report_id = mapped_report_id
        if mapped_case_ids is not None:
            case_ids = mapped_case_ids if case_ids is None else case_ids & mapped_case_ids
    if report_id is None:
        report_id = _deterministic_report_id(str(shard), case_ids)
    cases = load_cases(root)
    assert_manifest_config(cases, config, root)
    if case_ids is not None:
        unknown = sorted(case_ids - {case["case_id"] for case in cases})
        if unknown:
            raise ValueError("case list contains unknown case IDs: " + ", ".join(unknown[:10]))
    selected = [case for case in cases if case.get("shard") == shard and (case_ids is None or case["case_id"] in case_ids)]
    if not selected:
        raise ValueError(f"no manifest cases found for shard {shard}")
    completed_ids: set[str] = set()
    for case in cases:
        state_path = _state_path(root, case["case_id"])
        if state_path.is_file():
            try:
                existing_state = CaseState.load(state_path)
            except Exception:
                continue
            if existing_state.status in {REVIEW_PENDING, PUBLISHED}:
                completed_ids.add(case["case_id"])
    results: list[dict[str, Any]] = []
    storage_evidence: dict[str, Any] | None = None
    shard_failure: dict[str, str] | None = None
    unstarted_case_ids: list[str] = []
    for index, case in enumerate(selected):
        remaining_cases = len(cases) - len(completed_ids)
        try:
            storage_evidence = cached_storage_low_watermark(
                config,
                root,
                remaining_cases=remaining_cases,
                total_cases=len(cases),
            )
        except Exception as exc:
            shard_failure = {
                "code": "STORAGE_LOW_WATERMARK",
                "message": f"{type(exc).__name__}: {exc}",
            }
            unstarted_case_ids = [item["case_id"] for item in selected[index:]]
            break
        result = run_case(
            case,
            config,
            root,
            force=force,
        )
        results.append(result)
        if result["status"] in {REVIEW_PENDING, PUBLISHED}:
            completed_ids.add(case["case_id"])
    failed = sum(result["status"] not in {REVIEW_PENDING, PUBLISHED} for result in results)
    report = {
        "created_at": utc_now(),
        "report_id": report_id,
        "shard": shard,
        "case_count": len(selected),
        "attempted_count": len(results),
        "passed": len(results) - failed,
        "failed": failed,
        "results": results,
        "storage_low_watermark": storage_evidence,
        "unstarted_case_ids": unstarted_case_ids,
        "exit_code": 2 if failed or shard_failure is not None else 0,
    }
    if shard_failure is not None:
        report["shard_failure"] = shard_failure
    report_path = _write_shard_report(root, report_id, report)
    report["report_path"] = str(report_path)
    return report
