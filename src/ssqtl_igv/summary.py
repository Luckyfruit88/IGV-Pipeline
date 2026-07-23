from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
from collections import Counter
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from . import __version__
from .review import approved_review
from .runner import assert_manifest_config, load_cases
from .state import FAILED, PREPARED, PUBLISHED, RERUN, REVIEW_PENDING, CaseState
from .utils import atomic_write_json, sha256_file, sha256_json, safe_name, utc_now, write_tsv


FINAL_FIELDS = [
    "association_row",
    "case_id",
    "shard",
    "status",
    "action",
    "failure_code",
    "failure_message",
    "combined_sha256",
    "sample_table_sha256",
    "delivered_review_png",
    "delivered_sample_table",
]


PUBLIC_ARTIFACTS = (
    {
        "directory": "review_by_chr",
        "suffix": ".png",
        "source": "combined_png",
        "source_sha256": "combined_sha256",
        "published": "delivered_review_png",
        "published_sha256": "delivered_review_png_sha256",
    },
    {
        "directory": "tables",
        "suffix": ".samples.tsv",
        "source": "sample_table",
        "source_sha256": "sample_table_sha256",
        "published": "delivered_sample_table",
        "published_sha256": "delivered_sample_table_sha256",
    },
)


def _case_chromosome(case: dict[str, Any]) -> str:
    chromosome = str(case.get("ag", {}).get("chrom", ""))
    if (
        not chromosome
        or Path(chromosome).name != chromosome
        or chromosome in {".", ".."}
        or any(char.isspace() for char in chromosome)
    ):
        raise ValueError(f"invalid case chromosome for public output: {chromosome!r}")
    return chromosome


def _publication_chromosomes(
    config: WorkflowConfig, cases: list[dict[str, Any]]
) -> tuple[str, ...]:
    observed = {_case_chromosome(case) for case in cases}
    configured = tuple(str(value) for value in config.get("publication.chromosomes", []))
    if configured:
        if observed - set(configured):
            raise ValueError(
                "manifest contains chromosomes outside publication.chromosomes: "
                + ", ".join(sorted(observed - set(configured)))
            )
        return configured
    return tuple(sorted(observed))


def _public_relative_path(case: dict[str, Any], artifact: dict[str, str]) -> Path:
    return (
        Path(artifact["directory"])
        / _case_chromosome(case)
        / f"{case['case_id']}{artifact['suffix']}"
    )


def _read_state(run_root: Path, case: dict[str, Any]) -> CaseState | None:
    path = run_root / ".work" / "state" / f"{safe_name(case['case_id'])}.json"
    return CaseState.load(path) if path.is_file() else None


def _state_fingerprint(cases: list[dict[str, Any]], states: dict[str, CaseState | None]) -> str:
    snapshot = []
    for case in sorted(cases, key=lambda item: int(item["association_row"])):
        state = states.get(case["case_id"])
        snapshot.append(
            {
                "case_id": case["case_id"],
                "state": state.to_dict() if state else None,
            }
        )
    return sha256_json(snapshot)


def _public_checksums_valid(
    run_root: Path,
    publish_root: Path,
    chromosomes: tuple[str, ...],
) -> bool:
    required = {
        "final_status.tsv",
        "failed_cases.tsv",
        "rerun_manifest.tsv",
        "run_summary.json",
        "provenance.json",
        "telemetry.json",
        "SHA256SUMS",
    }
    public_directories = {artifact["directory"] for artifact in PUBLIC_ARTIFACTS}
    if not all((run_root / name).is_file() for name in required) or not all(
        (publish_root / name).is_dir() for name in public_directories
    ):
        return False
    expected_chromosomes = set(chromosomes)
    for directory in public_directories:
        observed = {path.name for path in (publish_root / directory).iterdir() if path.is_dir()}
        if observed != expected_chromosomes:
            return False
    try:
        lines = (run_root / "SHA256SUMS").read_text(encoding="utf-8").splitlines()
        recorded: dict[str, str] = {}
        for line in lines:
            digest, relative = line.split("  ", 1)
            path = Path(relative)
            if path.is_absolute() or ".." in path.parts:
                return False
            recorded[str(path)] = digest
        actual_files = required - {"SHA256SUMS"}
        for directory in public_directories:
            actual_files.update(
                str(path.relative_to(publish_root))
                for path in (publish_root / directory).rglob("*")
                if path.is_file()
            )
        if set(recorded) != actual_files:
            return False
        for relative, digest in recorded.items():
            relative_path = Path(relative)
            base = publish_root if relative_path.parts[0] in public_directories else run_root
            if sha256_file(base / relative_path) != digest:
                return False
        return True
    except (OSError, ValueError):
        return False


def _provenance(config: WorkflowConfig, run_root: Path) -> dict[str, Any]:
    manifest = run_root / ".work" / "manifests" / "case_manifest.jsonl"
    prepare_report = run_root / ".work" / "prepare_report.json"
    preflight_report = run_root / ".work" / "preflight.json"
    package_root = Path(__file__).resolve().parent
    source_files = sorted(
        [
            path
            for path in [
                *package_root.rglob("*.py"),
                *package_root.rglob("*.R"),
                *package_root.rglob("*.yaml"),
            ]
            if not path.name.startswith("._")
        ],
        key=lambda path: str(path.relative_to(package_root)),
    )
    source_inventory = [
        {"path": str(path.relative_to(package_root)), "sha256": sha256_file(path)}
        for path in source_files
    ]

    def installed_version(distribution: str) -> str | None:
        try:
            return version(distribution)
        except PackageNotFoundError:
            return None

    return {
        "created_at": utc_now(),
        "package": "igv-snapshot-workflow",
        "package_version": __version__,
        "installed_distribution_version": installed_version("igv-snapshot-workflow"),
        "package_source_root": str(package_root),
        "package_source_sha256": sha256_json(source_inventory),
        "package_source_file_count": len(source_inventory),
        "python_executable": sys.executable,
        "dependencies": {
            "Pillow": installed_version("Pillow"),
            "PyYAML": installed_version("PyYAML"),
            "setuptools": installed_version("setuptools"),
            "wheel": installed_version("wheel"),
        },
        "config": str(config.path),
        "config_fingerprint": sha256_json(config.data),
        "manifest": str(manifest),
        "manifest_sha256": sha256_file(manifest),
        "prepare_report": json.loads(prepare_report.read_text(encoding="utf-8")) if prepare_report.is_file() else None,
        "preflight_report": json.loads(preflight_report.read_text(encoding="utf-8")) if preflight_report.is_file() else None,
        "modules": config.modules,
    }


def _telemetry(cases: list[dict[str, Any]], states: dict[str, CaseState | None]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    unresolved_capture_failures = 0
    capture_codes = {
        "XVFB_EXITED",
        "XVFB_START_TIMEOUT",
        "IGV_EXIT_BEFORE_WINDOW",
        "IGV_WINDOW_NOT_FOUND",
        "WINDOW_CAPTURE_FAILED",
        "ROOT_FALLBACK_CAPTURE_FAILED",
        "ROOT_FALLBACK_GEOMETRY_INVALID",
        "ROOT_FALLBACK_CROP_INVALID",
        "PIXEL_STABILITY_TIMEOUT",
        "GUI_SETTLE_TIMEOUT",
        "IGV_EXIT_DURING_GUI_SETTLE",
        "TOOLBAR_LOCUS_REGION_INVALID",
        "TESSERACT_FAILED",
        "LOCUS_TEXT_NOT_DETECTED",
    }
    for case in sorted(cases, key=lambda item: int(item["association_row"])):
        state = states.get(case["case_id"])
        history = state.history if state else []
        first_failure = next(
            (
                event.get("detail", {})
                for event in history
                if event.get("status") == "FAILED"
            ),
            {},
        )
        telemetry = state.artifacts.get("telemetry", {}) if state else {}
        first_pass_failed = bool(first_failure)
        final_status = state.status if state else "PREPARED"
        if first_failure.get("code") in capture_codes and final_status not in {REVIEW_PENDING, PUBLISHED}:
            unresolved_capture_failures += 1
        rows.append(
            {
                "case_id": case["case_id"],
                "association_row": case["association_row"],
                "shard": case["shard"],
                "wall_time_seconds": telemetry.get("wall_time_seconds"),
                "peak_rss_gb": telemetry.get("peak_rss_gb"),
                "first_pass_failed": first_pass_failed,
                "first_failure_code": first_failure.get("code"),
                "first_failure_reason": first_failure.get("message"),
                "attempt_count": sum(event.get("status") == PREPARED for event in history),
                "rerun_result": final_status,
            }
        )
    return {
        "schema_version": "1.0-scheduler-telemetry",
        "created_at": utc_now(),
        "case_count": len(rows),
        "unresolved_igv_capture_failures": unresolved_capture_failures,
        "cases": rows,
    }


def _same_published_run(
    run_root: Path,
    publish_root: Path,
    manifest_sha256: str,
    cases: list[dict[str, Any]],
    states: dict[str, CaseState | None],
    chromosomes: tuple[str, ...],
) -> bool:
    complete = run_root / ".publish_complete.json"
    if not complete.is_file():
        return False
    try:
        value = json.loads(complete.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if (
        value.get("manifest_sha256") != manifest_sha256
        or value.get("state_fingerprint") != _state_fingerprint(cases, states)
        or not _public_checksums_valid(run_root, publish_root, chromosomes)
    ):
        return False
    for case in cases:
        state = states.get(case["case_id"])
        if state is None or state.status != PUBLISHED:
            return False
        for artifact in PUBLIC_ARTIFACTS:
            published = publish_root / _public_relative_path(case, artifact)
            expected_sha256 = state.artifacts.get(artifact["published_sha256"])
            if (
                not expected_sha256
                or state.artifacts.get(artifact["published"]) != str(published)
                or not published.is_file()
                or sha256_file(published) != expected_sha256
            ):
                return False
    return True


def _validate_publishable_artifacts(case: dict[str, Any], state: CaseState) -> None:
    for artifact in PUBLIC_ARTIFACTS:
        source_value = state.artifacts.get(artifact["source"])
        expected_sha256 = state.artifacts.get(artifact["source_sha256"])
        if not source_value:
            raise ValueError(
                f"publishable artifact path is missing ({artifact['source']}): {case['case_id']}"
            )
        if not expected_sha256:
            raise ValueError(
                f"publishable artifact hash is missing ({artifact['source_sha256']}): {case['case_id']}"
            )
        source = Path(source_value)
        if not source.is_file() or sha256_file(source) != expected_sha256:
            raise ValueError(
                f"publishable artifact is missing or changed ({artifact['source']}): {case['case_id']}"
            )


def _validate_public_directory(
    root: Path,
    directory: str,
    desired_names: set[str],
) -> None:
    public_directory = root / directory
    if public_directory.is_symlink():
        raise ValueError(f"published {directory} path must not be a symlink")
    if public_directory.exists() and not public_directory.is_dir():
        raise FileExistsError(f"published {directory} path is not a directory: {public_directory}")
    if not public_directory.is_dir():
        return
    existing = [path for path in public_directory.rglob("*") if path.is_file()]
    if any(path.is_symlink() for path in public_directory.rglob("*")):
        raise ValueError(f"published {directory} directory contains a symlink")
    existing_names = {str(path.relative_to(public_directory)) for path in existing}
    if not existing_names.issubset(desired_names):
        raise ValueError(f"published {directory} set conflicts with current case states")


def _publication_archive_signature(
    *,
    run_root: Path,
    manifest_sha256: str,
    case: dict[str, Any],
    state: CaseState,
    target: Path,
    relative_public_path: Path,
    prior_sha256: str,
    replacement_sha256: str,
) -> dict[str, str]:
    return {
        "run_id": run_root.name,
        "run_root": str(run_root.resolve()),
        "manifest_sha256": manifest_sha256,
        "case_id": str(case["case_id"]),
        "input_fingerprint": state.input_fingerprint,
        "public_path": str(target.resolve(strict=False)),
        "relative_public_path": str(relative_public_path),
        "prior_delivered_review_png_sha256": prior_sha256,
        "replacement_review_png_sha256": replacement_sha256,
    }


def _existing_archive_record(
    archive_case_root: Path,
    signature: dict[str, str],
) -> dict[str, Any] | None:
    if not archive_case_root.is_dir():
        return None
    for ledger_path in sorted(archive_case_root.glob("g*/ledger.json")):
        if ledger_path.is_symlink():
            raise ValueError(f"publication archive ledger must not be a symlink: {ledger_path}")
        try:
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid publication archive ledger: {ledger_path}") from exc
        if any(ledger.get(key) != value for key, value in signature.items()):
            continue
        archived_name = ledger.get("archived_artifact_name")
        if not isinstance(archived_name, str) or Path(archived_name).name != archived_name:
            raise ValueError(f"invalid archived artifact name in {ledger_path}")
        archived = ledger_path.parent / archived_name
        if (
            archived.is_symlink()
            or not archived.is_file()
            or sha256_file(archived) != signature["prior_delivered_review_png_sha256"]
        ):
            raise ValueError(f"publication archive evidence is missing or changed: {archived}")
        return {
            "generation": ledger_path.parent.name,
            "ledger": str(ledger_path),
            "archived_artifact": str(archived),
            "archived_sha256": signature["prior_delivered_review_png_sha256"],
        }
    return None


def _archive_previous_review(
    *,
    run_root: Path,
    authorization: dict[str, Any],
) -> dict[str, Any]:
    target = Path(authorization["target"])
    prior_sha256 = authorization["prior_sha256"]
    if target.is_symlink() or not target.is_file() or sha256_file(target) != prior_sha256:
        raise ValueError(f"review evidence changed before archival: {target}")

    case = authorization["case"]
    state = authorization["state"]
    archive_case_root = (
        run_root / ".work" / "publication_archive" / safe_name(str(case["case_id"]))
    )
    if archive_case_root.exists() and (
        archive_case_root.is_symlink() or not archive_case_root.is_dir()
    ):
        raise ValueError(f"invalid publication archive case directory: {archive_case_root}")
    archive_case_root.mkdir(parents=True, exist_ok=True)

    signature = _publication_archive_signature(
        run_root=run_root,
        manifest_sha256=authorization["manifest_sha256"],
        case=case,
        state=state,
        target=target,
        relative_public_path=authorization["relative_public_path"],
        prior_sha256=prior_sha256,
        replacement_sha256=authorization["replacement_sha256"],
    )
    existing = _existing_archive_record(archive_case_root, signature)
    if existing is not None:
        return existing

    generations = [
        int(match.group(1))
        for path in archive_case_root.iterdir()
        if path.is_dir() and (match := re.fullmatch(r"g([0-9]+)", path.name))
    ]
    generation = f"g{max(generations, default=0) + 1:04d}"
    final_generation = archive_case_root / generation
    if final_generation.exists():
        raise FileExistsError(f"publication archive generation already exists: {final_generation}")

    temporary = Path(tempfile.mkdtemp(prefix=".archive_staging_", dir=archive_case_root))
    try:
        archived = temporary / target.name
        shutil.copy2(target, archived)
        if sha256_file(archived) != prior_sha256:
            raise ValueError(f"archived review evidence hash mismatch: {archived}")
        ledger = {
            "schema_version": "1.0-review-publication-archive",
            "archived_at": utc_now(),
            "generation": generation,
            "status_before_replacement": state.status,
            "archived_artifact_name": target.name,
            "archived_artifact_sha256": prior_sha256,
            **signature,
        }
        atomic_write_json(temporary / "ledger.json", ledger)
        os.replace(temporary, final_generation)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary, ignore_errors=True)

    return {
        "generation": generation,
        "ledger": str(final_generation / "ledger.json"),
        "archived_artifact": str(final_generation / target.name),
        "archived_sha256": prior_sha256,
    }


def _manual_reject_rerun_is_proven(state: CaseState) -> bool:
    delivered_history_length = state.artifacts.get("delivered_history_length")
    if (
        not isinstance(delivered_history_length, int)
        or isinstance(delivered_history_length, bool)
        or delivered_history_length < 1
        or delivered_history_length > len(state.history)
    ):
        return False
    events = state.history[delivered_history_length:]
    cursor = 0
    for event in events:
        detail = event.get("detail", {})
        if (
            cursor == 0
            and event.get("status") == FAILED
            and detail.get("stage") == "manual_review"
            and detail.get("code") == "MANUAL_REVIEW_REJECTED"
        ):
            cursor = 1
        elif cursor == 1 and event.get("status") == RERUN:
            cursor = 2
        elif cursor == 2 and event.get("status") == PREPARED:
            cursor = 3
        elif cursor == 3 and event.get("status") == REVIEW_PENDING:
            return True
    return False


def _review_replacement_authorizations(
    *,
    run_root: Path,
    publish_root: Path,
    manifest_sha256: str,
    deliverable: list[tuple[dict[str, Any], CaseState, bool]],
) -> dict[str, dict[str, Any]]:
    review_artifact = PUBLIC_ARTIFACTS[0]
    authorizations: dict[str, dict[str, Any]] = {}
    for case, state, _approved in deliverable:
        for artifact in PUBLIC_ARTIFACTS:
            relative_public_path = _public_relative_path(case, artifact)
            target = publish_root / relative_public_path
            if target.is_symlink():
                raise ValueError(f"published evidence must not be a symlink: {target}")
            if not target.exists():
                continue
            if not target.is_file():
                raise ValueError(f"published evidence is not a file: {target}")
            replacement_sha256 = str(state.artifacts[artifact["source_sha256"]])
            observed_sha256 = sha256_file(target)
            expected_identity = {
                artifact["published"]: str(target),
                artifact["published_sha256"]: observed_sha256,
                "delivered_run_id": run_root.name,
                "delivered_run_root": str(run_root.resolve()),
                "delivered_manifest_sha256": manifest_sha256,
                "delivered_input_fingerprint": state.input_fingerprint,
            }
            mismatches = {
                key: {"expected": value, "observed": state.artifacts.get(key)}
                for key, value in expected_identity.items()
                if state.artifacts.get(key) != value
            }
            if mismatches:
                raise FileExistsError(
                    "refusing unproven existing published evidence for "
                    f"{case['case_id']}: {json.dumps(mismatches, sort_keys=True)}"
                )
            if observed_sha256 == replacement_sha256:
                continue
            if artifact["published"] != review_artifact["published"]:
                raise FileExistsError(f"published sample table is immutable: {target}")
            if state.status == PUBLISHED or any(
                event.get("status") == PUBLISHED for event in state.history
            ):
                raise FileExistsError(f"PUBLISHED review evidence is immutable: {target}")
            if state.status != REVIEW_PENDING:
                raise FileExistsError(
                    f"review evidence replacement requires REVIEW_PENDING state: {case['case_id']}"
                )
            if not _manual_reject_rerun_is_proven(state):
                raise FileExistsError(
                    "review evidence replacement requires a provenance-bound "
                    f"manual reject -> rerun -> REVIEW_PENDING sequence: {case['case_id']}"
                )
            relative_within_directory = relative_public_path.relative_to(
                review_artifact["directory"]
            )
            authorizations[str(relative_within_directory)] = {
                "case": case,
                "state": state,
                "target": str(target),
                "relative_public_path": relative_public_path,
                "prior_sha256": observed_sha256,
                "replacement_sha256": replacement_sha256,
                "manifest_sha256": manifest_sha256,
            }
    return authorizations


def _merge_staged_directory(
    publish_root: Path,
    staging: Path,
    directory: str,
    *,
    run_root: Path,
    replacements: dict[str, dict[str, Any]] | None = None,
) -> None:
    staged_directory = staging / directory
    public_directory = publish_root / directory
    replacements = replacements or {}
    if public_directory.is_symlink():
        raise ValueError(f"published {directory} path must not be a symlink")
    if public_directory.is_dir():
        for staged_artifact in sorted(path for path in staged_directory.rglob("*") if path.is_file()):
            relative = staged_artifact.relative_to(staged_directory)
            target = public_directory / relative
            if target.parent.is_symlink():
                raise ValueError(f"published evidence parent must not be a symlink: {target.parent}")
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                target_sha256 = sha256_file(target)
                staged_sha256 = sha256_file(staged_artifact)
                if target_sha256 == staged_sha256:
                    staged_artifact.unlink()
                    continue
                authorization = replacements.get(str(relative))
                if authorization is None:
                    raise FileExistsError(
                        f"refusing to overwrite changed published artifact: {target}"
                    )
                if (
                    target_sha256 != authorization["prior_sha256"]
                    or staged_sha256 != authorization["replacement_sha256"]
                    or Path(authorization["target"]) != target
                ):
                    raise ValueError(f"review replacement authorization no longer matches: {target}")
                archive_record = _archive_previous_review(
                    run_root=run_root,
                    authorization=authorization,
                )
                if sha256_file(target) != target_sha256:
                    raise ValueError(f"review evidence changed during archival: {target}")
                os.replace(staged_artifact, target)
                if sha256_file(target) != staged_sha256:
                    raise RuntimeError(f"review evidence replacement hash mismatch: {target}")
                authorization["archive_record"] = archive_record
            else:
                os.replace(staged_artifact, target)
        for directory_path in sorted(
            (path for path in staged_directory.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            directory_path.rmdir()
        staged_directory.rmdir()
    else:
        os.replace(staged_directory, public_directory)


def summarize_run(config: WorkflowConfig, run_root: str | Path) -> dict[str, Any]:
    root = config.validate_run_root(run_root, must_exist=True)
    publish_source = config.publish_root
    if publish_source.is_symlink():
        raise ValueError("paths.publish_root must not be a symlink")
    publish_root = publish_source.resolve(strict=False)
    cases = load_cases(root)
    assert_manifest_config(cases, config, root)
    publish_root.mkdir(parents=True, exist_ok=True)
    for forbidden in ("png", "svg"):
        if (publish_root / forbidden).exists():
            raise ValueError(f"delivery contract forbids {publish_root / forbidden}")
    manifest_sha = sha256_file(root / ".work" / "manifests" / "case_manifest.jsonl")
    chromosomes = _publication_chromosomes(config, cases)
    states = {case["case_id"]: _read_state(root, case) for case in cases}
    if _same_published_run(
        root,
        publish_root,
        manifest_sha,
        cases,
        states,
        chromosomes,
    ):
        summary = json.loads((root / "run_summary.json").read_text(encoding="utf-8"))
        return {**summary, "action": "SKIP_ALREADY_PUBLISHED", "exit_code": 2 if summary.get("failed", 0) else 0}

    rows: list[dict[str, Any]] = []
    deliverable: list[tuple[dict[str, Any], CaseState, bool]] = []
    for case in cases:
        state = states[case["case_id"]]
        status = state.status if state else "PREPARED"
        failure = state.failure if state and state.failure else {}
        review = approved_review(root, case, state) if state and status == REVIEW_PENDING else None
        published_status = PUBLISHED if status == PUBLISHED or review is not None else status
        if published_status == PUBLISHED:
            action = "PUBLISHED"
        elif status == REVIEW_PENDING:
            action = "REVIEW"
        else:
            action = "RERUN"
        combined_sha = state.artifacts.get("combined_sha256", "") if state else ""
        sample_table_sha = state.artifacts.get("sample_table_sha256", "") if state else ""
        ready_for_delivery = bool(state and status in {REVIEW_PENDING, PUBLISHED})
        delivered_names = {
            artifact["published"]: (
                str(publish_root / _public_relative_path(case, artifact))
                if ready_for_delivery
                else ""
            )
            for artifact in PUBLIC_ARTIFACTS
        }
        rows.append(
            {
                "association_row": case["association_row"],
                "case_id": case["case_id"],
                "shard": case["shard"],
                "status": published_status,
                "action": action,
                "failure_code": failure.get("code", ""),
                "failure_message": failure.get("message", ""),
                "combined_sha256": combined_sha,
                "sample_table_sha256": sample_table_sha,
                **delivered_names,
            }
        )
        if ready_for_delivery and state:
            _validate_publishable_artifacts(case, state)
            deliverable.append((case, state, review is not None))

    rows.sort(key=lambda row: int(row["association_row"]))
    staging_parent = root / ".work"
    staging_parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="publish_staging_", dir=staging_parent))
    try:
        for artifact in PUBLIC_ARTIFACTS:
            (staging / artifact["directory"]).mkdir()
            for chromosome in chromosomes:
                (staging / artifact["directory"] / chromosome).mkdir()
        for case, state, _approved in deliverable:
            for artifact in PUBLIC_ARTIFACTS:
                staged_path = staging / _public_relative_path(case, artifact)
                staged_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(
                    state.artifacts[artifact["source"]],
                    staged_path,
                )

        failed_rows = [row for row in rows if row["action"] == "RERUN"]
        review_rows = [row for row in rows if row["action"] == "REVIEW"]
        rerun_rows = [
            {
                "run_id": root.name,
                "manifest_sha256": manifest_sha,
                "case_id": row["case_id"],
                "shard": row["shard"],
                "failure_code": row["failure_code"],
                "failure_message": row["failure_message"],
            }
            for row in failed_rows
        ]
        write_tsv(staging / "final_status.tsv", FINAL_FIELDS, rows)
        write_tsv(staging / "failed_cases.tsv", FINAL_FIELDS, failed_rows)
        write_tsv(
            staging / "rerun_manifest.tsv",
            ["run_id", "manifest_sha256", "case_id", "shard", "failure_code", "failure_message"],
            rerun_rows,
        )
        counts = Counter(row["status"] for row in rows)
        run_summary = {
            "created_at": utc_now(),
            "run_id": root.name,
            "run_root": str(root),
            "publish_root": str(publish_root),
            "total": len(rows),
            "published": counts[PUBLISHED],
            "delivered_for_review": len(review_rows),
            "failed": len(failed_rows),
            "review_pending": len(review_rows),
            "status_counts": dict(sorted(counts.items())),
            "manifest_sha256": manifest_sha,
        }
        atomic_write_json(staging / "run_summary.json", run_summary)
        atomic_write_json(staging / "provenance.json", _provenance(config, root))
        atomic_write_json(staging / "telemetry.json", _telemetry(cases, states))

        checksum_lines = []
        checksum_targets = sorted(
            [path for path in staging.rglob("*") if path.is_file() and path.name != "SHA256SUMS"],
            key=lambda path: str(path.relative_to(staging)),
        )
        for path in checksum_targets:
            checksum_lines.append(f"{sha256_file(path)}  {path.relative_to(staging)}")
        (staging / "SHA256SUMS").write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")

        public_names = [
            *(artifact["directory"] for artifact in PUBLIC_ARTIFACTS),
            "final_status.tsv",
            "failed_cases.tsv",
            "rerun_manifest.tsv",
            "run_summary.json",
            "provenance.json",
            "telemetry.json",
            "SHA256SUMS",
        ]
        complete_path = root / ".publish_complete.json"
        if complete_path.exists():
            archived_markers = root / ".work" / "publish_markers"
            archived_markers.mkdir(parents=True, exist_ok=True)
            os.replace(complete_path, archived_markers / f"complete_{utc_now().replace(':', '').replace('+', '_')}.json")
        for artifact in PUBLIC_ARTIFACTS:
            desired_names = {
                str(_public_relative_path(case, artifact).relative_to(artifact["directory"]))
                for case, _state, _approved in deliverable
            }
            _validate_public_directory(publish_root, artifact["directory"], desired_names)
        review_replacements = _review_replacement_authorizations(
            run_root=root,
            publish_root=publish_root,
            manifest_sha256=manifest_sha,
            deliverable=deliverable,
        )
        for artifact in PUBLIC_ARTIFACTS:
            _merge_staged_directory(
                publish_root,
                staging,
                artifact["directory"],
                run_root=root,
                replacements=(
                    review_replacements
                    if artifact["published"] == "delivered_review_png"
                    else None
                ),
            )
        artifact_directory_count = len(PUBLIC_ARTIFACTS)
        for name in public_names[artifact_directory_count:]:
            os.replace(staging / name, root / name)

        for case, state, approved in deliverable:
            published_paths: dict[str, str] = {}
            publication_changed = False
            for artifact in PUBLIC_ARTIFACTS:
                published_path = publish_root / _public_relative_path(case, artifact)
                published_sha256 = sha256_file(published_path)
                if (
                    state.artifacts.get(artifact["published"]) != str(published_path)
                    or state.artifacts.get(artifact["published_sha256"]) != published_sha256
                ):
                    publication_changed = True
                state.artifacts[artifact["published"]] = str(published_path)
                state.artifacts[artifact["published_sha256"]] = published_sha256
                published_paths[artifact["published"]] = str(published_path)
            state.artifacts["delivered_run_id"] = root.name
            state.artifacts["delivered_run_root"] = str(root.resolve())
            state.artifacts["delivered_manifest_sha256"] = manifest_sha
            state.artifacts["delivered_input_fingerprint"] = state.input_fingerprint
            state.artifacts["delivered_history_length"] = len(state.history)
            if publication_changed or "delivered_at" not in state.artifacts:
                state.artifacts["delivered_at"] = utc_now()
            review_relative = str(
                _public_relative_path(case, PUBLIC_ARTIFACTS[0]).relative_to(
                    PUBLIC_ARTIFACTS[0]["directory"]
                )
            )
            replacement = review_replacements.get(review_relative)
            if replacement and replacement.get("archive_record"):
                archive_record = replacement["archive_record"]
                history = state.artifacts.setdefault("publication_archive_history", [])
                if not isinstance(history, list):
                    raise ValueError(
                        f"invalid publication archive history for {case['case_id']}"
                    )
                if not any(
                    item.get("ledger") == archive_record["ledger"]
                    for item in history
                    if isinstance(item, dict)
                ):
                    history.append(archive_record)
            if state.status == REVIEW_PENDING and approved:
                state.transition(PUBLISHED, detail={"artifacts": published_paths})
            state.save(root / ".work" / "state")
        final_states = {case["case_id"]: _read_state(root, case) for case in cases}
        complete = {
            "created_at": utc_now(),
            "manifest_sha256": manifest_sha,
            "state_fingerprint": _state_fingerprint(cases, final_states),
            "delivered_for_review": len(review_rows),
            "published": sum(state.status == PUBLISHED for state in final_states.values() if state),
            "failed": len(failed_rows),
            "review_pending": len(review_rows),
        }
        atomic_write_json(root / ".summary_complete.json", complete)
        if not failed_rows and not review_rows:
            atomic_write_json(complete_path, complete)
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    action = (
        "RERUN_REQUIRED"
        if failed_rows
        else "REVIEW_PENDING"
        if review_rows
        else "PUBLISHED"
    )
    return {**run_summary, "action": action, "exit_code": 2 if failed_rows else 0}
