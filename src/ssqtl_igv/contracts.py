from __future__ import annotations

import json
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .utils import read_regular_file_bytes, sha256_json

# Stable identifiers describe workflow behavior, not a particular dataset or site.
FIGURE_CONTRACT_ID = "v031_native_igv_pixel_exact"
GUI_SETTLE_CONTRACT_ID = "v031_toolbar_locus_settle_v1"
LOCUS_OCR_CONTRACT_ID = "v031_locus_field_ocr_v3"
LOCUS_OCR_RESAMPLING = "BICUBIC"
IGV_LOCAL_ONLY_STARTUP_CONTRACT_ID = "v031_igv_local_only_startup_v1"
DESKTOP_LAYOUT_SCHEMA = "3.1-native-pixel"
DESKTOP_CAPTURE_SCHEMA = "1.1-desktop-gui-settle"
SCIENTIFIC_QC_SCHEMA = "1.3-scientific-qc-mane-v1.5"
WHOLE_CASE_RSS_SOURCE = "whole_case_continuous_process_tree_plus_rusage"
WHOLE_CASE_RSS_CONTRACT_ID = "mane-v1.5-whole-attempt-rss-v1"
ATTEMPT_RSS_SAMPLE_INTERVAL_SECONDS = 0.25

# The bounded Grid Engine array chain is configurable; this is only the safe default.
SCHEDULER_THROTTLE_CONTRACT_ID = "bounded-array-strict-hold-v1"
DEFAULT_MAX_TASKS_PER_ARRAY = 8

# Safe defaults may be overridden by the deployment configuration.
DEFAULT_CLIENT_WIDTH = 1920
DEFAULT_CLIENT_HEIGHT = 2160
DEFAULT_CLIENT_DEPTH = 24
DEFAULT_LOCUS_FIELD_ROI = {"x": 0, "y": 0, "width": 400, "height": 80}
DEFAULT_LOCUS_FIELD_OCR_SCALE = 4
DEFAULT_LOCUS_FIELD_OCR_PSM = 13
DEFAULT_LOCUS_FIELD_OCR_WHITELIST = "chrXxYy0123456789:,-"
STORAGE_GATE_SCHEMA = "mane-v1.5-storage-gate-v1"


# Nextflow migration contracts. These constants are deliberately separate from
# legacy mutable CaseState values above: each dimension has one owner and no
# dimension silently implies another.
PIPELINE_VERSION = "2.0.0"
CANONICAL_SCHEMA_VERSION = "2.0"
V3_PIPELINE_VERSION = "3.0.0"
V3_CANONICAL_SCHEMA_VERSION = "3.0"
V3_GENERIC_MANUAL_ASSERTIONS = (
    "native_igv_complete_and_readable",
    "locus_and_tracks_visible",
    "track_order_reviewed",
)
V3_SSQTL_MANUAL_ASSERTIONS = (
    "native_igv_complete_and_readable",
    "annotation_track_and_model_visible",
    "strand_and_transcript_reviewed",
    "ag_site_and_reference_ag_context_reviewed",
    "splice_or_junction_presence_absence_judgeable",
    "violin_pair_matches",
)

RENDER_STATES = frozenset({"PENDING", "SUCCEEDED", "FAILED"})
EVIDENCE_STATES = frozenset({"COMPLETE", "EVIDENCE_INCOMPLETE", "UNAVAILABLE"})
ARTIFACT_REVIEW_STATES = frozenset({"REVIEW_PENDING", "APPROVE", "REJECT"})
SCIENTIFIC_INTERPRETATIONS = frozenset(
    {"PENDING", "SUPPORTED", "NOT_SUPPORTED", "INDETERMINATE"}
)
PUBLICATION_STATES = frozenset({"NOT_READY", "READY", "PUBLISHED", "WITHHELD"})

SCHEMA_FILES = {
    "task": "task.schema.json",
    "task-v3": "task-v3.schema.json",
    "case-result-v3": "case-result-v3.schema.json",
    "terminal-bundle-v3": "terminal-bundle-v3.schema.json",
    "execution-policy-v3": "execution-policy-v3.schema.json",
    "stage-result": "stage-result.schema.json",
    "case-result": "case-result.schema.json",
    "review": "review.schema.json",
    "shard-ledger": "shard-ledger.schema.json",
    "run-provenance": "run-provenance.schema.json",
    "campaign-v3": "campaign-v3.schema.json",
    "pilot-selection-v3": "pilot-selection-v3.schema.json",
    "batch-request-v3": "batch-request-v3.schema.json",
    "campaign-ledger-event-v3": "campaign-ledger-event-v3.schema.json",
}


class ContractValidationError(ValueError):
    """Raised when a canonical document violates syntax or cross-field rules."""


def schema_directory(value: str | Path | None = None) -> Path:
    """Resolve the explicit schema directory or the source-tree default.

    Production Nextflow commands pass ``--schema-dir ${projectDir}/schema``.
    The fallback exists for source-tree tests and intentionally fails when the
    package is detached from its release schemas.
    """

    if value is not None:
        directory = Path(value).expanduser().resolve(strict=False)
        if not directory.is_dir():
            raise ContractValidationError(f"schema directory is unavailable: {directory}")
        return directory
    candidates = (
        Path(__file__).resolve().parents[2] / "schema",
        Path(sys.prefix) / "share" / "igv-snapshot-workflow" / "schema",
    )
    for directory in candidates:
        if directory.is_dir():
            return directory
    raise ContractValidationError(
        "schema directory is unavailable: " + ", ".join(str(path) for path in candidates)
    )


def load_schema(name: str, *, schema_dir: str | Path | None = None) -> dict[str, Any]:
    try:
        filename = SCHEMA_FILES[name]
    except KeyError as exc:
        raise ContractValidationError(f"unknown schema contract: {name}") from exc
    path = schema_directory(schema_dir) / filename
    if path.is_symlink() or not path.is_file():
        raise ContractValidationError(f"schema must be a regular non-symlink file: {path}")
    try:
        value = json.loads(
            read_regular_file_bytes(path, label=f"{name} schema").decode("utf-8")
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractValidationError(f"cannot load schema {name}: {exc}") from exc
    if not isinstance(value, dict):
        raise ContractValidationError(f"schema {name} must contain one JSON object")
    return value


def validate_schema_document(
    document: Mapping[str, Any],
    schema_name: str,
    *,
    schema_dir: str | Path | None = None,
) -> None:
    """Validate one document with Draft 2020-12 and stable diagnostics."""

    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:  # pragma: no cover - dependency packaging guard
        raise RuntimeError("jsonschema is required for schema-v2 validation") from exc

    from datetime import datetime

    format_checker = FormatChecker()

    @format_checker.checks("date-time", raises=(TypeError, ValueError))
    def _is_timezone_aware_iso_datetime(value: object) -> bool:
        if not isinstance(value, str):
            return True
        normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
        parsed = datetime.fromisoformat(normalized)
        return parsed.tzinfo is not None

    schema = load_schema(schema_name, schema_dir=schema_dir)
    Draft202012Validator.check_schema(schema)
    errors = sorted(
        Draft202012Validator(schema, format_checker=format_checker).iter_errors(dict(document)),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        details = []
        for error in errors[:10]:
            location = "/" + "/".join(str(part) for part in error.absolute_path)
            details.append(f"{location}: {error.message}")
        if len(errors) > 10:
            details.append(f"... {len(errors) - 10} additional validation errors")
        raise ContractValidationError(f"{schema_name} schema validation failed: " + "; ".join(details))


def _require_unique(values: Iterable[Any], label: str) -> None:
    seen: set[Any] = set()
    duplicates: list[Any] = []
    for value in values:
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    if duplicates:
        raise ContractValidationError(
            f"duplicate {label}: " + ", ".join(str(value) for value in duplicates[:10])
        )


def validate_task_document(
    task: Mapping[str, Any], *, schema_dir: str | Path | None = None
) -> None:
    validate_schema_document(task, "task", schema_dir=schema_dir)
    chrom = task["ag"]["chrom"]
    if task["snp"]["chrom"] != chrom:
        raise ContractValidationError("AG and SNP chromosomes must match")
    if int(task["ag"]["start"]) > int(task["ag"]["end"]):
        raise ContractValidationError("AG start must not exceed AG end")
    for region_name in ("overview", "detail"):
        region = task["regions"][region_name]
        if region["chrom"] != chrom:
            raise ContractValidationError(f"{region_name} chromosome must match AG chromosome")
        if int(region["start"]) > int(region["end"]):
            raise ContractValidationError(f"{region_name} start must not exceed end")

    tracks = list(task.get("tracks", []))
    _require_unique((track["sample_id"] for track in tracks), "track sample_id")
    stage_names = [
        name
        for track in tracks
        for name in (track["stage_bam"], track["stage_bai"])
    ]
    stage_names.extend(
        resource["stage_name"] for resource in task["reference"]["resources"].values()
    )
    if task["plot"]["state"] == "PRESENT":
        stage_names.append(task["plot"]["stage_pdf"])
    _require_unique(stage_names, "staged input name")

    group_counts = {
        genotype: int(task["genotype_groups"][genotype]["selected_count"])
        for genotype in ("0/0", "0/1", "1/1")
    }
    for genotype, count in group_counts.items():
        if bool(task["genotype_groups"][genotype]["empty"]) != (count == 0):
            raise ContractValidationError(
                f"genotype_groups[{genotype}].empty must equal selected_count == 0"
            )
    observed_counts = {genotype: 0 for genotype in group_counts}
    expected_dosage = {"0/0": 0, "0/1": 1, "1/1": 2}
    for track in tracks:
        observed_counts[track["genotype"]] += 1
        if int(track["dosage"]) != expected_dosage[track["genotype"]]:
            raise ContractValidationError(
                f"track dosage does not match genotype for {track['sample_id']}"
            )
    if group_counts != observed_counts:
        raise ContractValidationError(
            f"genotype selected_count does not match tracks: expected {group_counts}, "
            f"observed {observed_counts}"
        )

    state = task["preflight_state"]
    errors = list(task["preflight_errors"])
    if state == "READY" and errors:
        raise ContractValidationError("READY task cannot contain preflight_errors")
    if state == "CASE_INPUT_INVALID" and not errors:
        raise ContractValidationError("CASE_INPUT_INVALID task requires preflight_errors")


def _without_source_paths(value: Any) -> Any:
    """Remove host mount locations while retaining declared portable identities."""

    if isinstance(value, Mapping):
        return {
            key: _without_source_paths(item)
            for key, item in value.items()
            if key != "source_path"
        }
    if isinstance(value, list):
        return [_without_source_paths(item) for item in value]
    return value


def v3_task_fingerprint(task: Mapping[str, Any]) -> str:
    """Return the host-path-independent cache identity for one schema-v3 task."""

    payload = {key: value for key, value in task.items() if key != "input_fingerprint"}
    return sha256_json(_without_source_paths(payload))


def validate_v3_task_document(
    task: Mapping[str, Any], *, schema_dir: str | Path | None = None
) -> None:
    """Validate a portable core task and its adapter-specific extension."""

    validate_schema_document(task, "task-v3", schema_dir=schema_dir)
    core = task["core"]
    locus = core["locus"]
    if int(locus["start"]) > int(locus["end"]):
        raise ContractValidationError("core locus start must not exceed end")

    tracks = list(core["tracks"])
    observed_order = [int(track["track_order"]) for track in tracks]
    if observed_order != list(range(1, len(tracks) + 1)):
        raise ContractValidationError(
            "track_order must be contiguous and preserve manifest row order"
        )

    stage_names = [
        resource["stage_name"]
        for track in tracks
        for resource in (track["bam"], track["bai"])
    ]
    stage_names.extend(
        resource["stage_name"] for resource in core["reference"]["resources"].values()
    )
    auxiliary = core["auxiliary"]
    if auxiliary["state"] == "PRESENT":
        stage_names.append(auxiliary["stage_name"])
        if auxiliary["kind"] != "PDF" and auxiliary["page"] is not None:
            raise ContractValidationError("auxiliary page is only valid for PDF inputs")
    _require_unique(stage_names, "v3 staged input name")

    portable_resources = _without_source_paths(core["reference"]["resources"])
    if core["reference"]["resource_fingerprint"] != sha256_json(portable_resources):
        raise ContractValidationError("reference resource_fingerprint does not match resources")

    render_contract = dict(core["render_contract"])
    render_fingerprint = render_contract.pop("policy_fingerprint")
    if render_fingerprint != sha256_json(render_contract):
        raise ContractValidationError("render policy_fingerprint does not match render contract")

    adapter_id = task["adapter_id"]
    adapter_data = task["adapter_data"]
    if adapter_id == "generic":
        if adapter_data["scientific_interpretation"] != "NOT_APPLICABLE":
            raise ContractValidationError(
                "generic tasks require scientific_interpretation NOT_APPLICABLE"
            )
    else:
        if adapter_data["adapter_schema_version"] != "3.0-ssqtl":
            raise ContractValidationError("ssQTL task requires native adapter schema 3.0-ssqtl")
        ag = adapter_data["ag"]
        if (
            locus["contig"] != ag["chrom"]
            or int(locus["start"]) != int(ag["start"])
            or int(locus["end"]) != int(ag["end"])
            or core["strand"] != adapter_data["reference_context"]["strand"]
        ):
            raise ContractValidationError("ssQTL core locus differs from native AG identity")
        samples = list(adapter_data["selected_samples"])
        if [int(sample["track_order"]) for sample in samples] != list(
            range(1, len(samples) + 1)
        ) or len(samples) != len(tracks):
            raise ContractValidationError("ssQTL selected samples differ from core track order")
        counts = {genotype: 0 for genotype in ("0/0", "0/1", "1/1")}
        for sample, track in zip(samples, tracks):
            expected_public_label = f"Track {int(track['track_order']):03d}"
            if (
                track["track_label"] != expected_public_label
                or sample["genotype"] != track["group"]
            ):
                raise ContractValidationError(
                    "ssQTL private sample mapping differs from opaque public track"
                )
            counts[sample["genotype"]] += 1
        for genotype, count in counts.items():
            group = adapter_data["genotype_groups"][genotype]
            if int(group["selected_count"]) != count or bool(group["empty"]) != (count == 0):
                raise ContractValidationError("ssQTL genotype-group evidence differs from tracks")
        violin = adapter_data["violin"]
        auxiliary = core["auxiliary"]
        if violin["match_key"] != {
            "ag_site": adapter_data["ag"]["raw"],
            "snp": adapter_data["snp"]["raw"],
        }:
            raise ContractValidationError("ssQTL violin exact-match key differs from AG/SNP")
        if violin["state"] == "PRESENT":
            if (
                auxiliary["state"] != "PRESENT"
                or auxiliary["kind"] != "PDF"
                or violin["page"] != auxiliary["page"]
                or violin["pdf_sha256"] != auxiliary["identity"].get("sha256")
            ):
                raise ContractValidationError("ssQTL violin evidence differs from core auxiliary")
        elif auxiliary["state"] != "ABSENT":
            raise ContractValidationError("unavailable ssQTL violin requires absent auxiliary")
        evidence = adapter_data["preparation_evidence"]
        if evidence["association_sha256"] != adapter_data["association"]["input_sha256"]:
            raise ContractValidationError("ssQTL association identity differs from preparation evidence")

    expected_fingerprint = v3_task_fingerprint(task)
    if task["input_fingerprint"] != expected_fingerprint:
        raise ContractValidationError("v3 input_fingerprint does not match canonical task")


def validate_case_result_document(
    result: Mapping[str, Any], *, schema_dir: str | Path | None = None
) -> None:
    validate_schema_document(result, "case-result", schema_dir=schema_dir)
    render = result["render_state"]
    evidence = result["evidence_state"]
    artifact_review = result["artifact_review_state"]
    scientific = result["scientific_interpretation"]
    publication = result["publication_state"]

    if render != "SUCCEEDED" and evidence == "COMPLETE":
        raise ContractValidationError("COMPLETE evidence requires render_state SUCCEEDED")
    if evidence == "EVIDENCE_INCOMPLETE" and scientific not in {"PENDING", "INDETERMINATE"}:
        raise ContractValidationError(
            "EVIDENCE_INCOMPLETE permits only PENDING or INDETERMINATE interpretation"
        )
    if evidence == "UNAVAILABLE" and scientific not in {"PENDING", "INDETERMINATE"}:
        raise ContractValidationError(
            "UNAVAILABLE evidence permits only PENDING or INDETERMINATE interpretation"
        )
    if artifact_review == "REJECT" and publication not in {"NOT_READY", "WITHHELD"}:
        raise ContractValidationError("REJECT artifact review cannot be ready or published")
    if publication in {"READY", "PUBLISHED"}:
        if artifact_review != "APPROVE":
            raise ContractValidationError("READY/PUBLISHED requires artifact APPROVE")
        if scientific == "PENDING":
            raise ContractValidationError("READY/PUBLISHED requires final scientific interpretation")


def validate_v3_case_result_document(
    result: Mapping[str, Any], *, schema_dir: str | Path | None = None
) -> None:
    """Validate a portable terminal case result and its cross-field closure."""

    validate_schema_document(result, "case-result-v3", schema_dir=schema_dir)
    eligible = result["eligible"] is True
    failures = list(result["failures"])
    if eligible != (not failures and result["render_state"] == "SUCCEEDED" and not result["debug_only"]):
        raise ContractValidationError(
            "eligible must equal successful, non-debug execution without failures"
        )
    if result["adapter_type"] == "generic":
        expected_assertions = V3_GENERIC_MANUAL_ASSERTIONS
        if result["adapter_evidence"] != {
            "adapter_schema_version": "3.0-generic",
            "scientific_interpretation": "NOT_APPLICABLE",
        }:
            raise ContractValidationError("generic adapter evidence differs from its fixed contract")
        if result["scientific_interpretation"] != "NOT_APPLICABLE":
            raise ContractValidationError("generic case results require NOT_APPLICABLE")
        if result["render_state"] == "SUCCEEDED":
            pixel = result["pixel_identity"]
            if not isinstance(pixel, Mapping) or pixel.get("igv_pixel_identity") is not True:
                raise ContractValidationError(
                    "successful generic results require exact decoded IGV pixel identity"
                )
    else:
        expected_assertions = V3_SSQTL_MANUAL_ASSERTIONS
        adapter_evidence = result["adapter_evidence"]
        if adapter_evidence.get("adapter_schema_version") != "3.0-ssqtl":
            raise ContractValidationError("ssqtl adapter evidence has the wrong schema")
        if result["render_state"] == "SUCCEEDED" and not result["debug_only"]:
            required = {
                "scientific_evidence_available",
                "scientific_case_evidence_sha256",
                "scientific_qc_evidence_sha256",
                "scientific_evidence_state",
                "scientific_result_interpretation",
                "scientific_failure_set_sha256",
                "empty_genotype_groups",
            }
            if adapter_evidence.get("scientific_evidence_available") is not True or not required.issubset(
                adapter_evidence
            ):
                raise ContractValidationError(
                    "successful ssqtl result lacks checksum-bound scientific evidence"
                )
            if (
                adapter_evidence["scientific_evidence_state"] != result["evidence_state"]
                or adapter_evidence["scientific_result_interpretation"]
                != result["scientific_interpretation"]
            ):
                raise ContractValidationError(
                    "ssqtl state differs from native scientific evidence"
                )
            artifacts = result["artifacts"]
            if not {"scientific_case_evidence", "scientific_qc_evidence"}.issubset(artifacts):
                raise ContractValidationError(
                    "successful ssqtl result lacks scientific evidence artifacts"
                )
            if (
                artifacts["scientific_case_evidence"]["sha256"]
                != adapter_evidence["scientific_case_evidence_sha256"]
                or artifacts["scientific_qc_evidence"]["sha256"]
                != adapter_evidence["scientific_qc_evidence_sha256"]
            ):
                raise ContractValidationError(
                    "ssqtl scientific artifact hashes differ from adapter evidence"
                )
            pixel = result["pixel_identity"]
            if not isinstance(pixel, Mapping) or pixel.get("igv_pixel_identity") is not True:
                raise ContractValidationError(
                    "successful ssqtl results require exact decoded IGV pixel identity"
                )
        if (
            result["evidence_state"] == "EVIDENCE_INCOMPLETE"
            and result["scientific_interpretation"] != "INDETERMINATE"
        ):
            raise ContractValidationError(
                "incomplete ssqtl evidence requires INDETERMINATE interpretation"
            )
    if tuple(result["required_manual_assertions"]) != expected_assertions:
        raise ContractValidationError(
            f"{result['adapter_type']} case result manual assertions differ from the fixed contract"
        )


def validate_v3_terminal_bundle_document(
    bundle: Mapping[str, Any],
    case_result: Mapping[str, Any],
    *,
    schema_dir: str | Path | None = None,
) -> None:
    """Validate a terminal marker against the complete case-result hash closure."""

    validate_schema_document(bundle, "terminal-bundle-v3", schema_dir=schema_dir)
    for field in (
        "run_id",
        "generation_id",
        "task_id",
        "manifest_order",
        "input_fingerprint",
    ):
        if bundle[field] != case_result[field]:
            raise ContractValidationError(
                f"terminal bundle differs from case result identity: {field}"
            )
    resource_exhausted = bool(
        case_result.get("failures")
        and case_result["failures"][0].get("code") == "RESOURCE_EXHAUSTED"
    )
    expected_status = (
        "SUCCEEDED"
        if case_result["eligible"]
        else "RESOURCE_EXHAUSTED"
        if resource_exhausted
        else "DOMAIN_FAILED"
    )
    if bundle["status"] != expected_status:
        raise ContractValidationError("terminal bundle status differs from case eligibility")
    if bundle["artifact_set_sha256"] != sha256_json(case_result["artifacts"]):
        raise ContractValidationError("terminal bundle artifact-set checksum drift")


def validate_review_document(
    review: Mapping[str, Any], *, schema_dir: str | Path | None = None
) -> None:
    validate_schema_document(review, "review", schema_dir=schema_dir)
    if review["artifact_review_state"] == "APPROVE":
        assertions = review["manual_assertions"]
        failed = sorted(key for key, value in assertions.items() if value is not True)
        if failed:
            raise ContractValidationError(
                "APPROVE review requires every manual assertion: " + ", ".join(failed)
            )


def validate_unique_task_set(tasks: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Return tasks in immutable manifest order after exact-set validation."""

    rows = [dict(task) for task in tasks]
    _require_unique((row["task_id"] for row in rows), "task_id")
    _require_unique((row["manifest_order"] for row in rows), "manifest_order")
    ordered = sorted(rows, key=lambda row: int(row["manifest_order"]))
    expected = list(range(1, len(ordered) + 1))
    observed = [int(row["manifest_order"]) for row in ordered]
    if observed != expected:
        raise ContractValidationError(
            f"manifest_order must be contiguous 1..{len(ordered)}; observed {observed[:10]}"
        )
    return ordered
