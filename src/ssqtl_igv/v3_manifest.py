from __future__ import annotations

import copy
import csv
import io
import os
import re
import shutil
import subprocess
import uuid
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from .contracts import (
    V3_CANONICAL_SCHEMA_VERSION,
    V3_PIPELINE_VERSION,
    validate_task_document,
    validate_unique_task_set,
    validate_v3_task_document,
    v3_task_fingerprint,
)
from .identity import file_identity, task_set_fingerprint
from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_regular_file_bytes,
    resource_contains_remote_url,
    reject_symlink_path_components,
    safe_name,
    sha256_file,
    sha256_json,
    write_jsonl,
    write_tsv,
)


GENERIC_MANIFEST_FIELDS = (
    "schema_version",
    "case_id",
    "locus",
    "strand",
    "bam",
    "bai",
    "track_label",
    "group",
    "aux_path",
    "aux_page",
)
REFERENCE_RESOURCE_ROLES = (
    "definition",
    "fasta",
    "fai",
    "cytoband",
    "annotation",
)
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
_LOCUS = re.compile(
    r"^(?P<contig>[A-Za-z0-9][A-Za-z0-9_.-]*):(?P<start>[1-9][0-9]*)-(?P<end>[1-9][0-9]*)$"
)
_URI_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_GLOB_CHARACTERS = frozenset("*?[]{}")

DEFAULT_RENDER_POLICY: dict[str, Any] = {
    "screen_width": 1920,
    "screen_height": 2160,
    "screen_depth": 24,
    "igv_version": "2.16.2",
    "igv_java_version": "11",
    "igv_heap_gb": 6,
    "locale": "C.UTF-8",
    "font_contract_id": "igv-v3-bundled-fonts-v1",
    "command_listener_enabled": False,
    "gui_settle_contract_id": "igv-v3-stable-frame-v1",
    "ocr_contract_id": "igv-v3-locus-ocr-v1",
    "pixel_contract_id": "igv-v3-decoded-rgb-sha256-v1",
}

_CASES_TEMPLATE = "\t".join(GENERIC_MANIFEST_FIELDS) + "\n"
_GENERIC_PROJECT_TEMPLATE = """\
schema_version: "3.0"
adapter: generic
inputs:
  cases: cases.tsv
reference: reference.yaml
"""
_SSQTL_PROJECT_TEMPLATE = """\
schema_version: "3.0"
adapter: ssqtl
inputs:
  associations: associations.csv
  rds_dir: rds
  bam_lookup: bam_lookup.csv
  violin_dir: violin
  config: ssqtl.yaml
reference: reference.yaml
"""
_SSQTL_CONFIG_TEMPLATE = """\
schema_version: 3.0-ssqtl
"""
_REFERENCE_TEMPLATE = """\
schema_version: "3.0"
id: hg38-local
display_name: Human GRCh38 local bundle
version: GRCh38
resources:
  definition:
    path: genome.json
    sha256: null
  fasta:
    path: genome.fa
    sha256: null
  fai:
    path: genome.fa.fai
    sha256: null
  cytoband:
    path: cytoband.txt.gz
    sha256: null
  annotation:
    path: annotation.gff.gz
    sha256: null
"""


def _require_safe_id(value: str, label: str, *, max_length: int = 80) -> str:
    normalized = str(value).strip()
    if not _SAFE_ID.fullmatch(normalized) or len(normalized) > max_length:
        raise ValueError(f"{label} must be a portable identifier: {value!r}")
    return normalized


def _relative_path(value: str, *, label: str) -> PurePosixPath:
    declared = str(value).strip()
    if not declared:
        raise ValueError(f"{label} path is empty")
    if "\\" in declared:
        raise ValueError(f"{label} path must use POSIX separators: {declared!r}")
    if any(ord(character) < 32 for character in declared) or any(
        character in declared for character in _GLOB_CHARACTERS
    ):
        raise ValueError(f"{label} path contains a forbidden glob or control character")
    if _URI_SCHEME.match(declared) or "://" in declared:
        raise ValueError(f"{label} path must be local, not a URI: {declared!r}")
    path = PurePosixPath(declared)
    parts = declared.split("/")
    if path.is_absolute() or any(part in {"", ".", ".."} for part in parts):
        raise ValueError(f"{label} path must be a normalized relative path: {declared!r}")
    return path


def _resolve_relative_file(root: Path, value: str, *, label: str) -> tuple[str, Path]:
    relative = _relative_path(value, label=label)
    candidate = root.joinpath(*relative.parts)
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} path escapes its mounted root: {relative}") from exc
    if not resolved.is_file():
        raise ValueError(f"{label} target is not a regular file: {relative}")
    return relative.as_posix(), resolved


def _portable_stage_name(relative_path: str, *, role: str, discriminator: str) -> str:
    name = PurePosixPath(relative_path).name
    suffix = "".join(PurePosixPath(name).suffixes)
    stem = name[: -len(suffix)] if suffix else name
    digest = sha256_json(
        {"declared_path": relative_path, "role": role, "discriminator": discriminator}
    )[:12]
    safe_suffix = "".join(
        character for character in suffix if character.isalnum() or character in "._-"
    )[:40]
    return f"{safe_name(role)[:24]}_{safe_name(stem)[:110]}_{digest}{safe_suffix}"


def _file_resource(
    root: Path,
    declared_path: str,
    *,
    role: str,
    discriminator: str,
    expected_sha256: str | None = None,
    hash_content: bool = False,
) -> dict[str, Any]:
    declared, source = _resolve_relative_file(root, declared_path, label=role)
    configured = str(expected_sha256 or "").strip().lower()
    if configured and not _SHA256.fullmatch(configured):
        raise ValueError(f"{role} SHA-256 is malformed")
    observed = sha256_file(source) if configured or hash_content else None
    if configured and observed != configured:
        raise ValueError(
            f"{role} SHA-256 mismatch: expected {configured}, observed {observed}"
        )
    return {
        "declared_path": declared,
        "source_path": str(source),
        "stage_name": _portable_stage_name(
            declared, role=role, discriminator=discriminator
        ),
        "identity": file_identity(source, sha256=observed),
    }


def _parse_locus(value: str, *, line_number: int) -> dict[str, Any]:
    raw = str(value).strip()
    match = _LOCUS.fullmatch(raw)
    if not match:
        raise ValueError(
            f"cases.tsv line {line_number}: locus must be contig:start-end using 1-based coordinates"
        )
    start = int(match.group("start"))
    end = int(match.group("end"))
    if start > end:
        raise ValueError(f"cases.tsv line {line_number}: locus start exceeds end")
    return {
        "raw": raw,
        "contig": match.group("contig"),
        "start": start,
        "end": end,
        "coordinate_system": "1-based-inclusive",
    }


def _infer_bai(input_root: Path, bam_path: str, *, line_number: int) -> str:
    bam = PurePosixPath(bam_path)
    candidates = [f"{bam.as_posix()}.bai", bam.with_suffix(".bai").as_posix()]
    candidates = list(dict.fromkeys(candidates))
    existing: list[str] = []
    for candidate in candidates:
        try:
            _resolve_relative_file(
                input_root, candidate, label=f"cases.tsv line {line_number} inferred BAI"
            )
        except FileNotFoundError:
            continue
        existing.append(candidate)
    if len(existing) != 1:
        detail = "none exist" if not existing else "multiple candidates exist"
        raise ValueError(
            f"cases.tsv line {line_number}: cannot infer BAI unambiguously ({detail}); "
            "set the bai column explicitly"
        )
    return existing[0]


def _pdf_page_count(path: Path) -> int | None:
    try:
        completed = subprocess.run(
            ["pdfinfo", str(path)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    match = re.search(r"^Pages:\s*([1-9][0-9]*)\s*$", completed.stdout, re.MULTILINE)
    return int(match.group(1)) if match else None


def _parse_auxiliary(
    input_root: Path,
    specs: list[tuple[str, str, int]],
    *,
    case_id: str,
) -> dict[str, Any]:
    if any(not path and page for path, page, _line in specs):
        raise ValueError(f"case {case_id}: aux_page requires aux_path")
    populated = {(path, page) for path, page, _line in specs if path}
    if not populated:
        return {"state": "ABSENT"}
    if len(populated) != 1:
        raise ValueError(f"case {case_id}: auxiliary path/page differs between track rows")
    declared_path, page_text = next(iter(populated))
    source_declared, source = _resolve_relative_file(
        input_root, declared_path, label=f"case {case_id} auxiliary"
    )
    suffix = source.suffix.lower()
    if suffix == ".png":
        kind = "PNG"
    elif suffix in {".jpg", ".jpeg"}:
        kind = "JPEG"
    elif suffix == ".pdf":
        kind = "PDF"
    else:
        raise ValueError(f"case {case_id}: auxiliary must be PNG, JPEG, or PDF")

    page: int | None = None
    if page_text:
        try:
            page = int(page_text)
        except ValueError as exc:
            raise ValueError(f"case {case_id}: aux_page must be a positive integer") from exc
        if page < 1 or str(page) != page_text:
            raise ValueError(f"case {case_id}: aux_page must be a positive integer")
    if kind != "PDF" and page is not None:
        raise ValueError(f"case {case_id}: aux_page is only valid for PDF inputs")
    if kind == "PDF":
        page_count = _pdf_page_count(source)
        if page_count is None and page is None:
            raise ValueError(
                f"case {case_id}: PDF page count is unavailable; set aux_page explicitly"
            )
        if page_count is not None:
            if page_count > 1 and page is None:
                raise ValueError(f"case {case_id}: multi-page PDF requires aux_page")
            if page is not None and page > page_count:
                raise ValueError(
                    f"case {case_id}: aux_page {page} exceeds PDF page count {page_count}"
                )
            if page is None:
                page = 1

    identity = file_identity(source, sha256=sha256_file(source))
    return {
        "state": "PRESENT",
        "kind": kind,
        "declared_path": source_declared,
        "source_path": str(source),
        "stage_name": _portable_stage_name(
            source_declared, role="auxiliary", discriminator=case_id
        ),
        "page": page,
        "identity": identity,
    }


def _render_contract(*, gui_settle_contract_id: str | None = None) -> dict[str, Any]:
    policy = copy.deepcopy(DEFAULT_RENDER_POLICY)
    if gui_settle_contract_id:
        policy["gui_settle_contract_id"] = str(gui_settle_contract_id)
    policy["policy_fingerprint"] = sha256_json(policy)
    return policy


def _load_reference(reference_path: str | Path) -> tuple[dict[str, Any], Path]:
    path = Path(reference_path).expanduser()
    try:
        payload = read_regular_file_bytes(path, label="reference.yaml")
        value = yaml.safe_load(payload.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot parse reference.yaml: {exc}") from exc
    if not isinstance(value, Mapping):
        raise ValueError("reference.yaml must contain one mapping")
    allowed = {"schema_version", "id", "display_name", "version", "resources"}
    if set(value) != allowed:
        raise ValueError(
            "reference.yaml fields must be exactly: " + ", ".join(sorted(allowed))
        )
    if str(value["schema_version"]) != V3_CANONICAL_SCHEMA_VERSION:
        raise ValueError("reference.yaml schema_version must be 3.0")
    path = path.resolve(strict=True)
    reference_root = path.parent
    resources_value = value["resources"]
    if not isinstance(resources_value, Mapping) or set(resources_value) != set(
        REFERENCE_RESOURCE_ROLES
    ):
        raise ValueError(
            "reference.yaml resources must define exactly: "
            + ", ".join(REFERENCE_RESOURCE_ROLES)
        )

    resources: dict[str, Any] = {}
    for role in REFERENCE_RESOURCE_ROLES:
        configured = resources_value[role]
        if not isinstance(configured, Mapping) or not set(configured).issubset(
            {"path", "sha256"}
        ) or "path" not in configured:
            raise ValueError(f"reference resource {role} requires path and optional sha256")
        resources[role] = _file_resource(
            reference_root,
            str(configured["path"]),
            role=f"reference_{role}",
            discriminator=str(value["id"]),
            expected_sha256=configured.get("sha256"),
            hash_content=True,
        )
    if resource_contains_remote_url(resources["definition"]["source_path"]):
        raise ValueError("reference genome definition contains a remote URL")

    for key in ("id", "display_name", "version"):
        if not str(value[key]).strip():
            raise ValueError(f"reference.yaml {key} must not be empty")
    portable_resources = {
        role: {key: item for key, item in resource.items() if key != "source_path"}
        for role, resource in resources.items()
    }
    reference = {
        "id": str(value["id"]).strip(),
        "display_name": str(value["display_name"]).strip(),
        "version": str(value["version"]).strip(),
        "resource_fingerprint": sha256_json(portable_resources),
        "resources": resources,
    }
    return reference, path


def _read_manifest(path: str | Path) -> tuple[list[tuple[int, dict[str, str]]], Path]:
    source = Path(path).expanduser()
    try:
        text = read_regular_file_bytes(source, label="generic cases.tsv").decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("cases.tsv must be UTF-8") from exc
    source = source.resolve(strict=True)
    reader = csv.DictReader(io.StringIO(text, newline=""), delimiter="\t")
    if tuple(reader.fieldnames or ()) != GENERIC_MANIFEST_FIELDS:
        raise ValueError(
            "cases.tsv header must exactly match: " + "\t".join(GENERIC_MANIFEST_FIELDS)
        )
    rows: list[tuple[int, dict[str, str]]] = []
    for row in reader:
        line_number = reader.line_num
        if None in row:
            raise ValueError(f"cases.tsv line {line_number}: unexpected extra column")
        normalized = {field: str(row.get(field) or "").strip() for field in GENERIC_MANIFEST_FIELDS}
        if not any(normalized.values()):
            continue
        rows.append((line_number, normalized))
    if not rows:
        raise ValueError("cases.tsv contains no track rows")
    return rows, source


def init_templates(output_dir: str | Path, *, adapter: str = "generic") -> dict[str, Any]:
    """Create an atomic pull-and-run project starter directory."""

    if adapter not in {"generic", "ssqtl"}:
        raise ValueError("adapter must be generic or ssqtl")

    destination = reject_symlink_path_components(
        output_dir, label="template output"
    ).resolve(strict=False)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"template output already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        atomic_write_text(
            staging / "project.yaml",
            _GENERIC_PROJECT_TEMPLATE if adapter == "generic" else _SSQTL_PROJECT_TEMPLATE,
        )
        atomic_write_text(staging / "reference.yaml", _REFERENCE_TEMPLATE)
        if adapter == "generic":
            atomic_write_text(staging / "cases.tsv", _CASES_TEMPLATE)
        else:
            atomic_write_text(staging / "associations.csv", "AG_site,SNP,strand\n")
            atomic_write_text(staging / "bam_lookup.csv", "sample_id,bam\n")
            atomic_write_text(staging / "ssqtl.yaml", _SSQTL_CONFIG_TEMPLATE)
            (staging / "rds").mkdir()
            (staging / "violin").mkdir()
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        "status": "INITIALIZED",
        "schema_version": V3_CANONICAL_SCHEMA_VERSION,
        "adapter": adapter,
        "output_dir": str(destination),
        "project": str(destination / "project.yaml"),
        "manifest": str(destination / "cases.tsv") if adapter == "generic" else None,
        "reference": str(destination / "reference.yaml"),
    }


def normalize_generic_manifest(
    manifest_path: str | Path,
    input_root: str | Path,
    reference_path: str | Path,
    output_dir: str | Path,
    run_id: str,
    generation_id: str,
) -> dict[str, Any]:
    """Validate generic inputs and atomically emit canonical schema-v3 tasks."""

    run_id = _require_safe_id(run_id, "run_id")
    generation_id = _require_safe_id(generation_id, "generation_id")
    root = Path(input_root).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"input_root is not a directory: {root}")
    manifest_rows, manifest_source = _read_manifest(manifest_path)
    reference, reference_source = _load_reference(reference_path)

    cases: dict[str, dict[str, Any]] = {}
    for line_number, row in manifest_rows:
        if row["schema_version"] != V3_CANONICAL_SCHEMA_VERSION:
            raise ValueError(f"cases.tsv line {line_number}: schema_version must be 3.0")
        case_id = _require_safe_id(row["case_id"], f"cases.tsv line {line_number} case_id", max_length=256)
        locus = _parse_locus(row["locus"], line_number=line_number)
        strand = row["strand"]
        if strand not in {"+", "-"}:
            raise ValueError(f"cases.tsv line {line_number}: strand must be + or -")
        if not row["track_label"]:
            raise ValueError(f"cases.tsv line {line_number}: track_label must not be empty")

        bam_declared, _bam_source = _resolve_relative_file(
            root, row["bam"], label=f"cases.tsv line {line_number} BAM"
        )
        if PurePosixPath(bam_declared).suffix.lower() != ".bam":
            raise ValueError(f"cases.tsv line {line_number}: only BAM inputs are supported")
        bai_value = row["bai"] or _infer_bai(root, bam_declared, line_number=line_number)
        if PurePosixPath(bai_value).suffix.lower() != ".bai":
            raise ValueError(f"cases.tsv line {line_number}: BAI path must end in .bai")

        case = cases.setdefault(
            case_id,
            {
                "manifest_order": len(cases) + 1,
                "locus": locus,
                "strand": strand,
                "tracks": [],
                "aux_specs": [],
            },
        )
        if case["locus"] != locus or case["strand"] != strand:
            raise ValueError(f"case {case_id}: locus/strand differs between track rows")
        track_order = len(case["tracks"]) + 1
        track_key = f"{case_id}:{track_order}"
        case["tracks"].append(
            {
                "track_order": track_order,
                "track_label": row["track_label"],
                "group": row["group"] or None,
                "bam": _file_resource(
                    root,
                    bam_declared,
                    role="bam",
                    discriminator=track_key,
                    hash_content=True,
                ),
                "bai": _file_resource(
                    root,
                    bai_value,
                    role="bai",
                    discriminator=track_key,
                    hash_content=True,
                ),
            }
        )
        case["aux_specs"].append((row["aux_path"], row["aux_page"], line_number))

    render_contract = _render_contract()
    tasks: list[dict[str, Any]] = []
    for case_id, case in cases.items():
        task: dict[str, Any] = {
            "schema_version": V3_CANONICAL_SCHEMA_VERSION,
            "pipeline_version": V3_PIPELINE_VERSION,
            "run_id": run_id,
            "generation_id": generation_id,
            "task_id": case_id,
            "manifest_order": case["manifest_order"],
            "adapter_id": "generic",
            "core": {
                "locus": case["locus"],
                "strand": case["strand"],
                "tracks": case["tracks"],
                "auxiliary": _parse_auxiliary(
                    root, case["aux_specs"], case_id=case_id
                ),
                "reference": copy.deepcopy(reference),
                "render_contract": copy.deepcopy(render_contract),
                "preflight": {"state": "READY", "warnings": [], "errors": []},
            },
            "adapter_data": {
                "adapter_schema_version": "3.0-generic",
                "scientific_interpretation": "NOT_APPLICABLE",
            },
            "estimated_runtime_seconds": 90.0,
        }
        task["input_fingerprint"] = v3_task_fingerprint(task)
        validate_v3_task_document(task)
        tasks.append(task)
    tasks = validate_unique_task_set(tasks)
    task_set_sha = task_set_fingerprint(tasks)

    destination = reject_symlink_path_components(
        output_dir, label="normalization output"
    ).resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"normalization output already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    try:
        tasks_path = staging / "tasks.jsonl"
        write_jsonl(tasks_path, tasks)
        write_tsv(
            staging / "normalized_manifest.tsv",
            [
                "manifest_order",
                "task_id",
                "locus",
                "strand",
                "track_count",
                "auxiliary_state",
                "adapter_id",
                "input_fingerprint",
            ],
            (
                {
                    "manifest_order": task["manifest_order"],
                    "task_id": task["task_id"],
                    "locus": task["core"]["locus"]["raw"],
                    "strand": task["core"]["strand"],
                    "track_count": len(task["core"]["tracks"]),
                    "auxiliary_state": task["core"]["auxiliary"]["state"],
                    "adapter_id": task["adapter_id"],
                    "input_fingerprint": task["input_fingerprint"],
                }
                for task in tasks
            ),
        )
        atomic_write_json(staging / "reference.json", reference)
        parameters = {
            "schema_version": V3_CANONICAL_SCHEMA_VERSION,
            "pipeline_version": V3_PIPELINE_VERSION,
            "run_id": run_id,
            "generation_id": generation_id,
            "adapter_id": "generic",
            "source_manifest": str(manifest_source),
            "source_manifest_sha256": sha256_file(manifest_source),
            "input_root": str(root),
            "source_reference": str(reference_source),
            "source_reference_sha256": sha256_file(reference_source),
        }
        atomic_write_json(staging / "parameters.json", parameters)
        validation = {
            "schema_version": V3_CANONICAL_SCHEMA_VERSION,
            "pipeline_version": V3_PIPELINE_VERSION,
            "status": "PASS",
            "adapter_id": "generic",
            "run_id": run_id,
            "generation_id": generation_id,
            "task_count": len(tasks),
            "task_set_sha256": task_set_sha,
            "tasks_sha256": sha256_file(tasks_path),
            "reference_resource_fingerprint": reference["resource_fingerprint"],
            "source_manifest_sha256": parameters["source_manifest_sha256"],
            "source_reference_sha256": parameters["source_reference_sha256"],
        }
        atomic_write_json(staging / "validation.json", validation)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **validation,
        "output_dir": str(destination),
        "tasks": str(destination / "tasks.jsonl"),
        "normalized_manifest": str(destination / "normalized_manifest.tsv"),
        "validation": str(destination / "validation.json"),
        "parameters": str(destination / "parameters.json"),
        "reference": str(destination / "reference.json"),
    }


def _rehash_v2_resource(
    source_value: str | Path,
    legacy_identity: Mapping[str, Any],
    *,
    label: str,
) -> tuple[str, dict[str, Any]]:
    source = Path(source_value).expanduser().resolve(strict=True)
    if not source.is_file():
        raise ValueError(f"{label} is not a regular file: {source}")
    stat = source.stat()
    if "size" in legacy_identity and int(legacy_identity["size"]) != stat.st_size:
        raise ValueError(f"{label} size differs from the frozen v2 task")
    if "mtime_ns" in legacy_identity and int(legacy_identity["mtime_ns"]) != stat.st_mtime_ns:
        raise ValueError(f"{label} mtime differs from the frozen v2 task")
    observed_sha = sha256_file(source)
    expected_sha = str(legacy_identity.get("sha256") or "").lower()
    if expected_sha and expected_sha != observed_sha:
        raise ValueError(f"{label} content differs from the frozen v2 task")
    return str(source), file_identity(source, sha256=observed_sha)


def _portable_ssqtl_resource(
    input_root: Path,
    source_value: str | Path,
    legacy_identity: Mapping[str, Any],
    *,
    role: str,
    discriminator: str,
) -> dict[str, Any]:
    """Rebind a selected ssQTL input to its portable /input-relative identity."""

    source, identity = _rehash_v2_resource(
        source_value,
        legacy_identity,
        label=role,
    )
    resolved = Path(source)
    try:
        declared = resolved.relative_to(input_root).as_posix()
    except ValueError as exc:
        raise ValueError(f"{role} escapes the declared ssQTL input root") from exc
    _relative_path(declared, label=role)
    return {
        "declared_path": declared,
        "source_path": source,
        "stage_name": _portable_stage_name(
            declared, role=role, discriminator=discriminator
        ),
        "identity": identity,
    }


def native_ssqtl_task_from_prepared(
    prepared_task: Mapping[str, Any],
    *,
    input_root: str | Path,
    reference: Mapping[str, Any],
    preparation_evidence: Mapping[str, Any],
    scientific_interpretation: str = "PENDING",
) -> dict[str, Any]:
    """Create one native v3 ssQTL task from private preparation-library output.

    ``prepared_task`` is an in-process compatibility representation emitted by
    the established R/normalization science library.  It is never accepted by
    a public command and is not embedded in the v3 document.
    """

    prepared = copy.deepcopy(dict(prepared_task))
    validate_task_document(prepared)
    root = Path(input_root).expanduser().resolve(strict=True)
    if not root.is_dir():
        raise ValueError(f"ssQTL input root is not a directory: {root}")
    if scientific_interpretation not in {
        "PENDING",
        "SUPPORTED",
        "NOT_SUPPORTED",
        "INDETERMINATE",
    }:
        raise ValueError(f"invalid ssQTL scientific interpretation: {scientific_interpretation}")

    tracks = []
    selected_samples: list[dict[str, Any]] = []
    for order, track in enumerate(prepared["tracks"], 1):
        bam = _portable_ssqtl_resource(
            root,
            str(track["bam"]),
            track["bam_identity"],
            role="ssqtl_bam",
            discriminator=f"{prepared['task_id']}:{order}",
        )
        bai = _portable_ssqtl_resource(
            root,
            str(track["bai"]),
            track["bai_identity"],
            role="ssqtl_bai",
            discriminator=f"{prepared['task_id']}:{order}",
        )
        tracks.append(
            {
                "track_order": order,
                # Public IGV pixels must never burn a subject/sample identifier
                # into the screenshot.  The private sample-to-track mapping is
                # retained below in adapter_data.selected_samples.
                "track_label": f"Track {order:03d}",
                "group": str(track["genotype"]),
                "bam": bam,
                "bai": bai,
            }
        )
        selected_samples.append(
            {
                "track_order": order,
                "sample_id": str(track["sample_id"]),
                "genotype": str(track["genotype"]),
                "dosage": int(track["dosage"]),
                "ratio": float(track["ratio"]),
                "selection_label": str(track["selection_label"]),
                "bai_fresh": track.get("bai_fresh"),
            }
        )

    prepared_plot = prepared["plot"]
    if prepared_plot["state"] == "PRESENT":
        pdf = _portable_ssqtl_resource(
            root,
            str(prepared_plot["pdf"]),
            prepared_plot["pdf_identity"],
            role="ssqtl_violin",
            discriminator=str(prepared["task_id"]),
        )
        auxiliary = {
            "state": "PRESENT",
            "kind": "PDF",
            **pdf,
            "page": int(prepared_plot["page"]),
        }
        violin = {
            "state": "PRESENT",
            "match_key": copy.deepcopy(prepared_plot["match_key"]),
            "page": int(prepared_plot["page"]),
            "pdf_sha256": str(pdf["identity"]["sha256"]),
        }
    else:
        auxiliary = {"state": "ABSENT"}
        violin = {
            "state": "UNAVAILABLE",
            "match_key": copy.deepcopy(prepared_plot["match_key"]),
            "page": None,
            "pdf_sha256": None,
        }

    native_reference = copy.deepcopy(dict(reference))
    prepared_resources = prepared["reference"]["resources"]
    for role, resource in native_reference["resources"].items():
        prepared_source = Path(prepared_resources[role]["source_path"]).resolve(strict=True)
        native_source = Path(resource["source_path"]).resolve(strict=True)
        if prepared_source != native_source:
            raise ValueError(f"ssQTL prepared reference differs from reference.yaml: {role}")
        expected_sha = str(resource["identity"].get("sha256") or "")
        if not expected_sha or sha256_file(prepared_source) != expected_sha:
            raise ValueError(f"ssQTL prepared reference identity differs: {role}")

    ag = prepared["ag"]
    task: dict[str, Any] = {
        "schema_version": V3_CANONICAL_SCHEMA_VERSION,
        "pipeline_version": V3_PIPELINE_VERSION,
        "run_id": prepared["run_id"],
        "generation_id": prepared["generation_id"],
        "task_id": prepared["task_id"],
        "manifest_order": prepared["manifest_order"],
        "adapter_id": "ssqtl",
        "core": {
            "locus": {
                "raw": str(ag["raw"]),
                "contig": str(ag["chrom"]),
                "start": int(ag["start"]),
                "end": int(ag["end"]),
                "coordinate_system": "1-based-inclusive",
            },
            "strand": prepared["strand"],
            "tracks": tracks,
            "auxiliary": auxiliary,
            "reference": native_reference,
            "render_contract": _render_contract(
                gui_settle_contract_id=prepared["gui_settle_contract_id"]
            ),
            "preflight": {
                "state": prepared["preflight_state"],
                "warnings": copy.deepcopy(prepared["preflight_warnings"]),
                "errors": copy.deepcopy(prepared["preflight_errors"]),
            },
        },
        "adapter_data": {
            "adapter_schema_version": "3.0-ssqtl",
            "scientific_interpretation": scientific_interpretation,
            "association": copy.deepcopy(prepared["association"]),
            "figure_contract_id": str(prepared["figure_contract_id"]),
            "gui_settle_contract_id": str(prepared["gui_settle_contract_id"]),
            "ag": copy.deepcopy(prepared["ag"]),
            "snp": copy.deepcopy(prepared["snp"]),
            "regions": copy.deepcopy(prepared["regions"]),
            "statistics": copy.deepcopy(prepared["statistics"]),
            "genotype_groups": copy.deepcopy(prepared["genotype_groups"]),
            "selected_samples": selected_samples,
            "violin": violin,
            "reference_context": copy.deepcopy(prepared["reference_context"]),
            "scientific_render_contract": copy.deepcopy(prepared["render_contract"]),
            "preparation_evidence": copy.deepcopy(dict(preparation_evidence)),
            "shard_hint": str(prepared["shard_hint"]),
        },
        "estimated_runtime_seconds": float(prepared["estimated_runtime_seconds"]),
    }
    task["input_fingerprint"] = v3_task_fingerprint(task)
    validate_v3_task_document(task)
    return task
