from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from .utils import read_regular_file_bytes, sha256_file, sha256_json
from .v3_manifest import _relative_path

PROJECT_SCHEMA_VERSION = "3.0"
PROJECT_ADAPTERS = ("generic", "ssqtl")

_TOP_LEVEL_FIELDS = {"schema_version", "adapter", "inputs", "reference"}
_INPUT_FIELDS = {
    "generic": {"cases"},
    "ssqtl": {"associations", "rds_dir", "bam_lookup", "violin_dir", "config"},
}
_DIRECTORY_INPUTS = {"rds_dir", "violin_dir"}


def _resolve_project_resource(
    root: Path,
    value: object,
    *,
    label: str,
    directory: bool = False,
) -> dict[str, Any]:
    if not isinstance(value, str):
        raise TypeError(f"{label} must be a relative path string")
    relative = _relative_path(value, label=label)
    candidate = root.joinpath(*relative.parts)
    try:
        resolved = candidate.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ValueError(f"{label} does not exist: {relative.as_posix()}") from exc
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} path escapes the project directory") from exc
    if directory:
        if not resolved.is_dir():
            raise ValueError(f"{label} must be a directory: {relative.as_posix()}")
    elif not resolved.is_file():
        raise ValueError(f"{label} must be a regular file: {relative.as_posix()}")
    return {
        "declared_path": relative.as_posix(),
        "source_path": str(resolved),
    }


def load_project_config(project_path: str | Path = "/project/project.yaml") -> dict[str, Any]:
    """Load the pull-and-run project entrypoint and bind every local input.

    Project paths are interpreted relative to ``project.yaml``.  Absolute paths,
    URIs, globs, backslashes, ``..`` and symlink escapes are rejected before any
    normalization or rendering work starts.
    """

    declared = Path(project_path).expanduser()
    payload = read_regular_file_bytes(declared, label="project.yaml")
    try:
        value = yaml.safe_load(payload.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot parse project.yaml: {exc}") from exc
    if not isinstance(value, Mapping):
        raise TypeError("project.yaml must contain one mapping")
    if set(value) != _TOP_LEVEL_FIELDS:
        raise ValueError(
            "project.yaml fields must be exactly: "
            + ", ".join(sorted(_TOP_LEVEL_FIELDS))
        )
    if value.get("schema_version") != PROJECT_SCHEMA_VERSION:
        raise ValueError('project.yaml schema_version must be the string "3.0"')
    adapter = value.get("adapter")
    if adapter not in PROJECT_ADAPTERS:
        raise ValueError("project.yaml adapter must be generic or ssqtl")
    inputs = value.get("inputs")
    if not isinstance(inputs, Mapping):
        raise TypeError("project.yaml inputs must contain one mapping")
    required = _INPUT_FIELDS[str(adapter)] - ({"config"} if adapter == "ssqtl" else set())
    allowed = _INPUT_FIELDS[str(adapter)]
    missing = required - set(inputs)
    unknown = set(inputs) - allowed
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append("missing=" + ",".join(sorted(missing)))
        if unknown:
            details.append("unexpected=" + ",".join(sorted(unknown)))
        raise ValueError(
            f"project.yaml {adapter} inputs are invalid ({'; '.join(details)})"
        )

    source = declared.resolve(strict=True)
    root = source.parent.resolve(strict=True)
    resolved_inputs: dict[str, Any] = {}
    for name in sorted(inputs):
        configured = inputs[name]
        resolved_inputs[name] = _resolve_project_resource(
            root,
            configured,
            label=f"project.yaml inputs.{name}",
            directory=name in _DIRECTORY_INPUTS,
        )
    reference = _resolve_project_resource(
        root,
        value.get("reference"),
        label="project.yaml reference",
    )
    return {
        "schema_version": PROJECT_SCHEMA_VERSION,
        "adapter": adapter,
        "project_path": str(source),
        "project_root": str(root),
        "project_sha256": sha256_file(source),
        "inputs": resolved_inputs,
        "reference": reference,
    }


def _project_file_binding(
    root: Path,
    source_value: str | Path,
    *,
    declared_path: str,
    label: str,
) -> dict[str, Any]:
    source = Path(source_value).resolve(strict=True)
    try:
        source.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} path escapes the project directory") from exc
    if not source.is_file():
        raise ValueError(f"{label} must be a regular file")
    return {
        "declared_path": declared_path,
        "size": source.stat().st_size,
        "sha256": sha256_file(source),
    }


def _project_directory_binding(
    root: Path,
    source_value: str | Path,
    *,
    declared_path: str,
    label: str,
) -> dict[str, Any]:
    """Inventory project-directory inputs without rereading large data files.

    Selected RDS/PDF inputs are content-hashed once during canonical
    normalization.  This pre-normalization binding only needs the same stable
    path/size/mtime metadata used by standard Nextflow file caching.
    """

    source = Path(source_value).resolve(strict=True)
    try:
        source.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{label} path escapes the project directory") from exc
    if not source.is_dir():
        raise ValueError(f"{label} must be a directory")

    files: list[dict[str, Any]] = []

    def visit(directory: Path, logical: Path, ancestors: frozenset[Path]) -> None:
        resolved_directory = directory.resolve(strict=True)
        if resolved_directory in ancestors:
            raise ValueError(f"{label} contains a symlink directory cycle")
        try:
            resolved_directory.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"{label} contains a symlink escape") from exc
        next_ancestors = ancestors | {resolved_directory}
        for child in sorted(directory.iterdir(), key=lambda item: item.name):
            logical_child = logical / child.name
            try:
                resolved_child = child.resolve(strict=True)
            except (OSError, RuntimeError) as exc:
                raise ValueError(
                    f"{label} contains an unavailable path: {logical_child.as_posix()}"
                ) from exc
            try:
                resolved_child.relative_to(root)
            except ValueError as exc:
                raise ValueError(
                    f"{label} contains a symlink escape: {logical_child.as_posix()}"
                ) from exc
            if resolved_child.is_dir():
                visit(child, logical_child, next_ancestors)
            elif resolved_child.is_file():
                stat = resolved_child.stat()
                files.append(
                    {
                        "path": logical_child.as_posix(),
                        "size": stat.st_size,
                        "mtime_ns": stat.st_mtime_ns,
                    }
                )
            else:
                raise ValueError(
                    f"{label} contains a non-file resource: {logical_child.as_posix()}"
                )

    visit(source, Path(declared_path), frozenset())
    return {
        "declared_path": declared_path,
        "file_count": len(files),
        "files": files,
        "inventory_sha256": sha256_json(files),
    }


def _lookup_regular_file_binding(
    root: Path,
    source: Path,
    *,
    role: str,
) -> dict[str, Any]:
    try:
        resolved = source.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"BAM lookup contains an unavailable {role}: {source}") from exc
    try:
        relative = resolved.relative_to(root).as_posix()
    except ValueError as exc:
        raise ValueError(f"BAM lookup {role} symlink escapes the project directory") from exc
    if not resolved.is_file():
        raise ValueError(f"BAM lookup {role} must resolve to a regular file: {relative}")
    stat = resolved.stat()
    return {
        "role": role,
        "path": relative,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def _ssqtl_bam_lookup_resource_binding(
    project: Mapping[str, Any], root: Path
) -> dict[str, Any]:
    """Freeze only BAM/BAI files eligible under the declared lookup rows.

    The R adapter searches one referenced directory per sample, first by its
    configured suffixes and then by a non-recursive ``sample_id.*.bam`` match.
    Binding the selected files' path/size/mtime metadata catches
    missing/ambiguous/selected-file drift without rereading large tracks or
    invalidating resume for unrelated BAMs in a shared directory.
    """

    # Import lazily to keep project schema loading independent of the heavier
    # scientific adapter while sharing its validated lookup/config semantics.
    from .ssqtl_adapter_v3 import _load_options, _resolved_bam_lookup_rows

    project_inputs = dict(project["inputs"])
    config_record = project_inputs.get("config")
    config_path = (
        Path(str(dict(config_record)["source_path"]))
        if config_record is not None
        else None
    )
    options, _options_sha256, _config_sha256 = _load_options(config_path)
    lookup_record = dict(project_inputs["bam_lookup"])
    _fields, id_column, _path_column, rows = _resolved_bam_lookup_rows(
        Path(str(lookup_record["source_path"])),
        root,
        id_candidates=options["bam_lookup_id_columns"],
        path_candidates=options["bam_lookup_path_columns"],
    )

    eligible_bams: dict[str, Path] = {}
    for row, lookup_path in rows:
        sample_id = row[id_column]
        if lookup_path.is_file():
            eligible_bams[str(lookup_path)] = lookup_path
            continue
        if lookup_path.name.lower().endswith(".bam"):
            raise ValueError(
                "BAM lookup directory ending in .bam is interpreted as a BAM by the "
                f"ssQTL resolver: {lookup_path.relative_to(root).as_posix()}"
            )

        direct_matches: list[Path] = []
        for suffix in options["bam_suffixes"]:
            candidate = lookup_path / f"{sample_id}{suffix}"
            if candidate.exists() or candidate.is_symlink():
                direct_matches.append(candidate)
        if direct_matches:
            # Match prepare_cases.R: configured suffix order is authoritative.
            selected = direct_matches[0]
            eligible_bams[str(selected)] = selected
            continue

        fallback = re.compile(re.escape(sample_id) + r".*[.]bam$", re.IGNORECASE)
        fallback_matches = [
            candidate
            for candidate in sorted(lookup_path.iterdir(), key=lambda item: item.name)
            if fallback.search(candidate.name)
        ]
        if len(fallback_matches) == 1:
            selected = fallback_matches[0]
            eligible_bams[str(selected)] = selected

    resources: dict[tuple[str, str], dict[str, Any]] = {}
    for bam in sorted(eligible_bams.values(), key=str):
        bam_record = _lookup_regular_file_binding(root, bam, role="BAM")
        resources[("BAM", bam_record["path"])] = bam_record
        bai_candidates = (Path(f"{bam}.bai"), bam.with_suffix(".bai"))
        existing_indexes = [
            bai for bai in bai_candidates if bai.exists() or bai.is_symlink()
        ]
        if existing_indexes:
            # Match prepare_cases.R: <bam>.bai precedes replacement .bai.
            bai = existing_indexes[0]
            bai_record = _lookup_regular_file_binding(root, bai, role="BAI")
            resources[("BAI", bai_record["path"])] = bai_record

    files = sorted(resources.values(), key=lambda item: (item["path"], item["role"]))
    return {
        "file_count": len(files),
        "files": files,
        "inventory_sha256": sha256_json(files),
    }


def build_project_source_binding(project: Mapping[str, Any]) -> dict[str, Any]:
    """Freeze metadata sources whose changes must invalidate ``--resume``."""

    root = Path(str(project["project_root"])).resolve(strict=True)
    project_path = Path(str(project["project_path"])).resolve(strict=True)
    project_declared = project_path.relative_to(root).as_posix()
    inputs: dict[str, Any] = {}
    for name, record_value in sorted(dict(project["inputs"]).items()):
        record = dict(record_value)
        declared = str(record["declared_path"])
        if name in _DIRECTORY_INPUTS:
            inputs[name] = _project_directory_binding(
                root,
                record["source_path"],
                declared_path=declared,
                label=f"project.yaml inputs.{name}",
            )
        else:
            inputs[name] = _project_file_binding(
                root,
                record["source_path"],
                declared_path=declared,
                label=f"project.yaml inputs.{name}",
            )
    if str(project["adapter"]) == "ssqtl":
        inputs["bam_lookup"]["eligible_resources"] = (
            _ssqtl_bam_lookup_resource_binding(project, root)
        )
    reference = dict(project["reference"])
    body = {
        "schema_version": "3.0-project-source-binding",
        "adapter": str(project["adapter"]),
        "project": _project_file_binding(
            root,
            project_path,
            declared_path=project_declared,
            label="project.yaml",
        ),
        "inputs": inputs,
        "reference": _project_file_binding(
            root,
            reference["source_path"],
            declared_path=str(reference["declared_path"]),
            label="project.yaml reference",
        ),
    }
    return {**body, "binding_sha256": sha256_json(body)}
