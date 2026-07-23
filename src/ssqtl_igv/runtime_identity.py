from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any, Mapping

import jsonschema
import yaml

from .utils import atomic_write_json, sha256_file, utc_now


RUNTIME_MANIFEST_SCHEMA = "igv-runtime-manifest-v1"
WORKFLOW_SCHEMA_VERSION = "3.0"
PIPELINE_NAME = "igv-pipeline"
PIPELINE_VERSION = "3.0.0"
PLATFORM = "linux/amd64"
OCI_REPOSITORY = "ghcr.io/luckyfruit88/igv-pipeline"
RUNTIME_MANIFEST_IMAGE_PATH = "/opt/igv-pipeline/runtime-manifest.json"
RUNTIME_MANIFEST_SCHEMA_PATH = "containers/runtime-manifest.schema.json"
RUNTIME_CONFIG_MANIFEST_PATH = "src/ssqtl_igv/resources/v3-runtime.yaml"
MATERIALS_MANIFEST_PATH = "containers/runtime-materials.lock.json"

FIXED_RENDER_CONTRACT = {
    "screen": "1920x2160x24",
    "igv_heap": "6g",
    "locale": "C.UTF-8",
    "font_family": "DejaVu Sans",
    "command_listener_enabled": False,
}
REQUIRED_TOOL_NAMES = (
    "igv",
    "igv_jre",
    "nextflow",
    "controller_java",
    "python",
    "r",
    "samtools",
    "poppler",
    "imagemagick",
    "fontconfig",
    "fonts",
    "tesseract",
    "xvfb",
)

_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_OCI_DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
_GIT_OBJECT = re.compile(r"^[a-f0-9]{40}$")


def _project_root() -> Path:
    configured = os.environ.get("IGV_SNAPSHOT_PIPELINE_DIR")
    candidates = [Path(configured).expanduser()] if configured else []
    candidates.extend(
        (
            Path(__file__).resolve().parents[2],
            Path.cwd(),
            Path(sys.prefix) / "share" / "igv-snapshot-workflow" / "pipeline",
        )
    )
    for candidate in candidates:
        resolved = candidate.resolve(strict=False)
        if (
            (resolved / RUNTIME_MANIFEST_SCHEMA_PATH).is_file()
            and (resolved / MATERIALS_MANIFEST_PATH).is_file()
            and (resolved / RUNTIME_CONFIG_MANIFEST_PATH).is_file()
        ):
            return resolved
    raise FileNotFoundError(
        "portable runtime manifest contract files are unavailable; "
        "set IGV_SNAPSHOT_PIPELINE_DIR"
    )


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key is forbidden: {key}")
        result[key] = value
    return result


def _read_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def _regular_file(
    value: str | Path,
    label: str,
    *,
    allow_symlink: bool = False,
) -> Path:
    source = Path(value).expanduser()
    if source.is_symlink() and not allow_symlink:
        raise ValueError(f"{label} must not be a symlink: {source}")
    try:
        resolved = source.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{label} is unavailable: {source}: {exc}") from exc
    if not resolved.is_file():
        raise ValueError(f"{label} must be a regular file: {resolved}")
    return resolved


def canonical_runtime_manifest_bytes(value: Mapping[str, Any]) -> bytes:
    """Return the deterministic JSON bytes used for runtime cache identity."""

    if not isinstance(value, Mapping):
        raise TypeError("runtime manifest must be a mapping")
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def runtime_manifest_fingerprint(value: Mapping[str, Any]) -> str:
    """Hash the complete unsigned reproducibility claim.

    OCI digests, SIF paths/checksums, timestamps, and host paths are deliberately
    absent from the runtime-manifest schema.  When observable they are recorded
    separately as provenance, so they never become user-supplied prerequisites.
    """

    return hashlib.sha256(canonical_runtime_manifest_bytes(value)).hexdigest()


def _validate_material_lock(
    materials_path: Path,
    *,
    explicit_lock_dir: str | Path | None = None,
    allow_staged_symlink: bool = False,
) -> dict[str, Any]:
    materials = _read_json(materials_path, "runtime material lock")
    if materials.get("schema_version") != "igv-runtime-materials-v1":
        raise ValueError("runtime material lock schema_version is invalid")
    if materials.get("platform") != PLATFORM:
        raise ValueError("runtime material lock platform must be linux/amd64")

    base = materials.get("base_image")
    if not isinstance(base, dict):
        raise ValueError("runtime material lock base_image must be an object")
    base_digest = str(base.get("linux_amd64_manifest_digest", ""))
    base_reference = str(base.get("reference", ""))
    if not _OCI_DIGEST.fullmatch(base_digest):
        raise ValueError("runtime material lock base-image digest is invalid")
    if not base_reference.endswith("@" + base_digest):
        raise ValueError("runtime material lock base-image reference/digest differ")

    artifacts = materials.get("external_artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError("runtime material lock has no external artifacts")
    required_artifacts = {
        "igv_linux_with_java",
        "temurin_controller_jre",
        "nextflow_one_jar",
        "nextflow_launcher",
    }
    missing_artifacts = required_artifacts - set(artifacts)
    if missing_artifacts:
        raise ValueError(
            "runtime material lock lacks required artifacts: "
            + ", ".join(sorted(missing_artifacts))
        )
    for name, raw in artifacts.items():
        if not isinstance(raw, dict):
            raise ValueError(f"runtime material {name} must be an object")
        if not str(raw.get("url", "")).startswith("https://"):
            raise ValueError(f"runtime material {name} must use an HTTPS URL")
        if not _SHA256.fullmatch(str(raw.get("sha256", ""))):
            raise ValueError(f"runtime material {name} lacks a lowercase SHA-256")
        if not str(raw.get("version", "")).strip():
            raise ValueError(f"runtime material {name} lacks an exact version")

    expected_tools = materials.get("tool_contract")
    if not isinstance(expected_tools, dict) or not expected_tools:
        raise ValueError("runtime material lock has no tool contract")
    unknown_tools = set(expected_tools) - set(REQUIRED_TOOL_NAMES)
    if unknown_tools:
        raise ValueError(
            "runtime material tool contract contains unknown tools: "
            + ", ".join(sorted(unknown_tools))
        )
    for name, version in expected_tools.items():
        if not isinstance(version, str) or not version.strip():
            raise ValueError(f"runtime material tool version is invalid: {name}")

    locked_render = materials.get("render_contract")
    expected_locked_render = {
        **FIXED_RENDER_CONTRACT,
        "network": "disabled_at_runtime",
    }
    if locked_render != expected_locked_render:
        raise ValueError("runtime material render contract differs from the fixed contract")

    explicit = materials.get("explicit_environment_locks")
    if not isinstance(explicit, dict) or not explicit:
        raise ValueError("runtime material lock has no explicit environment locks")
    lock_root_source = Path(explicit_lock_dir) if explicit_lock_dir else materials_path.parent
    try:
        lock_root = lock_root_source.expanduser().resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"explicit runtime lock directory is unavailable: {exc}") from exc
    if not lock_root.is_dir():
        raise ValueError(f"explicit runtime lock directory is not a directory: {lock_root}")
    verified: dict[str, str] = {}
    for relative_name, expected in explicit.items():
        relative = Path(str(relative_name))
        if (
            relative.is_absolute()
            or len(relative.parts) != 1
            or relative.name != str(relative_name)
        ):
            raise ValueError(f"unsafe explicit lock path: {relative_name}")
        if not _SHA256.fullmatch(str(expected)):
            raise ValueError(f"explicit lock checksum is invalid: {relative_name}")
        candidate = lock_root / relative
        if candidate.is_symlink() and not allow_staged_symlink:
            raise ValueError(f"explicit lock must not be a symlink: {relative_name}")
        try:
            lock_path = candidate.resolve(strict=True)
        except OSError as exc:
            raise ValueError(f"explicit lock is unavailable: {relative_name}: {exc}") from exc
        if not lock_path.is_file():
            raise ValueError(f"explicit lock is not a regular in-tree file: {relative_name}")
        observed = sha256_file(lock_path)
        if observed != expected:
            raise ValueError(f"explicit lock checksum differs: {relative_name}")
        verified[str(relative_name)] = observed

    helper_materials_path = lock_root / "materials.lock.json"
    helper_materials = _read_json(helper_materials_path, "helper material lock")
    environments = helper_materials.get("environments")
    if not isinstance(environments, dict):
        raise ValueError("helper material lock lacks environments")

    def package_version(environment: str, package: str) -> str:
        raw_environment = environments.get(environment)
        packages = (
            raw_environment.get("packages")
            if isinstance(raw_environment, dict)
            else None
        )
        if not isinstance(packages, list):
            raise ValueError(f"helper material environment is invalid: {environment}")
        matches = [
            str(item.get("version", ""))
            for item in packages
            if isinstance(item, dict) and item.get("name") == package
        ]
        if len(matches) != 1 or not matches[0]:
            raise ValueError(
                f"helper material package is not uniquely locked: {environment}/{package}"
            )
        return matches[0]

    locked_tool_sources = {
        "igv": str(artifacts["igv_linux_with_java"]["version"]),
        "igv_jre": str(artifacts["igv_linux_with_java"]["java_major"]),
        "nextflow": str(artifacts["nextflow_one_jar"]["version"]),
        "controller_java": str(artifacts["temurin_controller_jre"]["version"]),
        "python": package_version("helper", "python"),
        "r": package_version("helper", "r-base"),
        "samtools": package_version("samtools", "samtools"),
        "poppler": package_version("helper", "poppler"),
        "imagemagick": package_version("helper", "imagemagick"),
        "fontconfig": package_version("helper", "fontconfig"),
        "fonts": FIXED_RENDER_CONTRACT["font_family"],
        "tesseract": package_version("helper", "tesseract"),
    }
    if artifacts["nextflow_launcher"]["version"] != locked_tool_sources["nextflow"]:
        raise ValueError("Nextflow launcher and one-jar versions differ")
    for name, locked_version in locked_tool_sources.items():
        if expected_tools.get(name) != locked_version:
            raise ValueError(
                f"runtime material tool contract differs from its locked source: {name}"
            )
    return {
        "value": materials,
        "verified_locks": verified,
        "tool_contract": dict(expected_tools),
    }


def _runtime_render_contract(runtime_config_path: Path) -> dict[str, Any]:
    try:
        value = yaml.safe_load(runtime_config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError(f"cannot read v3 runtime configuration: {exc}") from exc
    if not isinstance(value, dict) or value.get("schema_version") != "3.0":
        raise ValueError("v3 runtime configuration schema_version is invalid")
    desktop = value.get("desktop")
    declared = value.get("runtime_manifest_contract")
    binaries = value.get("binaries")
    if not isinstance(desktop, dict) or not isinstance(declared, dict):
        raise ValueError("v3 runtime configuration lacks its manifest contract")
    if not isinstance(binaries, dict) or binaries.get("igv") != "/opt/igv/bin/igv":
        raise ValueError("v3 runtime configuration must use /opt/igv/bin/igv")
    if desktop.get("command_listener_enabled") is not False:
        raise ValueError("v3 production IGV command listener must be disabled")
    try:
        screen = (
            f"{int(desktop['screen_width'])}x{int(desktop['screen_height'])}x"
            f"{int(desktop['screen_depth'])}"
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("v3 runtime screen geometry is invalid") from exc
    observed = {
        "screen": screen,
        "igv_heap": str(declared.get("igv_heap", "")),
        "locale": str(declared.get("locale", "")),
        "font_family": str(declared.get("font_family", "")),
        "command_listener_enabled": declared.get("command_listener_enabled"),
    }
    if declared.get("platform") != PLATFORM:
        raise ValueError("v3 runtime platform contract must be linux/amd64")
    if observed != FIXED_RENDER_CONTRACT:
        raise ValueError("v3 runtime render contract differs from the fixed contract")
    return {
        **observed,
        "config_path": RUNTIME_CONFIG_MANIFEST_PATH,
        "config_sha256": sha256_file(runtime_config_path),
    }


def create_runtime_manifest(
    *,
    source_commit: str,
    source_tree: str,
    tools: Mapping[str, str],
    materials_path: str | Path | None = None,
    runtime_config_path: str | Path | None = None,
) -> dict[str, Any]:
    """Create the unsigned manifest embedded in the production image."""

    project = None
    if materials_path is None or runtime_config_path is None:
        project = _project_root()
    materials_source = _regular_file(
        materials_path or (project / MATERIALS_MANIFEST_PATH if project else ""),
        "runtime material lock",
        allow_symlink=True,
    )
    runtime_config_source = _regular_file(
        runtime_config_path
        or (project / RUNTIME_CONFIG_MANIFEST_PATH if project else ""),
        "v3 runtime configuration",
        allow_symlink=True,
    )
    material_result = _validate_material_lock(materials_source)
    normalized_tools = {str(key): str(value) for key, value in tools.items()}
    if set(normalized_tools) != set(REQUIRED_TOOL_NAMES):
        missing = sorted(set(REQUIRED_TOOL_NAMES) - set(normalized_tools))
        extra = sorted(set(normalized_tools) - set(REQUIRED_TOOL_NAMES))
        raise ValueError(
            f"runtime tool set differs from the fixed contract; missing={missing}, extra={extra}"
        )
    for name, expected in material_result["tool_contract"].items():
        if normalized_tools[name] != expected:
            raise ValueError(
                f"runtime tool {name} version differs from the material contract"
            )
    manifest: dict[str, Any] = {
        "schema_version": RUNTIME_MANIFEST_SCHEMA,
        "workflow_schema_version": WORKFLOW_SCHEMA_VERSION,
        "pipeline": {"name": PIPELINE_NAME, "version": PIPELINE_VERSION},
        "platform": PLATFORM,
        "source": {"commit": source_commit, "tree": source_tree},
        "materials": {
            "path": MATERIALS_MANIFEST_PATH,
            "sha256": sha256_file(materials_source),
        },
        "render_contract": _runtime_render_contract(runtime_config_source),
        "tools": normalized_tools,
    }
    # Use the JSON Schema as the final creator-side structural check.
    schema_project = project or _project_root()
    schema = _read_json(
        schema_project / RUNTIME_MANIFEST_SCHEMA_PATH,
        "runtime manifest schema",
    )
    try:
        jsonschema.Draft202012Validator(schema).validate(manifest)
    except jsonschema.ValidationError as exc:
        location = ".".join(str(part) for part in exc.absolute_path) or "<root>"
        raise ValueError(
            f"created runtime manifest violates schema at {location}: {exc.message}"
        ) from exc
    return manifest


def _normalized_observed_provenance(
    *,
    oci_digest: str | None,
    sif_sha256: str | None,
) -> dict[str, Any]:
    observed_oci: dict[str, str] | None = None
    if oci_digest is not None:
        normalized = oci_digest.strip().lower()
        if not _OCI_DIGEST.fullmatch(normalized):
            raise ValueError("observed runtime OCI digest is invalid")
        observed_oci = {
            "digest": normalized,
            "reference": f"{OCI_REPOSITORY}@{normalized}",
        }
    normalized_sif: str | None = None
    if sif_sha256 is not None:
        normalized_sif = sif_sha256.strip().lower()
        if not _SHA256.fullmatch(normalized_sif):
            raise ValueError("observed runtime SIF SHA-256 is invalid")
    return {"oci": observed_oci, "sif_sha256": normalized_sif}


def validate_runtime_manifest(
    manifest_path: str | Path,
    *,
    expected_manifest_sha256: str | None = None,
    expected_fingerprint_sha256: str | None = None,
    schema_path: str | Path | None = None,
    materials_path: str | Path | None = None,
    runtime_config_path: str | Path | None = None,
    explicit_lock_dir: str | Path | None = None,
    observed_oci_digest: str | None = None,
    observed_sif_sha256: str | None = None,
    allow_staged_symlink: bool = False,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Validate an embedded unsigned manifest and return its cache fingerprint."""

    project = None
    if schema_path is None or materials_path is None or runtime_config_path is None:
        project = _project_root()
    manifest_source = _regular_file(
        manifest_path, "runtime manifest", allow_symlink=allow_staged_symlink
    )
    schema_source = _regular_file(
        schema_path
        or (project / RUNTIME_MANIFEST_SCHEMA_PATH if project else ""),
        "runtime manifest schema",
        allow_symlink=True,
    )
    materials_source = _regular_file(
        materials_path or (project / MATERIALS_MANIFEST_PATH if project else ""),
        "runtime material lock",
        allow_symlink=True,
    )
    runtime_config_source = _regular_file(
        runtime_config_path
        or (project / RUNTIME_CONFIG_MANIFEST_PATH if project else ""),
        "v3 runtime configuration",
        allow_symlink=True,
    )

    manifest_sha256 = sha256_file(manifest_source)
    if expected_manifest_sha256 is not None:
        normalized = expected_manifest_sha256.strip().lower()
        if not _SHA256.fullmatch(normalized):
            raise ValueError("expected runtime manifest SHA-256 is invalid")
        if normalized != manifest_sha256:
            raise ValueError("runtime manifest SHA-256 differs from the declared value")

    manifest = _read_json(manifest_source, "runtime manifest")
    schema = _read_json(schema_source, "runtime manifest schema")
    try:
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.Draft202012Validator(schema).validate(manifest)
    except jsonschema.SchemaError as exc:
        raise ValueError(f"runtime manifest schema is invalid: {exc.message}") from exc
    except jsonschema.ValidationError as exc:
        location = ".".join(str(part) for part in exc.absolute_path) or "<root>"
        raise ValueError(
            f"runtime manifest schema violation at {location}: {exc.message}"
        ) from exc

    if manifest.get("schema_version") != RUNTIME_MANIFEST_SCHEMA:
        raise ValueError("runtime manifest schema_version is invalid")
    if manifest.get("workflow_schema_version") != WORKFLOW_SCHEMA_VERSION:
        raise ValueError("runtime manifest workflow schema must be 3.0")
    if manifest.get("pipeline") != {
        "name": PIPELINE_NAME,
        "version": PIPELINE_VERSION,
    }:
        raise ValueError("runtime manifest pipeline identity is invalid")
    if manifest.get("platform") != PLATFORM:
        raise ValueError("runtime manifest platform must be linux/amd64")

    source = manifest["source"]
    if not _GIT_OBJECT.fullmatch(str(source["commit"])):
        raise ValueError("runtime source commit must be a lowercase 40-hex Git object")
    if not _GIT_OBJECT.fullmatch(str(source["tree"])):
        raise ValueError("runtime source tree must be a lowercase 40-hex Git object")

    materials_result = _validate_material_lock(
        materials_source,
        explicit_lock_dir=explicit_lock_dir,
        allow_staged_symlink=allow_staged_symlink,
    )
    materials_sha256 = sha256_file(materials_source)
    if manifest["materials"]["path"] != MATERIALS_MANIFEST_PATH:
        raise ValueError("runtime manifest material-lock path is invalid")
    if manifest["materials"]["sha256"] != materials_sha256:
        raise ValueError("runtime manifest material-lock SHA-256 differs from the file")

    render_contract = _runtime_render_contract(runtime_config_source)
    if manifest["render_contract"] != render_contract:
        raise ValueError("runtime manifest render contract differs from v3-runtime.yaml")

    tools = manifest["tools"]
    if set(tools) != set(REQUIRED_TOOL_NAMES):
        raise ValueError("runtime manifest tool set differs from the fixed contract")
    for name, expected in materials_result["tool_contract"].items():
        if tools[name] != expected:
            raise ValueError(
                f"runtime tool {name} version differs from the material contract"
            )
    if tools["fonts"] != render_contract["font_family"]:
        raise ValueError("runtime font inventory differs from the render contract")

    fingerprint = runtime_manifest_fingerprint(manifest)
    if expected_fingerprint_sha256 is not None:
        normalized_fingerprint = expected_fingerprint_sha256.strip().lower()
        if not _SHA256.fullmatch(normalized_fingerprint):
            raise ValueError("expected runtime fingerprint SHA-256 is invalid")
        if normalized_fingerprint != fingerprint:
            raise ValueError(
                "runtime fingerprint differs from the declared cache identity"
            )
    observed_provenance = _normalized_observed_provenance(
        oci_digest=observed_oci_digest,
        sif_sha256=observed_sif_sha256,
    )
    report = {
        "schema_version": "3.0-runtime-manifest-validation",
        "status": "PASS",
        "validated_at": utc_now(),
        "runtime_manifest_path": str(manifest_source),
        "runtime_manifest_sha256": manifest_sha256,
        "runtime_fingerprint_sha256": fingerprint,
        "platform": PLATFORM,
        "pipeline": manifest["pipeline"],
        "source": source,
        "materials_path": str(materials_source),
        "materials_sha256": materials_sha256,
        "explicit_lock_sha256": materials_result["verified_locks"],
        "runtime_config_path": str(runtime_config_source),
        "runtime_config_sha256": sha256_file(runtime_config_source),
        "render_contract": render_contract,
        "tools": tools,
        "observed_provenance": observed_provenance,
    }
    if output_dir is not None:
        destination = Path(output_dir).expanduser().resolve(strict=False)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"runtime manifest validation output exists: {destination}")
        staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
        staging.mkdir(mode=0o700)
        try:
            atomic_write_json(staging / "validation.json", report)
            os.replace(staging, destination)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise
        report = {**report, "output_dir": str(destination)}
    return report
