from __future__ import annotations

import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from ssqtl_igv.runtime_identity import (
    RUNTIME_MANIFEST_IMAGE_PATH,
    REQUIRED_TOOL_NAMES,
    canonical_runtime_manifest_bytes,
    create_runtime_manifest,
    runtime_manifest_fingerprint,
    validate_runtime_manifest,
)
from ssqtl_igv.utils import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_SCHEMA = PROJECT_ROOT / "containers/runtime-manifest.schema.json"
MATERIALS_LOCK = PROJECT_ROOT / "containers/runtime-materials.lock.json"
RUNTIME_CONFIG = PROJECT_ROOT / "src/ssqtl_igv/resources/v3-runtime.yaml"


def _tools() -> dict[str, str]:
    return {
        "igv": "2.16.2",
        "igv_jre": "11",
        "nextflow": "25.04.7",
        "controller_java": "21.0.8+9",
        "python": "3.10.12",
        "r": "4.5.2",
        "samtools": "1.18",
        "poppler": "26.07.0",
        "imagemagick": "7.1.2_27",
        "fontconfig": "2.18.1",
        "fonts": "DejaVu Sans",
        "tesseract": "5.5.2",
        "xvfb": "1.20.11-28.el8_10.3",
    }


def _valid_manifest() -> dict:
    return create_runtime_manifest(
        source_commit="c" * 40,
        source_tree="d" * 40,
        tools=_tools(),
        materials_path=MATERIALS_LOCK,
        runtime_config_path=RUNTIME_CONFIG,
    )


def _write_manifest(tmp_path: Path, value: dict, name: str = "manifest.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _validate(manifest: Path, **kwargs: object) -> dict:
    return validate_runtime_manifest(
        manifest,
        schema_path=MANIFEST_SCHEMA,
        materials_path=MATERIALS_LOCK,
        runtime_config_path=RUNTIME_CONFIG,
        **kwargs,
    )


def test_embedded_manifest_path_is_fixed_and_user_independent() -> None:
    assert RUNTIME_MANIFEST_IMAGE_PATH == "/opt/igv-pipeline/runtime-manifest.json"


def test_manifest_creator_and_validator_return_deterministic_fingerprint(
    tmp_path: Path,
) -> None:
    value = _valid_manifest()
    manifest = _write_manifest(tmp_path, value)
    output = tmp_path / "validation"

    report = _validate(
        manifest,
        expected_manifest_sha256=sha256_file(manifest),
        output_dir=output,
    )

    assert report["status"] == "PASS"
    assert report["schema_version"] == "3.0-runtime-manifest-validation"
    assert report["runtime_manifest_sha256"] == sha256_file(manifest)
    assert report["runtime_fingerprint_sha256"] == runtime_manifest_fingerprint(value)
    assert report["tools"] == _tools()
    assert report["observed_provenance"] == {"oci": None, "sif_sha256": None}
    assert "certification" not in report
    assert "trust_root" not in json.dumps(report)
    persisted = json.loads((output / "validation.json").read_text(encoding="utf-8"))
    assert persisted["runtime_fingerprint_sha256"] == report[
        "runtime_fingerprint_sha256"
    ]


def test_fingerprint_is_independent_of_json_key_order_and_formatting(
    tmp_path: Path,
) -> None:
    value = _valid_manifest()
    reversed_value = dict(reversed(list(value.items())))
    compact = tmp_path / "compact.json"
    pretty = tmp_path / "pretty.json"
    compact.write_text(json.dumps(value, separators=(",", ":")), encoding="utf-8")
    pretty.write_text(json.dumps(reversed_value, indent=4), encoding="utf-8")

    compact_report = _validate(compact)
    pretty_report = _validate(pretty)

    assert compact_report["runtime_manifest_sha256"] != pretty_report[
        "runtime_manifest_sha256"
    ]
    assert compact_report["runtime_fingerprint_sha256"] == pretty_report[
        "runtime_fingerprint_sha256"
    ]
    assert canonical_runtime_manifest_bytes(value) == canonical_runtime_manifest_bytes(
        reversed_value
    )


def test_oci_and_sif_observations_are_optional_provenance_not_identity(
    tmp_path: Path,
) -> None:
    manifest = _write_manifest(tmp_path, _valid_manifest())
    baseline = _validate(manifest)
    observed = _validate(
        manifest,
        observed_oci_digest="sha256:" + "a" * 64,
        observed_sif_sha256="b" * 64,
    )

    assert observed["runtime_fingerprint_sha256"] == baseline[
        "runtime_fingerprint_sha256"
    ]
    assert observed["observed_provenance"] == {
        "oci": {
            "digest": "sha256:" + "a" * 64,
            "reference": "ghcr.io/luckyfruit88/igv-pipeline@sha256:" + "a" * 64,
        },
        "sif_sha256": "b" * 64,
    }


@pytest.mark.parametrize(
    ("keyword", "value", "message"),
    [
        ("observed_oci_digest", "latest", "OCI digest is invalid"),
        ("observed_sif_sha256", "not-a-sha", "SIF SHA-256 is invalid"),
    ],
)
def test_observed_provenance_syntax_is_checked(
    tmp_path: Path,
    keyword: str,
    value: str,
    message: str,
) -> None:
    manifest = _write_manifest(tmp_path, _valid_manifest())
    with pytest.raises(ValueError, match=message):
        _validate(manifest, **{keyword: value})


def test_manifest_schema_rejects_old_identity_and_certification_fields(
    tmp_path: Path,
) -> None:
    value = _valid_manifest()
    value["certification"] = {"state": "CERTIFIED"}
    manifest = _write_manifest(tmp_path, value)

    with pytest.raises(ValueError, match="schema violation"):
        _validate(manifest)


def test_runtime_contract_contains_no_trust_root_or_approval_semantics() -> None:
    sources = (
        PROJECT_ROOT / "src/ssqtl_igv/runtime_identity.py",
        MANIFEST_SCHEMA,
        MATERIALS_LOCK,
        PROJECT_ROOT / "bin/validate_runtime_identity.py",
        PROJECT_ROOT / "modules/local/validate_runtime_identity.nf",
    )
    forbidden = ("trust_root", "public_key", "risk_acceptance", "named_approver")
    for source in sources:
        text = source.read_text(encoding="utf-8").lower()
        for term in forbidden:
            assert term not in text, (source, term)


def test_material_lock_no_longer_pins_certification_files() -> None:
    materials = json.loads(MATERIALS_LOCK.read_text(encoding="utf-8"))
    assert "certification" not in materials
    assert not any(
        "certification" in name
        for name in materials["explicit_environment_locks"]
    )


def test_expected_manifest_file_checksum_mismatch_fails_closed(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, _valid_manifest())
    with pytest.raises(ValueError, match="differs from the declared value"):
        _validate(manifest, expected_manifest_sha256="a" * 64)


def test_expected_runtime_fingerprint_mismatch_fails_closed(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, _valid_manifest())
    with pytest.raises(ValueError, match="cache identity"):
        _validate(manifest, expected_fingerprint_sha256="a" * 64)


def test_material_and_render_policy_are_part_of_the_manifest_claim(
    tmp_path: Path,
) -> None:
    material_drift = _valid_manifest()
    material_drift["materials"]["sha256"] = "a" * 64
    with pytest.raises(ValueError, match="material-lock SHA-256 differs"):
        _validate(_write_manifest(tmp_path, material_drift, "materials.json"))

    changed_config = tmp_path / "v3-runtime.yaml"
    config = yaml.safe_load(RUNTIME_CONFIG.read_text(encoding="utf-8"))
    config["desktop"]["stable_interval_seconds"] = 2
    changed_config.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    stable_manifest = _write_manifest(tmp_path, _valid_manifest(), "render.json")
    with pytest.raises(ValueError, match="render contract differs"):
        validate_runtime_manifest(
            stable_manifest,
            schema_path=MANIFEST_SCHEMA,
            materials_path=MATERIALS_LOCK,
            runtime_config_path=changed_config,
        )


def test_locked_tool_version_drift_fails_closed(tmp_path: Path) -> None:
    value = _valid_manifest()
    value["tools"]["python"] = "3.11.0"
    manifest = _write_manifest(tmp_path, value)

    with pytest.raises(ValueError, match="python version differs"):
        _validate(manifest)


def test_every_render_affecting_tool_has_a_nonempty_version(tmp_path: Path) -> None:
    assert set(_tools()) == set(REQUIRED_TOOL_NAMES)
    value = _valid_manifest()
    value["tools"]["xvfb"] = ""
    manifest = _write_manifest(tmp_path, value)

    with pytest.raises(ValueError, match="schema violation"):
        _validate(manifest)


def test_source_and_tool_changes_create_new_fingerprints() -> None:
    baseline = _valid_manifest()
    source_changed = deepcopy(baseline)
    source_changed["source"]["commit"] = "e" * 40
    tool_changed = deepcopy(baseline)
    tool_changed["tools"]["fontconfig"] = "2.14.0"

    fingerprints = {
        runtime_manifest_fingerprint(baseline),
        runtime_manifest_fingerprint(source_changed),
        runtime_manifest_fingerprint(tool_changed),
    }
    assert len(fingerprints) == 3


def test_explicit_environment_lock_bytes_are_verified(tmp_path: Path) -> None:
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    materials = json.loads(MATERIALS_LOCK.read_text(encoding="utf-8"))
    for name in materials["explicit_environment_locks"]:
        source = PROJECT_ROOT / "containers" / name
        (lock_dir / name).write_bytes(source.read_bytes())
    (lock_dir / "fonts-local.conf").write_text("tampered\n", encoding="utf-8")
    manifest = _write_manifest(tmp_path, _valid_manifest())

    with pytest.raises(ValueError, match="explicit lock checksum differs"):
        validate_runtime_manifest(
            manifest,
            schema_path=MANIFEST_SCHEMA,
            materials_path=MATERIALS_LOCK,
            explicit_lock_dir=lock_dir,
            runtime_config_path=RUNTIME_CONFIG,
        )


def test_atomic_validation_bundle_refuses_overwrite(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, _valid_manifest())
    output = tmp_path / "validation"
    _validate(manifest, output_dir=output)

    with pytest.raises(FileExistsError, match="output exists"):
        _validate(manifest, output_dir=output)


def test_internal_validator_cli_uses_manifest_language() -> None:
    completed = subprocess.run(
        [sys.executable, str(PROJECT_ROOT / "bin/validate_runtime_identity.py"), "--help"],
        cwd=PROJECT_ROOT,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert "--runtime-manifest" in completed.stdout
    assert "--manifest-schema" in completed.stdout
    assert "certif" not in completed.stdout.lower()


def test_creator_cli_emits_one_new_embedded_manifest(tmp_path: Path) -> None:
    output = tmp_path / "runtime-manifest.json"
    command = [
        sys.executable,
        str(PROJECT_ROOT / "bin/create_runtime_manifest.py"),
        "--source-commit",
        "c" * 40,
        "--source-tree",
        "d" * 40,
        "--materials-lock",
        str(MATERIALS_LOCK),
        "--runtime-config",
        str(RUNTIME_CONFIG),
        "--output",
        str(output),
    ]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True, stdout=subprocess.PIPE)
    manifest = json.loads(output.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "igv-runtime-manifest-v1"
    assert manifest["tools"] == _tools()

    duplicate = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert duplicate.returncode != 0
