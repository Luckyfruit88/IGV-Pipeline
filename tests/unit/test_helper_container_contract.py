from __future__ import annotations

import hashlib
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTAINERS = PROJECT_ROOT / "containers"


def _json(name: str) -> dict[str, object]:
    return json.loads((CONTAINERS / name).read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _explicit_entries(path: Path) -> list[str]:
    lines = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    assert lines[0] == "@EXPLICIT"
    return lines[1:]


def test_definition_uses_certified_immutable_amd64_base() -> None:
    spec = _json("helper-lock-spec.json")
    definition = (CONTAINERS / "helpers.def").read_text(encoding="utf-8")
    base = spec["base_image"]

    assert spec["platform"] == "linux-64"
    assert spec["virtual_packages"] == {"__glibc": "2.28"}
    assert (
        f"From: almalinux@sha256:{base['linux_amd64_manifest_sha256']}" in definition
    )
    assert "From: almalinux:" not in definition
    assert "latest" not in definition.lower()


def test_materials_and_explicit_locks_are_byte_identified() -> None:
    materials = _json("materials.lock.json")
    for environment_name, environment in materials["environments"].items():
        lock = CONTAINERS / environment["lockfile"]
        assert _sha256(lock) == environment["lockfile_sha256"]
        expected = [
            f"{package['url']}#{package['sha256']}"
            for package in environment["packages"]
        ]
        assert _explicit_entries(lock) == expected, environment_name
        assert all(package["sha256"] for package in environment["packages"])


def test_top_level_tools_and_build_inputs_are_exactly_pinned() -> None:
    spec = _json("helper-lock-spec.json")
    helper_specs = set(spec["environments"]["helper"]["specs"])
    samtools_specs = set(spec["environments"]["samtools"]["specs"])

    assert {
        "python=3.10.12",
        "r-base=4.5.2",
        "pillow=11.3.0",
        "pyyaml=6.0.2",
        "jsonschema=4.25.1",
        "poppler=26.07.0",
        "imagemagick=7.1.2_27",
        "tesseract=5.5.2",
        "pip=26.1.2",
        "setuptools=75.1.0",
        "wheel=0.44.0",
    } == helper_specs
    assert {"samtools=1.18", "htslib=1.18"} == samtools_specs
    assert spec["micromamba"]["linux_64_sha256"] == (
        "e9683b483df06dbd3fdd8a37f1b6826d7e5caf4e85bf15a0af4fbad3d4ad1a58"
    )


def test_incompatible_zlib_families_are_isolated_in_two_prefixes() -> None:
    materials = _json("materials.lock.json")

    versions: dict[str, str] = {}
    for name, environment in materials["environments"].items():
        versions[name] = next(
            package["version"]
            for package in environment["packages"]
            if package["name"] == "libzlib"
        )

    assert versions == {"helper": "1.3.2", "samtools": "1.2.13"}
    definition = (CONTAINERS / "helpers.def").read_text(encoding="utf-8")
    assert "--prefix /opt/igv-helper" in definition
    assert "--prefix /opt/igv-samtools" in definition
    assert "LD_LIBRARY_PATH" not in definition


def test_builder_requires_clean_commit_and_never_reuses_a_named_output() -> None:
    script = (PROJECT_ROOT / "scripts/build-helper-sif.sh").read_text(
        encoding="utf-8"
    )

    assert "git -C \"${project_root}\" diff --quiet" in script
    assert "git -C \"${project_root}\" diff --cached --quiet" in script
    assert "ls-files --others --exclude-standard" in script
    assert "singularity-ce-4.5.0-1.el8.x86_64" in script
    assert '[[ ! -e "${candidate}" && ! -L "${candidate}" ]]' in script
    assert "singularity build --fakeroot --disable-cache" in script
    assert "git -C \"${project_root}\" archive --format=tar HEAD" in script
