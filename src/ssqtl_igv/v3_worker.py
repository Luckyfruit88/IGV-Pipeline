from __future__ import annotations

import argparse
import base64
import copy
import hashlib
import json
import os
import subprocess
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import asdict
from importlib.resources import files
from pathlib import Path
from typing import Any, Iterable

from .contracts import (
    V3_GENERIC_MANUAL_ASSERTIONS,
    V3_SSQTL_MANUAL_ASSERTIONS,
    validate_v3_case_result_document,
    validate_v3_task_document,
    validate_v3_terminal_bundle_document,
)
from .desktop import DesktopFailure, run_desktop_session
from .qc import inspect_png
from .utils import (
    atomic_write_json,
    atomic_write_text,
    read_jsonl,
    resource_contains_remote_url,
    sha256_file,
    sha256_json,
    utc_now,
)
from .violin import render_pdf_page


class PortableRenderConfig:
    def __init__(self, data: dict[str, Any]):
        self.data = data

    def get(self, dotted: str, default: Any = None) -> Any:
        value: Any = self.data
        for part in dotted.split("."):
            if not isinstance(value, dict) or part not in value:
                return default
            value = value[part]
        return value

    def path_value(self, dotted: str) -> Path:
        value = self.get(dotted)
        if value in (None, ""):
            raise ValueError(f"missing portable render path: {dotted}")
        return Path(str(value)).expanduser()


def _runtime_config(path: str | Path | None, genome: dict[str, Any]) -> PortableRenderConfig:
    source: Any
    if path is None:
        source = files("ssqtl_igv.resources").joinpath("v3-runtime.yaml")
        text = source.read_text(encoding="utf-8")
    else:
        source = Path(path).expanduser().resolve(strict=True)
        text = source.read_text(encoding="utf-8")
    try:
        import yaml
    except ModuleNotFoundError as exc:  # pragma: no cover - locked runtime dependency
        raise RuntimeError("PyYAML is required by the portable render runtime") from exc
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError(f"runtime configuration is not a mapping: {source}")
    data["genome"] = genome
    return PortableRenderConfig(data)


def _task_from_manifest(path: str | Path, task_id: str) -> dict[str, Any]:
    matches = [task for task in read_jsonl(path) if str(task.get("task_id")) == task_id]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one canonical task for {task_id}; observed {len(matches)}")
    return matches[0]


def _resources(task: dict[str, Any]) -> list[dict[str, Any]]:
    core = task["core"]
    resources: list[dict[str, Any]] = []
    for track in core["tracks"]:
        resources.extend((track["bam"], track["bai"]))
    resources.extend(core["reference"]["resources"].values())
    auxiliary = core["auxiliary"]
    if auxiliary["state"] == "PRESENT":
        resources.append(auxiliary)
    return resources


def _identity_matches(path: Path, identity: dict[str, Any]) -> bool:
    stat = path.stat()
    if "size" in identity and int(identity["size"]) != stat.st_size:
        return False
    if "mtime_ns" in identity and int(identity["mtime_ns"]) != stat.st_mtime_ns:
        return False
    expected = str(identity.get("sha256") or "").strip().lower()
    return not expected or sha256_file(path) == expected


def _validate_staged_inputs(
    task: dict[str, Any], staged: dict[str, str], alias_root: Path
) -> dict[str, Path]:
    expected = {str(item["stage_name"]): item for item in _resources(task)}
    if set(staged) != set(expected):
        raise RuntimeError(
            "staged input names differ from canonical task; "
            f"missing={sorted(set(expected) - set(staged))} "
            f"unexpected={sorted(set(staged) - set(expected))}"
        )
    alias_root.mkdir(parents=True, exist_ok=False)
    resolved: dict[str, Path] = {}
    for name, resource in expected.items():
        staged_path = Path(staged[name]).expanduser()
        path = staged_path.resolve(strict=True)
        if not path.is_file():
            raise RuntimeError(f"staged input is not a regular file: {name}:{path}")
        if not _identity_matches(path, resource["identity"]):
            raise RuntimeError(f"staged input identity changed after normalization: {name}")
        alias = alias_root / name
        alias.symlink_to(path)
        resolved[name] = alias
    return resolved


def _sequence_lengths(text: str, *, label: str) -> dict[str, int]:
    lengths: dict[str, int] = {}
    for line_number, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        fields = raw.split("\t")
        if len(fields) < 2:
            raise ValueError(f"{label} line {line_number} has fewer than two fields")
        name = fields[0]
        if name == "*":
            continue
        try:
            length = int(fields[1])
        except ValueError as exc:
            raise ValueError(f"{label} line {line_number} has an invalid length") from exc
        if not name or length < 1 or name in lengths:
            raise ValueError(f"{label} contains an invalid or duplicate contig: {name!r}")
        lengths[name] = length
    if not lengths:
        raise ValueError(f"{label} contains no positive-length contigs")
    return lengths


def _samtools_validate(
    tracks: Iterable[dict[str, Any]],
    staged: dict[str, Path],
    command: str,
    *,
    locus: dict[str, Any] | None = None,
    fai: Path | None = None,
) -> None:
    reference_length: int | None = None
    contig = str((locus or {}).get("contig", ""))
    locus_end = int((locus or {}).get("end", 0))
    if locus is not None:
        if not contig or locus_end < 1 or fai is None:
            raise ValueError("locus-aware BAM validation requires contig/end and the staged FAI")
        try:
            reference_lengths = _sequence_lengths(
                fai.read_text(encoding="utf-8"), label="reference FAI"
            )
        except (OSError, UnicodeDecodeError) as exc:
            raise ValueError(f"cannot read reference FAI: {exc}") from exc
        reference_length = reference_lengths.get(contig)
        if reference_length is None:
            raise ValueError(f"locus contig {contig} is absent from the reference FAI")
        if locus_end > reference_length:
            raise ValueError(
                f"locus end {locus_end} exceeds reference contig {contig} length {reference_length}"
            )
    for track in tracks:
        bam = staged[track["bam"]["stage_name"]]
        bai = staged[track["bai"]["stage_name"]]
        quick = subprocess.run(
            [command, "quickcheck", "-v", str(bam)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        if quick.returncode:
            raise ValueError(
                f"samtools quickcheck failed for {track['track_label']}: "
                + (quick.stderr.strip() or quick.stdout.strip())
            )
        index = subprocess.run(
            [command, "idxstats", "-X", str(bam), str(bai)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        if index.returncode or not index.stdout.strip():
            raise ValueError(
                f"samtools idxstats failed for {track['track_label']}: "
                + (index.stderr.strip() or index.stdout.strip())
            )
        if locus is not None:
            bam_lengths = _sequence_lengths(
                index.stdout, label=f"samtools idxstats for {track['track_label']}"
            )
            bam_length = bam_lengths.get(contig)
            if bam_length is None:
                raise ValueError(
                    f"locus contig {contig} is absent from BAM {track['track_label']}"
                )
            if bam_length != reference_length:
                raise ValueError(
                    f"BAM/reference contig length differs for {track['track_label']}:{contig}: "
                    f"{bam_length} != {reference_length}"
                )
            if locus_end > bam_length:
                raise ValueError(
                    f"locus end {locus_end} exceeds BAM contig {contig} length {bam_length}"
                )


def _samtools_check_explicit_index(
    command: str, bam: Path, bai: Path
) -> tuple[bool, str]:
    """Validate exactly the canonical BAM/BAI pair, never a default sidecar."""

    try:
        quick = subprocess.run(
            [command, "quickcheck", "-v", str(bam)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        index = subprocess.run(
            [command, "idxstats", "-X", str(bam), str(bai)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"explicit BAM/BAI validation failed: {exc}") from exc
    if quick.returncode:
        return False, (quick.stdout + quick.stderr).strip() or "samtools quickcheck failed"
    if index.returncode or not index.stdout.strip():
        return False, (index.stdout + index.stderr).strip() or "samtools idxstats -X failed"
    return True, ""


def _local_genome(
    task: dict[str, Any], staged: dict[str, Path], output: Path, alias_root: Path
) -> dict[str, Any]:
    reference = task["core"]["reference"]
    resources = reference["resources"]
    definition_source = staged[resources["definition"]["stage_name"]]
    if resource_contains_remote_url(definition_source):
        raise ValueError("genome definition contains a remote URL")
    try:
        definition = json.loads(definition_source.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"genome definition must be local JSON: {exc}") from exc
    if not isinstance(definition, dict):
        raise ValueError("genome definition must contain one JSON object")
    definition.update(
        {
            "id": reference["id"],
            "name": reference["display_name"],
            "fastaURL": str(staged[resources["fasta"]["stage_name"]]),
            "indexURL": str(staged[resources["fai"]["stage_name"]]),
            "cytobandURL": str(staged[resources["cytoband"]["stage_name"]]),
            "tracks": [],
        }
    )
    definition_path = output / "runtime" / "genome.json"
    atomic_write_json(definition_path, definition)
    if resource_contains_remote_url(definition_path):
        raise ValueError("materialized genome definition contains a remote URL")
    definition_alias = alias_root / "local-genome.json"
    definition_alias.symlink_to(definition_path.resolve(strict=True))
    return {
        "id": reference["id"],
        "display_name": reference["display_name"],
        "definition": str(definition_alias),
        "fasta": str(staged[resources["fasta"]["stage_name"]]),
        "fai": str(staged[resources["fai"]["stage_name"]]),
        "cytoband": str(staged[resources["cytoband"]["stage_name"]]),
        "annotation": str(staged[resources["annotation"]["stage_name"]]),
        "annotation_version": reference["version"],
    }


def _path_for_batch(path: str | Path) -> str:
    # Preserve the controlled, whitespace-free alias instead of resolving it
    # back to a user path whose basename may contain spaces or apostrophes.
    value = os.path.abspath(os.fspath(path))
    if any(character.isspace() for character in value):
        raise ValueError(f"IGV batch paths cannot contain whitespace: {value}")
    if "://" in value:
        raise ValueError(f"remote IGV paths are prohibited: {value}")
    return value


def _build_batch(
    task: dict[str, Any],
    staged: dict[str, Path],
    genome: dict[str, Any],
    output: Path,
    alias_root: Path,
) -> tuple[Path, Path]:
    control = alias_root / "control"
    control.mkdir(parents=True, exist_ok=True)
    ready = control / "batch_ready.png"
    session_root = ET.Element(
        "Session",
        {
            "genome": _path_for_batch(genome["definition"]),
            "hasGeneTrack": "false",
            "hasSequenceTrack": "true",
            "locus": task["core"]["locus"]["raw"],
            "version": "8",
        },
    )
    resources = ET.SubElement(session_root, "Resources")
    display_contract: list[dict[str, Any]] = []
    tracks = sorted(task["core"]["tracks"], key=lambda row: int(row["track_order"]))
    for track in tracks:
        order = int(track["track_order"])
        group = str(track.get("group") or "").strip()
        label = str(track["track_label"])
        display_name = f"{order:03d} | " + (f"[{group}] " if group else "") + label
        ET.SubElement(
            resources,
            "Resource",
            {
                "path": _path_for_batch(staged[track["bam"]["stage_name"]]),
                "index": _path_for_batch(staged[track["bai"]["stage_name"]]),
                "name": display_name,
                "label": display_name,
            },
        )
        display_contract.append(
            {
                "track_order": order,
                "track_label": label,
                "group": group or None,
                "igv_display_name": display_name,
            }
        )
    annotation = task["core"]["reference"]["resources"]["annotation"]
    annotation_name = f"Annotation | {task['core']['reference']['version']}"
    ET.SubElement(
        resources,
        "Resource",
        {
            "path": _path_for_batch(staged[annotation["stage_name"]]),
            "name": annotation_name,
            "label": annotation_name,
        },
    )
    session_path = output / "igv.session.xml"
    atomic_write_text(
        session_path,
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        + ET.tostring(session_root, encoding="unicode")
        + "\n",
    )
    atomic_write_json(
        output / "track_display_contract.json",
        {
            "schema_version": "3.0-track-display-contract",
            "meaning": "group controls display prefix only and has no scientific semantics",
            "tracks": display_contract,
            "annotation_display_name": annotation_name,
        },
    )
    session_alias = alias_root / "igv.session.xml"
    session_alias.symlink_to(session_path.resolve(strict=True))
    lines = [
        f"genome {_path_for_batch(genome['definition'])}",
        "new",
        "setSleepInterval 500",
        "maxPanelHeight 2160",
        f"snapshotDirectory {_path_for_batch(control)}",
        "preference SAM.SHOW_COV_TRACK true",
        "preference SAM.SHOW_ALIGNMENT_TRACK true",
        "preference SAM.SHOW_JUNCTION_TRACK true",
    ]
    lines.extend(
        [
            f"load {_path_for_batch(session_alias)}",
            "collapse",
            f"setSequenceStrand {task['core']['strand']}",
            f"goto {task['core']['locus']['raw']}",
            f"snapshot {ready.name}",
        ]
    )
    batch = output / "igv.batch.txt"
    atomic_write_text(batch, "\n".join(lines) + "\n")
    return batch, ready


def _decoded_pixel_sha(path: Path) -> str:
    from PIL import Image

    with Image.open(path) as source:
        image = source.convert("RGB")
        digest = hashlib.sha256()
        digest.update(f"RGB:{image.width}x{image.height}\0".encode("ascii"))
        digest.update(image.tobytes())
        return digest.hexdigest()


def _compose_review(
    task: dict[str, Any],
    staged: dict[str, Path],
    raw: Path,
    review: Path,
    config: PortableRenderConfig,
    *,
    fake: bool = False,
) -> dict[str, Any]:
    from PIL import Image, ImageChops

    auxiliary = task["core"]["auxiliary"]
    with Image.open(raw) as source:
        client = source.convert("RGB")
    source_sha = _decoded_pixel_sha(raw)
    if auxiliary["state"] == "ABSENT":
        review.parent.mkdir(parents=True, exist_ok=True)
        client.save(review, format="PNG", compress_level=6, optimize=False)
        return {
            "schema_version": "3.0",
            "mode": "IGV_ONLY",
            "canvas": [client.width, client.height],
            "igv_box": [0, 0, client.width, client.height],
            "source_igv_decoded_pixel_sha256": source_sha,
            "final_igv_decoded_pixel_sha256": _decoded_pixel_sha(review),
            "igv_pixel_identity": True,
        }

    source_path = staged[auxiliary["stage_name"]]
    converted = review.parent / "auxiliary.png"
    try:
        if auxiliary["kind"] == "PDF":
            if fake:
                from .test_doubles import fake_pdf_page

                fake_pdf_page(source_path, int(auxiliary["page"]), converted)
            else:
                render_pdf_page(
                    source_path,
                    int(auxiliary["page"]),
                    converted,
                    pdftoppm=config.get("binaries.pdftoppm", "pdftoppm"),
                    dpi=int(config.get("render.auxiliary_dpi", 180)),
                    timeout=int(config.get("timeouts.pdftoppm_seconds", 300)),
                )
            auxiliary_path = converted
        else:
            auxiliary_path = source_path
        with Image.open(auxiliary_path) as source:
            panel = source.convert("RGB")
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        raise ValueError(
            f"auxiliary {auxiliary['kind']} cannot be decoded or rendered: {exc}"
        ) from exc
    panel_width = int(config.get("compose.auxiliary_panel_width", 720))
    scale = min(panel_width / panel.width, client.height / panel.height)
    resized = panel.resize(
        (max(1, round(panel.width * scale)), max(1, round(panel.height * scale))),
        Image.Resampling.LANCZOS,
    )
    canvas = Image.new("RGB", (client.width + panel_width, client.height), "white")
    canvas.paste(client, (0, 0))
    x = client.width + (panel_width - resized.width) // 2
    y = (client.height - resized.height) // 2
    canvas.paste(resized, (x, y))
    review.parent.mkdir(parents=True, exist_ok=True)
    canvas.save(review, format="PNG", compress_level=6, optimize=False)
    with Image.open(review) as final:
        left = final.convert("RGB").crop((0, 0, client.width, client.height))
    identical = ImageChops.difference(client, left).getbbox() is None
    if not identical:
        raise RuntimeError("composition changed native IGV pixels")
    left_path = review.parent / ".left-verification.png"
    left.save(left_path, format="PNG")
    final_sha = _decoded_pixel_sha(left_path)
    left_path.unlink(missing_ok=True)
    return {
        "schema_version": "3.0",
        "mode": "IGV_WITH_AUXILIARY",
        "auxiliary_kind": auxiliary["kind"],
        "auxiliary_page": auxiliary.get("page"),
        "canvas": [canvas.width, canvas.height],
        "igv_box": [0, 0, client.width, client.height],
        "auxiliary_box": [client.width, 0, panel_width, client.height],
        "source_igv_decoded_pixel_sha256": source_sha,
        "final_igv_decoded_pixel_sha256": final_sha,
        "igv_pixel_identity": identical and source_sha == final_sha,
    }


def _fake_desktop(config: PortableRenderConfig, output: Path, metadata: Path) -> dict[str, Any]:
    from .test_doubles import fake_desktop_session

    result = fake_desktop_session(
        config,
        output_png=output,
        metadata_path=metadata,
        log_directory=output.parent.parent / "logs",
    )
    return {"test_double": True, "desktop": asdict(result)}


def _run_generic(
    task: dict[str, Any],
    staged: dict[str, Path],
    output: Path,
    config_path: str | Path | None,
    fake: bool,
    alias_root: Path,
) -> dict[str, Any]:
    genome = _local_genome(task, staged, output, alias_root)
    config = _runtime_config(config_path, genome)
    if not fake:
        reference_resources = task["core"]["reference"]["resources"]
        _samtools_validate(
            task["core"]["tracks"],
            staged,
            config.get("binaries.samtools", "samtools"),
            locus=task["core"]["locus"],
            fai=staged[reference_resources["fai"]["stage_name"]],
        )
    batch, ready = _build_batch(task, staged, genome, output, alias_root)
    raw = output / "raw" / "igv.png"
    capture_metadata = output / "raw" / "capture.json"
    if fake:
        desktop_evidence = _fake_desktop(config, raw, capture_metadata)
    else:
        try:
            desktop = run_desktop_session(
                config,
                batch_path=batch,
                ready_marker=ready,
                expected_locus=task["core"]["locus"]["raw"],
                igv_directory=output / "runtime" / "igv_home",
                log_directory=output / "logs",
                capture_directory=output / "raw" / "capture",
                output_png=raw,
                metadata_path=capture_metadata,
            )
        except DesktopFailure as exc:
            raise ValueError(f"{exc.code}: {exc}") from exc
        desktop_evidence = {
            "test_double": False,
            "wall_time_seconds": desktop.wall_time_seconds,
            "peak_rss_gb": desktop.peak_rss_gb,
        }
    raw_qc = inspect_png(raw, min_width=1700, min_height=1800, min_stddev=0.5)
    if raw_qc["status"] != "PASS":
        raise ValueError(f"raw IGV QC failed: {raw_qc}")
    review = output / "review.png"
    layout = _compose_review(task, staged, raw, review, config, fake=fake)
    review_qc = inspect_png(review, min_width=1700, min_height=1800, min_stddev=0.5)
    if review_qc["status"] != "PASS" or not layout["igv_pixel_identity"]:
        raise ValueError(f"review image QC failed: {review_qc}; layout={layout}")
    atomic_write_json(output / "raw_qc.json", raw_qc)
    atomic_write_json(output / "review_qc.json", review_qc)
    atomic_write_json(output / "layout.json", layout)
    atomic_write_json(
        output / "scientific_qc.json",
        {
            "schema_version": "3.0",
            "adapter_type": "generic",
            "scientific_interpretation": "NOT_APPLICABLE",
            "automatic_scope": "MECHANICAL_EVIDENCE_ONLY",
        },
    )
    return {
        "review": review,
        "scientific_qc": output / "scientific_qc.json",
        "raw_igv": raw,
        "capture_metadata": capture_metadata,
        "layout": output / "layout.json",
        "raw_qc": output / "raw_qc.json",
        "review_qc": output / "review_qc.json",
        "track_display_contract": output / "track_display_contract.json",
        "igv_session": output / "igv.session.xml",
        "pixel_identity": {
            "source_igv_decoded_pixel_sha256": layout[
                "source_igv_decoded_pixel_sha256"
            ],
            "final_igv_decoded_pixel_sha256": layout[
                "final_igv_decoded_pixel_sha256"
            ],
            "igv_pixel_identity": layout["igv_pixel_identity"],
        },
        "desktop": desktop_evidence,
        "test_double": fake,
    }


def _without_source_paths(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_source_paths(item)
            for key, item in value.items()
            if key != "source_path"
        }
    if isinstance(value, list):
        return [_without_source_paths(item) for item in value]
    return value


def _ssqtl_render_task(task: dict[str, Any]) -> dict[str, Any]:
    """Project only the native display locus; retain the schema-v3 core resources."""

    render_task = copy.deepcopy(task)
    overview = task["adapter_data"]["regions"]["overview"]
    render_task["core"]["locus"] = {
        "raw": f"{overview['chrom']}:{overview['start']}-{overview['end']}",
        "contig": str(overview["chrom"]),
        "start": int(overview["start"]),
        "end": int(overview["end"]),
        "coordinate_system": "1-based-inclusive",
    }
    return render_task


def _write_native_ssqtl_evidence(
    task: dict[str, Any], output: Path, mechanical: dict[str, Any]
) -> dict[str, Any]:
    """Freeze native schema-v3 ssQTL evidence without retaining v2 stage bundles."""

    adapter = task["adapter_data"]
    empty_groups = [
        genotype
        for genotype in ("0/0", "0/1", "1/1")
        if adapter["genotype_groups"][genotype]["empty"]
    ]
    violin = adapter["violin"]
    reference_context = adapter["reference_context"]
    incomplete_reasons: list[str] = []
    if empty_groups:
        incomplete_reasons.append("EMPTY_GENOTYPE_GROUPS")
    if violin["state"] != "PRESENT":
        incomplete_reasons.append("VIOLIN_UNAVAILABLE")
    if reference_context["available"] is not True:
        incomplete_reasons.append("REFERENCE_CONTEXT_UNAVAILABLE")
    evidence_state = "EVIDENCE_INCOMPLETE" if incomplete_reasons else "COMPLETE"
    interpretation = "INDETERMINATE" if incomplete_reasons else "PENDING"
    render_mode = "ANNOTATION_ONLY_NO_BAM" if not task["core"]["tracks"] else "BAM_TRACKS"
    preparation_sha256 = sha256_json(
        _without_source_paths(adapter["preparation_evidence"])
    )

    display_path = Path(mechanical["track_display_contract"])
    display = json.loads(display_path.read_text(encoding="utf-8"))
    display.update(
        {
            "adapter_type": "ssqtl",
            "meaning": (
                "group is only an IGV display prefix; genotype meaning is defined by "
                "adapter_data and remains subject to human scientific review"
            ),
            "render_mode": render_mode,
        }
    )
    atomic_write_json(display_path, display)

    checks = [
        {
            "code": "NATIVE_V3_TASK_CONTRACT",
            "status": "PASS",
            "evidence": {
                "adapter_schema_version": adapter["adapter_schema_version"],
                "input_fingerprint": task["input_fingerprint"],
            },
        },
        {
            "code": "EXPLICIT_BAM_BAI_VALIDATION",
            "status": "PASS",
            "evidence": {
                "track_count": len(task["core"]["tracks"]),
                "command_contract": "samtools idxstats -X BAM BAI",
                "not_applicable_reason": (
                    "no eligible BAM tracks; annotation-only evidence was rendered"
                    if not task["core"]["tracks"]
                    else None
                ),
            },
        },
        {
            "code": "GENOTYPE_GROUP_COVERAGE",
            "status": "INCOMPLETE" if empty_groups else "PASS",
            "evidence": {
                "empty_genotype_groups": empty_groups,
                "selected_sample_count": len(adapter["selected_samples"]),
                "render_mode": render_mode,
            },
        },
        {
            "code": "VIOLIN_EXACT_PAIR",
            "status": "PASS" if violin["state"] == "PRESENT" else "INCOMPLETE",
            "evidence": copy.deepcopy(violin),
        },
        {
            "code": "AG_REFERENCE_CONTEXT",
            "status": (
                "PASS" if reference_context["available"] is True else "INCOMPLETE"
            ),
            "evidence": copy.deepcopy(reference_context),
        },
        {
            "code": "NATIVE_IGV_PIXEL_IDENTITY",
            "status": "PASS",
            "evidence": copy.deepcopy(mechanical["pixel_identity"]),
        },
        {
            "code": "HUMAN_SCIENTIFIC_REVIEW_REQUIRED",
            "status": "PENDING",
            "evidence": {"required_assertions": list(V3_SSQTL_MANUAL_ASSERTIONS)},
        },
    ]
    scientific_qc = {
        "schema_version": "3.0-ssqtl-scientific-qc",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task["task_id"],
        "input_fingerprint": task["input_fingerprint"],
        "status": "PASS_WITH_INCOMPLETE_EVIDENCE" if incomplete_reasons else "PASS",
        "automatic_scope": "MECHANICAL_AND_INPUT_IDENTITY_ONLY",
        "evidence_state": evidence_state,
        "scientific_interpretation": interpretation,
        "empty_genotype_groups": empty_groups,
        "incomplete_reasons": incomplete_reasons,
        "checks": checks,
        "manual_review_required": list(V3_SSQTL_MANUAL_ASSERTIONS),
        "test_double": bool(mechanical.get("test_double")),
    }
    scientific_qc_path = output / "scientific_qc.json"
    atomic_write_json(scientific_qc_path, scientific_qc)

    scientific_case = {
        "schema_version": "3.0-ssqtl-scientific-case-evidence",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task["task_id"],
        "manifest_order": task["manifest_order"],
        "input_fingerprint": task["input_fingerprint"],
        "adapter_schema_version": "3.0-ssqtl",
        "render_state": "SUCCEEDED",
        "render_mode": render_mode,
        "evidence_state": evidence_state,
        "scientific_interpretation": interpretation,
        "empty_genotype_groups": empty_groups,
        "incomplete_reasons": incomplete_reasons,
        "selected_sample_count": len(adapter["selected_samples"]),
        "preparation_evidence_sha256": preparation_sha256,
        "scientific_qc_sha256": sha256_file(scientific_qc_path),
        "raw_igv_sha256": sha256_file(mechanical["raw_igv"]),
        "capture_metadata_sha256": sha256_file(mechanical["capture_metadata"]),
        "layout_sha256": sha256_file(mechanical["layout"]),
        "review_image_sha256": sha256_file(mechanical["review"]),
        "pixel_identity": copy.deepcopy(mechanical["pixel_identity"]),
        "failures": [],
    }
    scientific_case_path = output / "scientific_case_evidence.json"
    atomic_write_json(scientific_case_path, scientific_case)
    qc_evidence = {
        "schema_version": "3.0-ssqtl-scientific-qc-evidence",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task["task_id"],
        "input_fingerprint": task["input_fingerprint"],
        "status": scientific_qc["status"],
        "evidence_state": evidence_state,
        "scientific_interpretation": interpretation,
        "scientific_case_evidence_sha256": sha256_file(scientific_case_path),
        "scientific_qc_sha256": sha256_file(scientific_qc_path),
        "failure_set_sha256": sha256_json([]),
    }
    qc_evidence_path = output / "scientific_qc_evidence.json"
    atomic_write_json(qc_evidence_path, qc_evidence)

    mechanical.update(
        {
            "scientific_qc": scientific_qc_path,
            "scientific_case_evidence": scientific_case_path,
            "scientific_qc_evidence": qc_evidence_path,
            "scientific_qc_status": scientific_qc["status"],
            "evidence_state": evidence_state,
            "scientific_interpretation": interpretation,
            "adapter_evidence": {
                "adapter_schema_version": "3.0-ssqtl",
                "scientific_evidence_available": True,
                "scientific_case_evidence_sha256": sha256_file(scientific_case_path),
                "scientific_qc_evidence_sha256": sha256_file(qc_evidence_path),
                "scientific_evidence_state": evidence_state,
                "scientific_result_interpretation": interpretation,
                "scientific_failure_set_sha256": sha256_json([]),
                "empty_genotype_groups": empty_groups,
            },
            "render_status": "SUCCEEDED",
        }
    )
    return mechanical


def _run_ssqtl(
    task: dict[str, Any],
    staged: dict[str, Path],
    output: Path,
    fake: bool,
    schema_dir: str | Path | None,
    runtime_config: str | Path | None,
    alias_root: Path,
) -> dict[str, Any]:
    """Render the native v3 task and emit only native v3 scientific evidence."""

    del schema_dir
    render_task = _ssqtl_render_task(task)
    mechanical = _run_generic(
        render_task,
        staged,
        output,
        runtime_config,
        fake,
        alias_root,
    )
    return _write_native_ssqtl_evidence(task, output, mechanical)


def _case_result(
    task: dict[str, Any], output: Path, run_relative: str, evidence: dict[str, Any] | None, failure: str | None
) -> dict[str, Any]:
    adapter_type = "ssqtl" if task["adapter_id"] == "ssqtl" else "generic"
    test_double = bool(evidence and evidence.get("test_double"))
    eligible = failure is None and not test_double
    artifacts: dict[str, Any] = {}
    if evidence:
        for key, path_key in (
            ("review_image", "review"),
            ("scientific_qc", "scientific_qc"),
            ("raw_igv", "raw_igv"),
            ("capture_metadata", "capture_metadata"),
            ("layout", "layout"),
            ("raw_qc", "raw_qc"),
            ("review_qc", "review_qc"),
            ("track_display_contract", "track_display_contract"),
            ("igv_session", "igv_session"),
            ("scientific_case_evidence", "scientific_case_evidence"),
            ("scientific_qc_evidence", "scientific_qc_evidence"),
        ):
            if path_key not in evidence:
                continue
            path = Path(evidence[path_key])
            artifacts[key] = {
                "relative_path": f"{run_relative}/{path.relative_to(output)}",
                "sha256": sha256_file(path),
                "size": path.stat().st_size,
            }
    if adapter_type == "generic":
        adapter_evidence = {
            "adapter_schema_version": "3.0-generic",
            "scientific_interpretation": "NOT_APPLICABLE",
        }
    elif evidence and isinstance(evidence.get("adapter_evidence"), dict):
        adapter_evidence = dict(evidence["adapter_evidence"])
    else:
        adapter_evidence = {
            "adapter_schema_version": "3.0-ssqtl",
            "scientific_evidence_available": False,
        }
    evidence_state = (
        "UNAVAILABLE"
        if failure is not None or (test_double and adapter_type == "generic")
        else str(evidence.get("evidence_state", "COMPLETE"))
        if evidence
        else "COMPLETE"
    )
    scientific_interpretation = (
        "NOT_APPLICABLE"
        if adapter_type == "generic"
        else str(evidence.get("scientific_interpretation", "PENDING"))
        if evidence
        else "INDETERMINATE"
    )
    return {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task["task_id"],
        "manifest_order": task["manifest_order"],
        "input_fingerprint": task["input_fingerprint"],
        "adapter_type": adapter_type,
        "adapter_evidence": adapter_evidence,
        "eligible": eligible,
        "render_state": "SUCCEEDED" if failure is None else "FAILED",
        "evidence_state": evidence_state,
        "artifact_review_state": "REVIEW_PENDING",
        "scientific_interpretation": scientific_interpretation,
        "publication_state": "NOT_READY",
        "debug_only": test_double,
        "required_manual_assertions": list(
            V3_GENERIC_MANUAL_ASSERTIONS
            if adapter_type == "generic"
            else V3_SSQTL_MANUAL_ASSERTIONS
        ),
        "artifacts": artifacts,
        "pixel_identity": evidence.get("pixel_identity") if evidence else None,
        "failures": [] if failure is None else [{"code": "CASE_RENDER_FAILED", "message": failure}],
        "created_at": utc_now(),
    }


def run_portable_task(
    task: dict[str, Any],
    staged_inputs: dict[str, str],
    output_dir: str | Path,
    *,
    runtime_config: str | Path | None = None,
    fake_runtime: bool = False,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    validate_v3_task_document(task, schema_dir=schema_dir)
    output = Path(output_dir).expanduser().resolve(strict=False)
    if output.exists():
        raise FileExistsError(f"portable task output already exists: {output}")
    output.mkdir(parents=True)
    run_relative = f"results/cases/{task['task_id']}"
    failure: str | None = None
    evidence: dict[str, Any] | None = None
    try:
        preflight = task["core"]["preflight"]
        if preflight["state"] != "READY":
            # CASE_INPUT_INVALID is terminal before rendering and therefore cannot
            # produce an eligible review artifact or scientific interpretation.
            messages = [str(row.get("message", row)) for row in preflight.get("errors", [])]
            raise ValueError("canonical preflight failed: " + " | ".join(messages))
        with tempfile.TemporaryDirectory(prefix="igv-snapshot-v3-") as temporary:
            alias_root = Path(temporary)
            staged = _validate_staged_inputs(task, staged_inputs, alias_root / "inputs")
            if task["adapter_id"] == "generic":
                evidence = _run_generic(
                    task,
                    staged,
                    output,
                    runtime_config,
                    fake_runtime,
                    alias_root,
                )
            else:
                evidence = _run_ssqtl(
                    task,
                    staged,
                    output,
                    fake_runtime,
                    schema_dir,
                    runtime_config,
                    alias_root,
                )
    except (ValueError, subprocess.TimeoutExpired) as exc:
        failure = f"{type(exc).__name__}: {exc}"
    result = _case_result(task, output, run_relative, evidence, failure)
    validate_v3_case_result_document(result, schema_dir=schema_dir)
    atomic_write_json(output / "case_result.json", result)
    terminal = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "run_id": task["run_id"],
        "generation_id": task["generation_id"],
        "task_id": task["task_id"],
        "manifest_order": task["manifest_order"],
        "input_fingerprint": task["input_fingerprint"],
        "status": "SUCCEEDED" if result["eligible"] else "DOMAIN_FAILED",
        "case_result_sha256": sha256_file(output / "case_result.json"),
        "case_result_size": (output / "case_result.json").stat().st_size,
        "artifact_set_sha256": hashlib.sha256(
            json.dumps(
                result["artifacts"],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest(),
    }
    validate_v3_terminal_bundle_document(terminal, result, schema_dir=schema_dir)
    atomic_write_json(output / "terminal_bundle.json", terminal)
    return result


def _input_mapping(values: list[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        name, separator, path = value.partition("=")
        if not separator or not name or not path or name in result:
            raise ValueError(f"invalid or duplicate --staged-input: {value!r}")
        result[name] = path
    return result


def _encoded_input_mapping(values: list[str]) -> dict[str, str]:
    decoded: list[str] = []
    for value in values:
        try:
            padding = "=" * (-len(value) % 4)
            raw = base64.urlsafe_b64decode((value + padding).encode("ascii"))
            decoded.append(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, UnicodeEncodeError) as exc:
            raise ValueError("invalid --staged-input-b64 payload") from exc
    return _input_mapping(decoded)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one schema-3.0 portable IGV case")
    parser.add_argument("--task-manifest", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--staged-input", action="append", default=[])
    parser.add_argument("--staged-input-b64", action="append", default=[])
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--runtime-config")
    parser.add_argument("--schema-dir")
    parser.add_argument("--fake-runtime", action="store_true")
    args = parser.parse_args(argv)
    task = _task_from_manifest(args.task_manifest, args.task_id)
    if args.staged_input and args.staged_input_b64:
        parser.error("use only one staged input encoding")
    staged = (
        _encoded_input_mapping(args.staged_input_b64)
        if args.staged_input_b64
        else _input_mapping(args.staged_input)
    )
    result = run_portable_task(
        task,
        staged,
        args.output_dir,
        runtime_config=args.runtime_config,
        fake_runtime=args.fake_runtime,
        schema_dir=args.schema_dir,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
