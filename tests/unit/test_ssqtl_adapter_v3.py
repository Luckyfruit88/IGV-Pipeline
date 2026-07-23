from __future__ import annotations

import copy
import json
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

from ssqtl_igv.contracts import validate_v3_task_document, v3_task_fingerprint
from ssqtl_igv.identity import canonical_fingerprint, file_identity
from ssqtl_igv.ssqtl_adapter_v3 import normalize_ssqtl_inputs
from ssqtl_igv.utils import atomic_write_json, read_jsonl, sha256_file, sha256_json, write_jsonl
from ssqtl_igv.v3_cli import _parser
from ssqtl_igv.v3_manifest import _load_reference
from ssqtl_igv.v3_worker import _samtools_check_explicit_index, run_portable_task


def _reference(root: Path) -> Path:
    root.mkdir()
    resources = {
        "definition": ("genome.json", b"{}\n"),
        "fasta": ("genome.fa", b">chr1\n" + b"A" * 300 + b"\n"),
        "fai": ("genome.fa.fai", b"chr1\t300\t6\t300\t301\n"),
        "cytoband": ("cytoband.txt.gz", b"cytoband"),
        "annotation": ("annotation.gff.gz", b"annotation"),
    }
    for _role, (name, data) in resources.items():
        (root / name).write_bytes(data)
    document = {
        "schema_version": "3.0",
        "id": "fixture-hg38",
        "display_name": "Fixture GRCh38",
        "version": "fixture-v1",
        "resources": {
            role: {"path": name, "sha256": sha256_file(root / name)}
            for role, (name, _data) in resources.items()
        },
    }
    path = root / "reference.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return path


def _input_root(root: Path) -> Path:
    (root / "rds").mkdir(parents=True)
    (root / "violin").mkdir()
    (root / "tracks").mkdir()
    (root / "associations.csv").write_text(
        "AG_site,SNP,strand\nchr1:100-101,chr1.105_A.G,+\n", encoding="utf-8"
    )
    (root / "rds" / "AGratio_SNPgeno_pos_chr1_list.rds").write_bytes(b"rds-fixture")
    (root / "violin" / "violin_plots_pos_chr1.pdf").write_bytes(b"pdf-fixture")
    (root / "tracks" / "sample-1.bam").write_bytes(b"bam-fixture")
    (root / "tracks" / "sample-1.bam.bai").write_bytes(b"bai-fixture")
    (root / "bam_lookup.csv").write_text(
        "sample_id,bam\nsample-1,tracks/sample-1.bam\n", encoding="utf-8"
    )
    return root


def _prepared_task(
    input_root: Path,
    reference_path: Path,
    *,
    include_track_sha256: bool = True,
) -> dict[str, object]:
    reference, _source = _load_reference(reference_path)
    bam = input_root / "tracks" / "sample-1.bam"
    bai = input_root / "tracks" / "sample-1.bam.bai"
    pdf = input_root / "violin" / "violin_plots_pos_chr1.pdf"
    association_sha = sha256_file(input_root / "associations.csv")
    resources = {
        role: {
            "source_path": resource["source_path"],
            "stage_name": resource["stage_name"],
            "identity": copy.deepcopy(resource["identity"]),
        }
        for role, resource in reference["resources"].items()
    }
    task: dict[str, object] = {
        "schema_version": "2.0",
        "pipeline_version": "2.0.0",
        "run_id": "run_001",
        "generation_id": "generation_001",
        "task_id": "AG_chr1_100_101__SNP_chr1_105_A_G",
        "manifest_order": 1,
        "association": {"row": 1, "input_sha256": association_sha},
        "figure_contract_id": "v031_native_igv_pixel_exact",
        "gui_settle_contract_id": "v031_toolbar_locus_settle_v1",
        "ag": {
            "raw": "chr1:100-101", "chrom": "chr1", "source_start": 100,
            "source_end": 101, "start": 100, "end": 101,
        },
        "snp": {"raw": "chr1.105_A.G", "chrom": "chr1", "position": 105, "ref": "A", "alt": "G"},
        "strand": "+",
        "regions": {
            "overview": {"chrom": "chr1", "start": 45, "end": 156},
            "detail": {"chrom": "chr1", "start": 88, "end": 113},
        },
        "statistics": {
            "n_total": 1, "n_0": 1, "n_1": 0, "n_2": 0,
            "eligible_n_0": 1, "eligible_n_1": 0, "eligible_n_2": 0,
            "beta": 0.5, "abs_tvalue": 2.0,
        },
        "genotype_groups": {
            "0/0": {"selected_count": 1, "empty": False},
            "0/1": {"selected_count": 0, "empty": True},
            "1/1": {"selected_count": 0, "empty": True},
        },
        "tracks": [{
            "sample_id": "sample-1", "genotype": "0/0", "dosage": 0,
            "ratio": 0.25, "selection_label": "all", "bam": str(bam), "bai": str(bai),
            "stage_bam": "unused.bam", "stage_bai": "unused.bam.bai",
            "bam_identity": file_identity(
                bam, sha256=sha256_file(bam) if include_track_sha256 else None
            ),
            "bai_identity": file_identity(
                bai, sha256=sha256_file(bai) if include_track_sha256 else None
            ),
            "bai_fresh": True,
        }],
        "plot": {
            "state": "PRESENT", "pdf": str(pdf), "stage_pdf": "unused.pdf", "page": 1,
            "match_key": {"ag_site": "chr1:100-101", "snp": "chr1.105_A.G"},
            "pdf_identity": file_identity(pdf, sha256=sha256_file(pdf)),
        },
        "reference": {
            "id": reference["id"], "display_name": reference["display_name"],
            "annotation_version": reference["version"],
            "resource_fingerprint": sha256_json(resources), "resources": resources,
        },
        "reference_context": {
            "available": True, "chrom": "chr1", "start": 100, "end": 101,
            "strand": "+", "genomic_sequence": "AG", "transcript_sequence": "AG",
            "expected_transcript_sequence": "AG", "canonical_ag": True,
        },
        "render_contract": {
            "screen_width": 1920, "screen_height": 2160, "screen_depth": 24,
            "violin_panel_width": 720, "igv_version": "2.16.2",
            "overview_padding": 55, "detail_padding": 12,
            "policy_fingerprint": "a" * 64,
        },
        "preflight_state": "READY",
        "preflight_warnings": [{"code": "EMPTY_GENOTYPE_GROUP", "message": "0/1,1/1"}],
        "preflight_errors": [], "shard_hint": "chr1_pos",
        "estimated_runtime_seconds": 90.0,
    }
    task["input_fingerprint"] = canonical_fingerprint(task)
    return task


def _fake_science_normalizer(
    reference_path: Path, *, include_track_sha256: bool = True
):
    def run(_params: Path, output: Path, **kwargs):
        destination = Path(output)
        destination.mkdir()
        task = _prepared_task(
            Path(kwargs["associations"]).parent,
            reference_path,
            include_track_sha256=include_track_sha256,
        )
        task["run_id"] = kwargs["run_id"]
        task["generation_id"] = kwargs["generation_id"]
        task.pop("input_fingerprint")
        task["input_fingerprint"] = canonical_fingerprint(task)
        tasks = destination / "tasks.jsonl"
        write_jsonl(tasks, [task])
        return {"tasks": str(tasks)}
    return run


def _fake_r_prepare(
    _params: Path,
    associations: Path,
    _rds_dir: Path,
    bam_lookup: Path,
    output: Path,
    *,
    r_wrapper: Path,
    r_implementation: Path,
) -> dict[str, object]:
    destination = Path(output)
    destination.mkdir()
    cases = destination / "prepared_cases.tsv"
    samples = destination / "prepared_samples.tsv"
    stdout = destination / "r_prepare.stdout.log"
    stderr = destination / "r_prepare.stderr.log"
    cases.write_text("case_id\ncase-1\n", encoding="utf-8")
    samples.write_text("case_id\tsample_id\ncase-1\tsample-1\n", encoding="utf-8")
    stdout.write_text("fake R preparation completed\n", encoding="utf-8")
    stderr.write_text("", encoding="utf-8")
    report = {
        "schema_version": "2.0-r-prepare",
        "status": "PASS",
        "started_at": "2026-07-21T00:00:00Z",
        "finished_at": "2026-07-21T00:00:01Z",
        "wall_time_seconds": 1.0,
        "association_sha256": sha256_file(associations),
        "bam_lookup_sha256": sha256_file(bam_lookup),
        "r_wrapper_sha256": sha256_file(r_wrapper),
        "r_implementation_sha256": sha256_file(r_implementation),
        "case_count": 1,
        "sample_count": 1,
        "prepared_cases_sha256": sha256_file(cases),
        "prepared_samples_sha256": sha256_file(samples),
        "stdout_sha256": sha256_file(stdout),
        "stderr_sha256": sha256_file(stderr),
    }
    report_path = destination / "r_prepare.json"
    atomic_write_json(report_path, report)
    return {
        **report,
        "output_dir": str(destination),
        "prepared_cases": str(cases),
        "prepared_samples": str(samples),
        "report": str(report_path),
    }


def _normalize(
    monkeypatch: pytest.MonkeyPatch,
    input_root: Path,
    reference: Path,
    output: Path,
    *,
    include_track_sha256: bool = True,
):
    monkeypatch.setattr(
        "ssqtl_igv.ssqtl_adapter_v3.run_r_prepare",
        _fake_r_prepare,
    )
    monkeypatch.setattr(
        "ssqtl_igv.ssqtl_adapter_v3.normalize_manifest",
        _fake_science_normalizer(
            reference, include_track_sha256=include_track_sha256
        ),
    )
    return normalize_ssqtl_inputs(
        associations="associations.csv", rds_dir="rds", bam_lookup="bam_lookup.csv",
        violin_dir="violin", input_root=input_root, reference_path=reference,
        output_dir=output, run_id="run_001", generation_id="generation_001",
    )


@pytest.mark.skipif(shutil.which("Rscript") is None, reason="Rscript is unavailable")
def test_real_rds_preparation_reaches_native_v3_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "input"
    (root / "rds").mkdir(parents=True)
    (root / "violin").mkdir()
    (root / "tracks").mkdir()
    (root / "associations.csv").write_text(
        "AG_site,SNP,strand,n_total,n_0,n_1,n_2,Beta,abs_Tvalue\n"
        "chr1:100-101,chr1.105_A.G,+,1,1,0,0,0.5,2.0\n",
        encoding="utf-8",
    )
    bam = root / "tracks" / "sample-1.bam"
    bai = root / "tracks" / "sample-1.bam.bai"
    bam.write_bytes(b"bam-fixture")
    bai.write_bytes(b"bai-fixture")
    (root / "bam_lookup.csv").write_text(
        "sample_id,bam\nsample-1,tracks/sample-1.bam\n", encoding="utf-8"
    )
    (root / "violin" / "violin_plots_pos_chr1.pdf").write_bytes(b"pdf-fixture")

    r_script = tmp_path / "make_fixture.R"
    r_script.write_text(
        "args <- commandArgs(trailingOnly = TRUE)\n"
        "locus <- data.frame(sample_id='sample-1', ratio=0.25, check.names=FALSE)\n"
        "locus[['chr1.105_A.G']] <- '0/0'\n"
        "payload <- list()\n"
        "payload[['chr1:100-101']] <- locus\n"
        "saveRDS(payload, args[[1]])\n",
        encoding="utf-8",
    )
    rds = root / "rds" / "AGratio_SNPgeno_pos_chr1_list.rds"
    completed = subprocess.run(
        [str(shutil.which("Rscript")), str(r_script), str(rds)],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr

    monkeypatch.setattr(
        "ssqtl_igv.prepare.pdf_pages",
        lambda *_args, **_kwargs: ["chr1:100-101\nchr1.105_A.G\n"],
    )
    reference = _reference(tmp_path / "reference")
    result = normalize_ssqtl_inputs(
        associations="associations.csv",
        rds_dir="rds",
        bam_lookup="bam_lookup.csv",
        violin_dir="violin",
        input_root=root,
        reference_path=reference,
        output_dir=tmp_path / "normalized-real-rds",
        run_id="run_real_rds",
        generation_id="generation_001",
    )
    task = list(read_jsonl(result["tasks"]))[0]
    validate_v3_task_document(task)
    assert task["adapter_id"] == "ssqtl"
    assert task["adapter_data"]["adapter_schema_version"] == "3.0-ssqtl"
    assert task["adapter_data"]["selected_samples"][0]["sample_id"] == "sample-1"
    assert task["core"]["tracks"][0]["track_label"] == "Track 001"
    assert "sample-1" not in task["core"]["tracks"][0]["track_label"]
    assert task["core"]["tracks"][0]["bai"]["declared_path"] == (
        "tracks/sample-1.bam.bai"
    )
    report = json.loads(
        (Path(result["output_dir"]) / "r_prepare.json").read_text(encoding="utf-8")
    )
    assert report["schema_version"] == "3.0-ssqtl-r-prepare"
    assert (report["case_count"], report["sample_count"]) == (1, 1)


def _staged_inputs(task: dict[str, object]) -> dict[str, str]:
    core = task["core"]
    resources = [
        resource
        for track in core["tracks"]
        for resource in (track["bam"], track["bai"])
    ]
    resources.extend(core["reference"]["resources"].values())
    if core["auxiliary"]["state"] == "PRESENT":
        resources.append(core["auxiliary"])
    return {
        str(resource["stage_name"]): str(resource["source_path"])
        for resource in resources
    }


def _assert_no_persisted_v2_or_stage_result(root: Path) -> None:
    assert not (root / "scientific_stages").exists()
    for path in root.rglob("*.json"):
        text = path.read_text(encoding="utf-8")
        document = json.loads(text)
        if isinstance(document, dict):
            assert document.get("schema_version") != "2.0"
            assert document.get("pipeline_version") != "2.0.0"
        assert "legacy_task" not in text
        assert "stage_result" not in text


def test_raw_ssqtl_normalization_emits_native_v3_and_empty_group_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = _input_root(tmp_path / "input")
    reference = _reference(tmp_path / "reference")
    result = _normalize(monkeypatch, input_root, reference, tmp_path / "out")
    task = list(read_jsonl(result["tasks"]))[0]
    validate_v3_task_document(task)
    assert task["adapter_data"]["adapter_schema_version"] == "3.0-ssqtl"
    assert task["adapter_data"]["genotype_groups"]["0/1"] == {
        "selected_count": 0, "empty": True,
    }
    assert task["adapter_data"]["violin"]["match_key"] == {
        "ag_site": "chr1:100-101", "snp": "chr1.105_A.G",
    }
    assert "legacy_task" not in json.dumps(task)
    assert task["core"]["tracks"][0]["bai"]["source_path"].endswith(
        "sample-1.bam.bai"
    )
    assert task["adapter_data"]["preparation_evidence"]["rds"]["identity"]["sha256"]

    bundle = Path(result["output_dir"])
    assert {path.name for path in bundle.iterdir()} == {
        "tasks.jsonl",
        "normalized_manifest.tsv",
        "parameters.json",
        "ssqtl_preparation.json",
        "validation.json",
        "prepared_cases.tsv",
        "prepared_samples.tsv",
        "r_prepare.stdout.log",
        "r_prepare.stderr.log",
        "r_prepare.json",
    }
    receipt = json.loads((bundle / "ssqtl_preparation.json").read_text(encoding="utf-8"))
    for artifact in receipt["artifacts"].values():
        path = bundle / artifact["relative_path"]
        assert path.is_file()
        assert artifact["sha256"] == sha256_file(path)
        assert artifact["size"] == path.stat().st_size
    assert receipt["artifact_set_sha256"] == sha256_json(receipt["artifacts"])
    r_report = json.loads((bundle / "r_prepare.json").read_text(encoding="utf-8"))
    assert r_report["schema_version"] == "3.0-ssqtl-r-prepare"
    validation = json.loads((bundle / "validation.json").read_text(encoding="utf-8"))
    assert validation["preparation_receipt_sha256"] == sha256_file(
        bundle / "ssqtl_preparation.json"
    )
    assert validation["preparation_artifact_set_sha256"] == receipt[
        "artifact_set_sha256"
    ]
    _assert_no_persisted_v2_or_stage_result(bundle)


def test_ssqtl_master_normalization_does_not_hash_large_tracks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = _input_root(tmp_path / "input")
    reference = _reference(tmp_path / "reference")
    result = _normalize(
        monkeypatch,
        input_root,
        reference,
        tmp_path / "out",
        include_track_sha256=False,
    )
    task = next(read_jsonl(result["tasks"]))
    track = task["core"]["tracks"][0]
    assert set(track["bam"]["identity"]) == {"size", "mtime_ns"}
    assert set(track["bai"]["identity"]) == {"size", "mtime_ns"}
    assert task["core"]["auxiliary"]["identity"]["sha256"]
    assert all(
        resource["identity"]["sha256"]
        for resource in task["core"]["reference"]["resources"].values()
    )


def test_native_ssqtl_fingerprint_is_mount_independent_and_rds_sensitive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_root = _input_root(tmp_path / "input-a")
    second_root = tmp_path / "input-b"
    shutil.copytree(first_root, second_root, copy_function=shutil.copy2)
    reference = _reference(tmp_path / "reference")
    first = list(read_jsonl(_normalize(monkeypatch, first_root, reference, tmp_path / "out-a")["tasks"]))[0]
    second = list(read_jsonl(_normalize(monkeypatch, second_root, reference, tmp_path / "out-b")["tasks"]))[0]
    assert first["input_fingerprint"] == second["input_fingerprint"]
    (second_root / "rds" / "AGratio_SNPgeno_pos_chr1_list.rds").write_bytes(b"changed-rds")
    changed = list(read_jsonl(_normalize(monkeypatch, second_root, reference, tmp_path / "out-c")["tasks"]))[0]
    assert changed["input_fingerprint"] != second["input_fingerprint"]


def test_native_ssqtl_fake_worker_emits_only_v3_scientific_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = _input_root(tmp_path / "input")
    reference = _reference(tmp_path / "reference")
    task = list(
        read_jsonl(_normalize(monkeypatch, input_root, reference, tmp_path / "normalized")["tasks"])
    )[0]
    output = tmp_path / "case-output"
    result = run_portable_task(
        task,
        _staged_inputs(task),
        output,
        fake_runtime=True,
    )

    assert result["render_state"] == "SUCCEEDED"
    assert result["evidence_state"] == "EVIDENCE_INCOMPLETE"
    assert result["scientific_interpretation"] == "INDETERMINATE"
    assert result["debug_only"] is True
    assert result["eligible"] is False
    assert {"scientific_case_evidence", "scientific_qc_evidence"}.issubset(
        result["artifacts"]
    )
    assert "scientific_case_result" not in result["artifacts"]
    assert "scientific_qc_stage_result" not in result["artifacts"]
    case_evidence = json.loads(
        (output / "scientific_case_evidence.json").read_text(encoding="utf-8")
    )
    qc_evidence = json.loads(
        (output / "scientific_qc_evidence.json").read_text(encoding="utf-8")
    )
    assert case_evidence["schema_version"] == "3.0-ssqtl-scientific-case-evidence"
    assert qc_evidence["schema_version"] == "3.0-ssqtl-scientific-qc-evidence"
    _assert_no_persisted_v2_or_stage_result(output)


def test_all_empty_ssqtl_renders_annotation_only_reviewable_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = _input_root(tmp_path / "input")
    reference = _reference(tmp_path / "reference")
    task = list(
        read_jsonl(_normalize(monkeypatch, input_root, reference, tmp_path / "normalized")["tasks"])
    )[0]
    task["core"]["tracks"] = []
    task["adapter_data"]["selected_samples"] = []
    for group in task["adapter_data"]["genotype_groups"].values():
        group.update({"selected_count": 0, "empty": True})
    task["input_fingerprint"] = v3_task_fingerprint(task)
    validate_v3_task_document(task)

    output = tmp_path / "all-empty-output"
    result = run_portable_task(
        task,
        _staged_inputs(task),
        output,
        fake_runtime=True,
    )

    assert result["render_state"] == "SUCCEEDED"
    assert result["evidence_state"] == "EVIDENCE_INCOMPLETE"
    assert result["scientific_interpretation"] == "INDETERMINATE"
    for role in ("review_image", "raw_igv", "capture_metadata", "layout"):
        assert role in result["artifacts"]
    session = ET.parse(output / "igv.session.xml")
    resources = session.findall("./Resources/Resource")
    assert len(resources) == 1
    assert resources[0].attrib["name"].startswith("Annotation |")
    evidence = json.loads(
        (output / "scientific_case_evidence.json").read_text(encoding="utf-8")
    )
    assert evidence["render_mode"] == "ANNOTATION_ONLY_NO_BAM"
    assert evidence["empty_genotype_groups"] == ["0/0", "0/1", "1/1"]
    _assert_no_persisted_v2_or_stage_result(output)


def test_unavailable_violin_and_reference_context_are_incomplete_not_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = _input_root(tmp_path / "input")
    reference = _reference(tmp_path / "reference")
    task = list(
        read_jsonl(_normalize(monkeypatch, input_root, reference, tmp_path / "normalized")["tasks"])
    )[0]
    original_track = task["core"]["tracks"][0]
    original_sample = task["adapter_data"]["selected_samples"][0]
    tracks = []
    samples = []
    for order, genotype in enumerate(("0/0", "0/1", "1/1"), 1):
        track = copy.deepcopy(original_track)
        track.update(
            {
                "track_order": order,
                "track_label": f"Track {order:03d}",
                "group": genotype,
            }
        )
        track["bam"]["stage_name"] = f"context-{order}.bam"
        track["bai"]["stage_name"] = f"context-{order}.bam.bai"
        tracks.append(track)
        sample = copy.deepcopy(original_sample)
        sample.update(
            {
                "track_order": order,
                "sample_id": f"sample-{order}",
                "genotype": genotype,
                "dosage": order - 1,
            }
        )
        samples.append(sample)
    task["core"]["tracks"] = tracks
    task["core"]["auxiliary"] = {"state": "ABSENT"}
    task["adapter_data"]["selected_samples"] = samples
    for genotype in ("0/0", "0/1", "1/1"):
        task["adapter_data"]["genotype_groups"][genotype] = {
            "selected_count": 1,
            "empty": False,
        }
    task["adapter_data"]["violin"] = {
        "state": "UNAVAILABLE",
        "match_key": {
            "ag_site": task["adapter_data"]["ag"]["raw"],
            "snp": task["adapter_data"]["snp"]["raw"],
        },
        "page": None,
        "pdf_sha256": None,
    }
    task["adapter_data"]["reference_context"] = {
        "available": False,
        "chrom": "chr1",
        "start": 100,
        "end": 101,
        "strand": "+",
        "error": "reference context unavailable in fixture",
    }
    task["input_fingerprint"] = v3_task_fingerprint(task)
    validate_v3_task_document(task)

    output = tmp_path / "context-incomplete-output"
    result = run_portable_task(
        task,
        _staged_inputs(task),
        output,
        fake_runtime=True,
    )
    assert result["evidence_state"] == "EVIDENCE_INCOMPLETE"
    assert result["scientific_interpretation"] == "INDETERMINATE"
    qc = json.loads((output / "scientific_qc.json").read_text(encoding="utf-8"))
    assert qc["empty_genotype_groups"] == []
    assert set(qc["incomplete_reasons"]) == {
        "VIOLIN_UNAVAILABLE",
        "REFERENCE_CONTEXT_UNAVAILABLE",
    }
    checks = {check["code"]: check for check in qc["checks"]}
    assert checks["VIOLIN_EXACT_PAIR"]["status"] == "INCOMPLETE"
    assert checks["AG_REFERENCE_CONTEXT"]["status"] == "INCOMPLETE"


def test_case_input_invalid_stops_before_review_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_root = _input_root(tmp_path / "input")
    reference = _reference(tmp_path / "reference")
    task = list(
        read_jsonl(_normalize(monkeypatch, input_root, reference, tmp_path / "normalized")["tasks"])
    )[0]
    task["core"]["preflight"] = {
        "state": "CASE_INPUT_INVALID",
        "warnings": [],
        "errors": [
            {
                "code": "REFERENCE_CONTEXT_UNAVAILABLE",
                "message": "fixture is not eligible for rendering",
            }
        ],
    }
    task["input_fingerprint"] = v3_task_fingerprint(task)
    validate_v3_task_document(task)

    output = tmp_path / "preflight-invalid-output"
    result = run_portable_task(
        task,
        _staged_inputs(task),
        output,
        fake_runtime=True,
    )
    assert result["render_state"] == "FAILED"
    assert result["evidence_state"] == "UNAVAILABLE"
    assert result["scientific_interpretation"] == "INDETERMINATE"
    assert result["eligible"] is False
    assert result["artifacts"] == {}
    assert not (output / "review.png").exists()


def test_ssqtl_bam_lookup_rejects_absolute_and_parent_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reference = _reference(tmp_path / "reference")
    for index, unsafe in enumerate(("/outside/sample.bam", "../sample.bam"), 1):
        input_root = _input_root(tmp_path / f"input-{index}")
        (input_root / "bam_lookup.csv").write_text(
            f"sample_id,bam\nsample-1,{unsafe}\n", encoding="utf-8"
        )
        monkeypatch.setattr(
            "ssqtl_igv.ssqtl_adapter_v3.normalize_manifest",
            _fake_science_normalizer(reference),
        )
        with pytest.raises(ValueError, match="relative path|escapes"):
            normalize_ssqtl_inputs(
                associations="associations.csv", rds_dir="rds", bam_lookup="bam_lookup.csv",
                violin_dir="violin", input_root=input_root, reference_path=reference,
                output_dir=tmp_path / f"bad-{index}", run_id="run_001",
                generation_id="generation_001",
            )


def test_ssqtl_worker_uses_explicit_bai_for_idxstats(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="chr1\t300\t0\t0\n", stderr="")

    monkeypatch.setattr("ssqtl_igv.v3_worker.subprocess.run", fake_run)
    assert _samtools_check_explicit_index("samtools", Path("case.bam"), Path("chosen.bai")) == (True, "")
    assert calls[1] == ["samtools", "idxstats", "-X", "case.bam", "chosen.bai"]


def test_public_cli_uses_project_yaml_not_raw_ssqtl_flags() -> None:
    parser = _parser()
    args = parser.parse_args(["run", "--project", "project.yaml"])
    assert args.project == "project.yaml"
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--adapter", "ssqtl", "--associations", "a.csv"])


def test_ssqtl_preparation_is_a_non_cached_portable_nextflow_process() -> None:
    root = Path(__file__).resolve().parents[2]
    module = (root / "modules/local/normalize_ssqtl_v3.nf").read_text(encoding="utf-8")
    workflow = (root / "workflows/ssqtl_normalize.nf").read_text(encoding="utf-8")
    assert "label 'portable_runtime'" in module
    assert "label 'prepare'" in module
    assert "cache false" in module
    assert "NORMALIZE_SSQTL_V3" in workflow
    assert "VALIDATE_RUNTIME_IDENTITY" in workflow


@pytest.mark.skipif(
    not (Path(__file__).resolve().parents[2] / ".tools" / "nextflow-25.04.7" / "nextflow").is_file(),
    reason="pinned Nextflow executable is unavailable",
)
def test_ssqtl_nextflow_stub_contract_is_executable(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    identity = tmp_path / "runtime-identity.json"
    identity.write_text("{}\n", encoding="utf-8")
    bind_contract = tmp_path / "ssqtl-bind-contract.json"
    bind_contract.write_text("{}\n", encoding="utf-8")
    java_home = root / ".tools" / "jdk-21.0.4+7-jre" / "Contents" / "Home"
    java_command = tmp_path / "java21"
    java_command.symlink_to(java_home / "bin" / "java")
    output = tmp_path / "normalization"
    session = tmp_path / "session"
    command = [
        str(root / ".tools" / "nextflow-25.04.7" / "nextflow"),
        "run", str(root), "-entry", "SSQTL_NORMALIZE", "-profile", "test",
        "-stub-run", "-work-dir", str(tmp_path / "work"),
        "--run_id", "run_001", "--generation_id", "generation_001",
        "--ssqtl_associations", "associations.csv", "--ssqtl_rds_dir", "rds",
        "--ssqtl_bam_lookup", "bam_lookup.csv", "--ssqtl_violin_dir", "violin",
        "--ssqtl_input_root", str(tmp_path / "input"),
        "--ssqtl_reference", str(tmp_path / "reference" / "reference.yaml"),
        "--ssqtl_reference_root", str(tmp_path / "reference"),
        "--ssqtl_normalization_output", str(output),
        "--ssqtl_bind_contract", str(bind_contract),
        "--runtime_identity", str(identity),
        "--runtime_identity_sha256", "a" * 64,
        "--runtime_oci_digest", "sha256:" + "b" * 64,
        "--session_output", str(session),
    ]
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
        env={
            **os.environ,
            "NXF_JAVA_HOME": "",
            "JAVA_HOME": "",
            "JAVA_CMD": str(java_command),
            "NXF_VER": "25.04.7",
            "NXF_ANSI_LOG": "false",
            "NXF_HOME": str(tmp_path / "nxf-home"),
        },
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    bundle = output / "normalization_bundle"
    assert {path.name for path in bundle.iterdir()} == {
        "tasks.jsonl",
        "normalized_manifest.tsv",
        "parameters.json",
        "ssqtl_preparation.json",
        "validation.json",
        "prepared_cases.tsv",
        "prepared_samples.tsv",
        "r_prepare.stdout.log",
        "r_prepare.stderr.log",
        "r_prepare.json",
    }
    assert (session / "runtime_identity" / "runtime_identity_validation" / "validation.json").is_file()
