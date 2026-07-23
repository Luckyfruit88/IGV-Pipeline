from __future__ import annotations

import copy
import json
import subprocess
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from PIL import Image, ImageDraw

from ssqtl_igv.case_inputs import expected_stage_inputs, validate_case_inputs
from ssqtl_igv.composition import compose_case
from ssqtl_igv.contracts import validate_case_result_document, validate_task_document
from ssqtl_igv.desktop import DesktopFailure, DesktopResult
from ssqtl_igv.normalize import normalize_manifest
from ssqtl_igv.publication import publish_reviewed, verify_checksum_tree
from ssqtl_igv.qc_case import qc_case
from ssqtl_igv.r_prepare import run_r_prepare
from ssqtl_igv.reconcile import aggregate_run, summarize_shard
from ssqtl_igv.render_case import render_case
from ssqtl_igv.review_package import REVIEW_ASSERTIONS, build_review_package
from ssqtl_igv.review_records import REVIEW_BINDING_FIELDS, validate_reviews
from ssqtl_igv.sharding import SHARD_ALGORITHM, create_shards
from ssqtl_igv.task_io import staged_input_map, task_from_manifest
from ssqtl_igv.test_doubles import (
    fake_desktop_session,
    fake_evidence_evaluator,
    fake_pdf_page,
    fake_samtools_check,
    require_test_task,
)
from ssqtl_igv.utils import read_jsonl, sha256_file, sha256_json, write_jsonl


PROJECT_ROOT = Path(__file__).resolve().parents[2]
R_PREPARE_WRAPPER = PROJECT_ROOT / "bin" / "prepare_cases.R"
R_PREPARE_IMPLEMENTATION = (
    PROJECT_ROOT / "src" / "ssqtl_igv" / "resources" / "prepare_cases.R"
)


def _normalization_fixture(root: Path) -> tuple[Path, Path, Path]:
    inputs = root / "inputs"
    inputs.mkdir()
    associations = inputs / "associations.csv"
    associations.write_text("AG_site,SNP,strand\n", encoding="utf-8")
    bam_lookup = inputs / "bam_lookup.csv"
    bam_lookup.write_text("sample_id,bam\n", encoding="utf-8")
    rds = inputs / "rds"
    rds.mkdir()
    violin = inputs / "violin"
    violin.mkdir()
    pdf = violin / "violin_plots_pos_chrA.pdf"
    pdf.write_text("fixture PDF payload", encoding="utf-8")

    bam = inputs / "sample-1.bam"
    bai = inputs / "sample-1.bam.bai"
    bam.write_bytes(b"bam")
    bai.write_bytes(b"bai")
    genome = inputs / "genome.json"
    fasta = inputs / "genome.fa"
    fai = inputs / "genome.fa.fai"
    cytoband = inputs / "cytoband.txt.gz"
    annotation = inputs / "annotation.gff.gz"
    genome.write_text("{}\n", encoding="utf-8")
    fasta.write_text(">chrA\nAAGTC\n", encoding="utf-8")
    fai.write_text("chrA\t5\t6\t5\t6\n", encoding="utf-8")
    cytoband.write_bytes(b"cytoband")
    annotation.write_bytes(b"annotation")

    config = {
        "paths": {
            "associations": str(associations),
            "associations_sha256": sha256_file(associations),
            "rds_dir": str(rds),
            "bam_lookup": str(bam_lookup),
            "violin_dir": str(violin),
            "violin_pdf_template": "violin_plots_{strand_token}_{chrom}.pdf",
            "output_root": str(root / "unused-runs"),
            "publish_root": str(root / "unused-publish"),
        },
        "workflow": {
            "figure_contract_id": "v031_native_igv_pixel_exact",
            "gui_settle_contract_id": "v031_toolbar_locus_settle_v1",
        },
        "genome": {
            "id": "hg38_MANEv1.5",
            "display_name": "Fixture genome",
            "definition": str(genome),
            "definition_sha256": sha256_file(genome),
            "fasta": str(fasta),
            "fai": str(fai),
            "cytoband": str(cytoband),
            "cytoband_sha256": sha256_file(cytoband),
            "annotation": str(annotation),
            "annotation_version": "MANE v1.5",
            "annotation_sha256": sha256_file(annotation),
        },
        "binaries": {
            "rscript": "Rscript",
            "igv": "igv",
            "pdftotext": "pdftotext",
            "pdftoppm": "pdftoppm",
            "qsub": "qsub",
            "qacct": "qacct",
        },
        "execution": {"mode": "local"},
        "scheduler": {
            "max_parallel": 1,
            "max_tasks_per_array": 1,
            "cases_per_task": 1,
            "memory_gb": 8,
            "total_parallel_memory_gb": 8,
        },
        "inputs": {"expected_case_count": 1, "stale_bai_policy": "warn"},
        "render": {"overview_padding": 55, "detail_padding": 12},
        "desktop": {
            "screen_width": 1920,
            "screen_height": 2160,
            "screen_depth": 24,
            "toolbar_locus_roi": {"x": 0, "y": 0, "width": 1920, "height": 120},
            "locus_field_roi": {"x": 467, "y": 27, "width": 226, "height": 24},
        },
        "compose": {"violin_panel_width": 720},
        "publication": {"chromosomes": [], "generate_svg": False},
        "storage": {
            "provider": "filesystem",
            "minimum_free_gb": 0,
            "minimum_free_inodes": 0,
            "gate_max_age_seconds": 1800,
            "remaining_case_buffer_factor": 1,
            "work_gb_per_case": 0,
            "publish_gb_per_case": 0,
            "scratch_gb_per_parallel_task": 0,
            "reserve_gb": 0,
            "work_inodes_per_case": 0,
            "publish_inodes_per_case": 0,
            "reserve_inodes": 0,
        },
    }
    params = root / "params.json"
    params.write_text(json.dumps(config), encoding="utf-8")

    prepared_cases = root / "prepared_cases.tsv"
    prepared_cases.write_text(
        "\t".join(
            [
                "association_row",
                "case_id",
                "ag_site",
                "snp",
                "strand",
                "n_total",
                "n_0",
                "n_1",
                "n_2",
                "eligible_n_0",
                "eligible_n_1",
                "eligible_n_2",
                "beta",
                "abs_tvalue",
                "error_code",
                "error_message",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "1",
                "AG_chrA_2_3__SNP_chrA_4_T_C",
                "chrA:2-3",
                "chrA.4_T.C",
                "+",
                "1",
                "1",
                "0",
                "0",
                "1",
                "0",
                "0",
                "0.5",
                "2.0",
                "",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    prepared_samples = root / "prepared_samples.tsv"
    prepared_samples.write_text(
        "case_id\tgenotype\tsample_id\tdosage\tratio\tselection_label\tbam\tbai\tbai_fresh\n"
        f"AG_chrA_2_3__SNP_chrA_4_T_C\t0/0\tsample-1\t0\t0.25\tall\t{bam}\t{bai}\ttrue\n",
        encoding="utf-8",
    )
    return params, prepared_cases, prepared_samples


def test_normalization_is_declared_schema_v2_output(tmp_path: Path) -> None:
    params, prepared_cases, prepared_samples = _normalization_fixture(tmp_path)
    output = tmp_path / "plan_bundle"
    with patch("ssqtl_igv.prepare.pdf_pages", return_value=["chrA:2-3 chrA.4_T.C"]):
        report = normalize_manifest(
            params,
            output,
            run_id="run_001",
            generation_id="gen_001",
            prepared_cases=prepared_cases,
            prepared_samples=prepared_samples,
            expected_case_count=1,
        )

    assert report["status"] == "PASS"
    assert sorted(path.name for path in output.iterdir()) == [
        "normalized_manifest.tsv",
        "parameters.json",
        "r_prepare.json",
        "tasks.jsonl",
        "validation.json",
    ]
    assert json.loads((output / "r_prepare.json").read_text())["mode"] == "PROVIDED_FIXTURE"
    tasks = list(read_jsonl(output / "tasks.jsonl"))
    assert len(tasks) == 1
    task = tasks[0]
    validate_task_document(task)
    assert task["schema_version"] == "2.0"
    assert task["manifest_order"] == 1
    assert task["preflight_state"] == "READY"
    assert task["genotype_groups"]["0/1"] == {"selected_count": 0, "empty": True}
    assert task["reference_context"]["canonical_ag"] is True
    assert all(Path(track["bam"]).is_absolute() for track in task["tracks"])
    assert len({task["tracks"][0]["stage_bam"], task["tracks"][0]["stage_bai"]}) == 2


def test_r_prepare_command_has_declared_atomic_outputs(tmp_path: Path) -> None:
    params, _prepared_cases, _prepared_samples = _normalization_fixture(tmp_path)
    config = json.loads(params.read_text())

    def fake_r(command, **_kwargs):
        staged_wrapper = Path(command[1])
        assert staged_wrapper.name == "prepare_cases_wrapper.R"
        assert (staged_wrapper.parent / "prepare_cases_implementation.R").is_file()
        values = {
            token.split("=", 1)[0]: token.split("=", 1)[1]
            for token in command
            if token.startswith("--") and "=" in token
        }
        Path(values["--cases_out"]).write_text(
            "association_row\tcase_id\n1\tcase_1\n", encoding="utf-8"
        )
        Path(values["--samples_out"]).write_text(
            "case_id\tgenotype\ncase_1\t0/0\n", encoding="utf-8"
        )
        return subprocess.CompletedProcess(command, 0, "fixture stdout\n", "")

    output = tmp_path / "r-prepare-bundle"
    with patch("ssqtl_igv.r_prepare.subprocess.run", side_effect=fake_r):
        report = run_r_prepare(
            params,
            config["paths"]["associations"],
            config["paths"]["rds_dir"],
            config["paths"]["bam_lookup"],
            output,
            r_wrapper=R_PREPARE_WRAPPER,
            r_implementation=R_PREPARE_IMPLEMENTATION,
        )
    assert report["status"] == "PASS"
    assert report["case_count"] == 1
    assert report["sample_count"] == 1
    assert report["r_wrapper_sha256"] == sha256_file(R_PREPARE_WRAPPER)
    assert report["r_implementation_sha256"] == sha256_file(R_PREPARE_IMPLEMENTATION)
    assert sorted(path.name for path in output.iterdir()) == [
        "prepared_cases.tsv",
        "prepared_samples.tsv",
        "r_prepare.json",
        "r_prepare.stderr.log",
        "r_prepare.stdout.log",
    ]


def test_r_prepare_timeout_leaves_no_partial_bundle(tmp_path: Path) -> None:
    params, _prepared_cases, _prepared_samples = _normalization_fixture(tmp_path)
    config = json.loads(params.read_text())
    output = tmp_path / "r-timeout-bundle"
    with patch(
        "ssqtl_igv.r_prepare.subprocess.run",
        side_effect=subprocess.TimeoutExpired(["Rscript"], timeout=1),
    ):
        with pytest.raises(subprocess.TimeoutExpired):
            run_r_prepare(
                params,
                config["paths"]["associations"],
                config["paths"]["rds_dir"],
                config["paths"]["bam_lookup"],
                output,
                r_wrapper=R_PREPARE_WRAPPER,
                r_implementation=R_PREPARE_IMPLEMENTATION,
            )
    assert not output.exists()
    assert not list(tmp_path.glob(".r-timeout-bundle.tmp-*"))


def test_sharding_is_deterministic_and_conserves_exact_task_set(tmp_path: Path) -> None:
    params, prepared_cases, prepared_samples = _normalization_fixture(tmp_path)
    normalized = tmp_path / "normalized"
    with patch("ssqtl_igv.prepare.pdf_pages", return_value=["chrA:2-3 chrA.4_T.C"]):
        normalize_manifest(
            params,
            normalized,
            run_id="run_001",
            generation_id="gen_001",
            prepared_cases=prepared_cases,
            prepared_samples=prepared_samples,
        )
    template = next(read_jsonl(normalized / "tasks.jsonl"))
    tasks = []
    for order in range(1, 6):
        task = copy.deepcopy(template)
        task["task_id"] = f"case_{order:03d}"
        task["manifest_order"] = order
        task["association"]["row"] = order
        task["input_fingerprint"] = sha256_json({"order": order})
        tasks.append(task)
    tasks_path = tmp_path / "tasks.jsonl"
    write_jsonl(tasks_path, tasks)

    first = create_shards(tasks_path, tmp_path / "shards-a", max_cases_per_shard=2)
    second = create_shards(tasks_path, tmp_path / "shards-b", max_cases_per_shard=2)
    assert first["algorithm"] == SHARD_ALGORITHM
    assert first["shard_count"] == 3
    assert [row["task_count"] for row in first["shards"]] == [2, 2, 1]
    assert first["task_set_sha256"] == second["task_set_sha256"]
    assert [row["task_set_sha256"] for row in first["shards"]] == [
        row["task_set_sha256"] for row in second["shards"]
    ]
    observed = [
        task["task_id"]
        for shard_path in sorted((tmp_path / "shards-a" / "shards").glob("*.jsonl"))
        for task in read_jsonl(shard_path)
    ]
    assert observed == [f"case_{order:03d}" for order in range(1, 6)]


def test_sharding_rejects_symlink_input(tmp_path: Path) -> None:
    source = tmp_path / "tasks.jsonl"
    source.write_text("", encoding="utf-8")
    alias = tmp_path / "tasks-link.jsonl"
    alias.symlink_to(source)
    try:
        create_shards(alias, tmp_path / "output")
    except ValueError as exc:
        assert "symlink" in str(exc)
    else:  # pragma: no cover - explicit fail-closed assertion
        raise AssertionError("symlink task input was accepted")


def _normalized_task(tmp_path: Path) -> dict[str, object]:
    params, prepared_cases, prepared_samples = _normalization_fixture(tmp_path)
    output = tmp_path / "normalized-for-input-check"
    with patch("ssqtl_igv.prepare.pdf_pages", return_value=["chrA:2-3 chrA.4_T.C"]):
        normalize_manifest(
            params,
            output,
            run_id="run_001",
            generation_id="gen_001",
            prepared_cases=prepared_cases,
            prepared_samples=prepared_samples,
        )
    return next(read_jsonl(output / "tasks.jsonl"))


def _input_map(task: dict[str, object]) -> dict[str, str]:
    values: dict[str, str] = {}
    for track in task["tracks"]:
        values[track["stage_bam"]] = track["bam"]
        values[track["stage_bai"]] = track["bai"]
    for resource in task["reference"]["resources"].values():
        values[resource["stage_name"]] = resource["source_path"]
    if task["plot"]["state"] == "PRESENT":
        values[task["plot"]["stage_pdf"]] = task["plot"]["pdf"]
    return values


def test_case_input_validation_emits_success_bundle(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    output = tmp_path / "validated_bundle"
    with patch("ssqtl_igv.case_inputs._samtools_check", return_value=(True, "")):
        result = validate_case_inputs(
            task,
            _input_map(task),
            output,
            shard_id="shard_001",
            session_id="session_001",
        )
    assert result["status"] == "SUCCEEDED"
    assert result["session_id"] == "session_001"
    assert (output / "stage_result.json").is_file()
    assert not list(output.parent.glob(".validated_bundle.tmp-*"))


def test_case_input_domain_failure_is_data_not_process_exception(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    output = tmp_path / "invalid_index_bundle"
    with patch(
        "ssqtl_igv.case_inputs._samtools_check",
        return_value=(False, "index is not compatible"),
    ):
        result = validate_case_inputs(
            task,
            _input_map(task),
            output,
            shard_id="shard_001",
            session_id="session_001",
        )
    assert result["status"] == "DOMAIN_FAILED"
    assert result["failures"][0]["code"] == "BAI_INCOMPATIBLE_OR_UNREADABLE"
    assert result["failures"][0]["rerun_eligible"] is True
    assert (output / "stage_result.json").is_file()


def test_case_input_drift_is_infrastructure_failure(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    bam = Path(task["tracks"][0]["bam"])
    bam.write_bytes(b"changed after planning")
    output = tmp_path / "drift_bundle"
    with pytest.raises(RuntimeError, match="changed after canonical planning"):
        validate_case_inputs(
            task,
            _input_map(task),
            output,
            shard_id="shard_001",
            session_id="session_001",
        )
    assert not output.exists()
    assert not list(output.parent.glob(".drift_bundle.tmp-*"))


def test_nextflow_task_io_preserves_contract_order(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    manifest = tmp_path / "shard.jsonl"
    write_jsonl(manifest, [task])
    loaded = task_from_manifest(manifest, task["task_id"])
    staged = []
    for track in task["tracks"]:
        staged.extend((track["bam"], track["bai"]))
    for role in ("definition", "fasta", "fai", "cytoband", "annotation"):
        staged.append(task["reference"]["resources"][role]["source_path"])
    staged.append(task["plot"]["pdf"])
    assert loaded == task
    observed = staged_input_map(task, staged)
    assert list(observed) == list(expected_stage_inputs(task))
    assert list(observed.values()) == [str(Path(value).resolve()) for value in staged]


def test_fake_runtime_generates_exact_canvas_but_is_not_publishable(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    task["run_id"] = "test_fake_runtime"
    task["input_fingerprint"] = sha256_json(
        {key: value for key, value in task.items() if key != "input_fingerprint"}
    )
    require_test_task(task)
    inputs = _input_map(task)
    validation_dir = tmp_path / "fake-validation"
    validation = validate_case_inputs(
        task,
        inputs,
        validation_dir,
        shard_id="shard_001",
        session_id="fake_session",
        samtools_checker=fake_samtools_check,
    )
    render_dir = tmp_path / "fake-render"
    render = render_case(
        task,
        inputs,
        validation,
        tmp_path / "params.json",
        render_dir,
        shard_id="shard_001",
        session_id="fake_session",
        desktop_runner=fake_desktop_session,
    )
    compose_dir = tmp_path / "fake-compose"
    composition = compose_case(
        task,
        render_dir,
        task["plot"]["pdf"],
        tmp_path / "params.json",
        compose_dir,
        shard_id="shard_001",
        session_id="fake_session",
        pdf_renderer=fake_pdf_page,
    )
    qc_dir = tmp_path / "fake-qc"
    qc = qc_case(
        task,
        validation_dir,
        render_dir,
        compose_dir,
        qc_dir,
        shard_id="shard_001",
        session_id="fake_session",
        evidence_evaluator=fake_evidence_evaluator,
    )
    assert validation["status"] == "SUCCEEDED"
    assert render["status"] == "SUCCEEDED"
    assert composition["status"] == "SUCCEEDED"
    assert qc["status"] == "DOMAIN_FAILED"
    assert qc["failures"] == [
        {
            "class": "DOMAIN",
            "code": "TEST_DOUBLE_EVIDENCE_NOT_PUBLISHABLE",
            "message": "synthetic CI evidence is never eligible for review or publication",
            "rerun_eligible": False,
        }
    ]
    combined = compose_dir / "combined" / f"{task['task_id']}.png"
    with Image.open(combined) as image:
        assert image.size == (2640, 2160)
    case_result = json.loads((qc_dir / "case_result.json").read_text(encoding="utf-8"))
    assert case_result["render_state"] == "SUCCEEDED"
    assert case_result["publication_state"] == "NOT_READY"


def test_fake_runtime_rejects_non_test_run_id(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    with pytest.raises(ValueError, match="beginning with test_"):
        require_test_task(task)


def test_all_empty_groups_are_retained_but_not_retried(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    task["tracks"] = []
    for genotype in ("0/0", "0/1", "1/1"):
        task["genotype_groups"][genotype] = {"selected_count": 0, "empty": True}
    task["input_fingerprint"] = sha256_json(
        {key: value for key, value in task.items() if key != "input_fingerprint"}
    )
    output = tmp_path / "empty_bundle"
    result = validate_case_inputs(
        task,
        _input_map(task),
        output,
        shard_id="shard_001",
        session_id="session_001",
    )
    assert result["status"] == "DOMAIN_FAILED"
    assert result["failures"] == [
        {
            "class": "DOMAIN",
            "code": "NO_ELIGIBLE_SAMPLES",
            "message": "no eligible BAM tracks are available across genotype groups",
            "rerun_eligible": False,
        }
    ]


def _fake_desktop_session(
    _config,
    *,
    output_png,
    metadata_path,
    log_directory,
    **_kwargs,
) -> DesktopResult:
    output = Path(output_png)
    output.parent.mkdir(parents=True, exist_ok=True)
    image = Image.new("RGB", (1920, 2160), "white")
    ImageDraw.Draw(image).rectangle((0, 0, 959, 2159), fill="black")
    image.save(output)
    metadata = {
        "root_screenshot_publishable": False,
        "capture_mode": "window",
        "geometry_verified": True,
    }
    Path(metadata_path).write_text(json.dumps(metadata), encoding="utf-8")
    logs = Path(log_directory)
    logs.mkdir(parents=True, exist_ok=True)
    stdout = logs / "igv.stdout.log"
    stderr = logs / "igv.stderr.log"
    stdout.write_text("", encoding="utf-8")
    stderr.write_text("", encoding="utf-8")
    now = time.time()
    return DesktopResult(
        screenshot=output,
        metadata=metadata,
        started_at_epoch=now,
        ended_at_epoch=now + 1,
        wall_time_seconds=1.0,
        peak_rss_gb=2.5,
        stdout_path=stdout,
        stderr_path=stderr,
    )


def test_render_case_uses_native_contract_without_shared_state(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    inputs = _input_map(task)
    with patch("ssqtl_igv.case_inputs._samtools_check", return_value=(True, "")):
        validation = validate_case_inputs(
            task,
            inputs,
            tmp_path / "validated-for-render",
            shard_id="shard_001",
            session_id="session_001",
        )
    with patch("ssqtl_igv.render_case.run_desktop_session", side_effect=_fake_desktop_session):
        result = render_case(
            task,
            inputs,
            validation,
            tmp_path / "params.json",
            tmp_path / "render_bundle",
            shard_id="shard_001",
            session_id="session_001",
        )
    assert result["status"] == "SUCCEEDED"
    roles = {artifact["role"] for artifact in result["artifacts"]}
    assert {
        "local_genome_definition",
        "resolved_render_case",
        "resolved_render_config",
        "igv_batch",
        "raw_igv_png",
        "capture_metadata",
        "raw_qc",
        "render_runtime",
    }.issubset(roles)
    batch = (tmp_path / "render_bundle" / "batch" / "igv_batch.txt").read_text()
    assert "preference SAM.SHOW_COV_TRACK true" in batch
    assert "preference SAM.SHOW_ALIGNMENT_TRACK true" in batch
    assert "preference SAM.SHOW_JUNCTION_TRACK true" in batch
    resolved_case = json.loads(
        (tmp_path / "render_bundle" / "runtime" / "resolved_render_case.json").read_text()
    )
    assert Path(resolved_case["genome"]["definition"]).is_file()
    assert str(Path(resolved_case["genome"]["definition"])).startswith(
        str(tmp_path / "render_bundle")
    )
    assert not (tmp_path / "render_bundle" / ".work").exists()


def test_render_domain_failure_still_emits_bundle(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    inputs = _input_map(task)
    with patch("ssqtl_igv.case_inputs._samtools_check", return_value=(True, "")):
        validation = validate_case_inputs(
            task,
            inputs,
            tmp_path / "validated-for-render-failure",
            shard_id="shard_001",
            session_id="session_001",
        )
    with patch(
        "ssqtl_igv.render_case.run_desktop_session",
        side_effect=DesktopFailure("IGV_WINDOW_NOT_FOUND", "fixture failure"),
    ):
        result = render_case(
            task,
            inputs,
            validation,
            tmp_path / "params.json",
            tmp_path / "render_failure_bundle",
            shard_id="shard_001",
            session_id="session_001",
        )
    assert result["status"] == "DOMAIN_FAILED"
    assert result["failures"][0]["code"] == "IGV_WINDOW_NOT_FOUND"
    assert (tmp_path / "render_failure_bundle" / "stage_result.json").is_file()


def _fake_violin_render(_pdf, _page, output_png, **_kwargs) -> None:
    image = Image.new("RGB", (900, 1200), "white")
    ImageDraw.Draw(image).ellipse((100, 100, 800, 1100), fill="navy")
    image.save(output_png)


def test_compose_and_qc_emit_review_safe_incomplete_evidence(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    inputs = _input_map(task)
    validation_dir = tmp_path / "validation-for-compose"
    with patch("ssqtl_igv.case_inputs._samtools_check", return_value=(True, "")):
        validation = validate_case_inputs(
            task,
            inputs,
            validation_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    render_dir = tmp_path / "render-for-compose"
    with patch("ssqtl_igv.render_case.run_desktop_session", side_effect=_fake_desktop_session):
        render_case(
            task,
            inputs,
            validation,
            tmp_path / "params.json",
            render_dir,
            shard_id="shard_001",
            session_id="session_001",
        )

    compose_dir = tmp_path / "compose-bundle"
    with patch("ssqtl_igv.composition.render_pdf_page", side_effect=_fake_violin_render):
        composition = compose_case(
            task,
            render_dir,
            task["plot"]["pdf"],
            tmp_path / "params.json",
            compose_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    assert composition["status"] == "SUCCEEDED"
    sample_table = (compose_dir / "combined" / f"{task['task_id']}.samples.tsv").read_text()
    assert "bam" not in sample_table.lower()
    assert "bai" not in sample_table.lower()
    assert "sample-1" in sample_table

    qc_dir = tmp_path / "qc-bundle"
    with patch(
        "ssqtl_igv.qc_case.evaluate_evidence",
        return_value={
            "status": "PASS",
            "failed_codes": [],
            "automation_scope": "EVIDENCE_ONLY",
            "automatic_rerun": False,
            "control_action": "MANUAL_REVIEW",
        },
    ):
        qc = qc_case(
            task,
            validation_dir,
            render_dir,
            compose_dir,
            qc_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    assert qc["status"] == "SUCCEEDED"
    case_result = json.loads((qc_dir / "case_result.json").read_text())
    validate_case_result_document(case_result)
    assert case_result["render_state"] == "SUCCEEDED"
    assert case_result["evidence_state"] == "EVIDENCE_INCOMPLETE"
    assert case_result["scientific_interpretation"] == "INDETERMINATE"
    assert case_result["rerun_eligible"] is False
    assert [artifact["role"] for artifact in case_result["artifacts"]] == [
        "combined_png",
        "sample_table",
        "composition_layout",
        "combined_qc",
        "scientific_qc",
    ]


def test_all_empty_domain_path_reaches_nonrerunnable_case_result(tmp_path: Path) -> None:
    task = _normalized_task(tmp_path)
    task["tracks"] = []
    for genotype in ("0/0", "0/1", "1/1"):
        task["genotype_groups"][genotype] = {"selected_count": 0, "empty": True}
    task["input_fingerprint"] = sha256_json(
        {key: value for key, value in task.items() if key != "input_fingerprint"}
    )
    inputs = _input_map(task)
    validation_dir = tmp_path / "empty-validation"
    validation = validate_case_inputs(
        task,
        inputs,
        validation_dir,
        shard_id="shard_001",
        session_id="session_001",
    )
    render_dir = tmp_path / "empty-render"
    render_case(
        task,
        inputs,
        validation,
        tmp_path / "params.json",
        render_dir,
        shard_id="shard_001",
        session_id="session_001",
    )
    compose_dir = tmp_path / "empty-compose"
    compose_case(
        task,
        render_dir,
        task["plot"]["pdf"],
        tmp_path / "params.json",
        compose_dir,
        shard_id="shard_001",
        session_id="session_001",
    )
    qc_dir = tmp_path / "empty-qc"
    qc = qc_case(
        task,
        validation_dir,
        render_dir,
        compose_dir,
        qc_dir,
        shard_id="shard_001",
        session_id="session_001",
    )
    assert qc["status"] == "DOMAIN_FAILED"
    case_result = json.loads((qc_dir / "case_result.json").read_text())
    validate_case_result_document(case_result)
    assert case_result["empty_genotype_groups"] == ["0/0", "0/1", "1/1"]
    assert case_result["evidence_state"] == "EVIDENCE_INCOMPLETE"
    assert case_result["scientific_interpretation"] == "INDETERMINATE"
    assert case_result["rerun_eligible"] is False
    assert case_result["failures"][0]["code"] == "NO_ELIGIBLE_SAMPLES"


def _successful_qc_fixture(tmp_path: Path) -> tuple[dict, Path]:
    task = _normalized_task(tmp_path)
    inputs = _input_map(task)
    validation_dir = tmp_path / "reconcile-validation"
    with patch("ssqtl_igv.case_inputs._samtools_check", return_value=(True, "")):
        validation = validate_case_inputs(
            task,
            inputs,
            validation_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    render_dir = tmp_path / "reconcile-render"
    with patch("ssqtl_igv.render_case.run_desktop_session", side_effect=_fake_desktop_session):
        render_case(
            task,
            inputs,
            validation,
            tmp_path / "params.json",
            render_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    compose_dir = tmp_path / "reconcile-compose"
    with patch("ssqtl_igv.composition.render_pdf_page", side_effect=_fake_violin_render):
        compose_case(
            task,
            render_dir,
            task["plot"]["pdf"],
            tmp_path / "params.json",
            compose_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    qc_dir = tmp_path / "reconcile-qc"
    with patch(
        "ssqtl_igv.qc_case.evaluate_evidence",
        return_value={"status": "PASS", "failed_codes": []},
    ):
        qc_case(
            task,
            validation_dir,
            render_dir,
            compose_dir,
            qc_dir,
            shard_id="shard_001",
            session_id="session_001",
        )
    return task, qc_dir


def test_shard_and_run_reconciliation_are_exact_and_ordered(tmp_path: Path) -> None:
    task, qc_dir = _successful_qc_fixture(tmp_path)
    canonical = tmp_path / "canonical.jsonl"
    write_jsonl(canonical, [task])
    plan_root = tmp_path / "plan"
    create_shards(canonical, plan_root)
    shard_summary = tmp_path / "shard-summary"
    summary = summarize_shard(
        plan_root / "shards" / "shard_001.jsonl",
        [qc_dir],
        shard_summary,
        shard_id="shard_001",
        session_id="session_001",
        pipeline_commit="a" * 40,
    )
    assert summary["terminal_result_count"] == 1
    assert summary["status"] == "COMPLETED"
    assert (shard_summary / "rerun_manifest.tsv").read_text().splitlines() == [
        "\t".join(
            [
                "manifest_order",
                "task_id",
                "shard_id",
                "generation_id",
                "input_fingerprint",
                "failure_codes",
                "rerun_reason",
            ]
        )
    ]

    run_root = tmp_path / "aggregate"
    run = aggregate_run(
        canonical,
        plan_root / "shard_plan.json",
        [shard_summary],
        run_root,
    )
    assert run["status"] == "RECONCILED"
    assert run["expected_task_count"] == run["terminal_result_count"] == 1
    results = list(read_jsonl(run_root / "case_results.jsonl"))
    assert [result["task_id"] for result in results] == [task["task_id"]]


def test_shard_reconciliation_rejects_missing_or_duplicate_terminal_results(
    tmp_path: Path,
) -> None:
    task, qc_dir = _successful_qc_fixture(tmp_path)
    shard_manifest = tmp_path / "shard.jsonl"
    write_jsonl(shard_manifest, [task])
    with pytest.raises(ValueError, match="missing terminal QC"):
        summarize_shard(
            shard_manifest,
            [],
            tmp_path / "missing-summary",
            shard_id="shard_001",
            session_id="session_001",
            pipeline_commit="b" * 40,
        )
    with pytest.raises(ValueError, match="duplicate QC result"):
        summarize_shard(
            shard_manifest,
            [qc_dir, qc_dir],
            tmp_path / "duplicate-summary",
            shard_id="shard_001",
            session_id="session_001",
            pipeline_commit="b" * 40,
        )


def _review_package_fixture(tmp_path: Path) -> tuple[dict, Path, Path, Path]:
    task, qc_dir = _successful_qc_fixture(tmp_path)
    compose_dir = tmp_path / "reconcile-compose"
    canonical = tmp_path / "review-canonical.jsonl"
    write_jsonl(canonical, [task])
    plan_root = tmp_path / "review-plan"
    create_shards(canonical, plan_root)
    shard_root = tmp_path / "review-shard-summary"
    summarize_shard(
        plan_root / "shards" / "shard_001.jsonl",
        [qc_dir],
        shard_root,
        shard_id="shard_001",
        session_id="session_001",
        pipeline_commit="c" * 40,
    )
    aggregate_root = tmp_path / "review-aggregate"
    aggregate_run(
        canonical,
        plan_root / "shard_plan.json",
        [shard_root],
        aggregate_root,
    )
    package_root = tmp_path / "review-package"
    build_review_package(
        canonical,
        aggregate_root / "case_results.jsonl",
        [compose_dir],
        [qc_dir],
        package_root,
    )
    return task, aggregate_root, package_root, qc_dir


def test_review_package_is_path_redacted_and_review_binding_is_exact(tmp_path: Path) -> None:
    task, aggregate_root, package_root, _qc_dir = _review_package_fixture(tmp_path)
    for path in package_root.rglob("*"):
        if path.is_file() and path.suffix.lower() != ".png":
            text = path.read_text(encoding="utf-8").lower()
            assert "/tmp/" not in text
            assert ".bam" not in text
            assert ".bai" not in text
    contract = next(read_jsonl(package_root / "review_contract.jsonl"))
    review = {
        "schema_version": "2.0",
        "review_record_id": "review_001",
        **{field: contract[field] for field in REVIEW_BINDING_FIELDS},
        "artifact_review_state": "APPROVE",
        "scientific_interpretation": "INDETERMINATE",
        "reviewer": "fixture-reviewer",
        "reviewed_at": "2026-07-21T12:00:00Z",
        "notes": "fixture approval",
        "manual_assertions": {assertion: True for assertion in REVIEW_ASSERTIONS},
    }
    reviews = tmp_path / "reviews.jsonl"
    write_jsonl(reviews, [review])
    validated = tmp_path / "validated-reviews"
    report = validate_reviews(
        package_root / "review_contract.jsonl",
        reviews,
        aggregate_root / "case_results.jsonl",
        validated,
    )
    assert report["status"] == "PASS"
    reviewed = next(read_jsonl(validated / "reviewed_case_results.jsonl"))
    assert reviewed["task_id"] == task["task_id"]
    assert reviewed["artifact_review_state"] == "APPROVE"
    assert reviewed["scientific_interpretation"] == "INDETERMINATE"
    assert reviewed["publication_state"] == "READY"


def test_incomplete_evidence_rejects_non_indeterminate_human_interpretation(
    tmp_path: Path,
) -> None:
    _task, aggregate_root, package_root, _qc_dir = _review_package_fixture(tmp_path)
    contract = next(read_jsonl(package_root / "review_contract.jsonl"))
    review = {
        "schema_version": "2.0",
        "review_record_id": "review_001",
        **{field: contract[field] for field in REVIEW_BINDING_FIELDS},
        "artifact_review_state": "APPROVE",
        "scientific_interpretation": "SUPPORTED",
        "reviewer": "fixture-reviewer",
        "reviewed_at": "2026-07-21T12:00:00Z",
        "notes": "invalid policy choice",
        "manual_assertions": {assertion: True for assertion in REVIEW_ASSERTIONS},
    }
    reviews = tmp_path / "invalid-reviews.jsonl"
    write_jsonl(reviews, [review])
    with pytest.raises(ValueError, match="INDETERMINATE"):
        validate_reviews(
            package_root / "review_contract.jsonl",
            reviews,
            aggregate_root / "case_results.jsonl",
            tmp_path / "invalid-review-output",
        )


def _validated_review_fixture(tmp_path: Path) -> tuple[Path, Path]:
    _task, aggregate_root, package_root, _qc_dir = _review_package_fixture(tmp_path)
    contract = next(read_jsonl(package_root / "review_contract.jsonl"))
    review = {
        "schema_version": "2.0",
        "review_record_id": "review_001",
        **{field: contract[field] for field in REVIEW_BINDING_FIELDS},
        "artifact_review_state": "APPROVE",
        "scientific_interpretation": "INDETERMINATE",
        "reviewer": "fixture-reviewer",
        "reviewed_at": "2026-07-21T12:00:00Z",
        "notes": "fixture approval",
        "manual_assertions": {assertion: True for assertion in REVIEW_ASSERTIONS},
    }
    reviews = tmp_path / "publication-reviews.jsonl"
    write_jsonl(reviews, [review])
    validation_root = tmp_path / "publication-review-validation"
    validate_reviews(
        package_root / "review_contract.jsonl",
        reviews,
        aggregate_root / "case_results.jsonl",
        validation_root,
    )
    return package_root, validation_root


def test_publication_is_checksum_bound_reviewed_only_and_atomic(tmp_path: Path) -> None:
    package_root, validation_root = _validated_review_fixture(tmp_path)
    destination = tmp_path / "published"
    result = publish_reviewed(package_root, validation_root, destination)
    assert result["published_case_count"] == 1
    assert result["withheld_case_count"] == 0
    assert len(list((destination / "review_by_chr").rglob("*.png"))) == 1
    assert len(list((destination / "tables").rglob("*.samples.tsv"))) == 1
    verify_checksum_tree(destination)
    assert "PUBLISHED" in (destination / "final_status.tsv").read_text()
    for path in destination.rglob("*"):
        if path.is_file() and path.suffix.lower() != ".png":
            text = path.read_text(encoding="utf-8").lower()
            assert "/tmp/" not in text
            assert ".bam" not in text
            assert ".bai" not in text


def test_publication_failure_leaves_no_partial_destination(tmp_path: Path) -> None:
    package_root, validation_root = _validated_review_fixture(tmp_path)
    destination = tmp_path / "must-not-exist"
    with patch("ssqtl_igv.publication.shutil.copyfile", side_effect=OSError("injected")):
        with pytest.raises(OSError, match="injected"):
            publish_reviewed(package_root, validation_root, destination)
    assert not destination.exists()
    assert not list(tmp_path.glob(".must-not-exist.staging-*"))
