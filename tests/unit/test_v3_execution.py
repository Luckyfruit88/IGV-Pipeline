from __future__ import annotations

import json
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from ssqtl_igv.accounting import _terminal_bundle_inventory
from ssqtl_igv.contracts import validate_v3_case_result_document
from ssqtl_igv.desktop import _igv_command_listener_args
from ssqtl_igv.sharding_v3 import create_bounded_shards
from ssqtl_igv.orchestrator_v3 import (
    _admit_session_case_outputs,
    _recover_prior_attempt_accounting,
    _verify_canonical_input_identities,
    execute_portable_run,
    prepare_portable_run,
)
from ssqtl_igv.runtime_identity import create_runtime_manifest
from ssqtl_igv.utils import read_jsonl, sha256_file, sha256_json, write_tsv
from ssqtl_igv.v3_manifest import GENERIC_MANIFEST_FIELDS, normalize_generic_manifest
from ssqtl_igv.v3_worker import (
    PortableRenderConfig,
    RetryableRenderFailure,
    _case_result,
    _identity_matches,
    _samtools_validate,
    run_portable_task,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _runtime_identity_payload() -> dict:
    return create_runtime_manifest(
        source_commit="c" * 40,
        source_tree="d" * 40,
        tools={
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
        },
        materials_path=PROJECT_ROOT / "containers/runtime-materials.lock.json",
        runtime_config_path=PROJECT_ROOT / "src/ssqtl_igv/resources/v3-runtime.yaml",
    )


def _project_binding(*, source_sha: str = "a" * 64) -> dict:
    body = {
        "schema_version": "3.0-project-source-binding",
        "adapter": "generic",
        "project": {"declared_path": "project.yaml", "sha256": source_sha},
        "inputs": {"cases": {"declared_path": "cases.tsv", "sha256": source_sha}},
        "reference": {"declared_path": "reference.yaml", "sha256": source_sha},
    }
    return {**body, "binding_sha256": sha256_json(body)}


def _fixture(
    tmp_path: Path, *, cases: int = 1, special_input_name: bool = False
) -> tuple[Path, Path, Path]:
    input_root = tmp_path / "input"
    reference_root = tmp_path / "reference"
    input_root.mkdir()
    reference_root.mkdir()
    rows = []
    for index in range(1, cases + 1):
        bam_name = (
            f"case {index}'s alignment.bam" if special_input_name else f"case{index}.bam"
        )
        bam = input_root / bam_name
        bai = input_root / f"{bam_name}.bai"
        bam.write_bytes(f"bam-{index}".encode())
        bai.write_bytes(f"bai-{index}".encode())
        rows.append(
            [
                "3.0",
                f"case_{index}",
                f"chr1:{index}-{index + 1}",
                "+",
                bam.name,
                bai.name,
                f"track {index}",
                "group-a",
                "",
                "",
            ]
        )
    resources = {
        "definition": reference_root / "genome.json",
        "fasta": reference_root / "genome.fa",
        "fai": reference_root / "genome.fa.fai",
        "cytoband": reference_root / "cytoband.txt",
        "annotation": reference_root / "annotation.gff",
    }
    resources["definition"].write_text("{}\n", encoding="utf-8")
    resources["fasta"].write_text(">chr1\nACGT\n", encoding="utf-8")
    resources["fai"].write_text("chr1\t4\t6\t4\t5\n", encoding="utf-8")
    resources["cytoband"].write_text("chr1\t0\t4\tp1\tgneg\n", encoding="utf-8")
    resources["annotation"].write_text("##gff-version 3\n", encoding="utf-8")
    reference = reference_root / "reference.yaml"
    reference.write_text(
        yaml.safe_dump(
            {
                "schema_version": "3.0",
                "id": "fixture",
                "display_name": "Fixture",
                "version": "fixture-v1",
                "resources": {
                    role: {"path": path.name, "sha256": sha256_file(path)}
                    for role, path in resources.items()
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    manifest = tmp_path / "cases.tsv"
    manifest.write_text(
        "\t".join(GENERIC_MANIFEST_FIELDS)
        + "\n"
        + "".join("\t".join(row) + "\n" for row in rows),
        encoding="utf-8",
    )
    return manifest, input_root, reference


def _normalized(tmp_path: Path, *, cases: int = 1) -> dict:
    manifest, input_root, reference = _fixture(tmp_path, cases=cases)
    return normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        tmp_path / "contract",
        "test_run_001",
        "generation_001",
    )


def test_bounded_sharding_preserves_one_based_manifest_order(tmp_path: Path) -> None:
    normalized = _normalized(tmp_path, cases=3)
    plan = create_bounded_shards(
        normalized["tasks"], tmp_path / "shards", max_cases_per_shard=2
    )
    assert [row["case_count"] for row in plan["shards"]] == [2, 1]
    assert plan["scheduling_role"] == "LOGICAL_ONLY"
    assert [task["manifest_order"] for task in read_jsonl(plan["shards"][0]["path"])] == [1, 2]


def test_v3_render_policy_and_runtime_disable_igv_command_listener(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    policy = dict(task["core"]["render_contract"])
    fingerprint = policy.pop("policy_fingerprint")
    assert policy["command_listener_enabled"] is False
    assert fingerprint == sha256_json(policy)

    runtime = yaml.safe_load(
        (PROJECT_ROOT / "src/ssqtl_igv/resources/v3-runtime.yaml").read_text(
            encoding="utf-8"
        )
    )
    config = PortableRenderConfig(runtime)
    assert config.get("desktop.command_listener_enabled") is False
    assert _igv_command_listener_args(config, ":90") == []

    enabled = PortableRenderConfig(
        {"desktop": {"command_listener_enabled": True, "command_port_base": 61000}}
    )
    assert _igv_command_listener_args(enabled, ":90") == ["--port", "61090"]


def test_staged_identity_detects_same_size_mtime_replacement(tmp_path: Path) -> None:
    path = tmp_path / "input.bam"
    path.write_bytes(b"aaaa")
    identity = {"size": 4, "mtime_ns": path.stat().st_mtime_ns, "sha256": None}
    assert _identity_matches(path, identity)
    path.write_bytes(b"bbbb")
    changed = identity["mtime_ns"] + 1_000_000_000
    os.utime(path, ns=(changed, changed))
    assert not _identity_matches(path, identity)


def test_normalized_bam_uses_metadata_identity_and_nextflow_path_cache(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    bam = task["core"]["tracks"][0]["bam"]
    assert set(bam["identity"]) == {"size", "mtime_ns"}

    process = (PROJECT_ROOT / "modules/local/run_portable_case.nf").read_text(
        encoding="utf-8"
    )
    assert "cache true" in process
    assert "path(staged_inputs" in process


def test_prelaunch_revalidation_hashes_shared_fixed_resources_once(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path, cases=2)
    tasks = list(read_jsonl(normalized["tasks"]))

    with patch(
        "ssqtl_igv.orchestrator_v3.sha256_file", wraps=sha256_file
    ) as hash_file:
        result = _verify_canonical_input_identities(tasks)

    assert result["status"] == "PASS"
    assert result["unique_resource_count"] == 9
    assert hash_file.call_count == 5


def test_prelaunch_revalidation_requires_reference_sha256(tmp_path: Path) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    task["core"]["reference"]["resources"]["fasta"]["identity"].pop("sha256")

    with pytest.raises(ValueError, match="reference:fasta resource lacks content SHA-256"):
        _verify_canonical_input_identities([task])


def test_prelaunch_revalidation_requires_auxiliary_sha256(tmp_path: Path) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    source = tmp_path / "panel.png"
    source.write_bytes(b"synthetic-panel")
    task["core"]["auxiliary"] = {
        "state": "PRESENT",
        "source_path": str(source.resolve()),
        "identity": {
            "size": source.stat().st_size,
            "mtime_ns": source.stat().st_mtime_ns,
        },
    }

    with pytest.raises(ValueError, match="auxiliary resource lacks content SHA-256"):
        _verify_canonical_input_identities([task])


def test_prelaunch_revalidation_rejects_same_path_with_conflicting_identity(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    track = task["core"]["tracks"][0]
    track["bai"]["source_path"] = track["bam"]["source_path"]

    with pytest.raises(ValueError, match="canonical source has conflicting identities"):
        _verify_canonical_input_identities([task])


def test_samtools_validation_uses_the_explicit_nondefault_bai(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bam = tmp_path / "sample.bam"
    explicit_bai = tmp_path / "indexes" / "sample.custom.bai"
    default_bai = tmp_path / "sample.bam.bai"
    explicit_bai.parent.mkdir()
    bam.write_bytes(b"fixture-bam")
    explicit_bai.write_bytes(b"fixture-explicit-index")
    default_bai.write_bytes(b"wrong-default-index")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        if command[1] == "quickcheck":
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 0, "chr1\t10\t1\t0\n", "")

    monkeypatch.setattr("ssqtl_igv.v3_worker.subprocess.run", fake_run)
    _samtools_validate(
        [
            {
                "track_label": "explicit-index-track",
                "bam": {"stage_name": "track.bam"},
                "bai": {"stage_name": "track.explicit.bai"},
            }
        ],
        {"track.bam": bam, "track.explicit.bai": explicit_bai},
        "samtools",
    )

    assert calls[0][0] == ["samtools", "quickcheck", "-v", str(bam)]
    assert calls[1][0] == [
        "samtools",
        "idxstats",
        "-X",
        str(bam),
        str(explicit_bai),
    ]
    assert calls[1][0][-1] != str(default_bai)
    assert "env" not in calls[1][1]


def test_generic_fake_worker_is_debug_only_and_produces_review_pixels(tmp_path: Path) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for track in task["core"]["tracks"]
        for resource in (track["bam"], track["bai"])
    }
    staged.update(
        {
            resource["stage_name"]: resource["source_path"]
            for resource in task["core"]["reference"]["resources"].values()
        }
    )
    output = tmp_path / "case-output"
    result = run_portable_task(task, staged, output, fake_runtime=True)
    assert result["render_state"] == "SUCCEEDED"
    assert result["eligible"] is False
    assert result["debug_only"] is True
    assert result["scientific_interpretation"] == "NOT_APPLICABLE"
    assert (output / "review.png").is_file()
    assert (output / "terminal_bundle.json").is_file()
    capture = json.loads((output / "raw" / "capture.json").read_text(encoding="utf-8"))
    assert capture["command_listener_enabled"] is False
    assert capture["command_port"] is None
    assert capture["locale"] == "C.UTF-8"
    batch_lines = (output / "igv.batch.txt").read_text(encoding="utf-8").splitlines()
    assert batch_lines[-1] == "snapshot batch_ready.png"
    session = ET.parse(output / "igv.session.xml").getroot()
    resource_names = [row.attrib.get("name") for row in session.findall("./Resources/Resource")]
    assert resource_names[0] == "001 | [group-a] track 1"
    display = json.loads((output / "track_display_contract.json").read_text())
    assert display["meaning"].endswith("no scientific semantics")


def test_invalid_auxiliary_image_is_a_terminal_case_failure(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    auxiliary = input_root / "broken.png"
    auxiliary.write_bytes(b"not-a-png")
    lines = manifest.read_text(encoding="utf-8").splitlines()
    row = lines[1].split("\t")
    row[-2] = auxiliary.name
    manifest.write_text(lines[0] + "\n" + "\t".join(row) + "\n", encoding="utf-8")
    normalized = normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        tmp_path / "contract",
        "test_run_001",
        "generation_001",
    )
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for resource in (
            [item for track in task["core"]["tracks"] for item in (track["bam"], track["bai"])]
            + list(task["core"]["reference"]["resources"].values())
            + [task["core"]["auxiliary"]]
        )
    }
    output = tmp_path / "case-output"
    result = run_portable_task(task, staged, output, fake_runtime=True)

    assert result["render_state"] == "FAILED"
    assert result["failures"][0]["code"] == "CASE_RENDER_FAILED"
    terminal = json.loads((output / "terminal_bundle.json").read_text())
    assert terminal["status"] == "DOMAIN_FAILED"


def test_retryable_resource_failure_uses_two_retries_then_terminal_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for resource in (
            [
                item
                for track in task["core"]["tracks"]
                for item in (track["bam"], track["bai"])
            ]
            + list(task["core"]["reference"]["resources"].values())
        )
    }

    def resource_failure(*_args: object, **_kwargs: object) -> dict:
        raise RetryableRenderFailure(
            "TIMEOUT", "PIXEL_STABILITY_TIMEOUT", "fixture timeout"
        )

    monkeypatch.setattr("ssqtl_igv.v3_worker._run_generic", resource_failure)
    for attempt in (1, 2):
        with pytest.raises(RetryableRenderFailure):
            run_portable_task(
                task,
                staged,
                tmp_path / f"retry-{attempt}",
                attempt=attempt,
            )
        assert not (tmp_path / f"retry-{attempt}" / "terminal_bundle.json").exists()

    final_root = tmp_path / "retry-3"
    result = run_portable_task(task, staged, final_root, attempt=3)
    assert result["render_state"] == "FAILED"
    assert result["failures"][0]["code"] == "RESOURCE_EXHAUSTED"
    terminal = json.loads((final_root / "terminal_bundle.json").read_text())
    assert terminal["status"] == "RESOURCE_EXHAUSTED"
    assert terminal["attempt"] == 3


def test_render_process_uses_dynamic_attempt_policy_and_no_native_duplication() -> None:
    root = Path(__file__).resolve().parents[2]
    module = (root / "modules/local/run_portable_case.nf").read_text(
        encoding="utf-8"
    )
    assert "task.attempt" in module
    assert "task.exitStatus in [75, 137, 143]" in module
    assert "maxRetries 2" in module
    assert "IGV_HEAP='${attemptPolicy.igv_heap_argument}'" in module
    assert "enabled: params.publish_intermediate_case_outputs" in module

    base_config = (root / "conf/base.config").read_text(encoding="utf-8")
    assert "publish_intermediate_case_outputs = false" in base_config


def test_two_shard_recovery_keeps_shared_runtime_validator_lineage(tmp_path: Path) -> None:
    normalized = _normalized(tmp_path, cases=2)
    tasks = list(read_jsonl(normalized["tasks"]))
    run = tmp_path / "run"
    trace_fields = ["task_id", "hash", "native_id", "name", "status", "exit"]
    for index, task in enumerate(tasks, 1):
        staged = {
            resource["stage_name"]: resource["source_path"]
            for resource in (
                [
                    item
                    for track in task["core"]["tracks"]
                    for item in (track["bam"], track["bai"])
                ]
                + list(task["core"]["reference"]["resources"].values())
            )
        }
        run_portable_task(
            task,
            staged,
            run / "results" / "cases" / task["task_id"],
            fake_runtime=True,
        )
        attempt = run / "sessions" / f"shard-{index:04d}" / "attempt-0001"
        validation = attempt / "runtime_manifest" / "runtime_manifest_validation"
        validation.mkdir(parents=True)
        (validation / "validation.json").write_text(
            json.dumps(
                {
                    "schema_version": "3.0-runtime-manifest-validation",
                    "status": "PASS",
                    "runtime_manifest_sha256": "a" * 64,
                    "runtime_fingerprint_sha256": "b" * 64,
                }
            ),
            encoding="utf-8",
        )
        write_tsv(
            attempt / "trace.txt",
            trace_fields,
            [
                {
                    "task_id": "1",
                    "hash": "aa/shared-validator",
                    "native_id": "-",
                    "name": "PORTABLE_RUN:VALIDATE_RUNTIME_IDENTITY (test)",
                    "status": "COMPLETED",
                    "exit": "0",
                },
                {
                    "task_id": "2",
                    "hash": f"bb/case-{index}",
                    "native_id": "-",
                    "name": f"PORTABLE_RUN:RUN_PORTABLE_CASE ({task['task_id']})",
                    "status": "COMPLETED",
                    "exit": "0",
                },
            ],
        )

    recovered = _recover_prior_attempt_accounting(
        run, tasks, allow_debug_only=True
    )

    assert len(recovered) == 2
    assert all(row["status"] == "PASS" for row in recovered)
    second_expected = list(
        read_jsonl(run / "accounting" / "local" / "generation-0002" / "expected_tasks.jsonl")
    )
    assert {row.get("control_role") for row in second_expected} == {
        None,
        "runtime_manifest_validation",
    }


def test_case_output_admission_never_keeps_a_different_old_result(tmp_path: Path) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for resource in (
            [item for track in task["core"]["tracks"] for item in (track["bam"], track["bai"])]
            + list(task["core"]["reference"]["resources"].values())
        )
    }
    run = tmp_path / "run"
    first_session = tmp_path / "session-1"
    first_source = first_session / "case_outputs" / task["task_id"]
    run_portable_task(task, staged, first_source, fake_runtime=True)
    shard = {"shard_id": "shard-0001", "task_ids": [task["task_id"]]}
    first = _admit_session_case_outputs(
        run,
        first_session,
        shard,
        {task["task_id"]: task},
        allow_debug_only=True,
    )
    assert first["admissions"][0]["action"] == "ADMITTED"
    identical = _admit_session_case_outputs(
        run,
        first_session,
        shard,
        {task["task_id"]: task},
        allow_debug_only=True,
    )
    assert identical["admissions"][0]["action"] == "IDENTICAL_ALREADY_ADMITTED"

    second_session = tmp_path / "session-2"
    second_source = second_session / "case_outputs" / task["task_id"]
    shutil.copytree(first_source, second_source)
    review = second_source / "review.png"
    review.write_bytes(review.read_bytes() + b"different")
    with pytest.raises(ValueError, match="attempt artifact size drift"):
        _admit_session_case_outputs(
            run,
            second_session,
            shard,
            {task["task_id"]: task},
            allow_debug_only=True,
        )


def test_accounting_rejects_v3_terminal_downgrade_and_declared_artifact_drift(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for resource in (
            [
                item
                for track in task["core"]["tracks"]
                for item in (track["bam"], track["bai"])
            ]
            + list(task["core"]["reference"]["resources"].values())
        )
    }
    case_root = tmp_path / "accounting-run" / "results" / "cases" / task["task_id"]
    result = run_portable_task(task, staged, case_root, fake_runtime=True)
    terminal_path = case_root / "terminal_bundle.json"
    original_terminal = json.loads(terminal_path.read_text(encoding="utf-8"))

    downgraded = dict(original_terminal)
    downgraded.pop("schema_version")
    terminal_path.write_text(json.dumps(downgraded), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version must be exactly 3.0"):
        _terminal_bundle_inventory([terminal_path])

    terminal_path.write_text(json.dumps(original_terminal), encoding="utf-8")
    review_relative = Path(result["artifacts"]["review_image"]["relative_path"])
    review_path = tmp_path / "accounting-run" / review_relative
    review_path.write_bytes(review_path.read_bytes() + b"drift")
    with pytest.raises(ValueError, match="artifact size drift"):
        _terminal_bundle_inventory([terminal_path])


def test_ssqtl_v3_case_result_preserves_incomplete_native_evidence(tmp_path: Path) -> None:
    output = tmp_path / "ssqtl-case"
    output.mkdir()
    paths = {
        "review": output / "review.png",
        "scientific_qc": output / "scientific_qc.json",
        "raw_igv": output / "raw_igv.png",
        "capture_metadata": output / "capture_metadata.json",
        "layout": output / "layout.json",
        "raw_qc": output / "raw_qc.json",
        "review_qc": output / "review_qc.json",
        "scientific_case_evidence": output / "scientific_case_evidence.json",
        "scientific_qc_evidence": output / "scientific_qc_evidence.json",
    }
    for name, path in paths.items():
        path.write_text(json.dumps({"fixture": name}) + "\n", encoding="utf-8")
    adapter_evidence = {
        "adapter_schema_version": "3.0-ssqtl",
        "scientific_evidence_available": True,
        "scientific_case_evidence_sha256": sha256_file(paths["scientific_case_evidence"]),
        "scientific_qc_evidence_sha256": sha256_file(
            paths["scientific_qc_evidence"]
        ),
        "scientific_evidence_state": "EVIDENCE_INCOMPLETE",
        "scientific_result_interpretation": "INDETERMINATE",
        "scientific_failure_set_sha256": "0" * 64,
        "empty_genotype_groups": ["0/1", "1/1"],
    }
    result = _case_result(
        {
            "adapter_id": "ssqtl",
            "run_id": "run_001",
            "generation_id": "generation_001",
            "task_id": "case_1",
            "manifest_order": 1,
            "input_fingerprint": "f" * 64,
        },
        output,
        "results/cases/case_1",
        {
            **paths,
            "test_double": False,
            "evidence_state": "EVIDENCE_INCOMPLETE",
            "scientific_interpretation": "INDETERMINATE",
            "pixel_identity": {
                "source_igv_decoded_pixel_sha256": "a" * 64,
                "final_igv_decoded_pixel_sha256": "a" * 64,
                "igv_pixel_identity": True,
            },
            "adapter_evidence": adapter_evidence,
        },
        None,
    )
    validate_v3_case_result_document(result)
    assert result["eligible"] is True
    assert result["evidence_state"] == "EVIDENCE_INCOMPLETE"
    assert result["scientific_interpretation"] == "INDETERMINATE"

def test_production_case_output_admission_rejects_debug_only_result(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for resource in (
            [
                item
                for track in task["core"]["tracks"]
                for item in (track["bam"], track["bai"])
            ]
            + list(task["core"]["reference"]["resources"].values())
        )
    }
    session = tmp_path / "debug-session"
    source = session / "case_outputs" / task["task_id"]
    run_portable_task(task, staged, source, fake_runtime=True)
    shard = {"shard_id": "shard-0001", "task_ids": [task["task_id"]]}
    with pytest.raises(ValueError, match="debug_only|DEBUG_ONLY"):
        _admit_session_case_outputs(
            tmp_path / "production-run",
            session,
            shard,
            {task["task_id"]: task},
        )


def test_recovery_admits_attempt_output_before_freezing_interrupted_trace(
    tmp_path: Path,
) -> None:
    normalized = _normalized(tmp_path)
    task = next(read_jsonl(normalized["tasks"]))
    staged = {
        resource["stage_name"]: resource["source_path"]
        for resource in (
            [item for track in task["core"]["tracks"] for item in (track["bam"], track["bai"])]
            + list(task["core"]["reference"]["resources"].values())
        )
    }
    run = tmp_path / "run"
    attempt = run / "sessions" / "shard-0001" / "attempt-0001"
    run_portable_task(
        task,
        staged,
        attempt / "case_outputs" / task["task_id"],
        fake_runtime=True,
    )
    validation = attempt / "runtime_manifest" / "runtime_manifest_validation"
    validation.mkdir(parents=True)
    (validation / "validation.json").write_text(
        json.dumps(
            {
                "schema_version": "3.0-runtime-manifest-validation",
                "status": "PASS",
                "runtime_manifest_sha256": "a" * 64,
                "runtime_fingerprint_sha256": "b" * 64,
            }
        ),
        encoding="utf-8",
    )
    write_tsv(
        attempt / "trace.txt",
        ["task_id", "hash", "native_id", "name", "status", "exit"],
        [
            {
                "task_id": "1",
                "hash": "aa/validator",
                "native_id": "-",
                "name": "PORTABLE_RUN:VALIDATE_RUNTIME_IDENTITY (test)",
                "status": "COMPLETED",
                "exit": "0",
            },
            {
                "task_id": "2",
                "hash": "bb/case",
                "native_id": "-",
                "name": f"PORTABLE_RUN:RUN_PORTABLE_CASE ({task['task_id']})",
                "status": "COMPLETED",
                "exit": "0",
            },
        ],
    )
    assert not (run / "results" / "cases" / task["task_id"]).exists()

    recovered = _recover_prior_attempt_accounting(
        run, [task], allow_debug_only=True
    )

    assert len(recovered) == 1
    assert (run / "results" / "cases" / task["task_id"] / "terminal_bundle.json").is_file()
    assert recovered[0]["status"] == "PASS"


@pytest.mark.skipif(
    not (PROJECT_ROOT / ".tools" / "nextflow-25.04.7" / "nextflow").is_file(),
    reason="pinned Nextflow executable is unavailable",
)
def test_orchestrator_fake_run_closes_accounting_and_requires_new_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, input_root, reference = _fixture(tmp_path, special_input_name=True)
    identity = tmp_path / "runtime-identity.json"
    identity.write_text(json.dumps(_runtime_identity_payload()) + "\n", encoding="utf-8")
    java_home = PROJECT_ROOT / ".tools" / "jdk-21.0.4+7-jre" / "Contents" / "Home"
    java_command = tmp_path / "java21"
    java_command.symlink_to(java_home / "bin" / "java")
    monkeypatch.setenv("JAVA_CMD", str(java_command))
    monkeypatch.setenv("JAVA_HOME", "")
    monkeypatch.setenv("NXF_JAVA_HOME", "")
    monkeypatch.setenv("NXF_HOME", str(tmp_path / "nxf-home"))
    monkeypatch.setenv("NXF_VER", "25.04.7")
    run = tmp_path / "run"
    arguments = {
        "run_dir": run,
        "run_id": "test_run_001",
        "generation_id": "generation_001",
        "profile": "test",
        "adapter": "generic",
        "runtime_identity_path": identity,
    }
    prepared = prepare_portable_run(
        **arguments,
        manifest=manifest,
        input_root=input_root,
        reference=reference,
    )
    first = execute_portable_run(
        prepared,
        profile="test",
        runtime_identity_path=identity,
        nextflow=PROJECT_ROOT / ".tools" / "nextflow-25.04.7" / "nextflow",
        fake_runtime=True,
    )
    assert first["status"] == "CASE_FAILURES"
    assert first["exit_code"] == 2
    assert first["accounting"]["status"] == "PASS"
    assert first["shards"][0]["case_output_admission"]["status"] == "PASS"
    assert first["rerun"]["target_generation_policy"] == (
        "MUST_DIFFER_FROM_SOURCE_GENERATION"
    )
    rerun_root = run / first["rerun"]["relative_path"]
    assert (rerun_root / "rerun_manifest.jsonl").is_file()
    assert (rerun_root / "rerun_receipt.json").is_file()
    assert (rerun_root / "SHA256SUMS").is_file()

    with pytest.raises(ValueError, match="new generation"):
        prepare_portable_run(**arguments, resume=True)


def test_scc_prepare_freezes_site_adapter_and_resume_rejects_drift(
    tmp_path: Path,
) -> None:
    manifest, input_root, reference = _fixture(tmp_path, special_input_name=True)
    identity = tmp_path / "runtime-identity.json"
    identity.write_text(json.dumps(_runtime_identity_payload()) + "\n", encoding="utf-8")
    run = tmp_path / "run"
    arguments = {
        "run_dir": run,
        "run_id": "scc_run_001",
        "generation_id": "generation_001",
        "profile": "scc",
        "adapter": "generic",
        "runtime_identity_path": identity,
        "manifest": manifest,
        "input_root": input_root,
        "reference": reference,
        "scc_project": "fixture-project",
        "scc_qname": "fixture.q",
    }
    prepared = prepare_portable_run(**arguments)
    assert prepared["identity"]["scc_site_adapter"]["project"] == "fixture-project"
    assert prepared["identity"]["scc_site_adapter"]["qname"] == "fixture.q"
    assert prepare_portable_run(**arguments, resume=True)["identity"] == prepared["identity"]

    with pytest.raises(ValueError, match="resume identity differs"):
        prepare_portable_run(
            **{**arguments, "scc_project": "different-project"}, resume=True
        )
    with pytest.raises(ValueError, match="resume identity differs"):
        prepare_portable_run(**{**arguments, "scc_qname": "other.q"}, resume=True)


def test_fresh_pull_run_allows_entrypoint_bootstrap_directories(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    runtime_manifest = tmp_path / "runtime-manifest.json"
    runtime_manifest.write_text(
        json.dumps(_runtime_identity_payload()) + "\n", encoding="utf-8"
    )
    run = tmp_path / "output"
    (run / ".runtime" / "home").mkdir(parents=True)
    (run / ".runtime" / "nextflow").mkdir()
    (run / ".work").mkdir()

    prepared = prepare_portable_run(
        run_dir=run,
        run_id="pull_run_001",
        generation_id="generation_001",
        profile="test",
        adapter="generic",
        runtime_identity_path=runtime_manifest,
        project_binding=_project_binding(),
        manifest=manifest,
        input_root=input_root,
        reference=reference,
    )

    assert prepared["identity"]["run_id"] == "pull_run_001"
    assert "project_binding_sha256" in prepared["identity"]
    assert (run / "contract" / "run_identity.json").is_file()


def test_fresh_pull_run_rejects_unsafe_runtime_bootstrap_entry(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    runtime_manifest = tmp_path / "runtime-manifest.json"
    runtime_manifest.write_text(
        json.dumps(_runtime_identity_payload()) + "\n", encoding="utf-8"
    )
    run = tmp_path / "output"
    run.mkdir()
    (run / ".runtime").write_text("not a directory\n", encoding="utf-8")

    with pytest.raises(FileExistsError, match="non-empty run directory"):
        prepare_portable_run(
            run_dir=run,
            run_id="pull_run_001",
            generation_id="generation_001",
            profile="test",
            adapter="generic",
            runtime_identity_path=runtime_manifest,
            project_binding=_project_binding(),
            manifest=manifest,
            input_root=input_root,
            reference=reference,
        )


def test_resume_rejects_live_project_metadata_drift(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    runtime_manifest = tmp_path / "runtime-manifest.json"
    runtime_manifest.write_text(
        json.dumps(_runtime_identity_payload()) + "\n", encoding="utf-8"
    )
    run = tmp_path / "output"
    arguments = {
        "run_dir": run,
        "run_id": "pull_run_001",
        "generation_id": "generation_001",
        "profile": "test",
        "adapter": "generic",
        "runtime_identity_path": runtime_manifest,
        "manifest": manifest,
        "input_root": input_root,
        "reference": reference,
    }
    prepare_portable_run(**arguments, project_binding=_project_binding())

    with pytest.raises(ValueError, match="project metadata changed"):
        prepare_portable_run(
            **arguments,
            project_binding=_project_binding(source_sha="b" * 64),
            resume=True,
        )


@pytest.mark.skipif(
    not (PROJECT_ROOT / ".tools" / "nextflow-25.04.7" / "nextflow").is_file(),
    reason="pinned Nextflow executable is unavailable",
)
def test_portable_nextflow_stub_contract_is_executable(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path)
    normalized = normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        tmp_path / "contract",
        "test_run_001",
        "generation_001",
    )
    identity = tmp_path / "runtime-identity.json"
    identity.write_text(json.dumps(_runtime_identity_payload()), encoding="utf-8")
    session = tmp_path / "stub-session"
    java_home = PROJECT_ROOT / ".tools" / "jdk-21.0.4+7-jre" / "Contents" / "Home"
    java_command = tmp_path / "java21-stub"
    java_command.symlink_to(java_home / "bin" / "java")
    command = [
        str(PROJECT_ROOT / ".tools" / "nextflow-25.04.7" / "nextflow"),
        "run",
        str(PROJECT_ROOT),
        "-entry",
        "PORTABLE_RUN",
        "-profile",
        "test",
        "-stub-run",
        "-work-dir",
        str(tmp_path / "stub-work"),
        "--canonical_tasks",
        normalized["tasks"],
        "--run_output",
        str(tmp_path / "stub-output"),
        "--session_id",
        "stub-session",
        "--runtime_identity",
        str(identity),
        "--fake_runtime",
        "true",
        "--session_output",
        str(session),
        "--publish_intermediate_case_outputs",
        "true",
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
            "NXF_HOME": str(tmp_path / "stub-nxf-home"),
        },
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr
    assert (session / "runtime_identity" / "runtime_identity_validation" / "validation.json").is_file()
    assert (session / "case_outputs" / "case_1" / "terminal_bundle.json").is_file()


@pytest.mark.skipif(
    not (PROJECT_ROOT / ".tools" / "nextflow-25.04.7" / "nextflow").is_file(),
    reason="pinned Nextflow executable is unavailable",
)
def test_portable_nextflow_fake_runtime_executes_module_entrypoint(tmp_path: Path) -> None:
    manifest, input_root, reference = _fixture(tmp_path, special_input_name=True)
    normalized = normalize_generic_manifest(
        manifest,
        input_root,
        reference,
        tmp_path / "contract",
        "test_run_001",
        "generation_001",
    )
    identity = tmp_path / "runtime-identity.json"
    identity.write_text(
        json.dumps(_runtime_identity_payload()),
        encoding="utf-8",
    )
    output = tmp_path / "run"
    session = tmp_path / "session"
    second_session = tmp_path / "session-resume"
    work = tmp_path / "work"
    launch = tmp_path / "launch"
    launch.mkdir()
    nextflow = PROJECT_ROOT / ".tools" / "nextflow-25.04.7" / "nextflow"
    java_home = PROJECT_ROOT / ".tools" / "jdk-21.0.4+7-jre" / "Contents" / "Home"
    java_command = tmp_path / "java21"
    java_command.symlink_to(java_home / "bin" / "java")
    command = [
        str(nextflow),
        "run",
        str(PROJECT_ROOT),
        "-entry",
        "PORTABLE_RUN",
        "-profile",
        "test",
        "-work-dir",
        str(work),
        "--canonical_tasks",
        normalized["tasks"],
        "--run_output",
        str(output),
        "--session_id",
        "test-session",
        "--runtime_identity",
        str(identity),
        "--runtime_identity_sha256",
        sha256_file(identity),
        "--runtime_oci_digest",
        "sha256:" + "a" * 64,
        "--fake_runtime",
        "true",
        "--enable_reports",
        "true",
        "--session_output",
        str(session),
        "--publish_intermediate_case_outputs",
        "true",
    ]
    completed = subprocess.run(
        command,
        cwd=launch,
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
    result = json.loads((session / "case_outputs" / "case_1" / "case_result.json").read_text())
    assert result["debug_only"] is True
    assert (session / "trace.txt").is_file()
    resumed_command = command.copy()
    session_index = resumed_command.index(str(session))
    resumed_command[session_index] = str(second_session)
    resumed_command.append("-resume")
    resumed = subprocess.run(
        resumed_command,
        cwd=launch,
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
    assert resumed.returncode == 0, resumed.stdout + "\n" + resumed.stderr
    trace_text = (second_session / "trace.txt").read_text(encoding="utf-8")
    assert "\tCACHED\t" in trace_text
