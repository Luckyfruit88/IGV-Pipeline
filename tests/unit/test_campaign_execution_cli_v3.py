from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from ssqtl_igv import v3_cli


def _ssqtl_project(tmp_path: Path) -> dict:
    return {
        "adapter": "ssqtl",
        "project_root": str(tmp_path),
        "inputs": {
            "associations": {"declared_path": "associations.csv"},
            "rds_dir": {"declared_path": "rds"},
            "bam_lookup": {"declared_path": "bam_lookup.csv"},
            "violin_dir": {"declared_path": "violin"},
            "config": {"declared_path": "ssqtl.yaml"},
        },
        "reference": {"source_path": str(tmp_path / "reference.yaml")},
    }


def test_prepare_master_normalizes_then_freezes_without_rendering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = tmp_path / "runtime-manifest.json"
    runtime.write_text("{}\n", encoding="utf-8")
    temporary = tmp_path / "normalization-temporary"
    bundle = temporary / "bundle"
    bundle.mkdir(parents=True)
    (bundle / "tasks.jsonl").write_text("{}\n", encoding="utf-8")
    captured: dict[str, dict] = {}

    monkeypatch.setenv("IGV_RUNTIME_MANIFEST_INTERNAL", str(runtime))
    monkeypatch.setattr(
        v3_cli, "load_project_config", lambda _path: _ssqtl_project(tmp_path)
    )
    def normalize(**kwargs: object) -> dict:
        captured["normalize"] = dict(kwargs)
        return {
            "bundle": str(bundle),
            "temporary_root": str(temporary),
            "runtime_fingerprint_sha256": "b" * 64,
        }

    def freeze(
        master_tasks: str | Path,
        campaign_dir: str | Path,
        *,
        campaign_id: str,
        actor: str,
    ) -> dict:
        captured["freeze"] = {
            "master_tasks": Path(master_tasks),
            "campaign_dir": Path(campaign_dir),
            "campaign_id": campaign_id,
            "actor": actor,
        }
        assert Path(master_tasks).is_file()
        return {
            "schema_version": "3.0-campaign-prepare-result",
            "status": "PREPARED",
            "pilot_task_count": 100,
        }

    monkeypatch.setattr(v3_cli, "run_portable_ssqtl_normalization", normalize)
    monkeypatch.setattr(v3_cli, "prepare_campaign", freeze)
    args = v3_cli._parser().parse_args(
        [
            "campaign",
            "prepare-master",
            "--project",
            str(tmp_path / "project.yaml"),
            "--campaign-dir",
            str(tmp_path / "campaign"),
            "--campaign-id",
            "fhs-igv-v3",
            "--max-parallel",
            "4",
            "--actor",
            "operator",
        ]
    )

    result = v3_cli._prepare_campaign_master(args)

    assert result["status"] == "PREPARED"
    assert result["pilot_task_count"] == 100
    assert result["runtime_fingerprint_sha256"] == "b" * 64
    assert captured["normalize"]["run_id"] == "fhs-igv-v3"
    assert captured["normalize"]["generation_id"] == "master"
    assert captured["normalize"]["profile"] == "standalone"
    assert captured["normalize"]["max_parallel"] == 4
    assert captured["freeze"]["master_tasks"] == bundle / "tasks.jsonl"
    assert captured["freeze"]["campaign_id"] == "fhs-igv-v3"
    assert not temporary.exists()


def test_run_batch_derives_identity_and_executes_only_validated_subset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = tmp_path / "runtime-manifest.json"
    runtime.write_text("{}\n", encoding="utf-8")
    request = tmp_path / "campaign" / "batches" / "pilot-001" / "batch-request.json"
    request.parent.mkdir(parents=True)
    request.write_text("{}\n", encoding="utf-8")
    output = tmp_path / "output"
    captured: dict[str, dict] = {}

    monkeypatch.setenv("IGV_RUNTIME_MANIFEST_INTERNAL", str(runtime))

    def launch(**kwargs: object) -> tuple[dict, int]:
        captured["launch"] = dict(kwargs)
        return {"status": "SNAPSHOTS_READY", "exit_code": 0}, 0

    monkeypatch.setattr(v3_cli, "run_project_workflow", launch)
    monkeypatch.setattr(v3_cli, "_resume_identity", lambda _output: {})
    args = v3_cli._parser().parse_args(
        [
            "campaign",
            "run-batch",
            "--batch-request",
            str(request),
            "--output",
            str(output),
            "--resume",
            "--max-parallel",
            "8",
            "--max-cases-per-shard",
            "25",
        ]
    )

    result, code = v3_cli._run_campaign_batch(args)

    assert (result["status"], code) == ("SNAPSHOTS_READY", 0)
    assert captured["launch"] == {
        "project": None,
        "batch_request": str(request),
        "output": output,
        "work": None,
        "resume": True,
        "max_parallel": "8",
        "max_cases_per_shard": 25,
        "runtime_manifest": runtime,
        "igv_cpus": 1,
        "igv_memory": "8GiB",
        "igv_timeout": "30m",
        "normalization_cpus": 1,
        "normalization_memory": "12GiB",
        "normalization_timeout": "36h",
    }


def test_run_batch_rejects_unsafe_output_and_invalid_shard_size(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = tmp_path / "batch-request.json"
    request.write_text("{}\n", encoding="utf-8")
    real_output = tmp_path / "real-output"
    real_output.mkdir()
    output_link = tmp_path / "output-link"
    output_link.symlink_to(real_output, target_is_directory=True)
    unsafe = v3_cli._parser().parse_args(
        [
            "campaign",
            "run-batch",
            "--batch-request",
            str(request),
            "--output",
            str(output_link),
        ]
    )
    with pytest.raises(ValueError, match="symlink"):
        v3_cli._run_campaign_batch(unsafe)

    invalid = v3_cli._parser().parse_args(
        [
            "campaign",
            "run-batch",
            "--batch-request",
            str(request),
            "--output",
            str(real_output),
            "--max-cases-per-shard",
            "257",
        ]
    )
    with pytest.raises(ValueError, match="between 1 and 256"):
        v3_cli._run_campaign_batch(invalid)


def test_campaign_execution_options_do_not_change_public_run_options() -> None:
    parser = v3_cli._parser()
    top_level = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    run_parser = top_level.choices["run"]
    options = {
        option
        for action in run_parser._actions
        for option in action.option_strings
        if option not in {"-h", "--help"}
    }
    assert options == {
        "--project",
        "--output",
        "--work",
        "--resume",
        "--max-parallel",
        "--max-cases-per-shard",
        "--igv-cpus",
        "--igv-memory",
        "--igv-timeout",
        "--normalization-cpus",
        "--normalization-memory",
        "--normalization-timeout",
    }
