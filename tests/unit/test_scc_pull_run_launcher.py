from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = PROJECT_ROOT / "scripts" / "submit-bu-scc-pull-run.sh"
EXAMPLE_CONFIG = PROJECT_ROOT / "config" / "bu-scc-site.example.json"


def _fixture_paths(tmp_path: Path) -> tuple[Path, Path, Path]:
    project = tmp_path / "project"
    output = tmp_path / "output"
    software = tmp_path / "software"
    project.mkdir()
    output.mkdir()
    software.mkdir()
    (project / "project.yaml").write_text(
        'schema_version: "3.0"\nadapter: generic\n', encoding="utf-8"
    )
    sif = software / "igv-pipeline_3.0.0.sif"
    sif.write_bytes(b"fixture-sif")
    return project, output, sif


def _write_site_config(
    tmp_path: Path,
    *,
    project: str = "example-project",
    qname: str | None = None,
    pe: str = "omp",
    engine: str = "apptainer",
    slots: int = 8,
    memory_per_slot: str = "8GiB",
    walltime: str = "04:00:00",
) -> Path:
    path = tmp_path / "bu-scc-site.json"
    path.write_text(
        json.dumps(
            {
                "project": project,
                "qname": qname,
                "pe": pe,
                "engine": engine,
                "slots": slots,
                "memory_per_slot": memory_per_slot,
                "walltime": walltime,
            }
        ),
        encoding="utf-8",
    )
    return path


def _command(
    config: Path, project: Path, output: Path, sif: Path, *extra: str
) -> list[str]:
    return [
        "bash",
        str(LAUNCHER),
        "--site-config",
        str(config),
        "--sif",
        str(sif),
        "--project-dir",
        str(project),
        "--output-dir",
        str(output),
        *extra,
    ]


def test_example_config_is_the_minimal_site_contract() -> None:
    value = json.loads(EXAMPLE_CONFIG.read_text(encoding="utf-8"))
    assert value == {
        "project": "YOUR_SCC_PROJECT",
        "qname": None,
        "pe": "omp",
        "engine": "apptainer",
        "slots": 8,
        "memory_per_slot": "8GiB",
        "walltime": "04:00:00",
    }


def test_dry_run_freezes_one_job_resources_and_local_container_execution(
    tmp_path: Path,
) -> None:
    project, output, sif = _fixture_paths(tmp_path)
    config = _write_site_config(
        tmp_path,
        qname="scc-cpu.q",
        slots=4,
        memory_per_slot="16GiB",
        walltime="06:30:00",
    )

    result = subprocess.run(
        _command(config, project, output, sif, "--resume", "--dry-run"),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    command = result.stdout
    assert command.count("qsub") == 1
    assert " -P example-project" in command
    assert " -q scc-cpu.q" in command
    assert " -pe omp 4" in command
    assert " -l mem_per_core=16G" in command
    assert " -l h_rt=06:30:00" in command
    assert " -b y apptainer run" in command
    assert " --cleanenv --containall --no-home --net --network none" in command
    assert " --env IGV_SCC_SLOTS=4" in command
    assert " --env IGV_SCC_MEMORY_PER_SLOT=16GiB" in command
    assert " --env IGV_SCC_WALLTIME=06:30:00" in command
    assert f" --bind {project}:/project:ro" in command
    assert f" --bind {output}:/output:rw" in command
    assert f" {sif} run --max-parallel auto --resume" in command
    assert "nextflow" not in command
    assert "qacct" not in command
    assert "signature" not in command
    assert "runtime-identity" not in command
    assert command.endswith("Dry run only; no job was submitted.\n")


def test_real_submission_invokes_qsub_once_and_reports_job_id(tmp_path: Path) -> None:
    project, output, sif = _fixture_paths(tmp_path)
    config = _write_site_config(tmp_path, engine="singularity")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture = tmp_path / "qsub-arguments.jsonl"
    qsub = fake_bin / "qsub"
    qsub.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$QSUB_CAPTURE\"\nprintf '73642\\n'\n",
        encoding="utf-8",
    )
    singularity = fake_bin / "singularity"
    singularity.write_text("#!/bin/sh\nexit 99\n", encoding="utf-8")
    qsub.chmod(0o755)
    singularity.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}",
        "QSUB_CAPTURE": str(capture),
    }

    result = subprocess.run(
        _command(config, project, output, sif),
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.endswith("Submitted BU SCC pull-run job: 73642\n")
    arguments = capture.read_text(encoding="utf-8").splitlines()
    assert arguments[:7] == [
        "-terse",
        "-N",
        "igv-snapshot-v3",
        "-P",
        "example-project",
        "-pe",
        "omp",
    ]
    assert arguments.count("-pe") == 1
    assert arguments[arguments.index("-pe") + 2] == "8"
    assert "-q" not in arguments
    assert "mem_per_core=8G" in arguments
    assert "h_rt=04:00:00" in arguments
    binary_index = arguments.index("-b") + 2
    assert arguments[binary_index] == str(singularity)
    assert arguments[binary_index + 1 : binary_index + 8] == [
        "run",
        "--cleanenv",
        "--containall",
        "--no-home",
        "--net",
        "--network",
        "none",
    ]
    assert arguments[binary_index + 8 : binary_index + 14] == [
        "--env",
        "IGV_SCC_SLOTS=8",
        "--env",
        "IGV_SCC_MEMORY_PER_SLOT=8GiB",
        "--env",
        "IGV_SCC_WALLTIME=04:00:00",
    ]
    assert arguments[-3:] == ["run", "--max-parallel", "auto"]


def test_batch_request_mode_adds_read_only_campaign_bind_and_keeps_one_job(
    tmp_path: Path,
) -> None:
    project, output, sif = _fixture_paths(tmp_path)
    config = _write_site_config(tmp_path)
    campaign = tmp_path / "campaign"
    request = campaign / "batches" / "pilot-001" / "batch-request.json"
    request.parent.mkdir(parents=True)
    request.write_text("{}\n", encoding="utf-8")

    result = subprocess.run(
        _command(
            config,
            project,
            output,
            sif,
            "--batch-request",
            str(request),
            "--resume",
            "--dry-run",
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    command = result.stdout
    assert command.count("qsub") == 1
    assert f" --bind {project}:/project:ro" in command
    assert f" --bind {campaign}:/campaign:ro" in command
    assert f" --bind {output}:/output:rw" in command
    assert (
        f" {sif} campaign run-batch"
        " --batch-request /campaign/batches/pilot-001/batch-request.json"
        " --output /output --max-parallel auto --resume"
    ) in command
    assert f" {sif} run " not in command
    assert "qacct" not in command


def test_site_config_rejects_unknown_or_shell_active_values(tmp_path: Path) -> None:
    project, output, sif = _fixture_paths(tmp_path)
    base = {
        "project": "example-project",
        "qname": None,
        "pe": "omp",
        "engine": "apptainer",
        "slots": 8,
        "memory_per_slot": "8GiB",
        "walltime": "04:00:00",
    }
    cases = [
        {**base, "project": "example-project;id"},
        {**base, "qname": "queue@node*"},
        {**base, "pe": "omp 8"},
        {**base, "engine": "docker"},
        {**base, "key": "unused"},
        {key: value for key, value in base.items() if key != "slots"},
        {**base, "slots": True},
        {**base, "slots": 0},
        {**base, "slots": 9},
        {**base, "memory_per_slot": "8G"},
        {**base, "memory_per_slot": "0GiB"},
        {**base, "walltime": "4:00:00"},
        {**base, "walltime": "04:60:00"},
        {**base, "walltime": "00:30:00"},
    ]

    for index, value in enumerate(cases):
        config = tmp_path / f"invalid-{index}.json"
        config.write_text(json.dumps(value), encoding="utf-8")
        result = subprocess.run(
            _command(config, project, output, sif, "--dry-run"),
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 2
        assert "invalid BU SCC site config" in result.stderr


def test_launcher_rejects_unsafe_paths_and_overlapping_mounts(tmp_path: Path) -> None:
    project, output, sif = _fixture_paths(tmp_path)
    config = _write_site_config(tmp_path)

    relative = subprocess.run(
        _command(config, Path("relative-project"), output, sif, "--dry-run"),
        text=True,
        capture_output=True,
        check=False,
    )
    assert relative.returncode == 2
    assert "project directory must be absolute" in relative.stderr

    nested_output = project / "output"
    nested_output.mkdir()
    overlap = subprocess.run(
        _command(config, project, nested_output, sif, "--dry-run"),
        text=True,
        capture_output=True,
        check=False,
    )
    assert overlap.returncode == 2
    assert "project and output directories must not overlap" in overlap.stderr

    symlink_project = tmp_path / "project-link"
    symlink_project.mkdir()
    (symlink_project / "project.yaml").symlink_to(project / "project.yaml")
    symlink_result = subprocess.run(
        _command(config, symlink_project, output, sif, "--dry-run"),
        text=True,
        capture_output=True,
        check=False,
    )
    assert symlink_result.returncode == 2
    assert "project.yaml must not be a symlink" in symlink_result.stderr


def test_launcher_rejects_unsafe_batch_request_paths(tmp_path: Path) -> None:
    project, output, sif = _fixture_paths(tmp_path)
    config = _write_site_config(tmp_path)
    campaign = tmp_path / "campaign"
    request = campaign / "batches" / "pilot-001" / "batch-request.json"
    request.parent.mkdir(parents=True)
    request.write_text("{}\n", encoding="utf-8")

    request_link = tmp_path / "batch-request-link.json"
    request_link.symlink_to(request)
    linked = subprocess.run(
        _command(
            config,
            project,
            output,
            sif,
            "--batch-request",
            str(request_link),
            "--dry-run",
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert linked.returncode == 2
    assert "batch-request must not be a symlink" in linked.stderr

    malformed = tmp_path / "wrong" / "batch-request.json"
    malformed.parent.mkdir()
    malformed.write_text("{}\n", encoding="utf-8")
    wrong_tree = subprocess.run(
        _command(
            config,
            project,
            output,
            sif,
            "--batch-request",
            str(malformed),
            "--dry-run",
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    assert wrong_tree.returncode == 2
    assert "CAMPAIGN_ROOT/batches/BATCH_ID/batch-request.json" in wrong_tree.stderr


def test_launcher_source_has_no_nested_scheduler_or_accounting_gate() -> None:
    source = LAUNCHER.read_text(encoding="utf-8")
    assert source.count('qsub_command=(') == 1
    assert "qacct" not in source
    assert "nextflow run" not in source
    assert "--profile" not in source
    assert "certif" not in source.lower()
    assert "signature" not in source.lower()
    assert "runtime-identity" not in source
