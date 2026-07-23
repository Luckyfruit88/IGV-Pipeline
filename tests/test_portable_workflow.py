from __future__ import annotations

import copy
import json
import re
import tempfile
import unittest
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image

from ssqtl_igv.cli import main as cli_main
from ssqtl_igv.compose import compose_desktop_case
from ssqtl_igv.config import ConfigError, WorkflowConfig
from ssqtl_igv.preflight import run_preflight
from ssqtl_igv.prepare import _run_r_prepare, prepare_run
from ssqtl_igv.qacct import collect_qacct_evidence
from ssqtl_igv.runner import CaseFailure, _validate_case_inputs, assert_manifest_config
from ssqtl_igv.scheduler import (
    _hard_throttle_array_ranges,
    _scheduler_plan_identity,
    _write_chunk_map,
    create_resume_submission,
    create_submission,
)
from ssqtl_igv.state import REVIEW_PENDING, CaseState
from ssqtl_igv.storage import collect_storage_evidence, validate_storage_evidence
from ssqtl_igv.summary import _publication_chromosomes, summarize_run
from ssqtl_igv.utils import command_prefix, sha256_file, sha256_json
from ssqtl_igv.violin import pdf_pages
from ssqtl_igv.validation_lineage import (
    _validated_qacct_rows,
    observed_peak_concurrency,
    parse_task_range,
)


class PortableWorkflowTests(unittest.TestCase):
    def _fixture(self, root: Path, *, mode: str = "local") -> tuple[WorkflowConfig, Path]:
        inputs = root / "inputs"
        inputs.mkdir()
        associations = inputs / "associations.csv"
        associations.write_text("AG_site,SNP,strand\n", encoding="utf-8")
        lookup = inputs / "lookup.csv"
        lookup.write_text("sample_id,bam\n", encoding="utf-8")
        rds = inputs / "rds"
        violin = inputs / "violin"
        rds.mkdir()
        violin.mkdir()
        genome = inputs / "genome.json"
        fasta = inputs / "genome.fa"
        fai = inputs / "genome.fa.fai"
        cytoband = inputs / "cytoband.txt"
        annotation = inputs / "annotation.gff"
        genome.write_text("{}", encoding="utf-8")
        fasta.write_text(">chrA\nAG\n", encoding="utf-8")
        fai.write_text("chrA\t2\t6\t2\t3\n", encoding="utf-8")
        cytoband.write_text("chrA\n", encoding="utf-8")
        annotation.write_text("##gff-version 3\n", encoding="utf-8")
        output = root / "runs"
        publish = root / "publish"
        output.mkdir()
        publish.mkdir()
        data = {
            "paths": {
                "associations": str(associations),
                "rds_dir": str(rds),
                "bam_lookup": str(lookup),
                "violin_dir": str(violin),
                "output_root": str(output),
                "publish_root": str(publish),
            },
            "workflow": {
                "figure_contract_id": "v031_native_igv_pixel_exact",
                "gui_settle_contract_id": "v031_toolbar_locus_settle_v1",
            },
            "genome": {
                "id": "fixture",
                "display_name": "Fixture genome",
                "definition": str(genome),
                "fasta": str(fasta),
                "fai": str(fai),
                "cytoband": str(cytoband),
                "annotation": str(annotation),
                "annotation_version": "fixture-v1",
            },
            "binaries": {
                "rscript": "Rscript",
                "igv": "igv",
                "qsub": "qsub",
                "qacct": "qacct",
            },
            "execution": {"mode": mode},
            "scheduler": {
                "project": "fixture" if mode == "grid_engine" else None,
                "max_parallel": 2,
                "max_tasks_per_array": 2,
                "cases_per_task": 2,
                "memory_gb": 4,
                "total_parallel_memory_gb": 8,
            },
            "desktop": {
                "screen_width": 16,
                "screen_height": 12,
                "screen_depth": 24,
                "toolbar_locus_roi": {"x": 0, "y": 0, "width": 16, "height": 2},
                "locus_field_roi": {"x": 0, "y": 0, "width": 8, "height": 2},
            },
            "compose": {"violin_panel_width": 300},
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
        path = root / "config.json"
        path.write_text(json.dumps(data), encoding="utf-8")
        return WorkflowConfig.load(path), output

    def _rewrite_config(self, config: WorkflowConfig, update) -> WorkflowConfig:
        data = json.loads(config.path.read_text(encoding="utf-8"))
        update(data)
        config.path.write_text(json.dumps(data), encoding="utf-8")
        return WorkflowConfig.load(config.path)

    def _manifest_contract(
        self,
        config: WorkflowConfig,
        run_root: Path,
        *,
        case_id: str = "case-1",
    ) -> tuple[list[dict[str, object]], Path, Path]:
        manifest = run_root / ".work" / "manifests" / "case_manifest.jsonl"
        snapshot = run_root / ".work" / "inputs" / "associations.csv"
        manifest.parent.mkdir(parents=True, exist_ok=True)
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        source = config.path_value("paths.associations")
        snapshot.write_bytes(source.read_bytes())
        snapshot.chmod(0o444)
        association_sha = sha256_file(source)
        case: dict[str, object] = {
            "schema_version": "1.0",
            "run_id": run_root.name,
            "workflow_config_fingerprint": sha256_json(config.data),
            "associations_sha256": association_sha,
            "association_row": 1,
            "case_id": case_id,
            "shard": "chrA_plus",
            "ag": {"chrom": "chrA"},
        }
        case["input_fingerprint"] = sha256_json(case)
        manifest.write_text(json.dumps(case, sort_keys=True) + "\n", encoding="utf-8")
        shards = manifest.parent / "shards.tsv"
        shards.write_text("task_id\tshard\tcase_count\n1\tchrA_plus\t1\n", encoding="utf-8")
        report = {
            "run_root": str(run_root.resolve(strict=False)),
            "manifest": str(manifest.resolve(strict=False)),
            "manifest_sha256": sha256_file(manifest),
            "shards": str(shards.resolve(strict=False)),
            "case_count": 1,
            "config_fingerprint": sha256_json(config.data),
            "associations_sha256": association_sha,
            "associations_snapshot": str(snapshot),
            "associations_snapshot_sha256": association_sha,
        }
        report_path = run_root / ".work" / "prepare_report.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return [case], manifest, report_path

    def test_bounded_grid_engine_ranges_are_configurable(self):
        ranges = _hard_throttle_array_ranges(7, 2)
        self.assertEqual(ranges, ["1-2", "3-4", "5-6", "7"])
        self.assertEqual(
            [task for value in ranges for task in parse_task_range(value)],
            list(range(1, 8)),
        )

    def test_execution_mode_and_capacity_are_validated(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, _ = self._fixture(root)
            broken = json.loads(config.path.read_text(encoding="utf-8"))
            broken["execution"]["mode"] = "invalid"
            config.path.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaises(ConfigError):
                WorkflowConfig.load(config.path)

    def test_optional_hash_contract_is_validated_when_present(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, _ = self._fixture(root)
            data = json.loads(config.path.read_text(encoding="utf-8"))
            data["paths"]["associations_sha256"] = "bad"
            config.path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaises(ConfigError):
                WorkflowConfig.load(config.path)

    def test_null_association_sha_prepares_with_observed_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root)
            data = json.loads(config.path.read_text(encoding="utf-8"))
            data["paths"]["associations_sha256"] = None
            config.path.write_text(json.dumps(data), encoding="utf-8")
            config = WorkflowConfig.load(config.path)
            prepared_cases = root / "prepared_cases.tsv"
            prepared_samples = root / "prepared_samples.tsv"
            prepared_cases.write_text(
                "association_row\tcase_id\tag_site\tsnp\tstrand\terror_code\terror_message\n"
                "1\tbad-case\tbad\tbad\t+\t\t\n",
                encoding="utf-8",
            )
            prepared_samples.write_text(
                "case_id\tgenotype\tbam\tbai\tsample_id\tdosage\tratio\tselection_label\tbai_fresh\n",
                encoding="utf-8",
            )
            association_sha = sha256_file(config.path_value("paths.associations"))

            report = prepare_run(
                config,
                output / "run-null-sha",
                prepared_cases=prepared_cases,
                prepared_samples=prepared_samples,
            )

            self.assertEqual(report["associations_expected_sha256"], "")
            self.assertEqual(report["associations_sha256"], association_sha)
            self.assertEqual(report["associations_snapshot_sha256"], association_sha)
            snapshot = Path(report["associations_snapshot"])
            self.assertTrue(snapshot.is_file())
            self.assertEqual(snapshot.stat().st_mode & 0o777, 0o444)

    def test_grid_project_and_pquota_nulls_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            for index, value in enumerate((None, "", "   ")):
                with self.subTest(contract="grid-project", value=value):
                    root = Path(directory) / f"grid-{index}"
                    root.mkdir()
                    config, _ = self._fixture(root, mode="grid_engine")
                    data = json.loads(config.path.read_text(encoding="utf-8"))
                    data["scheduler"]["project"] = value
                    config.path.write_text(json.dumps(data), encoding="utf-8")
                    with self.assertRaisesRegex(ConfigError, r"scheduler\.project"):
                        WorkflowConfig.load(config.path)
            for index, (key, value) in enumerate(
                (
                    ("binaries.pquota", None),
                    ("storage.project_quota_path", None),
                    ("storage.project_quota_path", "   "),
                )
            ):
                with self.subTest(contract="pquota", key=key, value=value):
                    root = Path(directory) / f"pquota-{index}"
                    root.mkdir()
                    config, _ = self._fixture(root)
                    data = json.loads(config.path.read_text(encoding="utf-8"))
                    data["storage"]["provider"] = "pquota"
                    data["storage"]["project_quota_path"] = "/project/quota"
                    data["binaries"]["pquota"] = ["env", "pquota"]
                    section, name = key.split(".")
                    data[section][name] = value
                    config.path.write_text(json.dumps(data), encoding="utf-8")
                    with self.assertRaises(ConfigError):
                        WorkflowConfig.load(config.path)

    def test_r_adapter_receives_schema_parameters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, _ = self._fixture(root)
            data = json.loads(config.path.read_text(encoding="utf-8"))
            data["inputs"] = {
                "association_columns": {"ag_site": "site", "snp": "variant", "strand": "direction"},
                "rds_filename_template": "locus_{chrom}_{strand_token}.rds",
                "locus_sample_columns": ["sample"],
                "ratio_column": "usage",
                "bam_lookup_id_columns": ["sample"],
                "bam_lookup_path_columns": ["path"],
                "bam_suffixes": [".cram", ".bam"],
            }
            config.path.write_text(json.dumps(data), encoding="utf-8")
            config = WorkflowConfig.load(config.path)
            completed = SimpleNamespace(returncode=0, stdout="", stderr="")
            with patch("ssqtl_igv.prepare.subprocess.run", return_value=completed) as run:
                _run_r_prepare(
                    config,
                    config.path_value("paths.associations"),
                    root / "cases.tsv",
                    root / "samples.tsv",
                )
            argv = run.call_args.args[0]
            self.assertIn("--ag_column=site", argv)
            self.assertIn("--rds_filename_template=locus_{chrom}_{strand_token}.rds", argv)
            self.assertIn("--bam_suffixes=.cram,.bam", argv)
            self.assertEqual(run.call_args.kwargs["timeout"], 129600)

    def test_command_prefix_preserves_argv_and_does_not_split_shell_text(self):
        configured = ["env", "PROFILE=fixture", "/opt/tools/tool"]
        self.assertEqual(command_prefix(configured), configured)
        self.assertIsNot(command_prefix(configured), configured)
        self.assertEqual(command_prefix("tool --flag"), ["tool --flag"])
        self.assertEqual(command_prefix(None, default="fallback"), ["fallback"])
        for invalid in (None, "", " tool", "tool ", "   ", [], ["tool", ""], ["tool", "  "]):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    command_prefix(invalid)

    def test_configured_command_prefix_reaches_external_tool_argv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, _ = self._fixture(root)
            config = self._rewrite_config(
                config,
                lambda data: data["binaries"].update(
                    {
                        "rscript": ["env", "R_PROFILE=fixture", "Rscript"],
                        "pdftotext": ["env", "PDF_PROFILE=fixture", "pdftotext"],
                    }
                ),
            )
            completed = SimpleNamespace(returncode=0, stdout=b"page\f", stderr=b"")
            with patch("ssqtl_igv.violin.subprocess.run", return_value=completed) as run:
                self.assertEqual(
                    pdf_pages(root / "input.pdf", config.get("binaries.pdftotext")),
                    ["page"],
                )
            self.assertEqual(
                run.call_args.args[0][:3],
                ["env", "PDF_PROFILE=fixture", "pdftotext"],
            )

            r_completed = SimpleNamespace(returncode=0, stdout="", stderr="")
            with patch("ssqtl_igv.prepare.subprocess.run", return_value=r_completed) as run:
                _run_r_prepare(
                    config,
                    config.path_value("paths.associations"),
                    root / "cases.tsv",
                    root / "samples.tsv",
                )
            self.assertEqual(
                run.call_args.args[0][:3],
                ["env", "R_PROFILE=fixture", "Rscript"],
            )

    def test_chunk_map_covers_every_case_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cases = [
                {"case_id": f"case-{index}", "association_row": index, "shard": shard}
                for index, shard in enumerate(("a", "a", "a", "b", "b"), 1)
            ]
            chunk_map, rows = _write_chunk_map(root, cases, 2)
            observed: list[str] = []
            for row in rows:
                observed.extend(Path(row["case_list"]).read_text(encoding="utf-8").splitlines()[1:])
            self.assertEqual(sorted(observed), sorted(case["case_id"] for case in cases))
            self.assertEqual(len(observed), len(set(observed)))
            self.assertTrue(chunk_map.is_file())

    def test_scheduler_submission_apis_reject_local_mode_before_filesystem_access(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root)
            missing_run = output / "missing-run"
            rerun_manifest = root / "missing-rerun.tsv"
            for operation in (
                lambda: create_submission(config, missing_run),
                lambda: create_resume_submission(config, missing_run, rerun_manifest),
            ):
                with self.subTest(operation=operation):
                    with self.assertRaisesRegex(ValueError, r"execution\.mode=grid_engine"):
                        operation()

    def test_cli_run_fail_closed_before_prepare_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "local").mkdir()
            local, local_output = self._fixture(root / "local")
            (root / "grid").mkdir()
            grid, grid_output = self._fixture(root / "grid", mode="grid_engine")
            calls = (
                (
                    ["run", "--config", str(grid.path), "--run-root", str(grid_output / "run-1")],
                    "ssqtl_igv.cli.prepare_run",
                ),
                (
                    [
                        "run",
                        "--config",
                        str(local.path),
                        "--run-root",
                        str(local_output / "run-1"),
                        "--submit",
                    ],
                    "ssqtl_igv.cli.prepare_run",
                ),
            )
            for argv, target in calls:
                with self.subTest(argv=argv), patch(target) as side_effect:
                    with redirect_stderr(StringIO()):
                        self.assertEqual(cli_main(argv), 1)
                    side_effect.assert_not_called()

    def test_grid_run_delegates_preflight_once_to_scheduler(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root, mode="grid_engine")
            run_root = output / "run-grid"
            prepared = {"manifest": "manifest.jsonl", "shards": "shards.tsv"}
            submission = {"action": "PLAN", "exit_code": 0}
            with patch("ssqtl_igv.cli.prepare_run", return_value=prepared), patch(
                "ssqtl_igv.cli.run_preflight"
            ) as preflight, patch(
                "ssqtl_igv.cli.create_submission", return_value=submission
            ) as submit:
                self.assertEqual(
                    cli_main(
                        [
                            "run",
                            "--config",
                            str(config.path),
                            "--run-root",
                            str(run_root),
                            "--dry-run",
                        ]
                    ),
                    0,
                )
            preflight.assert_not_called()
            submit.assert_called_once_with(config, str(run_root), execute=False, generation=1)

    def test_cli_submit_and_resume_fail_closed_before_scheduler_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local, output = self._fixture(root)
            submit_argv = [
                "submit",
                "--config",
                str(local.path),
                "--run-root",
                str(output / "run-1"),
                "--submit",
            ]
            resume_argv = [
                "resume",
                "--config",
                str(local.path),
                "--run-root",
                str(output / "run-1"),
                "--rerun-manifest",
                str(root / "rerun.tsv"),
                "--submit",
            ]
            with patch("ssqtl_igv.cli.create_submission") as submit:
                with redirect_stderr(StringIO()):
                    self.assertEqual(cli_main(submit_argv), 1)
                submit.assert_not_called()
            with patch("ssqtl_igv.cli.create_resume_submission") as resume:
                with redirect_stderr(StringIO()):
                    self.assertEqual(cli_main(resume_argv), 1)
                resume.assert_not_called()

    def test_storage_formula_uses_fixture_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            config, _ = self._fixture(Path(directory))
            evidence = collect_storage_evidence(config, remaining_cases=3, total_cases=5)
            self.assertEqual(evidence["total_cases"], 5)
            self.assertEqual(evidence["remaining_cases"], 3)
            self.assertNotIn("8973", evidence["storage_formula"])
            validate_storage_evidence(evidence, config, remaining_cases=3, total_cases=5)

    def test_dynamic_publication_chromosomes(self):
        with tempfile.TemporaryDirectory() as directory:
            config, _ = self._fixture(Path(directory))
            cases = [{"ag": {"chrom": value}} for value in ("chrZ", "chrA", "chrZ")]
            self.assertEqual(_publication_chromosomes(config, cases), ("chrA", "chrZ"))

    def test_composer_preserves_configured_native_pixels(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, _ = self._fixture(root)
            client = root / "client.png"
            violin = root / "violin.png"
            combined = root / "combined.png"
            layout = root / "layout.json"
            Image.new("RGB", (16, 12), (10, 20, 30)).save(client)
            Image.new("RGB", (4, 8), (200, 100, 50)).save(violin)
            result = compose_desktop_case(
                {"case_id": "fixture", "violin": {"match_key": {}}},
                client,
                violin,
                combined,
                layout,
                {"root_screenshot_publishable": False, "capture_mode": "window", "geometry_verified": True},
                config,
            )
            with Image.open(client) as before, Image.open(combined) as after:
                self.assertEqual(before.tobytes(), after.crop((0, 0, 16, 12)).tobytes())
            self.assertTrue(result["evidence"]["left_pixel_identity"])

    def test_qacct_uses_configured_prefix_and_records_effective_concurrency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root, mode="grid_engine")
            run_root = output / "run-qacct"
            run_root.mkdir()
            scheduler_root = run_root / ".work" / "scheduler" / "full_g001"
            scheduler_root.mkdir(parents=True)
            runner_script = scheduler_root / "run_shards.qsub"
            runner_script.write_text(
                "#!/bin/bash\n#$ -N igv_run-qacct\n#$ -P fixture\n",
                encoding="utf-8",
            )
            plan = {
                "schema_version": "portable-grid-engine-scheduler-plan-v1",
                "run_root": str(run_root),
                "manifest_sha256": "a" * 64,
                "config_fingerprint": sha256_json(config.data),
                "associations_sha256": "b" * 64,
                "submission_generation": 1,
                "scheduler_task_count": 2,
                "array_range": "1-2",
                "array_range_role": "logical_task_coverage_only",
                "array_ranges": ["1-2"],
                "array_job_count": 1,
                "max_batch_task_count": 2,
                "throttle_contract_id": "bounded-array-strict-hold-v1",
                "hard_max_tasks_per_array": 2,
                "scheduler_tc_requested": 3,
                "scheduler_tc_role": "defense_in_depth_only",
                "serial_hold_chain_required": True,
                "max_parallel": 3,
                "requested_memory_gb": 4,
                "expected_owner": "analyst",
                "expected_job_name": "igv_run-qacct",
                "expected_project": "fixture",
                "chunk_map": "",
                "chunk_map_sha256": None,
                "shard_map": None,
                "shard_map_sha256": None,
                "rerun_manifest": None,
                "rerun_manifest_sha256": None,
                "runner_script": str(runner_script),
                "runner_script_sha256": sha256_file(runner_script),
                "summary_script": "",
                "summary_script_sha256": None,
                "status": "ARRAY_AND_SUMMARY_SUBMITTED",
                "array_jobs": [
                    {
                        "sequence": 1,
                        "job_id": "123",
                        "array_range": "1-2",
                        "task_ids": [1, 2],
                        "task_count": 2,
                        "hold_jid": "",
                    }
                ],
            }
            plan["plan_identity_sha256"] = _scheduler_plan_identity(plan)
            jobs = scheduler_root / "jobs.json"
            jobs.write_text(json.dumps(plan), encoding="utf-8")
            block = """==============================================================
jobnumber 123
jobname igv_run-qacct
owner analyst
project fixture
qname queue
hostname node
qsub_time Thu Jul 16 10:00:00 2026
start_time Thu Jul 16 10:01:00 2026
end_time Thu Jul 16 10:02:00 2026
failed 0
exit_status 0
ru_wallclock 60
ru_maxrss 1G
maxvmem 2G
taskid {task}
"""
            commands: list[list[str]] = []

            def runner(argv: list[str]) -> str:
                commands.append(argv)
                return block.format(task=1) + block.format(task=2)

            evidence = collect_qacct_evidence(
                jobs,
                run_root=run_root,
                qacct_command=["env", "QACCT_PROFILE=fixture", "qacct"],
                runner=runner,
            )
            self.assertEqual(
                commands,
                [["env", "QACCT_PROFILE=fixture", "qacct", "-j", "123", "-t", "1-2"]],
            )
            self.assertEqual(evidence["effective_max_parallel"], 2)
            self.assertEqual(evidence["observed_peak_concurrency"], 2)
            self.assertTrue(evidence["hard_limit_pass"])

    def test_qacct_identity_and_concurrency(self):
        block = """==============================================================
jobnumber 123
jobname igv_run
owner analyst
project project
qname queue
hostname node
qsub_time Thu Jul 16 10:00:00 2026
start_time Thu Jul 16 10:01:00 2026
end_time Thu Jul 16 10:02:00 2026
failed 0
exit_status 0
ru_wallclock 60
ru_maxrss 1G
maxvmem 2G
taskid {task}
"""
        text = block.format(task=1) + block.format(task=2)
        rows = _validated_qacct_rows(
            text,
            job_id="123",
            array_range="1-2",
            owner="analyst",
            job_name="igv_run",
            project="project",
        )
        self.assertEqual([row["task_id"] for row in rows], [1, 2])
        self.assertEqual(observed_peak_concurrency(rows), 2)
        with self.assertRaises(ValueError):
            _validated_qacct_rows(
                text,
                job_id="123",
                array_range="1-2",
                owner="someone-else",
                job_name="igv_run",
                project="project",
            )

    def test_manifest_prepare_report_snapshot_and_input_fingerprint_tampering_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root)
            run_root = output / "run-identity"
            run_root.mkdir()
            cases, manifest, report_path = self._manifest_contract(config, run_root)
            assert_manifest_config(cases, config, run_root)

            tampered = copy.deepcopy(cases)
            tampered[0]["shard"] = "changed"
            with self.assertRaisesRegex(ValueError, "input fingerprint mismatch"):
                assert_manifest_config(tampered, config, run_root)

            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["manifest_sha256"] = "0" * 64
            report_path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "prepare report identity"):
                assert_manifest_config(cases, config, run_root)

            report["manifest_sha256"] = sha256_file(manifest)
            report_path.write_text(json.dumps(report), encoding="utf-8")
            snapshot = Path(report["associations_snapshot"])
            snapshot.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "snapshot is missing, mutable, or differs"):
                assert_manifest_config(cases, config, run_root)

            snapshot.chmod(0o644)
            snapshot.write_text("tampered\n", encoding="utf-8")
            snapshot.chmod(0o444)
            with self.assertRaisesRegex(ValueError, "snapshot is missing, mutable, or differs"):
                assert_manifest_config(cases, config, run_root)

    def test_case_input_validation_binds_cytoband_and_configured_resource_hashes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, _ = self._fixture(root)
            genome = {
                key: str(config.path_value(f"genome.{key}"))
                for key in ("definition", "fasta", "fai", "cytoband", "annotation")
            }
            genome["resource_identity"] = {
                key: {
                    "size": Path(value).stat().st_size,
                    "mtime_ns": Path(value).stat().st_mtime_ns,
                }
                for key, value in genome.items()
                if key != "resource_identity"
            }
            pdf = root / "violin.pdf"
            pdf.write_bytes(b"fixture")
            case = {
                "case_id": "case-resource",
                "genotypes": {
                    "0/0": [
                        {
                            "bam": str(config.path_value("paths.associations")),
                            "bai": str(config.path_value("paths.bam_lookup")),
                        }
                    ],
                    "0/1": [],
                    "1/1": [],
                },
                "genome": genome,
                "violin": {
                    "pdf": str(pdf),
                    "page": 1,
                    "pdf_identity": {
                        "size": pdf.stat().st_size,
                        "mtime_ns": pdf.stat().st_mtime_ns,
                        "sha256": sha256_file(pdf),
                    },
                },
            }
            self.assertEqual(_validate_case_inputs(case, config), [])
            cytoband = config.path_value("genome.cytoband")
            cytoband.write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(CaseFailure, "cytoband"):
                _validate_case_inputs(case, config)

            genome["resource_identity"]["cytoband"] = {
                "size": cytoband.stat().st_size,
                "mtime_ns": cytoband.stat().st_mtime_ns,
            }
            data = json.loads(config.path.read_text(encoding="utf-8"))
            data["genome"]["cytoband_sha256"] = "0" * 64
            config.path.write_text(json.dumps(data), encoding="utf-8")
            pinned = WorkflowConfig.load(config.path)
            with self.assertRaises(CaseFailure) as raised:
                _validate_case_inputs(case, pinned)
            self.assertEqual(raised.exception.code, "RESOURCE_SHA256_MISMATCH")

    def test_preflight_and_template_omit_removed_unused_tools(self):
        removed_keys = {"xvfb_run", "samtools", "pdfinfo"}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root)
            executable = root / "tool"
            executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            executable.chmod(0o755)
            config = self._rewrite_config(
                config,
                lambda data: data["binaries"].update(
                    {
                        "rscript": str(executable),
                        "igv": str(executable),
                        "xvfb": str(executable),
                        "xwininfo": str(executable),
                        "xprop": str(executable),
                        "import": str(executable),
                        "pdftotext": str(executable),
                        "pdftoppm": str(executable),
                        "tesseract": str(executable),
                        "xvfb_run": "/missing/xvfb-run",
                        "samtools": "/missing/samtools",
                        "pdfinfo": "/missing/pdfinfo",
                    }
                ),
            )
            result = run_preflight(config, run_root=output / "run-preflight")
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(
                set(result["commands"]),
                {
                    "Rscript",
                    "IGV",
                    "Xvfb",
                    "xwininfo",
                    "xprop",
                    "ImageMagick import",
                    "pdftotext",
                    "pdftoppm",
                    "tesseract",
                },
            )

        template = (
            Path(__file__).resolve().parents[1]
            / "src"
            / "ssqtl_igv"
            / "resources"
            / "workflow.example.yaml"
        )
        text = template.read_text(encoding="utf-8")
        for key in removed_keys:
            with self.subTest(template_key=key):
                self.assertNotRegex(text, rf"(?m)^\s*{key}\s*:")

    def test_summary_outcome_prioritizes_rerun_over_review_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, output = self._fixture(root)
            run_root = output / "run-summary"
            run_root.mkdir()
            cases, manifest, report_path = self._manifest_contract(config, run_root)
            second = copy.deepcopy(cases[0])
            second["association_row"] = 2
            second["case_id"] = "case-2"
            second["input_fingerprint"] = sha256_json(
                {key: value for key, value in second.items() if key != "input_fingerprint"}
            )
            cases.append(second)
            manifest.write_text(
                "".join(json.dumps(case, sort_keys=True) + "\n" for case in cases),
                encoding="utf-8",
            )
            report = json.loads(report_path.read_text(encoding="utf-8"))
            report["manifest_sha256"] = sha256_file(manifest)
            report["case_count"] = len(cases)
            report_path.write_text(json.dumps(report), encoding="utf-8")

            combined = root / "combined.png"
            sample_table = root / "samples.tsv"
            combined.write_bytes(b"png")
            sample_table.write_text("case_id\ncase-1\n", encoding="utf-8")
            review_state = CaseState.fresh("case-1", str(cases[0]["input_fingerprint"]))
            review_state.status = REVIEW_PENDING
            review_state.history.append({"status": REVIEW_PENDING, "at": "fixture"})
            review_state.artifacts.update(
                {
                    "combined_png": str(combined),
                    "combined_sha256": sha256_file(combined),
                    "sample_table": str(sample_table),
                    "sample_table_sha256": sha256_file(sample_table),
                }
            )
            review_state.save(run_root / ".work" / "state")

            with patch("ssqtl_igv.summary._provenance", return_value={}), patch(
                "ssqtl_igv.summary._telemetry", return_value={}
            ):
                result = summarize_run(config, run_root)

            self.assertEqual(result["action"], "RERUN_REQUIRED")
            self.assertEqual(result["exit_code"], 2)
            self.assertEqual(result["failed"], 1)
            self.assertEqual(result["review_pending"], 1)
            self.assertEqual(result["delivered_for_review"], 1)
            self.assertEqual(result["published"], 0)

    def test_init_config_uses_packaged_template(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "workflow.yaml"
            self.assertEqual(cli_main(["init-config", "--output", str(output)]), 0)
            self.assertTrue(output.is_file())
            self.assertIn("execution:", output.read_text(encoding="utf-8"))

    def test_bilingual_readmes_share_operational_structure(self):
        root = Path(__file__).resolve().parents[1]
        readme = (root / "README.md").read_text(encoding="utf-8")
        pointer = (root / "README.zh-CN.md").read_text(encoding="utf-8")

        self.assertIn("# IGV Pipeline 3.0", readme)
        self.assertLess(readme.index("### Quick Start"), readme.index("### 快速开始"))
        self.assertEqual(
            re.findall(
                r"^### (Quick Start|Production Usage|SCC Pilot Qualification|Developer Architecture)$",
                readme,
                re.MULTILINE,
            ),
            [
                "Quick Start",
                "Production Usage",
                "SCC Pilot Qualification",
                "Developer Architecture",
            ],
        )
        self.assertEqual(
            re.findall(r"^### (快速开始|生产使用|SCC Pilot 验证|开发者架构)$", readme, re.MULTILINE),
            ["快速开始", "生产使用", "SCC Pilot 验证", "开发者架构"],
        )
        for token in (
            "schema_version",
            "SNAPSHOTS_READY",
            "linux/amd64",
            "127.0.0.1",
            "igv-snapshot init",
            "igv-snapshot doctor",
            "igv-snapshot run",
            "igv-snapshot review",
            "igv-snapshot publish",
            "igv-snapshot import-v2",
            "docker pull ghcr.io/luckyfruit88/igv-pipeline:3.0.0",
        ):
            self.assertIn(token, readme)
        self.assertIn("README.md#中文--chinese", pointer)
        self.assertNotIn("### 生产使用", pointer)

    def test_package_has_no_restricted_payloads_or_retired_scc_gates(self):
        root = Path(__file__).resolve().parents[1]
        banned = (
            "test64",
            "8895",
            "4-case",
            "8-way",
            "64-case",
            "nwgcid",
            "nwgc_id",
            "batch.q",
        )
        matches: list[str] = []
        paths = [
            root / "README.md",
            root / "config" / "production.yaml",
            *(root / "src").rglob("*.py"),
            *(root / "src").rglob("*.R"),
        ]
        for path in paths:
            text = path.read_text(encoding="utf-8", errors="ignore").lower()
            for token in banned:
                if token.lower() in text:
                    matches.append(f"{path.relative_to(root)}:{token}")
        self.assertEqual(matches, [])
        forbidden_suffixes = {".bam", ".bai", ".rds", ".cram", ".crai"}
        self.assertEqual(
            [str(path.relative_to(root)) for path in root.rglob("*") if path.suffix.lower() in forbidden_suffixes],
            [],
        )
        template = root / "src" / "ssqtl_igv" / "resources" / "workflow.example.yaml"
        self.assertTrue(template.is_file())
        self.assertEqual(sha256_file(template), sha256_file(root / "config" / "production.yaml"))


if __name__ == "__main__":
    unittest.main()
