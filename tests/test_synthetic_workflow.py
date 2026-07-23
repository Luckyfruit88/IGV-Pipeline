from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from PIL import Image

from ssqtl_igv.config import WorkflowConfig
from ssqtl_igv.desktop import DesktopResult
from ssqtl_igv.prepare import prepare_run
from ssqtl_igv.preflight import run_preflight
from ssqtl_igv.review import REQUIRED_MANUAL_ASSERTIONS, record_reviews
from ssqtl_igv.runner import run_shard
from ssqtl_igv.state import PUBLISHED, REVIEW_PENDING, CaseState
from ssqtl_igv.summary import summarize_run
from ssqtl_igv.utils import safe_name, sha256_file


class SyntheticWorkflowTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[WorkflowConfig, Path, Path, Path]:
        inputs = root / "inputs"
        inputs.mkdir()
        associations = inputs / "associations.csv"
        associations.write_text(
            "AG_site,SNP,strand\nchrA:2-3,chrA.4_T.C,+\n",
            encoding="utf-8",
        )
        rds = inputs / "rds"
        violin = inputs / "violin"
        rds.mkdir()
        violin.mkdir()
        violin_pdf = violin / "violin_plots_pos_chrA.pdf"
        violin_pdf.write_bytes(b"synthetic-pdf")
        bam = inputs / "sample.bam"
        bai = inputs / "sample.bam.bai"
        bam.write_bytes(b"synthetic-bam")
        bai.write_bytes(b"synthetic-bai")
        lookup = inputs / "lookup.csv"
        lookup.write_text(f"sample_id,bam\nsample-1,{bam}\n", encoding="utf-8")
        definition = inputs / "genome.json"
        fasta = inputs / "genome.fa"
        fai = inputs / "genome.fa.fai"
        cytoband = inputs / "cytoband.txt"
        annotation = inputs / "annotation.gff"
        definition.write_text("{}\n", encoding="utf-8")
        fasta.write_text(">chrA\nCAGTC\n", encoding="utf-8")
        fai.write_text("chrA\t5\t6\t5\t6\n", encoding="utf-8")
        cytoband.write_text("chrA\t0\t5\tp1\tgneg\n", encoding="utf-8")
        annotation.write_text("##gff-version 3\n", encoding="utf-8")
        output = root / "runs"
        publish = root / "publish"
        output.mkdir()
        publish.mkdir()
        executable = sys.executable
        config_data = {
            "paths": {
                "associations": str(associations),
                "associations_sha256": None,
                "rds_dir": str(rds),
                "bam_lookup": str(lookup),
                "violin_dir": str(violin),
                "violin_pdf_template": "violin_plots_{strand_token}_{chrom}.pdf",
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
                "definition": str(definition),
                "definition_sha256": None,
                "fasta": str(fasta),
                "fasta_sha256": None,
                "fai": str(fai),
                "fai_sha256": None,
                "cytoband": str(cytoband),
                "cytoband_sha256": None,
                "annotation": str(annotation),
                "annotation_version": "fixture-v1",
                "annotation_sha256": None,
            },
            "binaries": {
                "rscript": executable,
                "igv": executable,
                "xvfb": executable,
                "xwininfo": executable,
                "xprop": executable,
                "import": executable,
                "pdftotext": executable,
                "pdftoppm": executable,
                "tesseract": executable,
            },
            "execution": {"mode": "local"},
            "scheduler": {
                "max_parallel": 1,
                "max_tasks_per_array": 1,
                "cases_per_task": 1,
                "memory_gb": 1,
                "total_parallel_memory_gb": 1,
            },
            "desktop": {
                "screen_width": 64,
                "screen_height": 48,
                "screen_depth": 24,
                "minimum_window_width": 32,
                "minimum_window_height": 24,
                "toolbar_locus_roi": {"x": 0, "y": 0, "width": 64, "height": 8},
                "locus_field_roi": {"x": 0, "y": 0, "width": 32, "height": 8},
            },
            "compose": {"violin_panel_width": 300},
            "qc": {
                "final_min_width": 300,
                "final_min_height": 40,
                "min_stddev": 0.5,
            },
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
        config_path = root / "workflow.json"
        config_path.write_text(json.dumps(config_data), encoding="utf-8")
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
                    "2",
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
            f"AG_chrA_2_3__SNP_chrA_4_T_C\t0/0\tsample-1\t0\t0.25\tmedian\t{bam}\t{bai}\ttrue\n",
            encoding="utf-8",
        )
        run_root = output / "run_001"
        return WorkflowConfig.load(config_path), run_root, prepared_cases, prepared_samples

    @staticmethod
    def _pattern(path: Path, size: tuple[int, int]) -> None:
        image = Image.new("RGB", size)
        pixels = image.load()
        for y in range(size[1]):
            for x in range(size[0]):
                pixels[x, y] = ((x * 13) % 256, (y * 17) % 256, ((x + y) * 19) % 256)
        path.parent.mkdir(parents=True, exist_ok=True)
        image.save(path)

    def test_control_plane_reaches_review_then_final_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config, run_root, prepared_cases, prepared_samples = self._fixture(root)
            with patch(
                "ssqtl_igv.prepare.pdf_pages",
                return_value=["chrA:2-3 chrA.4_T.C"],
            ):
                prepared = prepare_run(
                    config,
                    run_root,
                    prepared_cases=prepared_cases,
                    prepared_samples=prepared_samples,
                )
            self.assertEqual(prepared["failed_preparation_count"], 0)
            self.assertEqual(prepared["associations_expected_sha256"], "")
            self.assertEqual(
                run_preflight(config, run_root=run_root, manifest=prepared["manifest"])[
                    "status"
                ],
                "PASS",
            )

            def fake_desktop(
                _config,
                *,
                output_png,
                metadata_path,
                log_directory,
                on_capture,
                on_settle,
                on_stable,
                **_kwargs,
            ):
                output = Path(output_png)
                self._pattern(output, (64, 48))
                metadata = {
                    "root_screenshot_publishable": False,
                    "capture_mode": "window",
                    "geometry_verified": True,
                }
                Path(metadata_path).write_text(json.dumps(metadata), encoding="utf-8")
                logs = Path(log_directory)
                logs.mkdir(parents=True, exist_ok=True)
                stdout = logs / "stdout.log"
                stderr = logs / "stderr.log"
                stdout.write_text("", encoding="utf-8")
                stderr.write_text("", encoding="utf-8")
                on_capture({"status": "PASS", "capture_mode": "window"})
                on_settle({"status": "PASS"})
                on_stable(
                    {
                        "status": "PASS",
                        "toolbar_locus_guard": {"status": "PASS"},
                    }
                )
                now = time.time()
                return DesktopResult(
                    screenshot=output,
                    metadata=metadata,
                    started_at_epoch=now,
                    ended_at_epoch=now,
                    wall_time_seconds=0.0,
                    peak_rss_gb=0.01,
                    stdout_path=stdout,
                    stderr_path=stderr,
                )

            def fake_violin(_pdf, _page, output_png, **_kwargs):
                self._pattern(Path(output_png), (300, 300))

            scientific = {
                "status": "PASS",
                "failed_codes": [],
                "manual_review_required": list(REQUIRED_MANUAL_ASSERTIONS),
            }
            with (
                patch("ssqtl_igv.runner.run_desktop_session", side_effect=fake_desktop),
                patch("ssqtl_igv.runner.render_pdf_page", side_effect=fake_violin),
                patch("ssqtl_igv.runner.scientific_qc", return_value=scientific),
            ):
                execution = run_shard(config, run_root, shard="chrA_pos")
            self.assertEqual(execution["exit_code"], 0)
            self.assertEqual(execution["passed"], 1)
            self.assertEqual(execution["results"][0]["status"], REVIEW_PENDING)

            state_path = (
                run_root
                / ".work"
                / "state"
                / f"{safe_name('AG_chrA_2_3__SNP_chrA_4_T_C')}.json"
            )
            self.assertEqual(CaseState.load(state_path).status, REVIEW_PENDING)
            first = summarize_run(config, run_root)
            self.assertEqual(first["action"], "REVIEW_PENDING")
            self.assertEqual(first["exit_code"], 0)
            self.assertTrue((run_root / ".summary_complete.json").is_file())
            self.assertFalse((run_root / ".publish_complete.json").exists())
            delivered = config.publish_root / "review_by_chr" / "chrA"
            self.assertEqual(len(list(delivered.glob("*.png"))), 1)

            record_reviews(
                config,
                run_root,
                case_ids={"AG_chrA_2_3__SNP_chrA_4_T_C"},
                decision="approve",
                reviewer="synthetic-test",
                manual_assertions={key: True for key in REQUIRED_MANUAL_ASSERTIONS},
            )
            second = summarize_run(config, run_root)
            self.assertEqual(second["action"], "PUBLISHED")
            self.assertEqual(second["exit_code"], 0)
            self.assertTrue((run_root / ".publish_complete.json").is_file())
            self.assertEqual(CaseState.load(state_path).status, PUBLISHED)
            self.assertTrue((run_root / "SHA256SUMS").is_file())
            self.assertEqual(
                sha256_file(next(delivered.glob("*.png"))),
                CaseState.load(state_path).artifacts["delivered_review_png_sha256"],
            )


if __name__ == "__main__":
    unittest.main()
