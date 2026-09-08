"""Optional, historical research campaign commands; not a product prerequisite."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .campaign_v3 import create_next_batch, prepare_campaign, reduce_campaign_status
from .orchestrator_v3 import resolve_max_parallel, run_portable_ssqtl_normalization
from .project_launcher import run_project_workflow
from .project_v3 import load_project_config
from .utils import reject_symlink_path_components
from .v3_cli import _add_resource_options, _embedded_runtime_manifest, _emit, _resume_identity


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ssqtl_igv.benchmark_cli",
        description="Historical SCC research campaigns (not required for normal runs)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    campaign = subparsers.add_parser(
        "campaign", help="manage optional scientific campaign authorization"
    )
    campaign_commands = campaign.add_subparsers(dest="campaign_command", required=True)
    campaign_prepare = campaign_commands.add_parser(
        "prepare", help="freeze the master task set and deterministic 100-case QA pilot"
    )
    campaign_prepare.add_argument("--campaign-dir", required=True)
    campaign_prepare.add_argument("--campaign-id", required=True)
    campaign_prepare.add_argument("--master-tasks", required=True)
    campaign_prepare.add_argument("--actor", default=os.environ.get("USER", "operator"))

    campaign_prepare_master = campaign_commands.add_parser(
        "prepare-master",
        help="normalize one ssQTL project and freeze its master/pilot task sets",
    )
    campaign_prepare_master.add_argument(
        "--project", default="/project/project.yaml"
    )
    campaign_prepare_master.add_argument("--campaign-dir", required=True)
    campaign_prepare_master.add_argument("--campaign-id", required=True)
    campaign_prepare_master.add_argument("--work")
    campaign_prepare_master.add_argument(
        "--max-parallel", default="auto", metavar="auto|N"
    )
    campaign_prepare_master.add_argument(
        "--actor", default=os.environ.get("USER", "operator")
    )

    campaign_run_batch = campaign_commands.add_parser(
        "run-batch",
        help="execute exactly one validated immutable campaign batch-request",
    )
    campaign_run_batch.add_argument("--batch-request", required=True)
    campaign_run_batch.add_argument("--output", default="/output")
    campaign_run_batch.add_argument("--work")
    campaign_run_batch.add_argument("--resume", action="store_true")
    campaign_run_batch.add_argument(
        "--max-parallel", default="auto", metavar="auto|N"
    )
    campaign_run_batch.add_argument(
        "--max-cases-per-shard", type=int, default=256
    )
    _add_resource_options(campaign_run_batch)
    campaign_run_batch.add_argument("--campaign-output", help="reconcile the campaign into this separate collection after the batch")
    campaign_run_batch.add_argument("--campaign-runs", help="parent directory containing the campaign batch workspaces")

    campaign_status = campaign_commands.add_parser(
        "status", help="reduce live authoritative sources without writing campaign state"
    )
    campaign_status.add_argument("--campaign-dir", required=True)
    campaign_status.add_argument("--batch-id")
    campaign_status.add_argument("--nextflow-trace")
    campaign_status.add_argument("--raw-qacct")
    campaign_status.add_argument("--accounting-attestation")
    campaign_status.add_argument("--publication-completion")

    campaign_reconcile = campaign_commands.add_parser("reconcile", help="accept completed batches and reconcile the full frozen task set without rendering")
    campaign_reconcile.add_argument("--campaign-dir", required=True)
    campaign_reconcile.add_argument("--runs-dir", required=True)
    campaign_reconcile.add_argument("--output", required=True)

    campaign_next = campaign_commands.add_parser(
        "next", help="authorize the next <=256-case request after verified publication"
    )
    campaign_next.add_argument("--campaign-dir", required=True)
    campaign_next.add_argument("--publication-completion", required=True)
    campaign_next.add_argument("--actor", default=os.environ.get("USER", "operator"))
    return parser


def _prepare_campaign_master(args: argparse.Namespace) -> dict[str, Any]:
    project = load_project_config(args.project)
    if project["adapter"] != "ssqtl":
        raise ValueError("campaign prepare-master requires an ssQTL project")
    campaign_dir = reject_symlink_path_components(
        args.campaign_dir, label="campaign directory"
    ).resolve(strict=False)
    work = (
        reject_symlink_path_components(args.work, label="work directory").resolve(
            strict=False
        )
        if args.work
        else None
    )
    if work is not None and (
        work == campaign_dir
        or campaign_dir in work.parents
        or work in campaign_dir.parents
    ):
        raise ValueError(
            "campaign prepare-master work and campaign directories must not overlap"
        )
    max_parallel = resolve_max_parallel(args.max_parallel)
    inputs = project["inputs"]
    normalization = run_portable_ssqtl_normalization(
        run_dir=campaign_dir,
        run_id=args.campaign_id,
        generation_id="master",
        profile="standalone",
        associations=inputs["associations"]["declared_path"],
        rds_dir=inputs["rds_dir"]["declared_path"],
        bam_lookup=inputs["bam_lookup"]["declared_path"],
        violin_dir=inputs["violin_dir"]["declared_path"],
        input_root=project["project_root"],
        reference=project["reference"]["source_path"],
        adapter_config=(inputs.get("config") or {}).get("declared_path"),
        runtime_identity_path=_embedded_runtime_manifest(),
        nextflow=None,
        work_dir=work,
        max_parallel=max_parallel,
    )
    try:
        result = prepare_campaign(
            Path(normalization["bundle"]) / "tasks.jsonl",
            campaign_dir,
            campaign_id=args.campaign_id,
            actor=args.actor,
        )
    finally:
        shutil.rmtree(normalization["temporary_root"], ignore_errors=True)
    return {
        **result,
        "runtime_fingerprint_sha256": normalization[
            "runtime_fingerprint_sha256"
        ],
    }



def _run_campaign_batch(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    collection = getattr(args, "campaign_output", None)
    runs = getattr(args, "campaign_runs", None)
    if bool(collection) != bool(runs):
        raise ValueError("--campaign-output and --campaign-runs must be provided together")
    output = reject_symlink_path_components(
        args.output, label="output directory"
    ).resolve(strict=False)
    if args.resume:
        _resume_identity(output)
    result, code = run_project_workflow(
        project=None,
        batch_request=args.batch_request,
        output=output,
        work=args.work,
        resume=args.resume,
        max_parallel=args.max_parallel,
        max_cases_per_shard=args.max_cases_per_shard,
        runtime_manifest=_embedded_runtime_manifest(),
        igv_cpus=args.igv_cpus,
        igv_memory=args.igv_memory,
        igv_timeout=args.igv_timeout,
        normalization_cpus=args.normalization_cpus,
        normalization_memory=args.normalization_memory,
        normalization_timeout=args.normalization_timeout,
    )
    if collection:
        from .campaign_reconcile import reconcile_campaign
        campaign = Path(args.batch_request).resolve(strict=True).parents[2]
        result["campaign_reconciliation"] = reconcile_campaign(campaign, runs, collection)
    return result, code



def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.campaign_command == "run-batch":
            output = reject_symlink_path_components(args.output, label="batch output").resolve(strict=False)
            for name in ("home", "nextflow"):
                directory = reject_symlink_path_components(output / ".runtime" / name, label="batch runtime")
                directory.mkdir(parents=True, exist_ok=True)
            os.environ["HOME"] = str(output / ".runtime/home")
            os.environ["NXF_HOME"] = str(output / ".runtime/nextflow")
            self_test = Path("/usr/local/bin/runtime-self-test")
            if self_test.is_file():
                subprocess.run([str(self_test)], check=True, stdout=sys.stderr)
        if args.campaign_command == "prepare":
            result = prepare_campaign(
                args.master_tasks,
                args.campaign_dir,
                campaign_id=args.campaign_id,
                actor=args.actor,
            )
            code = 0
        elif args.campaign_command == "prepare-master":
            result = _prepare_campaign_master(args)
            code = 0
        elif args.campaign_command == "run-batch":
            result, code = _run_campaign_batch(args)
        elif args.campaign_command == "status":
            result = reduce_campaign_status(
                args.campaign_dir,
                batch_id=args.batch_id,
                nextflow_trace=args.nextflow_trace,
                raw_qacct=args.raw_qacct,
                accounting_attestation=args.accounting_attestation,
                publication_completion=args.publication_completion,
            )
            code = 2 if result.get("status") == "INCONSISTENT" else 0
        elif args.campaign_command == "reconcile":
            from .campaign_reconcile import reconcile_campaign
            result = reconcile_campaign(args.campaign_dir, args.runs_dir, args.output)
            code = int(result["exit_code"])
        elif args.campaign_command == "next":
            result = create_next_batch(
                args.campaign_dir,
                args.publication_completion,
                actor=args.actor,
            )
            code = 0
        else:  # pragma: no cover - argparse owns completeness
            raise AssertionError(args.campaign_command)
    except (OSError, TypeError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        _emit({"status": "INFRASTRUCTURE_FATAL", "error_type": type(exc).__name__, "message": str(exc)}, stream=sys.stderr)
        return 1
    _emit(result)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
