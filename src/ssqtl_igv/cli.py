from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from importlib.resources import files
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .preflight import run_preflight
from .prepare import prepare_run
from .qacct import collect_qacct_evidence, submitted_job_records
from .review import record_reviews
from .runner import run_shard
from .scheduler import create_resume_submission, create_submission, resume_cases
from .summary import summarize_run
from .utils import optional_text


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="igv-snapshot-workflow",
        description="Portable closed-loop native IGV evidence workflow",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_config = subparsers.add_parser(
        "init-config", help="write the packaged workflow configuration template"
    )
    init_config.add_argument("--output", required=True)
    init_config.add_argument("--force", action="store_true")

    preflight = subparsers.add_parser("preflight", help="validate tools, immutable resources, and optional manifest inputs")
    preflight.add_argument("--config", required=True)
    preflight.add_argument("--run-root")
    preflight.add_argument("--manifest")

    prepare = subparsers.add_parser("prepare", help="build authoritative case manifest and chromosome/strand shards")
    prepare.add_argument("--config", required=True)
    prepare.add_argument("--associations")
    prepare.add_argument("--run-root", required=True)
    prepare.add_argument("--prepared-cases", help=argparse.SUPPRESS)
    prepare.add_argument("--prepared-samples", help=argparse.SUPPRESS)

    shard = subparsers.add_parser("run-shard", help="run one chromosome/strand shard")
    shard.add_argument("--config", required=True)
    shard.add_argument("--run-root", required=True)
    group = shard.add_mutually_exclusive_group(required=False)
    group.add_argument("--shard")
    group.add_argument("--shard-index", type=int, default=None)
    shard.add_argument("--force", action="store_true")
    shard.add_argument("--shard-map")
    shard.add_argument("--case-list")

    submit = subparsers.add_parser("submit", help="plan or submit a Grid Engine array and dependent summary job")
    submit.add_argument("--config", required=True)
    submit.add_argument("--run-root", required=True)
    submit_action = submit.add_mutually_exclusive_group(required=True)
    submit_action.add_argument("--dry-run", action="store_true")
    submit_action.add_argument("--submit", action="store_true")
    submit.add_argument("--generation", type=int, default=1, help="audited qsub retry generation")

    run = subparsers.add_parser("run", help="prepare, preflight, then execute all cases; stops at review pending")
    run.add_argument("--config", required=True)
    run.add_argument("--run-root", required=True)
    run.add_argument("--associations")
    run_mode = run.add_mutually_exclusive_group()
    run_mode.add_argument("--dry-run", action="store_true", help="plan Grid Engine submission without qsub")
    run_mode.add_argument("--submit", action="store_true", help="submit Grid Engine arrays")

    qacct = subparsers.add_parser("collect-qacct", help="freeze qacct evidence for submitted Grid Engine arrays")
    qacct.add_argument("--config", required=True)
    qacct.add_argument("--run-root", required=True)
    qacct.add_argument("--jobs", action="append")

    resume = subparsers.add_parser("resume", help="rerun only case IDs in a rerun manifest")
    resume.add_argument("--config", required=True)
    resume.add_argument("--run-root", required=True)
    resume.add_argument("--rerun-manifest", required=True)
    resume_mode = resume.add_mutually_exclusive_group(required=True)
    resume_mode.add_argument("--dry-run", action="store_true")
    resume_mode.add_argument("--submit", action="store_true")
    resume_mode.add_argument(
        "--local",
        action="store_true",
        help="run serially in the current allocated session instead of qsub",
    )
    resume.add_argument("--generation", type=int, default=1, help="audited qsub retry generation")

    summarize = subparsers.add_parser(
        "summarize", help="deliver review artifacts and finalize approved cases"
    )
    summarize.add_argument("--config", required=True)
    summarize.add_argument("--run-root", required=True)

    review = subparsers.add_parser("review", help="record explicit human approval or rejection")
    review.add_argument("--config", required=True)
    review.add_argument("--run-root", required=True)
    review.add_argument("--case-id", action="append", required=True)
    review.add_argument("--decision", choices=("approve", "reject"), required=True)
    review.add_argument("--reviewer", required=True)
    review.add_argument("--notes", default="")
    review.add_argument("--confirm-native-igv", action="store_true")
    review.add_argument("--confirm-annotation-visible", action="store_true")
    review.add_argument("--confirm-strand-transcript", action="store_true")
    review.add_argument("--confirm-ag-site-context", action="store_true")
    review.add_argument("--confirm-splice-junction-judgeable", action="store_true")
    review.add_argument("--confirm-violin-pair", action="store_true")
    return parser


def _emit(value: dict[str, Any]) -> None:
    print(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")))


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "init-config":
            output = Path(args.output).expanduser().resolve(strict=False)
            if output.exists() and not args.force:
                raise FileExistsError(f"configuration already exists: {output}")
            output.parent.mkdir(parents=True, exist_ok=True)
            template = files("ssqtl_igv.resources").joinpath("workflow.example.yaml")
            output.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
            result = {"status": "CREATED", "config": str(output)}
            code = 0
            _emit(result)
            return code
        config = WorkflowConfig.load(args.config)
        mode = optional_text(config.get("execution.mode", "local")).lower()
        if args.command == "run":
            scheduler_action = bool(args.dry_run or args.submit)
            if mode == "grid_engine" and not scheduler_action:
                raise ValueError("grid_engine mode requires explicit --dry-run or --submit")
            if mode == "local" and scheduler_action:
                raise ValueError("local mode does not accept --dry-run or --submit")
        if args.command == "resume" and mode == "local" and not args.local:
            raise ValueError("local mode resume requires explicit --local")
        if args.command in {"submit", "collect-qacct"} and mode != "grid_engine":
            raise ValueError(f"{args.command} requires execution.mode=grid_engine")
        if args.command == "preflight":
            result = run_preflight(config, run_root=args.run_root, manifest=args.manifest)
            code = 0 if result["status"] in {"PASS", "PASS_WITH_CASE_FAILURES"} else 1
        elif args.command == "prepare":
            result = prepare_run(
                config,
                args.run_root,
                associations=args.associations,
                prepared_cases=args.prepared_cases,
                prepared_samples=args.prepared_samples,
            )
            code = 2 if result["failed_preparation_count"] else 0
        elif args.command == "run-shard":
            shard_index = args.shard_index
            if shard_index is None and args.shard is None:
                shard_index = int(os.environ.get("SGE_TASK_ID", "0")) or None
            case_ids = None
            if args.case_list:
                with open(args.case_list, encoding="utf-8", newline="") as handle:
                    case_ids = {row["case_id"] for row in csv.DictReader(handle, delimiter="\t") if row.get("case_id")}
            result = run_shard(
                config,
                args.run_root,
                shard=args.shard,
                shard_index=shard_index,
                shard_map=args.shard_map,
                case_ids=case_ids,
                force=args.force,
            )
            code = result["exit_code"]
        elif args.command == "submit":
            result = create_submission(
                config,
                args.run_root,
                execute=args.submit,
                generation=args.generation,
            )
            code = 0
        elif args.command == "run":
            prepared = prepare_run(config, args.run_root, associations=args.associations)
            if mode == "local":
                preflight = run_preflight(
                    config, run_root=args.run_root, manifest=prepared["manifest"]
                )
                if preflight["status"] not in {"PASS", "PASS_WITH_CASE_FAILURES"}:
                    result, code = {"prepare": prepared, "preflight": preflight}, 1
                else:
                    run_result = run_shard(
                        config,
                        args.run_root,
                        shard_map=prepared["shards"],
                    )
                    result = {
                        "prepare": prepared,
                        "preflight": preflight,
                        "execution": run_result,
                    }
                    code = run_result["exit_code"]
            else:
                submission = create_submission(
                    config, args.run_root, execute=args.submit, generation=1
                )
                result = {"prepare": prepared, "submission": submission}
                code = int(submission.get("exit_code", 0))
        elif args.command == "collect-qacct":
            run_root = config.validate_run_root(args.run_root, must_exist=True)
            records = args.jobs or [str(path) for path in submitted_job_records(run_root)]
            if not records:
                raise ValueError("no submitted scheduler jobs records found")
            result = {
                "count": len(records),
                "evidence": [
                    collect_qacct_evidence(
                        path,
                        run_root=run_root,
                        qacct_command=config.get("binaries.qacct"),
                    )
                    for path in records
                ],
            }
            code = 0
        elif args.command == "resume":
            if args.local:
                result = resume_cases(config, args.run_root, args.rerun_manifest)
                code = result["exit_code"]
            else:
                result = create_resume_submission(
                    config,
                    args.run_root,
                    args.rerun_manifest,
                    execute=args.submit,
                    generation=args.generation,
                )
                code = int(result.get("exit_code", 0))
        elif args.command == "summarize":
            result = summarize_run(config, args.run_root)
            code = result["exit_code"]
        elif args.command == "review":
            result = record_reviews(
                config,
                args.run_root,
                case_ids=set(args.case_id),
                decision=args.decision,
                reviewer=args.reviewer,
                notes=args.notes,
                manual_assertions={
                    "native_igv_complete_and_readable": args.confirm_native_igv,
                    "annotation_track_and_model_visible": args.confirm_annotation_visible,
                    "strand_and_transcript_reviewed": args.confirm_strand_transcript,
                    "ag_site_and_reference_ag_context_reviewed": args.confirm_ag_site_context,
                    "splice_or_junction_presence_absence_judgeable": args.confirm_splice_junction_judgeable,
                    "violin_pair_matches": args.confirm_violin_pair,
                },
            )
            code = 0
        else:  # pragma: no cover
            raise AssertionError(args.command)
        _emit(result)
        return code
    except Exception as exc:
        error = {"status": "FATAL", "error_type": type(exc).__name__, "message": str(exc)}
        print(json.dumps(error, ensure_ascii=False, separators=(",", ":")), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
