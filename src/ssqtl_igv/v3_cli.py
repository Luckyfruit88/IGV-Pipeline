from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from .probes_v3 import collect_doctor_report
from .project_launcher import reconcile_project_output, run_project_workflow, validate_project_postflight
from .project_v3 import load_project_config
from .public_rerun_v3 import (
    build_failed_rerun_plan,
    failed_only_rerun_lock,
    reconcile_failed_rerun,
    validate_live_project_binding,
)
from .runtime_identity import RUNTIME_MANIFEST_IMAGE_PATH
from .utils import reject_symlink_path_components, sha256_file
from .v3_manifest import init_templates


def _add_resource_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--igv-cpus", type=int, default=1)
    parser.add_argument("--igv-memory", default="8GiB")
    parser.add_argument("--igv-timeout", default="30m")
    parser.add_argument("--normalization-cpus", type=int, default=1)
    parser.add_argument("--normalization-memory", default="12GiB")
    parser.add_argument("--normalization-timeout", default="36h")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="igv-snapshot",
        description="Pull-and-run Nextflow and IGV Desktop snapshot workflow",
    )
    parser.add_argument("--version", action="version", version="%(prog)s 3.0.0")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init = subparsers.add_parser("init", help="create a pull-and-run project template")
    init.add_argument("--adapter", choices=("generic", "ssqtl"), default="generic")
    init.add_argument("--output", default="igv-snapshot-project")

    smoke = subparsers.add_parser("smoke-test", help="optionally run a tiny real IGV example; no pilot required")
    smoke.add_argument("--output", default="/output/smoke-test")

    doctor = subparsers.add_parser("doctor", help="validate the project and embedded runtime")
    doctor.add_argument("--project", default="/project/project.yaml")
    doctor.add_argument("--output", default="/output")
    doctor.add_argument("--work")

    run = subparsers.add_parser(
        "run", help="validate metadata and produce IGV snapshots plus QC"
    )
    run.add_argument("--project", default="/project/project.yaml")
    run.add_argument("--output", default="/output")
    run.add_argument("--work")
    run.add_argument("--resume", action="store_true")
    run.add_argument("--max-parallel", default="auto", metavar="auto|N")
    run.add_argument("--max-cases-per-shard", type=int, default=256, help=argparse.SUPPRESS)
    _add_resource_options(run)

    reconcile = subparsers.add_parser(
        "reconcile", help="rebuild run status from retained evidence without rendering"
    )
    reconcile.add_argument("--output", default="/output")

    export_snapshots = subparsers.add_parser("export-snapshots", help="export a pinned snapshot view as ordinary files")
    export_snapshots.add_argument("--output", required=True)
    export_snapshots.add_argument("--destination", required=True)

    rerun_failed = subparsers.add_parser(
        "rerun-failed",
        help="run only checksum-bound failed cases in a new generation",
    )
    rerun_failed.add_argument("--project", default="/project/project.yaml")
    rerun_failed.add_argument("--output", default="/output")
    rerun_failed.add_argument("--work")
    rerun_failed.add_argument("--resume", action="store_true")
    rerun_failed.add_argument("--max-parallel", default="auto", metavar="auto|N")
    rerun_failed.add_argument("--max-cases-per-shard", type=int, default=256, help=argparse.SUPPRESS)
    _add_resource_options(rerun_failed)

    review = subparsers.add_parser(
        "review", help="optionally serve the localhost review UI or finalize decisions"
    )
    review.add_argument("--output", default="/output")
    review.add_argument("--reviewer")
    review.add_argument("--host", default="127.0.0.1")
    review.add_argument("--port", type=int, default=0)
    review.add_argument("--finalize", action="store_true")

    publish = subparsers.add_parser(
        "publish", help="optionally export finalized reviewed artifacts atomically"
    )
    publish.add_argument("--output", default="/output")
    publish.add_argument("--destination", required=True)
    publish.add_argument("--review-receipt")
    publish.add_argument("--staging")

    import_v2 = subparsers.add_parser(
        "import-v2", help="create a read-only v2 inventory receipt"
    )
    import_v2.add_argument("--source", required=True)
    import_v2.add_argument("--output", required=True)

    return parser


def _emit(value: dict[str, Any], *, stream: Any | None = None) -> None:
    print(
        json.dumps(value, sort_keys=True, ensure_ascii=False),
        file=sys.stdout if stream is None else stream,
    )


def _embedded_runtime_manifest() -> Path:
    # The override is intentionally internal: it supports source-tree tests and
    # image construction without restoring a public runtime-identity argument.
    return Path(
        os.environ.get("IGV_RUNTIME_MANIFEST_INTERNAL", RUNTIME_MANIFEST_IMAGE_PATH)
    ).expanduser()


def _resume_identity(output: Path) -> dict[str, Any]:
    path = output / "contract" / "run_identity.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read resume identity {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"resume identity is not an object: {path}")
    if not value.get("runtime_fingerprint_sha256") or not value.get(
        "project_binding_sha256"
    ):
        raise ValueError(
            "this output uses the unpublished legacy runtime-identity contract; "
            "start a new output directory instead of resuming it"
        )
    return value


def _finalized_review_receipt(output: Path, explicit: str | None) -> Path:
    if explicit:
        value = Path(explicit).expanduser()
        if value.is_symlink() or not value.resolve(strict=True).is_file():
            raise ValueError(
                f"explicit review receipt must be a regular non-symlink file: {value}"
            )
        return value.resolve(strict=True)
    pointer_path = output / "review" / "finalized_review.json"
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read finalized review pointer: {pointer_path}: {exc}") from exc
    if not isinstance(pointer, dict) or pointer.get("schema_version") != (
        "3.0-finalized-review-pointer"
    ):
        raise ValueError("finalized review pointer is invalid")
    relative = Path(str(pointer.get("receipt_relative_path", "")))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise ValueError("finalized review receipt path is unsafe")
    receipt = output / "review" / relative
    if receipt.is_symlink() or not receipt.is_file():
        raise ValueError(f"finalized review receipt is unavailable: {receipt}")
    if sha256_file(receipt) != pointer.get("receipt_sha256"):
        raise ValueError("finalized review receipt checksum differs from its pointer")
    return receipt.resolve(strict=True)


def _run(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    output = reject_symlink_path_components(args.output, label="output directory").resolve(
        strict=False
    )
    if args.resume:
        _resume_identity(output)
    return run_project_workflow(
        project=args.project,
        batch_request=None,
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


def _rerun_failed(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    output = reject_symlink_path_components(
        args.output, label="output directory"
    ).resolve(strict=True)
    runtime_manifest = _embedded_runtime_manifest()
    with failed_only_rerun_lock(output):
        plan = build_failed_rerun_plan(
            output,
            runtime_manifest=runtime_manifest,
        )
        if plan is None:
            return (
                {
                    "schema_version": "3.0-failed-only-rerun",
                    "status": "SNAPSHOTS_READY",
                    "action": "NO_RERUN_REQUIRED",
                    "exit_code": 0,
                },
                0,
            )

        validate_live_project_binding(plan["source_run"], args.project)
        generation_output = Path(plan["generation_output"])
        complete = all(
            path.is_file() and not path.is_symlink()
            for path in (
                generation_output / "snapshots.tsv",
                generation_output / "failed_cases.tsv",
                generation_output / "run_summary.json",
                generation_output / "reports" / "trace.txt",
                generation_output
                / ".igv-pipeline"
                / "contract"
                / "run_identity.json",
            )
        )
        if complete:
            generation_result = validate_project_postflight(generation_output)
            generation_code = int(generation_result["exit_code"])
        else:
            if (
                generation_output.exists()
                and any(generation_output.iterdir())
                and not args.resume
            ):
                raise ValueError(
                    "the failed-only generation is incomplete; rerun with --resume"
                )
            generation_result, generation_code = run_project_workflow(
                project=None,
                batch_request=None,
                rerun_source_run=plan["source_run"],
                rerun_receipt=plan["rerun_receipt"],
                run_id=plan["source_run_id"],
                generation_id=plan["generation_id"],
                output=generation_output,
                work=args.work or plan["default_work"],
                resume=args.resume,
                max_parallel=args.max_parallel,
                max_cases_per_shard=args.max_cases_per_shard,
                runtime_manifest=runtime_manifest,
                igv_cpus=args.igv_cpus,
                igv_memory=args.igv_memory,
                igv_timeout=args.igv_timeout,
                normalization_cpus=args.normalization_cpus,
                normalization_memory=args.normalization_memory,
                normalization_timeout=args.normalization_timeout,
                persist_fatal_summary=False,
            )
        if generation_code == 1:
            return (
                {
                    "schema_version": "3.0-failed-only-rerun",
                    "status": "INFRASTRUCTURE_FATAL",
                    "exit_code": 1,
                    "generation_id": plan["generation_id"],
                    "generation": generation_result,
                },
                1,
            )
        reconciliation = reconcile_failed_rerun(
            output,
            source_run=plan["source_run"],
            rerun_receipt=plan["rerun_receipt"],
            incoming_output=generation_output,
        )
        code = int(reconciliation["exit_code"])
        return (
            {
                "schema_version": "3.0-failed-only-rerun",
                "status": (
                    "CASE_FAILURES" if code == 2 else "SNAPSHOTS_READY"
                ),
                "exit_code": code,
                "generation_id": plan["generation_id"],
                "rerun_case_count": plan["rerun_case_count"],
                "generation": generation_result,
                "reconciliation": reconciliation,
            },
            code,
        )


def _publish(args: argparse.Namespace) -> dict[str, Any]:
    from .publication import build_publication_promotion_receipt, promote_publication
    from .publication_v3 import build_publication_staging

    output_value = Path(args.output).expanduser()
    if output_value.is_symlink() or not output_value.resolve(strict=True).is_dir():
        raise ValueError(f"output must be a regular non-symlink directory: {output_value}")
    output = output_value.resolve(strict=True)
    destination = reject_symlink_path_components(
        args.destination, label="publication destination"
    ).resolve(strict=False)
    receipt_path = _finalized_review_receipt(output, args.review_receipt)
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    staging = (
        reject_symlink_path_components(args.staging, label="publication staging").resolve(
            strict=False
        )
        if args.staging
        else output / "publication" / "staging" / str(receipt["review_generation_id"])
    )
    promotion_path = (
        output
        / "publication"
        / "receipts"
        / f"{receipt['review_generation_id']}.promotion.json"
    )
    if destination.exists() or destination.is_symlink():
        if promotion_path.is_file() and not promotion_path.is_symlink() and not staging.exists():
            return {
                "staging_receipt": json.loads(promotion_path.read_text(encoding="utf-8")),
                "publication": promote_publication(staging, destination, promotion_path),
            }
        raise FileExistsError(f"publication destination already exists: {destination}")
    if not staging.exists():
        build_publication_staging(output, receipt_path, staging)
    if promotion_path.is_file() and not promotion_path.is_symlink():
        promotion = json.loads(promotion_path.read_text(encoding="utf-8"))
    else:
        promotion = build_publication_promotion_receipt(
            staging,
            destination,
            receipt_path,
            output=promotion_path,
        )
    return {
        "staging_receipt": promotion,
        "publication": promote_publication(staging, destination, promotion_path),
    }


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "init":
            result, code = init_templates(args.output, adapter=args.adapter), 0
        elif args.command == "reconcile":
            result = reconcile_project_output(args.output)
            code = int(result["exit_code"])
        elif args.command == "export-snapshots":
            from .snapshot_store import export_snapshot_outputs
            result = export_snapshot_outputs(args.output, args.destination)
            code = 0
        elif args.command == "smoke-test":
            from .smoke_test import run_smoke_test

            result = run_smoke_test(args.output)
            code = int(result["exit_code"])
        elif args.command == "doctor":
            project = load_project_config(args.project)
            output = Path(args.output).expanduser().resolve(strict=False)
            work = Path(args.work).expanduser().resolve(strict=False) if args.work else output / ".work"
            result = collect_doctor_report(
                "standalone",
                runtime_manifest=_embedded_runtime_manifest(),
                run_dir=output,
                work_dir=work,
            )
            result["project"] = {
                "adapter": project["adapter"],
                "project_sha256": project["project_sha256"],
            }
            code = 0 if result["status"] == "PASS" else 1
        elif args.command == "run":
            result, code = _run(args)
        elif args.command == "rerun-failed":
            result, code = _rerun_failed(args)
        elif args.command == "review":
            from .review_server import finalize_review, serve_review

            if args.finalize:
                result = finalize_review(args.output)
            else:
                if not args.reviewer:
                    raise ValueError("reviewer is required unless --finalize is used")
                result = serve_review(
                    args.output,
                    host=args.host,
                    port=args.port,
                    reviewer=args.reviewer,
                )
            code = 0
        elif args.command == "publish":
            result, code = _publish(args), 0
        elif args.command == "import-v2":
            from .migration_v3 import import_v2_read_only

            result, code = import_v2_read_only(args.source, args.output), 0
        else:  # pragma: no cover - argparse owns completeness
            raise AssertionError(args.command)
    except (
        OSError,
        TypeError,
        ValueError,
        RuntimeError,
        subprocess.SubprocessError,
    ) as exc:
        _emit(
            {
                "schema_version": "3.0",
                "status": "INFRASTRUCTURE_FATAL",
                "error_type": type(exc).__name__,
                "message": str(exc),
            },
            stream=sys.stderr,
        )
        return 1
    _emit(result)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
