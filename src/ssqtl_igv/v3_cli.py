from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .campaign_v3 import (
    create_next_batch,
    load_and_validate_batch_request,
    prepare_campaign,
    reduce_campaign_status,
)
from .migration_v3 import import_v2_read_only
from .orchestrator_v3 import (
    execute_portable_run,
    prepare_portable_run,
    resolve_max_parallel,
    run_portable_ssqtl_normalization,
)
from .probes_v3 import collect_doctor_report
from .project_v3 import build_project_source_binding, load_project_config
from .publication import build_publication_promotion_receipt, promote_publication
from .publication_v3 import build_publication_staging
from .review_server import finalize_review, serve_review
from .runtime_identity import RUNTIME_MANIFEST_IMAGE_PATH
from .utils import reject_symlink_path_components, sha256_file
from .v3_manifest import init_templates


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
    run.add_argument("--max-cases-per-shard", type=int, default=256)

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

    campaign_status = campaign_commands.add_parser(
        "status", help="reduce live authoritative sources without writing campaign state"
    )
    campaign_status.add_argument("--campaign-dir", required=True)
    campaign_status.add_argument("--batch-id")
    campaign_status.add_argument("--nextflow-trace")
    campaign_status.add_argument("--raw-qacct")
    campaign_status.add_argument("--accounting-attestation")
    campaign_status.add_argument("--publication-completion")

    campaign_next = campaign_commands.add_parser(
        "next", help="authorize the next <=256-case request after verified publication"
    )
    campaign_next.add_argument("--campaign-dir", required=True)
    campaign_next.add_argument("--publication-completion", required=True)
    campaign_next.add_argument("--actor", default=os.environ.get("USER", "operator"))
    return parser


def _emit(value: dict[str, Any], *, stream: Any | None = None) -> None:
    print(
        json.dumps(value, sort_keys=True, ensure_ascii=False),
        file=sys.stdout if stream is None else stream,
    )


def _default_run_id() -> str:
    return "run_" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
    project = load_project_config(args.project)
    project_binding = build_project_source_binding(project)
    output = reject_symlink_path_components(args.output, label="output directory").resolve(
        strict=False
    )
    work_value = args.work if args.work else output / ".work"
    work = reject_symlink_path_components(work_value, label="work directory").resolve(
        strict=False
    )
    runtime_manifest = _embedded_runtime_manifest()
    max_parallel = resolve_max_parallel(args.max_parallel)
    if not 1 <= args.max_cases_per_shard <= 256:
        raise ValueError("--max-cases-per-shard must be between 1 and 256")

    if args.resume:
        frozen = _resume_identity(output)
        run_id = str(frozen["run_id"])
        generation_id = str(frozen["generation_id"])
        if frozen.get("adapter") != project["adapter"]:
            raise ValueError("project adapter differs from the immutable resumed run")
    else:
        run_id = _default_run_id()
        generation_id = "generation-001"

    normalization_execution: dict[str, Any] | None = None
    if project["adapter"] == "ssqtl" and not args.resume:
        inputs = project["inputs"]
        normalization_execution = run_portable_ssqtl_normalization(
            run_dir=output,
            run_id=run_id,
            generation_id=generation_id,
            profile="standalone",
            associations=inputs["associations"]["declared_path"],
            rds_dir=inputs["rds_dir"]["declared_path"],
            bam_lookup=inputs["bam_lookup"]["declared_path"],
            violin_dir=inputs["violin_dir"]["declared_path"],
            input_root=project["project_root"],
            reference=project["reference"]["source_path"],
            adapter_config=(inputs.get("config") or {}).get("declared_path"),
            runtime_identity_path=runtime_manifest,
            nextflow=None,
            work_dir=work,
            max_parallel=max_parallel,
        )
    try:
        prepared = prepare_portable_run(
            run_dir=output,
            run_id=run_id,
            generation_id=generation_id,
            profile="standalone",
            adapter=project["adapter"],
            runtime_identity_path=runtime_manifest,
            project_binding=project_binding,
            manifest=(
                project["inputs"]["cases"]["source_path"]
                if project["adapter"] == "generic"
                else None
            ),
            input_root=project["project_root"] if project["adapter"] == "generic" else None,
            reference=(
                project["reference"]["source_path"]
                if project["adapter"] == "generic"
                else None
            ),
            ssqtl_normalization_bundle=(normalization_execution or {}).get("bundle"),
            ssqtl_normalization_execution=normalization_execution,
            max_cases_per_shard=args.max_cases_per_shard,
            max_parallel=max_parallel,
            resume=args.resume,
        )
    finally:
        if normalization_execution:
            shutil.rmtree(normalization_execution["temporary_root"], ignore_errors=True)

    result = execute_portable_run(
        prepared,
        profile="standalone",
        runtime_identity_path=runtime_manifest,
        nextflow=None,
        work_dir=work,
        resume=args.resume,
        max_parallel=max_parallel,
    )
    return result, int(result.get("exit_code", 1))


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
    binding = load_and_validate_batch_request(args.batch_request)
    request = binding["request"]
    output = reject_symlink_path_components(
        args.output, label="output directory"
    ).resolve(strict=False)
    work_value = args.work if args.work else output / ".work"
    work = reject_symlink_path_components(
        work_value, label="work directory"
    ).resolve(strict=False)
    max_parallel = resolve_max_parallel(args.max_parallel)
    if not 1 <= args.max_cases_per_shard <= 256:
        raise ValueError("--max-cases-per-shard must be between 1 and 256")
    prepared = prepare_portable_run(
        run_dir=output,
        run_id=str(request["execution_run_id"]),
        generation_id=str(request["execution_generation_id"]),
        profile="standalone",
        adapter="ssqtl",
        runtime_identity_path=_embedded_runtime_manifest(),
        batch_request=binding["request_path"],
        max_cases_per_shard=args.max_cases_per_shard,
        max_parallel=max_parallel,
        resume=args.resume,
    )
    result = execute_portable_run(
        prepared,
        profile="standalone",
        runtime_identity_path=_embedded_runtime_manifest(),
        nextflow=None,
        work_dir=work,
        resume=args.resume,
        max_parallel=max_parallel,
    )
    return result, int(result.get("exit_code", 1))


def _publish(args: argparse.Namespace) -> dict[str, Any]:
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
        elif args.command == "review":
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
            result, code = import_v2_read_only(args.source, args.output), 0
        elif args.command == "campaign":
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
            elif args.campaign_command == "next":
                result = create_next_batch(
                    args.campaign_dir,
                    args.publication_completion,
                    actor=args.actor,
                )
                code = 0
            else:  # pragma: no cover - argparse owns completeness
                raise AssertionError(args.campaign_command)
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
