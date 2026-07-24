from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any, Mapping

from .campaign_v3 import materialize_batch_tasks
from .contracts import validate_v3_task_document
from .identity import task_set_fingerprint
from .orchestrator_v3 import (
    _SSQTL_BUNDLE_FILES,
    _admit_session_case_outputs,
    _ssqtl_bind_contract,
    _validated_terminal_case_results,
    _write_direct_output_tables,
    _write_run_summary,
)
from .project_v3 import build_project_source_binding, load_project_config
from .runtime_identity import validate_runtime_manifest
from .sharding_v3 import create_bounded_shards
from .utils import (
    atomic_write_json,
    read_jsonl,
    reject_symlink_path_components,
    sha256_file,
    sha256_json,
    write_jsonl,
)
from .v3_manifest import normalize_generic_manifest


def _object(path: Path, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def _safe_output_directory(value: str | Path, *, label: str) -> tuple[Path, Path]:
    destination = reject_symlink_path_components(value, label=label).resolve(strict=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"{label} already exists: {destination}")
    staging = destination.parent / f".{destination.name}.tmp-{uuid.uuid4().hex}"
    staging.mkdir(mode=0o700)
    return destination, staging


def _default_run_id(project_binding: Mapping[str, Any]) -> str:
    """Derive a stable run identity so native ``-resume`` can reuse one DAG."""

    return "run_" + str(project_binding["binding_sha256"])[:20]


def _runtime_claim(runtime_manifest: str | Path) -> tuple[Path, dict[str, Any]]:
    source = Path(runtime_manifest).expanduser()
    if source.is_symlink() or not source.resolve(strict=True).is_file():
        raise ValueError("runtime manifest must be a regular non-symlink file")
    source = source.resolve(strict=True)
    validation = validate_runtime_manifest(source)
    return source, {
        "runtime_manifest_sha256": validation["runtime_manifest_sha256"],
        "runtime_fingerprint_sha256": validation["runtime_fingerprint_sha256"],
        "observed_provenance": validation["observed_provenance"],
    }


def _write_batch_normalization(
    tasks: list[dict[str, Any]],
    binding: Mapping[str, Any],
    destination: Path,
) -> None:
    request = dict(binding["request"])
    destination.mkdir(mode=0o700)
    write_jsonl(destination / "tasks.jsonl", tasks)
    validation = {
        "schema_version": "3.0",
        "pipeline_version": "3.0.0",
        "status": "PASS",
        "adapter_id": "ssqtl",
        "adapter_schema_version": "3.0-ssqtl",
        "run_id": request["execution_run_id"],
        "generation_id": request["execution_generation_id"],
        "task_count": len(tasks),
        "ready_task_count": sum(
            task["core"]["preflight"]["state"] == "READY" for task in tasks
        ),
        "case_input_invalid_count": sum(
            task["core"]["preflight"]["state"] != "READY" for task in tasks
        ),
        "tasks_sha256": sha256_file(destination / "tasks.jsonl"),
        "task_set_sha256": task_set_fingerprint(tasks),
        "source": "immutable_batch_request",
    }
    atomic_write_json(destination / "validation.json", validation)
    atomic_write_json(
        destination / "parameters.json",
        {
            "schema_version": "3.0",
            "adapter_id": "ssqtl",
            "run_id": request["execution_run_id"],
            "generation_id": request["execution_generation_id"],
            "batch_request_sha256": binding["request_sha256"],
        },
    )


def resolve_project_entry(
    output_dir: str | Path,
    *,
    runtime_manifest: str | Path,
    project: str | Path | None = None,
    batch_request: str | Path | None = None,
    run_id: str | None = None,
    generation_id: str | None = None,
    profile: str = "standalone",
) -> dict[str, Any]:
    """Validate one public entry and emit only immutable scientific source facts.

    This function never launches Nextflow and never records task execution state.
    Raw ssQTL metadata is represented by a bind contract for the downstream
    normalization process; generic and campaign inputs can immediately emit a
    canonical normalization bundle.
    """

    if (project is None) == (batch_request is None):
        raise ValueError("exactly one of project or batch_request must be supplied")
    runtime_source, runtime_claim = _runtime_claim(runtime_manifest)
    destination, staging = _safe_output_directory(
        output_dir, label="project entry source"
    )
    try:
        if batch_request is not None:
            tasks, binding_value = materialize_batch_tasks(batch_request)
            binding = dict(binding_value)
            request = dict(binding["request"])
            effective_run_id = str(request["execution_run_id"])
            effective_generation_id = str(request["execution_generation_id"])
            if run_id is not None and run_id != effective_run_id:
                raise ValueError("run_id differs from immutable batch-request")
            if generation_id is not None and generation_id != effective_generation_id:
                raise ValueError(
                    "generation_id differs from immutable batch-request"
                )
            normalization = staging / "normalization"
            _write_batch_normalization(tasks, binding, normalization)
            source_request = Path(str(binding["request_path"])).resolve(strict=True)
            shutil.copyfile(source_request, staging / "batch-request.json")
            campaign_binding = {
                "schema_version": "3.0-batch-admission",
                "campaign_id": binding["campaign_id"],
                "campaign_contract_sha256": binding[
                    "campaign_contract_sha256"
                ],
                "campaign_root": binding["campaign_root"],
                "batch_id": request["batch_id"],
                "batch_index": request["batch_index"],
                "purpose": request["purpose"],
                "batch_request_sha256": binding["request_sha256"],
                "master_task_count": request["master_task_count"],
                "master_tasks_sha256": request["master_tasks_sha256"],
                "master_task_set_sha256": request["master_task_set_sha256"],
                "pilot_selection_sha256": request["pilot_selection_sha256"],
                "task_count": request["task_count"],
                "tasks_sha256": binding["tasks_sha256"],
                "task_set_sha256": binding["task_set_sha256"],
            }
            atomic_write_json(staging / "campaign_binding.json", campaign_binding)
            descriptor = {
                "schema_version": "3.0-project-entry-source",
                "entry_kind": "batch",
                "adapter": "ssqtl",
                "normalization_required": False,
                "run_id": effective_run_id,
                "generation_id": effective_generation_id,
                "profile": profile,
                "batch_request_path": str(source_request),
                "batch_request_sha256": binding["request_sha256"],
                **runtime_claim,
            }
        else:
            loaded = load_project_config(project)
            project_binding = build_project_source_binding(loaded)
            atomic_write_json(staging / "project_binding.json", project_binding)
            effective_run_id = run_id or _default_run_id(project_binding)
            effective_generation_id = generation_id or "generation-001"
            adapter = str(loaded["adapter"])
            descriptor = {
                "schema_version": "3.0-project-entry-source",
                "entry_kind": "project",
                "adapter": adapter,
                "normalization_required": adapter == "ssqtl",
                "run_id": effective_run_id,
                "generation_id": effective_generation_id,
                "profile": profile,
                "project_path": loaded["project_path"],
                "project_root": loaded["project_root"],
                "project_sha256": loaded["project_sha256"],
                "project_binding_sha256": sha256_file(
                    staging / "project_binding.json"
                ),
                **runtime_claim,
            }
            if adapter == "generic":
                normalize_generic_manifest(
                    loaded["inputs"]["cases"]["source_path"],
                    loaded["project_root"],
                    loaded["reference"]["source_path"],
                    staging / "normalization",
                    effective_run_id,
                    effective_generation_id,
                )
            else:
                inputs = dict(loaded["inputs"])
                bind_contract = _ssqtl_bind_contract(
                    root=Path(str(loaded["project_root"])).resolve(strict=True),
                    reference=Path(
                        str(loaded["reference"]["source_path"])
                    ).resolve(strict=True),
                    associations=str(inputs["associations"]["declared_path"]),
                    rds_dir=str(inputs["rds_dir"]["declared_path"]),
                    bam_lookup=str(inputs["bam_lookup"]["declared_path"]),
                    violin_dir=str(inputs["violin_dir"]["declared_path"]),
                    adapter_config=(
                        str(inputs["config"]["declared_path"])
                        if inputs.get("config")
                        else None
                    ),
                    runtime_identity=runtime_claim,
                )
                atomic_write_json(staging / "ssqtl_bind_contract.json", bind_contract)
                descriptor["ssqtl"] = {
                    "associations": inputs["associations"]["declared_path"],
                    "rds_dir": inputs["rds_dir"]["declared_path"],
                    "bam_lookup": inputs["bam_lookup"]["declared_path"],
                    "violin_dir": inputs["violin_dir"]["declared_path"],
                    "input_root": loaded["project_root"],
                    "reference": loaded["reference"]["source_path"],
                    "reference_root": str(
                        Path(str(loaded["reference"]["source_path"])).parent
                    ),
                    "config": (
                        inputs["config"]["declared_path"]
                        if inputs.get("config")
                        else ""
                    ),
                    "bind_contract_sha256": sha256_file(
                        staging / "ssqtl_bind_contract.json"
                    ),
                }

        descriptor["descriptor_sha256"] = sha256_json(descriptor)
        atomic_write_json(staging / "descriptor.json", descriptor)
        shutil.copyfile(runtime_source, staging / "runtime_manifest.snapshot.json")
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **descriptor,
        "output_dir": str(destination),
        "descriptor": str(destination / "descriptor.json"),
    }


def _validate_normalization_bundle(
    source: Path,
    descriptor: Mapping[str, Any],
    *,
    allow_staged_symlink: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if (source.is_symlink() and not allow_staged_symlink) or not source.resolve(
        strict=True
    ).is_dir():
        raise ValueError("normalization bundle must be a regular non-symlink directory")
    source = source.resolve(strict=True)
    adapter = str(descriptor["adapter"])
    if adapter == "ssqtl" and bool(descriptor["normalization_required"]):
        observed = {path.name for path in source.iterdir()}
        if observed != _SSQTL_BUNDLE_FILES:
            raise ValueError("raw ssQTL normalization bundle file set differs")
    required = {"tasks.jsonl", "validation.json", "parameters.json"}
    if not required.issubset({path.name for path in source.iterdir()}):
        raise ValueError("normalization bundle is incomplete")
    for path in source.iterdir():
        if path.is_symlink() or not path.is_file():
            raise ValueError("normalization bundle contains a non-regular file")
    tasks = list(read_jsonl(source / "tasks.jsonl"))
    if not tasks:
        raise ValueError("canonical task set is empty")
    for task in tasks:
        validate_v3_task_document(task)
        if (
            task.get("adapter_id") != adapter
            or task.get("run_id") != descriptor["run_id"]
            or task.get("generation_id") != descriptor["generation_id"]
        ):
            raise ValueError("canonical task identity differs from project entry")
    if [int(task["manifest_order"]) for task in tasks] != list(
        range(1, len(tasks) + 1)
    ):
        raise ValueError("canonical manifest_order is not contiguous")
    validation = _object(source / "validation.json", label="normalization validation")
    if (
        validation.get("status") not in {"PASS", "PASS_WITH_CASE_INPUT_ERRORS"}
        or validation.get("adapter_id") != adapter
        or validation.get("run_id") != descriptor["run_id"]
        or validation.get("generation_id") != descriptor["generation_id"]
        or int(validation.get("task_count", -1)) != len(tasks)
        or validation.get("tasks_sha256") != sha256_file(source / "tasks.jsonl")
        or validation.get("task_set_sha256") != task_set_fingerprint(tasks)
    ):
        raise ValueError("normalization validation differs from canonical tasks")
    return tasks, validation


def admit_project_tasks(
    entry_source: str | Path,
    normalization_bundle: str | Path,
    execution_policy: str | Path,
    output_dir: str | Path,
    *,
    max_cases_per_shard: int,
    allow_staged_symlink: bool = False,
) -> dict[str, Any]:
    """Freeze canonical tasks and logical shards after source normalization."""

    entry = Path(entry_source).resolve(strict=True)
    descriptor = _object(entry / "descriptor.json", label="project entry descriptor")
    expected_descriptor_sha = descriptor.pop("descriptor_sha256", None)
    if expected_descriptor_sha != sha256_json(descriptor):
        raise ValueError("project entry descriptor checksum differs")
    descriptor["descriptor_sha256"] = expected_descriptor_sha
    policy = Path(execution_policy).resolve(strict=True)
    if policy.is_symlink() or not policy.is_file():
        raise ValueError("execution policy must be a regular non-symlink file")
    tasks, validation = _validate_normalization_bundle(
        Path(normalization_bundle),
        descriptor,
        allow_staged_symlink=allow_staged_symlink,
    )
    destination, staging = _safe_output_directory(
        output_dir, label="project task admission"
    )
    try:
        contract = staging / "contract"
        contract.mkdir()
        shutil.copyfile(
            Path(normalization_bundle).resolve(strict=True) / "tasks.jsonl",
            contract / "tasks.jsonl",
        )
        shutil.copyfile(
            entry / "runtime_manifest.snapshot.json",
            contract / "runtime_manifest.snapshot.json",
        )
        shutil.copyfile(policy, contract / "execution_policy.json")
        for name in (
            "project_binding.json",
            "campaign_binding.json",
            "batch-request.json",
            "ssqtl_bind_contract.json",
        ):
            source = entry / name
            if source.is_file() and not source.is_symlink():
                shutil.copyfile(source, contract / name)
        create_bounded_shards(
            contract / "tasks.jsonl",
            staging / "shards",
            max_cases_per_shard=max_cases_per_shard,
            relative_paths=True,
        )
        shard_plan = staging / "shards" / "shard_plan.json"
        normalization_claim = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "status": validation["status"],
            "adapter_id": descriptor["adapter"],
            "run_id": descriptor["run_id"],
            "generation_id": descriptor["generation_id"],
            "task_count": len(tasks),
            "tasks_sha256": sha256_file(contract / "tasks.jsonl"),
            "task_set_sha256": task_set_fingerprint(tasks),
            "source_validation_sha256": sha256_file(
                Path(normalization_bundle).resolve(strict=True) / "validation.json"
            ),
        }
        atomic_write_json(contract / "normalization.json", normalization_claim)
        identity = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "run_id": descriptor["run_id"],
            "generation_id": descriptor["generation_id"],
            "profile": descriptor["profile"],
            "adapter": descriptor["adapter"],
            "canonical_tasks_sha256": sha256_file(contract / "tasks.jsonl"),
            "canonical_task_set_sha256": task_set_fingerprint(tasks),
            "shard_plan_sha256": sha256_file(shard_plan),
            "runtime_manifest_sha256": descriptor["runtime_manifest_sha256"],
            "runtime_manifest_snapshot_sha256": sha256_file(
                contract / "runtime_manifest.snapshot.json"
            ),
            "runtime_fingerprint_sha256": descriptor[
                "runtime_fingerprint_sha256"
            ],
            "execution_policy_sha256": sha256_file(
                contract / "execution_policy.json"
            ),
            "entry_descriptor_sha256": sha256_file(entry / "descriptor.json"),
        }
        for name, key in (
            ("project_binding.json", "project_binding_sha256"),
            ("campaign_binding.json", "campaign_binding_sha256"),
            ("batch-request.json", "batch_request_sha256"),
            ("ssqtl_bind_contract.json", "ssqtl_bind_contract_sha256"),
        ):
            path = contract / name
            if path.is_file():
                identity[key] = sha256_file(path)
        atomic_write_json(contract / "run_identity.json", identity)
        atomic_write_json(
            staging / "admission.json",
            {
                "schema_version": "3.0-project-task-admission",
                "status": "READY",
                "adapter": descriptor["adapter"],
                "run_id": descriptor["run_id"],
                "generation_id": descriptor["generation_id"],
                "task_count": len(tasks),
                "logical_shard_count": len(
                    _object(shard_plan, label="logical shard plan")["shards"]
                ),
                "canonical_tasks_sha256": identity["canonical_tasks_sha256"],
                "canonical_task_set_sha256": identity[
                    "canonical_task_set_sha256"
                ],
                "runtime_fingerprint_sha256": identity[
                    "runtime_fingerprint_sha256"
                ],
                "execution_policy_sha256": identity["execution_policy_sha256"],
            },
        )
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        "status": "READY",
        "output_dir": str(destination),
        "tasks": str(destination / "contract" / "tasks.jsonl"),
        "task_count": len(tasks),
        "run_identity": str(destination / "contract" / "run_identity.json"),
        "shard_plan": str(destination / "shards" / "shard_plan.json"),
    }


def finalize_cases(
    admission_bundle: str | Path,
    case_bundle_root: str | Path,
    output_dir: str | Path,
    *,
    allow_debug_only: bool = False,
) -> dict[str, Any]:
    """Validate terminal bundles and build direct outputs without reading trace."""

    admission = Path(admission_bundle).resolve(strict=True)
    tasks = list(read_jsonl(admission / "contract" / "tasks.jsonl"))
    task_map = {str(task["task_id"]): task for task in tasks}
    if len(task_map) != len(tasks):
        raise ValueError("canonical task set contains duplicate task IDs")
    source_root = Path(case_bundle_root).resolve(strict=True)
    candidates: dict[str, Path] = {}
    for group in sorted(source_root.iterdir(), key=lambda path: path.name):
        if group.is_symlink() or not group.is_dir():
            raise ValueError("staged case bundle root contains an invalid group")
        for staged_case in sorted(group.iterdir(), key=lambda path: path.name):
            resolved_case = staged_case.resolve(strict=True)
            if not resolved_case.is_dir() or not (
                resolved_case / "terminal_bundle.json"
            ).is_file():
                raise ValueError("staged case bundle is incomplete")
            if staged_case.name in candidates:
                raise ValueError("staged case bundle contains duplicate task IDs")
            candidates[staged_case.name] = resolved_case
    if set(candidates) != set(task_map):
        raise ValueError("terminal case bundle set differs from canonical tasks")
    destination, staging = _safe_output_directory(
        output_dir, label="finalized case output"
    )
    try:
        shutil.copytree(admission / "contract", staging / "contract")
        shutil.copytree(admission / "shards", staging / "shards")
        session = staging / ".case-admission"
        case_outputs = session / "case_outputs"
        case_outputs.mkdir(parents=True)
        for task_id, source in candidates.items():
            shutil.copytree(source, case_outputs / task_id, symlinks=False)
        _admit_session_case_outputs(
            staging,
            session,
            {"shard_id": "logical-all", "task_ids": [str(t["task_id"]) for t in tasks]},
            task_map,
            allow_debug_only=allow_debug_only,
        )
        shutil.rmtree(session)
        case_results, failures = _validated_terminal_case_results(staging, tasks)
        direct_outputs = _write_direct_output_tables(staging, tasks, case_results)
        summary = {
            "schema_version": "3.0",
            "pipeline_version": "3.0.0",
            "status": "CASE_FAILURES" if failures else "SNAPSHOTS_READY",
            "exit_code": 2 if failures else 0,
            "profile": _object(
                staging / "contract" / "run_identity.json", label="run identity"
            )["profile"],
            "expected_case_count": len(tasks),
            "observed_case_count": len(case_results),
            "failed_case_ids": failures,
            "shards": _object(
                staging / "shards" / "shard_plan.json", label="logical shard plan"
            )["shards"],
            "direct_outputs": direct_outputs,
            "publication_state": "NOT_READY",
            "human_review_required": False,
            "review_gate": False,
            "effective_max_parallel": _object(
                staging / "contract" / "execution_policy.json",
                label="execution policy",
            )["concurrency"]["effective_max_parallel"],
        }
        _write_run_summary(staging, summary)
        os.replace(staging, destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return {
        **summary,
        "output_dir": str(destination),
        "run_summary": str(destination / "run_summary.json"),
    }
