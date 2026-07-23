from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Callable

from .bundles import StageBundle
from .contracts import validate_task_document
from .utils import resource_contains_remote_url, sha256_file


def expected_stage_inputs(task: dict[str, Any]) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for track in task["tracks"]:
        expected[track["stage_bam"]] = {
            "role": "bam",
            "identity": track["bam_identity"],
            "track": track,
        }
        expected[track["stage_bai"]] = {
            "role": "bai",
            "identity": track["bai_identity"],
            "track": track,
        }
    for role in ("definition", "fasta", "fai", "cytoband", "annotation"):
        resource = task["reference"]["resources"][role]
        expected[resource["stage_name"]] = {
            "role": f"reference_{role}",
            "identity": resource["identity"],
            "resource": resource,
        }
    if task["plot"]["state"] == "PRESENT":
        expected[task["plot"]["stage_pdf"]] = {
            "role": "violin_pdf",
            "identity": task["plot"]["pdf_identity"],
            "plot": task["plot"],
        }
    return expected


def _samtools_check(samtools: str, bam: Path) -> tuple[bool, str]:
    try:
        quickcheck = subprocess.run(
            [samtools, "quickcheck", "-v", str(bam)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
        idxstats = subprocess.run(
            [samtools, "idxstats", str(bam)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"samtools executable is unavailable: {samtools}") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"samtools validation timed out: {bam}") from exc
    if quickcheck.returncode != 0:
        return False, (quickcheck.stdout + quickcheck.stderr).strip() or "samtools quickcheck failed"
    if idxstats.returncode != 0 or not idxstats.stdout.strip():
        return False, (idxstats.stdout + idxstats.stderr).strip() or "samtools idxstats failed"
    return True, ""


def validate_case_inputs(
    task: dict[str, Any],
    input_map: dict[str, str],
    output_dir: str | Path,
    *,
    shard_id: str,
    session_id: str,
    attempt: int = 1,
    samtools: str = "samtools",
    samtools_checker: Callable[[str, Path], tuple[bool, str]] | None = None,
    schema_dir: str | Path | None = None,
) -> dict[str, Any]:
    validate_task_document(task, schema_dir=schema_dir)
    with StageBundle(
        output_dir,
        run_id=task["run_id"],
        generation_id=task["generation_id"],
        shard_id=shard_id,
        session_id=session_id,
        task_id=task["task_id"],
        manifest_order=task["manifest_order"],
        attempt=attempt,
        stage="VALIDATE_CASE_INPUTS",
        input_fingerprint=task["input_fingerprint"],
        schema_dir=schema_dir,
    ) as bundle:
        for warning in task["preflight_warnings"]:
            bundle.add_warning(warning["code"], warning["message"])
        if task["preflight_state"] == "CASE_INPUT_INVALID":
            for failure in task["preflight_errors"]:
                bundle.add_domain_failure(
                    failure["code"], failure["message"], rerun_eligible=False
                )
            return bundle.finish("DOMAIN_FAILED")

        expected = expected_stage_inputs(task)
        if set(input_map) != set(expected):
            missing = sorted(set(expected) - set(input_map))
            unexpected = sorted(set(input_map) - set(expected))
            raise RuntimeError(
                f"staged input map differs from task contract; missing={missing} unexpected={unexpected}"
            )
        resolved: dict[str, Path] = {}
        for stage_name, contract in expected.items():
            raw = Path(input_map[stage_name]).expanduser()
            try:
                path = raw.resolve(strict=True)
            except FileNotFoundError:
                bundle.add_domain_failure(
                    "STAGED_INPUT_MISSING", f"{stage_name}: {raw}", rerun_eligible=True
                )
                continue
            if not path.is_file() or not path.stat().st_mode & 0o444:
                bundle.add_domain_failure(
                    "STAGED_INPUT_UNREADABLE", f"{stage_name}: {path}", rerun_eligible=True
                )
                continue
            if any(character.isspace() for character in str(raw.resolve(strict=False))):
                raise RuntimeError(f"native task input path contains whitespace: {raw}")
            identity = contract["identity"]
            stat = path.stat()
            if stat.st_size != int(identity["size"]) or stat.st_mtime_ns != int(identity["mtime_ns"]):
                raise RuntimeError(f"input changed after canonical planning: {stage_name}")
            configured_sha = identity.get("sha256")
            if configured_sha and sha256_file(path) != configured_sha:
                raise RuntimeError(f"input checksum changed after canonical planning: {stage_name}")
            resolved[stage_name] = path

        if bundle.failures:
            return bundle.finish("DOMAIN_FAILED")
        if not task["tracks"]:
            bundle.add_domain_failure(
                "NO_ELIGIBLE_SAMPLES",
                "no eligible BAM tracks are available across genotype groups",
                rerun_eligible=False,
            )
            return bundle.finish("DOMAIN_FAILED")

        for track in task["tracks"]:
            bam = resolved[track["stage_bam"]]
            bai = resolved[track["stage_bai"]]
            if bai.stat().st_mtime_ns < bam.stat().st_mtime_ns:
                bundle.add_warning("BAI_OLDER_THAN_BAM", f"{track['sample_id']}: {bai}")
            compatible, detail = (samtools_checker or _samtools_check)(samtools, bam)
            if not compatible:
                bundle.add_domain_failure(
                    "BAI_INCOMPATIBLE_OR_UNREADABLE",
                    f"{track['sample_id']}: {detail}",
                    rerun_eligible=True,
                )

        definition = resolved[task["reference"]["resources"]["definition"]["stage_name"]]
        if resource_contains_remote_url(definition):
            raise RuntimeError(f"reference definition contains a remote URL: {definition}")
        plot = task["plot"]
        if plot["state"] != "PRESENT":
            bundle.add_domain_failure(
                "VIOLIN_MAPPING_INVALID", json.dumps(plot, sort_keys=True), rerun_eligible=False
            )
        elif sha256_file(resolved[plot["stage_pdf"]]) != plot["pdf_identity"].get("sha256"):
            raise RuntimeError("violin PDF checksum changed after canonical planning")

        return bundle.finish("DOMAIN_FAILED" if bundle.failures else "SUCCEEDED")
