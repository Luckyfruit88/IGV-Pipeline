from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from .accounting import verify_local_accounting_receipt, verify_scc_accounting_receipt


def _object(path: Path, *, label: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must contain one JSON object: {path}")
    return value


def locate_verified_accounting(
    run_root: str | Path,
    canonical_tasks: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Resolve accounting from its frozen provider tree, never run_summary.json.

    The provider is fixed by the immutable run identity.  SCC has one
    generation-named request; local execution may retain provisional earlier
    generations, so the latest generation that re-verifies the exact canonical
    case set is selected.
    """

    root_value = Path(run_root).expanduser()
    if root_value.is_symlink() or not root_value.resolve(strict=True).is_dir():
        raise ValueError(f"run root must be a regular non-symlink directory: {root_value}")
    root = root_value.resolve(strict=True)
    identity = _object(root / "contract" / "run_identity.json", label="run identity")
    profile = str(identity.get("profile", ""))
    if profile == "scc":
        state = root / "accounting" / "scc" / str(identity.get("generation_id", ""))
        verified = verify_scc_accounting_receipt(state, expected_cases=canonical_tasks)
        provider = "sge_qacct"
    else:
        accounting_root = root / "accounting" / "local"
        candidates = (
            sorted(
                (path for path in accounting_root.glob("generation-*") if path.is_dir()),
                reverse=True,
            )
            if accounting_root.is_dir()
            else []
        )
        failures: list[str] = []
        verified = None
        state = None
        for candidate in candidates:
            try:
                observed = verify_local_accounting_receipt(
                    candidate,
                    expected_cases=canonical_tasks,
                    run_root=root,
                    frozen_run_root=identity.get("execution_run_root"),
                )
            except (OSError, ValueError) as exc:
                failures.append(f"{candidate.name}:{exc}")
                continue
            verified = observed
            state = candidate
            break
        if verified is None or state is None:
            detail = "; ".join(failures[:3])
            raise ValueError(
                "no local accounting generation verifies the exact canonical task set"
                + (f": {detail}" if detail else "")
            )
        provider = "nextflow_trace"
    try:
        relative = str(Path(verified["output_dir"]).resolve(strict=True).relative_to(root))
    except ValueError as exc:
        raise ValueError("verified accounting tree is outside the run root") from exc
    report = verified.get("report")
    receipt = verified.get("receipt")
    if (
        not isinstance(report, Mapping)
        or report.get("status") != "PASS"
        or not isinstance(receipt, Mapping)
        or receipt.get("status") != "PASS"
        or receipt.get("provider") != provider
    ):
        raise ValueError("verified accounting provider did not return a passing receipt")
    return {
        **verified,
        "provider": provider,
        "status": "PASS",
        "qacct_used": provider == "sge_qacct",
        "output_relative_path": relative,
    }
