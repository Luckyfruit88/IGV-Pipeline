from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .contracts import FIGURE_CONTRACT_ID, GUI_SETTLE_CONTRACT_ID
from .runner import assert_manifest_config, load_cases
from .state import RERUN, REVIEW_PENDING, CaseState
from .utils import atomic_write_json, safe_name, utc_now


REQUIRED_MANUAL_ASSERTIONS = (
    "native_igv_complete_and_readable",
    "annotation_track_and_model_visible",
    "strand_and_transcript_reviewed",
    "ag_site_and_reference_ag_context_reviewed",
    "splice_or_junction_presence_absence_judgeable",
    "violin_pair_matches",
)


def _state_path(root: Path, case_id: str) -> Path:
    return root / ".work" / "state" / f"{safe_name(case_id)}.json"


def _review_path(root: Path, case_id: str) -> Path:
    return root / ".work" / "reviews" / f"{safe_name(case_id)}.json"


def approved_review(root: Path, case: dict[str, Any], state: CaseState) -> dict[str, Any] | None:
    path = _review_path(root, case["case_id"])
    if not path.is_file():
        return None
    try:
        review = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    expected = {
        "case_id": case["case_id"],
        "decision": "approve",
        "input_fingerprint": state.input_fingerprint,
        "combined_sha256": state.artifacts.get("combined_sha256"),
        "scientific_qc_sha256": state.artifacts.get("scientific_qc_sha256"),
        "figure_contract_id": FIGURE_CONTRACT_ID,
        "gui_settle_contract_id": GUI_SETTLE_CONTRACT_ID,
        "left_pixel_sha256": state.artifacts.get("left_pixel_sha256"),
    }
    if path.is_symlink() or review.get("schema_version") != "portable-review-v1":
        return None
    if any(review.get(key) != value for key, value in expected.items()):
        return None
    assertions = review.get("manual_assertions", {})
    if any(assertions.get(key) is not True for key in REQUIRED_MANUAL_ASSERTIONS):
        return None
    if not str(review.get("reviewer", "")).strip() or not str(review.get("reviewed_at", "")).strip():
        return None
    return review


def record_reviews(
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    case_ids: set[str],
    decision: str,
    reviewer: str,
    notes: str = "",
    manual_assertions: dict[str, bool] | None = None,
) -> dict[str, Any]:
    root = config.validate_run_root(run_root, must_exist=True)
    cases = load_cases(root)
    assert_manifest_config(cases, config, root)
    if decision not in {"approve", "reject"}:
        raise ValueError("decision must be approve or reject")
    if not reviewer.strip():
        raise ValueError("reviewer is required")
    assertions = dict(manual_assertions or {})
    if decision == "approve":
        missing_assertions = [
            key for key in REQUIRED_MANUAL_ASSERTIONS if assertions.get(key) is not True
        ]
        if missing_assertions:
            raise ValueError(
                "approval requires every explicit visual assertion: "
                + ", ".join(missing_assertions)
            )
    known = {case["case_id"] for case in cases}
    if not case_ids or not case_ids.issubset(known):
        raise ValueError("case_ids must be a non-empty subset of the manifest")
    rows: list[dict[str, Any]] = []
    for case in cases:
        if case["case_id"] not in case_ids:
            continue
        state_path = _state_path(root, case["case_id"])
        if not state_path.is_file():
            raise ValueError(f"case state missing: {case['case_id']}")
        state = CaseState.load(state_path)
        if state.status != REVIEW_PENDING:
            raise ValueError(f"case is not REVIEW_PENDING: {case['case_id']}:{state.status}")
        if decision == "approve" and any(
            not state.artifacts.get(key)
            for key in (
                "combined_png",
                "combined_sha256",
                "sample_table",
                "sample_table_sha256",
                "scientific_qc_sha256",
                "left_pixel_sha256",
            )
        ):
            raise ValueError(f"case lacks approval-bound artifacts: {case['case_id']}")
        reviewed_at = utc_now()
        record = {
            "schema_version": "portable-review-v1",
            "case_id": case["case_id"],
            "decision": decision,
            "reviewer": reviewer.strip(),
            "reviewed_at": reviewed_at,
            "notes": notes,
            "input_fingerprint": state.input_fingerprint,
            "combined_sha256": state.artifacts.get("combined_sha256"),
            "scientific_qc_sha256": state.artifacts.get("scientific_qc_sha256"),
            "figure_contract_id": FIGURE_CONTRACT_ID,
            "gui_settle_contract_id": GUI_SETTLE_CONTRACT_ID,
            "left_pixel_sha256": state.artifacts.get("left_pixel_sha256"),
            "manual_assertions": {
                key: assertions.get(key) is True for key in REQUIRED_MANUAL_ASSERTIONS
            },
        }
        review_path = _review_path(root, case["case_id"])
        atomic_write_json(review_path, record)
        if decision == "reject":
            state.fail("manual_review", "MANUAL_REVIEW_REJECTED", notes or "reviewer rejected evidence figure")
            if state.status != RERUN:  # defensive contract check
                raise RuntimeError("manual rejection did not enter RERUN")
            state.save(state_path.parent)
        rows.append({"case_id": case["case_id"], "decision": decision, "review": str(review_path)})
    return {
        "run_id": root.name,
        "reviewed": len(rows),
        "decision": decision,
        "records": rows,
    }
