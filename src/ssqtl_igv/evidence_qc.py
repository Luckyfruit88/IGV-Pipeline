from __future__ import annotations

from typing import Any

from .scientific_qc import scientific_qc


def evaluate_evidence(
    case: dict[str, Any],
    *,
    batch_text: str,
    capture: dict[str, Any],
    layout: dict[str, Any],
    violin_qc: dict[str, Any],
    final_png_qc: dict[str, Any],
    config: Any,
) -> dict[str, Any]:
    """Run automated identity checks without making a biological judgment."""

    result = scientific_qc(
        case,
        batch_text=batch_text,
        capture=capture,
        layout=layout,
        violin_qc=violin_qc,
        final_png_qc=final_png_qc,
        config=config,
    )
    result.pop("decision", None)
    result.update(
        {
            "automation_scope": "EVIDENCE_ONLY",
            "automatic_rerun": False,
            "control_action": (
                "MANUAL_REVIEW" if result["status"] == "PASS" else "MANUAL_TRIAGE"
            ),
        }
    )
    return result
