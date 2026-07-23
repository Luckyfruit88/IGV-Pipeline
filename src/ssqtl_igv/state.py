from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .utils import atomic_write_json, safe_name, utc_now


PREPARED = "PREPARED"
IGV_STARTING = "IGV_STARTING"
WINDOW_CAPTURED = "WINDOW_CAPTURED"
GUI_SETTLED = "GUI_SETTLED"
RENDER_STABLE = "RENDER_STABLE"
COMPOSED = "COMPOSED"
QC_PASS = "QC_PASS"
REVIEW_PENDING = "REVIEW_PENDING"
PUBLISHED = "PUBLISHED"
FAILED = "FAILED"
RERUN = "RERUN"

TERMINAL_STATES = {REVIEW_PENDING, PUBLISHED, RERUN}
ALLOWED = {
    PREPARED: {IGV_STARTING, FAILED},
    IGV_STARTING: {WINDOW_CAPTURED, FAILED},
    WINDOW_CAPTURED: {GUI_SETTLED, FAILED},
    GUI_SETTLED: {RENDER_STABLE, FAILED},
    RENDER_STABLE: {COMPOSED, FAILED},
    COMPOSED: {QC_PASS, FAILED},
    QC_PASS: {REVIEW_PENDING, FAILED},
    REVIEW_PENDING: {PUBLISHED, RERUN, FAILED},
    PUBLISHED: {RERUN},
    FAILED: {RERUN},
    RERUN: {PREPARED, FAILED},
}

# Compatibility names remain importable for readers of old checkpoints, but
# the portable desktop runner never emits them.
PENDING = PREPARED
PREFLIGHT_OK = PREPARED
RENDERED = RENDER_STABLE
COMBINED = COMPOSED


@dataclass
class CaseState:
    case_id: str
    status: str
    input_fingerprint: str
    history: list[dict[str, Any]]
    artifacts: dict[str, Any]
    failure: dict[str, Any] | None = None

    @classmethod
    def fresh(cls, case_id: str, fingerprint: str) -> "CaseState":
        return cls(case_id, PREPARED, fingerprint, [{"status": PREPARED, "at": utc_now()}], {})

    @classmethod
    def load(cls, path: str | Path) -> "CaseState":
        import json

        value = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls(
            case_id=value["case_id"],
            status=value["status"],
            input_fingerprint=value["input_fingerprint"],
            history=value.get("history", []),
            artifacts=value.get("artifacts", {}),
            failure=value.get("failure"),
        )

    def transition(self, status: str, *, detail: dict[str, Any] | None = None) -> None:
        if status not in ALLOWED.get(self.status, set()):
            raise ValueError(f"invalid case transition {self.status} -> {status}")
        self.status = status
        event: dict[str, Any] = {"status": status, "at": utc_now()}
        if detail:
            event["detail"] = detail
        self.history.append(event)
        if status not in {FAILED, RERUN}:
            self.failure = None

    def fail(self, stage: str, code: str, message: str, *, queue_rerun: bool = True) -> None:
        detail = {"stage": stage, "code": code, "message": message}
        if self.status == FAILED:
            self.failure = detail
            self.history.append({"status": FAILED, "at": utc_now(), "detail": detail})
        else:
            self.transition(FAILED, detail=detail)
        self.failure = detail
        if queue_rerun:
            self.transition(RERUN, detail={"reason": "case failure", "failure": detail})
            self.failure = detail

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "status": self.status,
            "input_fingerprint": self.input_fingerprint,
            "history": self.history,
            "artifacts": self.artifacts,
            "failure": self.failure,
        }

    def save(self, directory: str | Path) -> Path:
        path = Path(directory) / f"{safe_name(self.case_id)}.json"
        atomic_write_json(path, self.to_dict())
        return path
