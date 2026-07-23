from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .desktop import DesktopResult


def require_test_task(task: dict[str, Any]) -> None:
    run_id = str(task.get("run_id", ""))
    if not run_id.startswith("test_"):
        raise ValueError("test doubles require a run_id beginning with test_")


def fake_samtools_check(_samtools: str, _bam: Path) -> tuple[bool, str]:
    return True, ""


def _pattern(path: Path, size: tuple[int, int], *, panel: bool = False) -> None:
    from PIL import Image, ImageDraw

    image = Image.new("RGB", size, "white")
    draw = ImageDraw.Draw(image)
    if panel:
        margin = max(10, min(size) // 12)
        draw.ellipse(
            (margin, margin, size[0] - margin, size[1] - margin),
            fill=(20, 45, 120),
        )
    else:
        draw.rectangle((0, 0, max(1, size[0] // 2), size[1]), fill="black")
        draw.line((0, 0, size[0] - 1, size[1] - 1), fill=(180, 30, 30), width=8)
    path.parent.mkdir(parents=True, exist_ok=True)
    image.save(path, format="PNG")


def fake_desktop_session(
    config: Any,
    *,
    output_png: str | Path,
    metadata_path: str | Path,
    log_directory: str | Path,
    **_kwargs: Any,
) -> DesktopResult:
    output = Path(output_png)
    width = int(config.get("desktop.screen_width", 1920))
    height = int(config.get("desktop.screen_height", 2160))
    _pattern(output, (width, height))
    metadata = {
        "test_double": True,
        "command_listener_enabled": bool(
            config.get("desktop.command_listener_enabled", True)
        ),
        "command_port": None,
        "locale": "C.UTF-8",
        "root_screenshot_publishable": False,
        "capture_mode": "window",
        "geometry_verified": True,
        "canvas": {"width": width, "height": height},
        "window": {"title": "IGV test double", "wm_class": "IGV-test-double"},
    }
    Path(metadata_path).write_text(json.dumps(metadata, sort_keys=True) + "\n", encoding="utf-8")
    logs = Path(log_directory)
    logs.mkdir(parents=True, exist_ok=True)
    stdout = logs / "igv.stdout.log"
    stderr = logs / "igv.stderr.log"
    stdout.write_text("test double\n", encoding="utf-8")
    stderr.write_text("", encoding="utf-8")
    now = time.time()
    return DesktopResult(
        screenshot=output,
        metadata=metadata,
        started_at_epoch=now,
        ended_at_epoch=now + 0.01,
        wall_time_seconds=0.01,
        peak_rss_gb=0.01,
        stdout_path=stdout,
        stderr_path=stderr,
    )


def fake_pdf_page(
    _pdf: str | Path,
    _page: int,
    output_png: str | Path,
    **_kwargs: Any,
) -> None:
    _pattern(Path(output_png), (900, 1200), panel=True)


def fake_evidence_evaluator(
    _case: dict[str, Any],
    *,
    batch_text: str,
    capture: dict[str, Any],
    layout: dict[str, Any],
    violin_qc: dict[str, Any],
    final_png_qc: dict[str, Any],
    config: Any,
) -> dict[str, Any]:
    del batch_text, capture, layout, violin_qc, final_png_qc, config
    return {
        "status": "PASS",
        "failed_codes": [],
        "automation_scope": "EVIDENCE_ONLY",
        "automatic_rerun": False,
        "control_action": "MANUAL_TRIAGE",
        "test_double": True,
    }
