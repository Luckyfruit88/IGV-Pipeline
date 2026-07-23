from __future__ import annotations

from pathlib import Path
from typing import Any

from .utils import sha256_file


def inspect_png(
    path: str | Path,
    *,
    min_width: int = 600,
    min_height: int = 300,
    min_stddev: float = 0.5,
) -> dict[str, Any]:
    try:
        from PIL import Image, ImageStat
    except ModuleNotFoundError as exc:
        raise RuntimeError("Pillow is required for PNG QC; install requirements.lock") from exc
    source = Path(path)
    if not source.is_file():
        return {"status": "FAIL", "code": "PNG_MISSING", "path": str(source)}
    try:
        with Image.open(source) as image:
            image.verify()
        with Image.open(source) as image:
            image.load()
            width, height = image.size
            grayscale = image.convert("L")
            stddev = float(ImageStat.Stat(grayscale).stddev[0])
            extrema = grayscale.getextrema()
    except Exception as exc:
        return {"status": "FAIL", "code": "PNG_INVALID", "path": str(source), "message": str(exc)}
    code = "PASS"
    if width < min_width or height < min_height:
        code = "PNG_TOO_SMALL"
    elif stddev < min_stddev or extrema[0] == extrema[1]:
        code = "PNG_BLANK"
    return {
        "status": "PASS" if code == "PASS" else "FAIL",
        "code": code,
        "path": str(source),
        "width": width,
        "height": height,
        "stddev": round(stddev, 4),
        "sha256": sha256_file(source),
    }


def qc_rendered_panels(paths: dict[str, dict[str, Path]], config: Any) -> dict[str, Any]:
    results: dict[str, Any] = {}
    passed = True
    for genotype, views in paths.items():
        results[genotype] = {}
        for view, path in views.items():
            result = inspect_png(
                path,
                min_width=int(config.get("qc.raw_min_width", 1000)),
                min_height=int(config.get("qc.raw_min_height", 400)),
                min_stddev=float(config.get("qc.min_stddev", 0.5)),
            )
            results[genotype][view] = result
            passed = passed and result["status"] == "PASS"
    return {"status": "PASS" if passed else "FAIL", "panels": results}


def qc_shared_panels(paths: dict[str, Path], config: Any) -> dict[str, Any]:
    """QC the single shared overview and single shared detail snapshots."""

    results: dict[str, Any] = {}
    passed = True
    for view in ("overview", "detail"):
        path = paths.get(view)
        if path is None:
            result = {"status": "FAIL", "code": "PANEL_MISSING", "view": view}
        else:
            result = inspect_png(
                path,
                min_width=int(config.get("qc.raw_min_width", 1000)),
                min_height=int(
                    config.get(
                        f"qc.raw_{view}_min_height",
                        config.get("qc.raw_min_height", 400),
                    )
                ),
                min_stddev=float(config.get("qc.min_stddev", 0.5)),
            )
        results[view] = result
        passed = passed and result["status"] == "PASS"
    return {"status": "PASS" if passed else "FAIL", "panels": results}
