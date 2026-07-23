from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any

from .contracts import DEFAULT_CLIENT_HEIGHT, DEFAULT_CLIENT_WIDTH, DESKTOP_LAYOUT_SCHEMA, FIGURE_CONTRACT_ID
from .utils import atomic_write_json


def _atomic_save_png(image: Any, destination: str | Path) -> Path:
    output = Path(destination)
    output.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{output.name}.", suffix=".png", dir=output.parent)
    os.close(fd)
    try:
        image.save(temporary, format="PNG", compress_level=6, optimize=False)
        os.replace(temporary, output)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise
    return output


def _fit_contain(canvas: Any, image: Any, box: tuple[int, int, int, int]) -> list[int]:
    from PIL import Image

    x, y, width, height = box
    ratio = min(width / image.width, height / image.height)
    target = (max(1, round(image.width * ratio)), max(1, round(image.height * ratio)))
    resized = image.resize(target, Image.Resampling.LANCZOS)
    paste_x = x + (width - target[0]) // 2
    paste_y = y + (height - target[1]) // 2
    canvas.paste(resized, (paste_x, paste_y))
    return [paste_x, paste_y, target[0], target[1]]


def _pixel_sha256(image: Any) -> str:
    digest = hashlib.sha256()
    digest.update(f"{image.mode}:{image.width}x{image.height}\0".encode("ascii"))
    digest.update(image.tobytes())
    return digest.hexdigest()


def compose_desktop_case(
    case: dict[str, Any],
    client_png: str | Path,
    violin_png: str | Path,
    output_png: str | Path,
    layout_json: str | Path,
    capture_metadata: dict[str, Any],
    config: Any,
) -> dict[str, Any]:
    """Append a violin page without modifying a single IGV client pixel."""

    try:
        from PIL import Image, ImageChops
    except ModuleNotFoundError as exc:
        raise RuntimeError("Pillow is required for image composition; install requirements.lock") from exc

    if capture_metadata.get("root_screenshot_publishable") is not False:
        raise ValueError("root screenshots cannot enter the desktop compositor")
    capture_mode = capture_metadata.get("capture_mode")
    if capture_mode not in {"window", "root_fallback_crop"}:
        raise ValueError(f"unverified desktop capture mode: {capture_mode!r}")
    if capture_mode == "root_fallback_crop" and capture_metadata.get("geometry_verified") is not True:
        raise ValueError("root fallback crop lacks verified client geometry")

    with Image.open(client_png) as source:
        source.load()
        if source.mode != "RGB":
            raise ValueError(f"native IGV capture must be RGB, observed {source.mode}")
        client = source.copy()
    source_client_size = [client.width, client.height]
    client_width = int(config.get("desktop.screen_width", DEFAULT_CLIENT_WIDTH))
    client_height = int(config.get("desktop.screen_height", DEFAULT_CLIENT_HEIGHT))
    if source_client_size != [client_width, client_height]:
        raise ValueError(
            f"native IGV capture must be {client_width}x{client_height}, observed "
            f"{client.width}x{client.height}"
        )
    with Image.open(violin_png) as source:
        source.load()
        violin = source.convert("RGB")
    source_violin_size = [violin.width, violin.height]

    violin_width = int(config.get("compose.violin_panel_width", 720))
    if violin_width < 300:
        raise ValueError("compose.violin_panel_width must be at least 300 pixels")
    width = client_width + violin_width
    height = client_height
    canvas = Image.new("RGB", (width, height), "white")
    canvas.paste(client, (0, 0))
    violin_box = (client_width, 0, violin_width, client_height)
    violin_actual = _fit_contain(canvas, violin, violin_box)

    left = canvas.crop((0, 0, client_width, client_height))
    source_pixel_sha = _pixel_sha256(client)
    left_pixel_sha = _pixel_sha256(left)
    left_pixel_identity = (
        source_pixel_sha == left_pixel_sha
        and ImageChops.difference(client, left).getbbox() is None
    )
    if not left_pixel_identity:
        raise ValueError("final left region is not pixel-identical to native IGV capture")

    layout: dict[str, Any] = {
        "schema_version": DESKTOP_LAYOUT_SCHEMA,
        "figure_contract_id": FIGURE_CONTRACT_ID,
        "case_id": case["case_id"],
        "canvas": [width, height],
        "metadata_lines": [],
        "dosage_labels": [],
        "panels": {
            "igv_client": {
                "box": [0, 0, client_width, client_height],
                "source_size": source_client_size,
                "capture_mode": capture_mode,
                "full_client": True,
                "crop_applied": False,
                "resize_count": 0,
                "pixel_sha256": source_pixel_sha,
            },
            "violin": {
                "box": list(violin_box),
                "actual": violin_actual,
                "source_size": source_violin_size,
                "match_key": case.get("violin", {}).get("match_key"),
            },
        },
        "evidence": {
            "figure_contract_id": FIGURE_CONTRACT_ID,
            "native_igv_gui_preserved": True,
            "client_resize_count": 0,
            "client_crop_applied": False,
            "client_post_capture_overlays": 0,
            "external_header_present": False,
            "divider_present": False,
            "dosage_badges_present": False,
            "root_screenshot_publishable": False,
            "capture_mode": capture_mode,
            "geometry_verified": capture_metadata.get("geometry_verified") is True,
            "left_origin": [0, 0],
            "left_size": [client_width, client_height],
            "source_client_pixel_sha256": source_pixel_sha,
            "final_left_pixel_sha256": left_pixel_sha,
            "left_pixel_identity": left_pixel_identity,
            "final_png_width": width,
            "final_png_height": height,
            "violin_resize_count": 1,
        },
        "outputs": {"png": str(Path(output_png))},
    }
    _atomic_save_png(canvas, output_png)
    atomic_write_json(layout_json, layout)
    return layout
