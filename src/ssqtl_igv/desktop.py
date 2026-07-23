from __future__ import annotations

import base64
import fcntl
import os
import re
import shutil
import signal
import subprocess
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

from .config import WorkflowConfig
from .contracts import (
    DESKTOP_CAPTURE_SCHEMA,
    GUI_SETTLE_CONTRACT_ID,
    IGV_LOCAL_ONLY_STARTUP_CONTRACT_ID,
    DEFAULT_LOCUS_FIELD_OCR_PSM,
    DEFAULT_LOCUS_FIELD_OCR_SCALE,
    DEFAULT_LOCUS_FIELD_OCR_WHITELIST,
    DEFAULT_LOCUS_FIELD_ROI,
    LOCUS_OCR_CONTRACT_ID,
    LOCUS_OCR_RESAMPLING,
)
from .utils import atomic_write_json, atomic_write_text, command_prefix, sha256_file, utc_now


_LOCUS_CLIPBOARD_PROBE_JAVA = r"""
import java.awt.Robot;
import java.awt.Toolkit;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.StringSelection;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class LocusClipboardProbe {
    public static void main(String[] args) throws Exception {
        int x = Integer.parseInt(args[0]);
        int y = Integer.parseInt(args[1]);
        Toolkit toolkit = Toolkit.getDefaultToolkit();
        toolkit.getSystemClipboard().setContents(
            new StringSelection("__SSQTL_LOCUS_COPY_NOT_OBSERVED__"), null
        );
        Robot robot = new Robot();
        robot.setAutoDelay(60);
        robot.mouseMove(x, y);
        robot.mousePress(InputEvent.BUTTON1_MASK);
        robot.mouseRelease(InputEvent.BUTTON1_MASK);
        robot.keyPress(KeyEvent.VK_CONTROL);
        robot.keyPress(KeyEvent.VK_A);
        robot.keyRelease(KeyEvent.VK_A);
        robot.keyRelease(KeyEvent.VK_CONTROL);
        robot.keyPress(KeyEvent.VK_CONTROL);
        robot.keyPress(KeyEvent.VK_C);
        robot.keyRelease(KeyEvent.VK_C);
        robot.keyRelease(KeyEvent.VK_CONTROL);
        Thread.sleep(300L);
        Object value = toolkit.getSystemClipboard().getData(DataFlavor.stringFlavor);
        robot.keyPress(KeyEvent.VK_ESCAPE);
        robot.keyRelease(KeyEvent.VK_ESCAPE);
        robot.keyPress(KeyEvent.VK_TAB);
        robot.keyRelease(KeyEvent.VK_TAB);
        String text = value == null ? "" : value.toString();
        System.out.print(Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8)));
    }
}
""".strip()


class DesktopFailure(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class WindowGeometry:
    window_id: str
    title: str
    wm_class: str
    x: int
    y: int
    width: int
    height: int


@dataclass(frozen=True)
class DesktopResult:
    screenshot: Path
    metadata: dict[str, Any]
    started_at_epoch: float
    ended_at_epoch: float
    wall_time_seconds: float
    peak_rss_gb: float
    stdout_path: Path
    stderr_path: Path


def _command(value: Any, default: str) -> list[str]:
    return command_prefix(value, default=default)


def command_port_for_display(config: WorkflowConfig, display: str) -> int:
    match = re.fullmatch(r":(\d+)(?:\.\d+)?", display)
    if not match:
        raise ValueError(f"invalid X display: {display!r}")
    base = int(config.get("desktop.command_port_base", 61000))
    port = base + int(match.group(1))
    if port < 1024 or port > 65535:
        raise ValueError(f"IGV command port is outside 1024..65535: {port}")
    return port


def _igv_command_listener_args(config: WorkflowConfig, display: str) -> list[str]:
    enabled = config.get("desktop.command_listener_enabled", True)
    if not isinstance(enabled, bool):
        raise ValueError("desktop.command_listener_enabled must be a boolean")
    if not enabled:
        return []
    return ["--port", str(command_port_for_display(config, display))]


def parse_xwininfo_tree(text: str, title_pattern: str = r"\bIGV\b") -> list[dict[str, Any]]:
    """Parse top-level candidates from ``xwininfo -root -tree`` output."""

    pattern = re.compile(title_pattern, re.IGNORECASE)
    geometry = re.compile(
        r'^\s*(0x[0-9a-fA-F]+)\s+"([^"]*)".*?\s(\d+)x(\d+)\+(-?\d+)\+(-?\d+)(?:\s|$)'
    )
    candidates: list[dict[str, Any]] = []
    for line in text.splitlines():
        match = geometry.search(line)
        if not match or not pattern.search(match.group(2)):
            continue
        candidates.append(
            {
                "window_id": match.group(1),
                "title": match.group(2),
                "width": int(match.group(3)),
                "height": int(match.group(4)),
                "x": int(match.group(5)),
                "y": int(match.group(6)),
            }
        )
    return candidates


def parse_xwininfo_geometry(text: str) -> tuple[int, int, int, int]:
    values: dict[str, int] = {}
    fields = {
        "x": r"Absolute upper-left X:\s*(-?\d+)",
        "y": r"Absolute upper-left Y:\s*(-?\d+)",
        "width": r"Width:\s*(\d+)",
        "height": r"Height:\s*(\d+)",
    }
    for name, expression in fields.items():
        match = re.search(expression, text)
        if not match:
            raise ValueError(f"xwininfo output lacks {name}")
        values[name] = int(match.group(1))
    return values["x"], values["y"], values["width"], values["height"]


def pixel_difference(
    previous: str | Path,
    current: str | Path,
    region: tuple[int, int, int, int] | None = None,
) -> dict[str, float]:
    try:
        from PIL import Image, ImageChops, ImageStat
    except ModuleNotFoundError as exc:  # pragma: no cover - preflight owns this failure
        raise RuntimeError("Pillow is required for pixel-stability checks") from exc
    with Image.open(previous) as left_source, Image.open(current) as right_source:
        left = left_source.convert("RGB")
        right = right_source.convert("RGB")
        if left.size != right.size:
            return {"mean_absolute_fraction": 1.0, "changed_pixel_fraction": 1.0}
        if region is not None:
            x, y, width, height = region
            if x < 0 or y < 0 or width < 1 or height < 1:
                raise ValueError(f"invalid pixel-difference region: {region}")
            box = (x, y, x + width, y + height)
            if box[2] > left.width or box[3] > left.height:
                raise ValueError(f"pixel-difference region {region} is outside image {left.size}")
            left = left.crop(box)
            right = right.crop(box)
        difference = ImageChops.difference(left, right)
        means = ImageStat.Stat(difference).mean
        mean_absolute_fraction = sum(means) / (len(means) * 255.0)
        grayscale = difference.convert("L")
        histogram = grayscale.histogram()
        changed = sum(histogram[1:])
        total = max(1, left.width * left.height)
        return {
            "mean_absolute_fraction": mean_absolute_fraction,
            "changed_pixel_fraction": changed / total,
        }


def normalize_locus_text(value: str) -> str:
    """Normalize commas, dash variants, and OCR whitespace without weakening identity."""

    normalized = value.lower().replace("−", "-").replace("–", "-").replace("—", "-")
    # Preserve the chromosome/coordinate separators.  Removing ':' and '-'
    # would make distinct loci such as chr1:12-34 and chr11:2-34 collide.
    return re.sub(r"[\s,]+", "", normalized)


def _toolbar_locus_roi(config: WorkflowConfig, geometry: WindowGeometry) -> dict[str, int]:
    configured = config.get(
        "desktop.toolbar_locus_roi",
        {"x": 0, "y": 0, "width": geometry.width, "height": 120},
    )
    if not isinstance(configured, dict):
        raise DesktopFailure("TOOLBAR_LOCUS_REGION_INVALID", repr(configured))
    try:
        roi = {key: int(configured[key]) for key in ("x", "y", "width", "height")}
    except (KeyError, TypeError, ValueError) as exc:
        raise DesktopFailure("TOOLBAR_LOCUS_REGION_INVALID", repr(configured)) from exc
    if (
        roi["x"] < 0
        or roi["y"] < 0
        or roi["width"] < 1
        or roi["height"] < 1
        or roi["x"] + roi["width"] > geometry.width
        or roi["y"] + roi["height"] > geometry.height
    ):
        raise DesktopFailure(
            "TOOLBAR_LOCUS_REGION_INVALID",
            f"ROI {roi} is outside verified window {geometry.width}x{geometry.height}",
        )
    return roi


def _locus_field_roi(config: WorkflowConfig, geometry: WindowGeometry) -> dict[str, int]:
    configured = config.get("desktop.locus_field_roi", DEFAULT_LOCUS_FIELD_ROI)
    if not isinstance(configured, dict):
        raise DesktopFailure("LOCUS_FIELD_REGION_INVALID", repr(configured))
    try:
        roi = {key: int(configured[key]) for key in ("x", "y", "width", "height")}
    except (KeyError, TypeError, ValueError) as exc:
        raise DesktopFailure("LOCUS_FIELD_REGION_INVALID", repr(configured)) from exc
    if (
        roi["x"] < 0
        or roi["y"] < 0
        or roi["width"] < 1
        or roi["height"] < 1
        or roi["x"] + roi["width"] > geometry.width
        or roi["y"] + roi["height"] > geometry.height
    ):
        raise DesktopFailure(
            "LOCUS_FIELD_REGION_INVALID",
            f"ROI {roi} is outside verified window {geometry.width}x{geometry.height}",
        )
    return roi


def _write_roi_png(source: Path, destination: Path, roi: dict[str, int]) -> Path:
    try:
        from PIL import Image
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Pillow is required for toolbar/locus ROI checks") from exc
    destination.parent.mkdir(parents=True, exist_ok=True)
    box = (
        roi["x"],
        roi["y"],
        roi["x"] + roi["width"],
        roi["y"] + roi["height"],
    )
    with Image.open(source) as image:
        image.convert("RGB").crop(box).save(destination, format="PNG")
    return destination


def _probe_locus_control_text(
    *,
    evidence_dir: Path,
    screen_point: tuple[int, int],
    env: dict[str, str] | None,
) -> dict[str, Any]:
    """Read the complete native JTextField value when its pixels are clipped.

    The probe performs Ctrl+A/C only after the immutable screenshot and OCR
    evidence have been captured.  It never edits the field or the screenshot.
    """

    probe_dir = evidence_dir / "native_control_probe"
    probe_dir.mkdir(parents=True, exist_ok=True)
    source = probe_dir / "LocusClipboardProbe.java"
    atomic_write_text(source, _LOCUS_CLIPBOARD_PROBE_JAVA + "\n")
    runtime_env = {**os.environ, **(env or {})}
    javac = shutil.which("javac", path=runtime_env.get("PATH"))
    java = shutil.which("java", path=runtime_env.get("PATH"))
    if not javac or not java:
        return {
            "status": "UNAVAILABLE",
            "method": "java_awt_robot_clipboard_exact",
            "message": "java/javac unavailable in the desktop runtime",
        }
    compiled = _run(
        [javac, str(source)],
        env=runtime_env,
        timeout=30,
    )
    if compiled.returncode != 0:
        return {
            "status": "ERROR",
            "method": "java_awt_robot_clipboard_exact",
            "message": compiled.stderr.strip() or compiled.stdout.strip(),
        }
    completed = _run(
        [
            java,
            "-Djava.awt.headless=false",
            "-cp",
            str(probe_dir),
            "LocusClipboardProbe",
            str(int(screen_point[0])),
            str(int(screen_point[1])),
        ],
        env=runtime_env,
        timeout=30,
    )
    if completed.returncode != 0:
        return {
            "status": "ERROR",
            "method": "java_awt_robot_clipboard_exact",
            "message": completed.stderr.strip() or completed.stdout.strip(),
        }
    try:
        observed = base64.b64decode(completed.stdout.strip(), validate=True).decode("utf-8")
    except Exception as exc:
        return {
            "status": "ERROR",
            "method": "java_awt_robot_clipboard_exact",
            "message": f"invalid base64 probe output: {type(exc).__name__}: {exc}",
        }
    return {
        "status": "OBSERVED",
        "method": "java_awt_robot_clipboard_exact",
        "screen_point": {"x": int(screen_point[0]), "y": int(screen_point[1])},
        "observed_text": observed[:4000],
        "observed_normalized": normalize_locus_text(observed)[:8000],
    }


def verify_expected_locus_text(
    config: WorkflowConfig,
    *,
    frame: str | Path,
    roi: dict[str, int],
    locus_roi: dict[str, int] | None = None,
    expected_locus: str,
    evidence_path: str | Path,
    env: dict[str, str] | None = None,
    control_screen_point: tuple[int, int] | None = None,
) -> dict[str, Any]:
    """Require the exact locus from a dedicated crop of the native locus field."""

    source = Path(frame)
    toolbar_roi_path = _write_roi_png(source, Path(evidence_path), roi)
    field_roi = locus_roi or roi
    locus_roi_path = toolbar_roi_path.with_name(f"{toolbar_roi_path.stem}.locus_native.png")
    _write_roi_png(source, locus_roi_path, field_roi)
    scale = int(config.get("desktop.locus_field_ocr_scale", DEFAULT_LOCUS_FIELD_OCR_SCALE))
    ocr_input_path = toolbar_roi_path.with_name(f"{toolbar_roi_path.stem}.locus_ocr.png")
    try:
        from PIL import Image
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Pillow is required for locus-field OCR") from exc
    with Image.open(locus_roi_path) as image:
        rgb = image.convert("RGB")
        rgb.resize(
            (rgb.width * scale, rgb.height * scale),
            getattr(Image.Resampling, LOCUS_OCR_RESAMPLING),
        ).save(ocr_input_path, format="PNG")
    tesseract = _command(config.get("binaries.tesseract"), "tesseract")
    psm = int(config.get("desktop.locus_field_ocr_psm", DEFAULT_LOCUS_FIELD_OCR_PSM))
    language = str(config.get("desktop.locus_ocr_language", "eng"))
    whitelist = str(
        config.get("desktop.locus_field_ocr_whitelist", DEFAULT_LOCUS_FIELD_OCR_WHITELIST)
    )
    completed = _run(
        tesseract
        + [
            str(ocr_input_path),
            "stdout",
            "--psm",
            str(psm),
            "-l",
            language,
            "-c",
            f"tessedit_char_whitelist={whitelist}",
        ],
        env={**os.environ, **(env or {})},
        timeout=float(config.get("timeouts.locus_ocr_seconds", 60)),
    )
    if completed.returncode != 0:
        raise DesktopFailure(
            "TESSERACT_FAILED",
            completed.stderr.strip() or completed.stdout.strip() or str(ocr_input_path),
        )
    observed = completed.stdout.strip()
    expected_normalized = normalize_locus_text(expected_locus)
    observed_normalized = normalize_locus_text(observed)
    matched = expected_normalized == observed_normalized
    result = {
        "status": "PASS" if matched else "FAIL",
        "contract_id": LOCUS_OCR_CONTRACT_ID,
        "engine": "tesseract",
        "psm": psm,
        "language": language,
        "whitelist": whitelist,
        "scale": scale,
        "resampling": LOCUS_OCR_RESAMPLING,
        "match_mode": "exact_normalized",
        "toolbar_roi_png": str(toolbar_roi_path),
        "roi": field_roi,
        "roi_png": str(locus_roi_path),
        "ocr_input_png": str(ocr_input_path),
        "expected_locus": expected_locus,
        "expected_normalized": expected_normalized,
        "observed_text": observed[:4000],
        "observed_normalized": observed_normalized[:8000],
        "matched": matched,
    }
    if not matched and control_screen_point is not None:
        fallback = _probe_locus_control_text(
            evidence_dir=toolbar_roi_path.parent,
            screen_point=control_screen_point,
            env=env,
        )
        fallback_matched = (
            fallback.get("status") == "OBSERVED"
            and fallback.get("observed_normalized") == expected_normalized
        )
        fallback["expected_normalized"] = expected_normalized
        fallback["matched"] = fallback_matched
        result["native_control_fallback"] = fallback
        if fallback_matched:
            result.update(
                {
                    "status": "PASS",
                    "matched": True,
                    "verification_mode": "native_control_clipboard_exact",
                    "ocr_matched": False,
                    "ocr_observed_text": observed[:4000],
                    "ocr_observed_normalized": observed_normalized[:8000],
                    "observed_text": fallback["observed_text"],
                    "observed_normalized": fallback["observed_normalized"],
                }
            )
    return result


def crop_verified_root(
    root_png: str | Path,
    output_png: str | Path,
    geometry: WindowGeometry,
) -> Path:
    try:
        from PIL import Image
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("Pillow is required for fallback cropping") from exc
    source = Path(root_png)
    output = Path(output_png)
    with Image.open(source) as image:
        left, top = geometry.x, geometry.y
        right = left + geometry.width
        bottom = top + geometry.height
        if left < 0 or top < 0 or right > image.width or bottom > image.height:
            raise DesktopFailure(
                "ROOT_FALLBACK_GEOMETRY_INVALID",
                f"verified window box {(left, top, right, bottom)} is outside root canvas {image.size}",
            )
        cropped = image.convert("RGB").crop((left, top, right, bottom))
        output.parent.mkdir(parents=True, exist_ok=True)
        cropped.save(output, format="PNG")
    return output


def _frame_is_valid(path: Path, minimum_width: int, minimum_height: int) -> bool:
    try:
        from PIL import Image, ImageStat

        with Image.open(path) as image:
            image.load()
            if image.width < minimum_width or image.height < minimum_height:
                return False
            return float(ImageStat.Stat(image.convert("L")).stddev[0]) >= 0.5
    except Exception:
        return False


def _run(
    argv: list[str],
    *,
    env: dict[str, str],
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        timeout=timeout,
    )


def _process_tree_rss_kb(root_pids: set[int]) -> int:
    if not root_pids:
        return 0
    try:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,rss="],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 0
    rows: dict[int, tuple[int, int]] = {}
    for line in completed.stdout.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        try:
            pid, parent, rss = map(int, parts)
        except ValueError:
            continue
        rows[pid] = (parent, rss)
    selected = set(root_pids)
    changed = True
    while changed:
        changed = False
        for pid, (parent, _rss) in rows.items():
            if parent in selected and pid not in selected:
                selected.add(pid)
                changed = True
    return sum(rows.get(pid, (0, 0))[1] for pid in selected)


class _RSSMonitor:
    def __init__(self) -> None:
        self.pids: set[int] = set()
        self.peak_kb = 0

    def add(self, process: subprocess.Popen[Any]) -> None:
        self.pids.add(process.pid)
        self.sample()

    def sample(self) -> None:
        self.peak_kb = max(self.peak_kb, _process_tree_rss_kb(self.pids))


@contextmanager
def reserve_display(config: WorkflowConfig) -> Iterator[str]:
    minimum = int(config.get("desktop.display_min", 90))
    maximum = int(config.get("desktop.display_max", 199))
    if minimum < 1 or maximum < minimum:
        raise ValueError("invalid desktop display range")
    for number in range(minimum, maximum + 1):
        lock_path = Path(f"/tmp/ssqtl_igv_display_{number}.lock")
        handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            continue
        socket_path = Path(f"/tmp/.X11-unix/X{number}")
        legacy_lock = Path(f"/tmp/.X{number}-lock")
        if socket_path.exists() or legacy_lock.exists():
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            continue
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} at={utc_now()}\n")
        handle.flush()
        try:
            yield f":{number}"
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
        return
    raise DesktopFailure("DISPLAY_EXHAUSTED", f"no free X display in {minimum}-{maximum}")


def _terminate_group(process: subprocess.Popen[Any] | None, grace_seconds: float = 5.0) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        process.wait(timeout=grace_seconds)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            pass


def _wait_x_server(
    config: WorkflowConfig,
    display: str,
    process: subprocess.Popen[Any],
    env: dict[str, str],
    monitor: _RSSMonitor,
) -> dict[str, int]:
    xwininfo = _command(config.get("binaries.xwininfo"), "xwininfo")
    deadline = time.monotonic() + float(config.get("timeouts.xvfb_start_seconds", 30))
    last_error = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise DesktopFailure("XVFB_EXITED", f"Xvfb exited with {process.returncode}: {last_error}")
        try:
            completed = _run(xwininfo + ["-display", display, "-root"], env=env, timeout=3)
        except (OSError, subprocess.TimeoutExpired) as exc:
            last_error = str(exc)
        else:
            if completed.returncode == 0:
                x, y, width, height = parse_xwininfo_geometry(completed.stdout)
                expected = (
                    int(config.get("desktop.screen_width", 1920)),
                    int(config.get("desktop.screen_height", 2160)),
                )
                if (width, height) != expected or (x, y) != (0, 0):
                    raise DesktopFailure(
                        "XVFB_CANVAS_MISMATCH",
                        f"expected root 0,0 {expected[0]}x{expected[1]}, got {x},{y} {width}x{height}",
                    )
                return {"x": x, "y": y, "width": width, "height": height}
            last_error = completed.stderr.strip()
        monitor.sample()
        time.sleep(0.25)
    raise DesktopFailure("XVFB_START_TIMEOUT", last_error or display)


def _discover_window(
    config: WorkflowConfig,
    display: str,
    process: subprocess.Popen[Any],
    env: dict[str, str],
    monitor: _RSSMonitor,
) -> WindowGeometry:
    xwininfo = _command(config.get("binaries.xwininfo"), "xwininfo")
    xprop = _command(config.get("binaries.xprop"), "xprop")
    title_pattern = str(config.get("desktop.window_title_regex", r"\bIGV\b"))
    title_regex = re.compile(title_pattern, re.IGNORECASE)
    minimum_width = int(config.get("desktop.minimum_window_width", 1700))
    minimum_height = int(config.get("desktop.minimum_window_height", 1800))
    deadline = time.monotonic() + float(config.get("timeouts.window_seconds", 180))
    last_tree = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise DesktopFailure("IGV_EXIT_BEFORE_WINDOW", f"IGV exited with {process.returncode}")
        try:
            tree = _run(xwininfo + ["-display", display, "-root", "-tree"], env=env, timeout=5)
        except (OSError, subprocess.TimeoutExpired) as exc:
            last_tree = str(exc)
        else:
            last_tree = tree.stderr.strip() or tree.stdout[-2000:]
            candidates = sorted(
                parse_xwininfo_tree(tree.stdout, title_pattern),
                key=lambda item: item["width"] * item["height"],
                reverse=True,
            )
            for candidate in candidates:
                if candidate["width"] < minimum_width or candidate["height"] < minimum_height:
                    continue
                properties = _run(
                    xprop
                    + [
                        "-display",
                        display,
                        "-id",
                        candidate["window_id"],
                        "WM_CLASS",
                        "WM_NAME",
                        "_NET_WM_NAME",
                    ],
                    env=env,
                    timeout=5,
                )
                combined = f"{candidate['title']}\n{properties.stdout}"
                if properties.returncode != 0 or not title_regex.search(combined):
                    continue
                exact = _run(
                    xwininfo + ["-display", display, "-id", candidate["window_id"]],
                    env=env,
                    timeout=5,
                )
                if exact.returncode != 0:
                    continue
                x, y, width, height = parse_xwininfo_geometry(exact.stdout)
                if width < minimum_width or height < minimum_height:
                    continue
                return WindowGeometry(
                    window_id=candidate["window_id"],
                    title=candidate["title"],
                    wm_class=properties.stdout.strip(),
                    x=x,
                    y=y,
                    width=width,
                    height=height,
                )
        monitor.sample()
        time.sleep(0.5)
    raise DesktopFailure("IGV_WINDOW_NOT_FOUND", last_tree or display)


def _capture_frame(
    config: WorkflowConfig,
    display: str,
    geometry: WindowGeometry,
    destination: Path,
    root_destination: Path,
    env: dict[str, str],
) -> tuple[str, str | None]:
    capture = _command(config.get("binaries.import"), "import")
    destination.parent.mkdir(parents=True, exist_ok=True)
    window = _run(
        capture + ["-display", display, "-window", geometry.window_id, str(destination)],
        env=env,
        timeout=float(config.get("timeouts.capture_seconds", 60)),
    )
    if window.returncode == 0 and _frame_is_valid(
        destination,
        int(config.get("desktop.minimum_window_width", 1700)),
        int(config.get("desktop.minimum_window_height", 1800)),
    ):
        return "window", None
    if not bool(config.get("desktop.root_fallback_enabled", True)):
        raise DesktopFailure("WINDOW_CAPTURE_FAILED", window.stderr.strip() or str(destination))
    root_destination.parent.mkdir(parents=True, exist_ok=True)
    root = _run(
        capture + ["-display", display, "-window", "root", str(root_destination)],
        env=env,
        timeout=float(config.get("timeouts.capture_seconds", 60)),
    )
    if root.returncode != 0 or not root_destination.is_file():
        raise DesktopFailure(
            "ROOT_FALLBACK_CAPTURE_FAILED",
            root.stderr.strip() or window.stderr.strip() or str(root_destination),
        )
    crop_verified_root(root_destination, destination, geometry)
    if not _frame_is_valid(
        destination,
        int(config.get("desktop.minimum_window_width", 1700)),
        int(config.get("desktop.minimum_window_height", 1800)),
    ):
        raise DesktopFailure("ROOT_FALLBACK_CROP_INVALID", str(destination))
    return "root_fallback_crop", str(root_destination)


def _stable_chain_frame_names(
    comparisons: list[dict[str, Any]],
    required_pairs: int,
) -> list[str]:
    tail = comparisons[-required_pairs:]
    if len(tail) != required_pairs or any(item.get("stable") is not True for item in tail):
        raise ValueError("cannot retain an incomplete stability chain")
    return [str(tail[0]["from"]), *[str(item["to"]) for item in tail]]


def _prune_stability_evidence(
    stability_root: Path,
    retained_frames: int,
    *,
    successful_chains: dict[str, list[str]] | None = None,
    timeout: bool = False,
) -> dict[str, Any]:
    """Keep bounded, audit-complete GUI/render chains or timeout evidence."""

    frames = sorted(stability_root.glob("frame_*.png")) if stability_root.is_dir() else []
    if successful_chains:
        names = {name for chain in successful_chains.values() for name in chain}
        keep = {path for path in frames if path.name in names}
        missing = sorted(names - {path.name for path in keep})
        if missing:
            raise ValueError("stability retention references missing frames: " + ", ".join(missing))
    elif timeout and frames:
        keep = {frames[0], *frames[-retained_frames:]}
    else:
        keep = set(frames[-retained_frames:])
    removed_frames = 0
    for path in frames:
        if path not in keep:
            path.unlink(missing_ok=True)
            removed_frames += 1
    removed_roots = 0
    transient_roots = (
        [
            path
            for path in stability_root.glob("root_*.png")
            if re.fullmatch(r"root_\d{3}\.png", path.name)
        ]
        if stability_root.is_dir()
        else []
    )
    for path in transient_roots:
        path.unlink(missing_ok=True)
        removed_roots += 1
    return {
        "retained_per_chain": retained_frames,
        "mode": "success_chains" if successful_chains else "timeout" if timeout else "tail",
        "successful_chains": successful_chains or {},
        "retained_frames": [path.name for path in sorted(keep)],
        "removed_frames": removed_frames,
        "removed_transient_roots": removed_roots,
    }


def _retain_root_evidence(source: str | Path, destination: Path) -> str:
    """Atomically make the final retained root image correspond to the current crop."""

    source_path = Path(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source_path.resolve(strict=False) != destination.resolve(strict=False):
        os.replace(source_path, destination)
    if not destination.is_file():
        raise DesktopFailure("ROOT_FALLBACK_EVIDENCE_MISSING", str(destination))
    return str(destination)


def _verify_final_root_evidence(
    root_evidence: Path,
    output: Path,
    geometry: WindowGeometry,
) -> dict[str, Any]:
    recomputed = output.with_name(f".{output.name}.root_recomputed.png")
    try:
        crop_verified_root(root_evidence, recomputed, geometry)
        root_sha = sha256_file(root_evidence)
        output_sha = sha256_file(output)
        recomputed_sha = sha256_file(recomputed)
        if output_sha != recomputed_sha:
            raise DesktopFailure(
                "ROOT_FALLBACK_NOT_RECOMPUTABLE",
                f"final={output_sha} recomputed={recomputed_sha}",
            )
        return {
            "status": "PASS",
            "root_path": str(root_evidence),
            "root_sha256": root_sha,
            "cropped_client_path": str(output),
            "cropped_client_sha256": output_sha,
            "recomputed_crop_sha256": recomputed_sha,
            "geometry": asdict(geometry),
            "recomputable": True,
        }
    finally:
        recomputed.unlink(missing_ok=True)


def _wait_for_ready_marker(
    marker: Path,
    process: subprocess.Popen[Any],
    config: WorkflowConfig,
    monitor: _RSSMonitor,
) -> None:
    deadline = time.monotonic() + float(config.get("timeouts.batch_ready_seconds", 900))
    while time.monotonic() < deadline:
        if marker.is_file() and marker.stat().st_size > 0:
            return
        if process.poll() is not None:
            raise DesktopFailure("IGV_EXIT_BEFORE_BATCH_READY", f"IGV exited with {process.returncode}")
        monitor.sample()
        time.sleep(0.5)
    raise DesktopFailure("IGV_BATCH_READY_TIMEOUT", str(marker))


def _wait_gui_settle_delay(
    process: subprocess.Popen[Any],
    delay_seconds: float,
    monitor: _RSSMonitor,
) -> float:
    started = time.monotonic()
    deadline = started + delay_seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise DesktopFailure("IGV_EXIT_DURING_GUI_SETTLE", f"IGV exited with {process.returncode}")
        monitor.sample()
        time.sleep(min(0.5, max(0.01, deadline - time.monotonic())))
    return time.monotonic() - started


def run_desktop_session(
    config: WorkflowConfig,
    *,
    batch_path: str | Path,
    ready_marker: str | Path,
    expected_locus: str,
    igv_directory: str | Path,
    log_directory: str | Path,
    capture_directory: str | Path,
    output_png: str | Path,
    metadata_path: str | Path,
    on_capture: Callable[[dict[str, Any]], None] | None = None,
    on_settle: Callable[[dict[str, Any]], None] | None = None,
    on_stable: Callable[[dict[str, Any]], None] | None = None,
) -> DesktopResult:
    """Run one isolated Xvfb/IGV session and capture the verified client window.

    The root canvas is never returned as a publishable screenshot.  It is only
    retained as fallback evidence when a verified client-window capture fails,
    and its geometry-validated crop becomes the downstream image.
    """

    started = time.monotonic()
    started_epoch = time.time()
    batch = Path(batch_path).resolve(strict=False)
    marker = Path(ready_marker).resolve(strict=False)
    igv_home = Path(igv_directory).resolve(strict=False)
    logs = Path(log_directory).resolve(strict=False)
    captures = Path(capture_directory).resolve(strict=False)
    output = Path(output_png).resolve(strict=False)
    metadata_file = Path(metadata_path).resolve(strict=False)
    for directory in (igv_home, logs, captures, output.parent, metadata_file.parent):
        directory.mkdir(parents=True, exist_ok=True)

    width = int(config.get("desktop.screen_width", 1920))
    height = int(config.get("desktop.screen_height", 2160))
    depth = int(config.get("desktop.screen_depth", 24))
    genome_definition = config.path_value("genome.definition").resolve(strict=False)
    if not genome_definition.is_file() or str(genome_definition).startswith(("http://", "https://")):
        raise DesktopFailure(
            "IGV_LOCAL_GENOME_INVALID",
            f"local genome definition is missing or remote: {genome_definition}",
        )
    prefs = igv_home / "startup_prefs.properties"
    genome_server_registry = igv_home / "local_genomes.tsv"
    data_server_registry = igv_home / "local_data_registry.txt"
    startup_preferences = {
        "IGV.Bounds": f"0,0,{width},{height}",
        # Batch mode in IGV 2.16.2 uses this preference before it considers
        # the CLI --genome argument unless the first batch command is genome.
        "DEFAULT_GENOME_KEY": str(genome_definition),
        "AUTO_UPDATE_GENOMES": "false",
        "SHOW_GENOME_SERVER_WARNING": "false",
        "IGV.genome.sequence.dir": str(genome_server_registry),
        "MASTER_RESOURCE_FILE_KEY": str(data_server_registry),
    }
    atomic_write_text(
        prefs,
        "".join(f"{key}={value}\n" for key, value in startup_preferences.items()),
    )
    atomic_write_text(
        genome_server_registry,
        (
            f"{config.get('genome.display_name')}\t{genome_definition}\t"
            f"{config.get('genome.id')}\n"
        ),
    )
    atomic_write_text(data_server_registry, "# local-only: no remote data registries\n")
    stdout_path = logs / "igv.stdout.log"
    stderr_path = logs / "igv.stderr.log"
    xvfb_stdout_path = logs / "xvfb.stdout.log"
    xvfb_stderr_path = logs / "xvfb.stderr.log"
    monitor = _RSSMonitor()
    xvfb_process: subprocess.Popen[Any] | None = None
    igv_process: subprocess.Popen[Any] | None = None
    stability_root: Path | None = None
    evidence_pruned = False
    metadata: dict[str, Any] = {
        "schema_version": DESKTOP_CAPTURE_SCHEMA,
        "gui_settle_contract_id": GUI_SETTLE_CONTRACT_ID,
        "command_listener_enabled": bool(
            config.get("desktop.command_listener_enabled", True)
        ),
        "root_screenshot_publishable": False,
        "alignment_policy": "collapse_hide",
        "locale": "C.UTF-8",
        "expected_locus": expected_locus,
        "igv_local_only_startup_contract_id": IGV_LOCAL_ONLY_STARTUP_CONTRACT_ID,
        "startup_preferences": startup_preferences,
        "startup_preferences_path": str(prefs),
        "startup_preferences_sha256": sha256_file(prefs),
        "local_genome_server_registry": str(genome_server_registry),
        "local_genome_server_registry_sha256": sha256_file(genome_server_registry),
        "local_data_server_registry": str(data_server_registry),
        "local_data_server_registry_sha256": sha256_file(data_server_registry),
    }

    with reserve_display(config) as display:
        environment = {
            **os.environ,
            "DISPLAY": display,
            "LC_ALL": "C.UTF-8",
            "LANG": "C.UTF-8",
        }
        command_listener_args = _igv_command_listener_args(config, display)
        command_port = (
            int(command_listener_args[1]) if command_listener_args else None
        )
        xvfb_argv = _command(config.get("binaries.xvfb"), "Xvfb") + [
            display,
            "-screen",
            "0",
            f"{width}x{height}x{depth}",
            "-nolisten",
            "tcp",
            "-noreset",
        ]
        with xvfb_stdout_path.open("wb") as xvfb_stdout, xvfb_stderr_path.open("wb") as xvfb_stderr:
            xvfb_process = subprocess.Popen(
                xvfb_argv,
                stdout=xvfb_stdout,
                stderr=xvfb_stderr,
                start_new_session=True,
                env=environment,
            )
        monitor.add(xvfb_process)
        try:
            canvas = _wait_x_server(config, display, xvfb_process, environment, monitor)
            igv_argv = _command(config.get("binaries.igv"), "igv") + [
                "--igvDirectory",
                str(igv_home),
                "--preferences",
                str(prefs),
                "--genomeServerURL",
                str(genome_server_registry),
                "--dataServerURL",
                str(data_server_registry),
                *command_listener_args,
                "--genome",
                str(genome_definition),
                "--batch",
                str(batch),
            ]
            with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
                igv_process = subprocess.Popen(
                    igv_argv,
                    cwd=batch.parent,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                    env=environment,
                )
            monitor.add(igv_process)
            geometry = _discover_window(config, display, igv_process, environment, monitor)
            _wait_for_ready_marker(marker, igv_process, config, monitor)

            requested_settle_delay = float(config.get("desktop.gui_settle_delay_seconds", 5.0))
            observed_settle_delay = _wait_gui_settle_delay(
                igv_process,
                requested_settle_delay,
                monitor,
            )
            roi = _toolbar_locus_roi(config, geometry)
            locus_roi = _locus_field_roi(config, geometry)
            roi_region = (roi["x"], roi["y"], roi["width"], roi["height"])

            stability_root = captures / "stability"
            stability_root.mkdir(parents=True, exist_ok=True)
            first_frame = stability_root / "frame_000.png"
            root_evidence = captures / "root_fallback.png"
            initial_root = stability_root / "root_000.png"
            mode, root_path = _capture_frame(
                config,
                display,
                geometry,
                first_frame,
                initial_root,
                environment,
            )
            if root_path:
                root_path = _retain_root_evidence(root_path, root_evidence)
            else:
                root_evidence.unlink(missing_ok=True)
            capture_metadata = {
                "display": display,
                "command_port": command_port,
                "canvas": canvas,
                "window": asdict(geometry),
                "capture_mode": mode,
                "client_path": str(first_frame),
                "root_fallback_path": root_path,
                "root_screenshot_publishable": False,
                "geometry_verified": True,
                "batch_ready_marker": str(marker),
                "gui_settle_delay_requested_seconds": requested_settle_delay,
                "gui_settle_delay_observed_seconds": round(observed_settle_delay, 3),
                "toolbar_locus_roi": roi,
                "locus_field_roi": locus_roi,
                "locus_ocr_contract_id": LOCUS_OCR_CONTRACT_ID,
            }
            metadata.update(capture_metadata)
            atomic_write_json(metadata_file, metadata)
            if on_capture:
                on_capture(capture_metadata)

            interval = float(config.get("desktop.stable_interval_seconds", 1.0))
            roi_required = int(config.get("desktop.toolbar_locus_stable_consecutive_frames", 3))
            roi_maximum_mean = float(
                config.get("desktop.toolbar_locus_stable_mean_absolute_fraction", 0.0)
            )
            roi_maximum_changed = float(
                config.get("desktop.toolbar_locus_stable_changed_pixel_fraction", 0.0)
            )
            settle_deadline = time.monotonic() + float(config.get("timeouts.gui_settle_seconds", 120))
            settle_comparisons: list[dict[str, Any]] = []
            previous = first_frame
            settle_pairs = 0
            frame_index = 1
            while time.monotonic() < settle_deadline and settle_pairs < roi_required:
                if igv_process.poll() is not None:
                    raise DesktopFailure(
                        "IGV_EXIT_DURING_GUI_SETTLE",
                        f"IGV exited with {igv_process.returncode}",
                    )
                time.sleep(interval)
                current = stability_root / f"frame_{frame_index:03d}.png"
                current_root = stability_root / f"root_{frame_index:03d}.png"
                current_mode, current_root_path = _capture_frame(
                    config,
                    display,
                    geometry,
                    current,
                    current_root,
                    environment,
                )
                mode_changed = current_mode != mode
                if mode_changed:
                    settle_pairs = 0
                    mode = current_mode
                if current_root_path:
                    root_path = _retain_root_evidence(current_root_path, root_evidence)
                else:
                    root_evidence.unlink(missing_ok=True)
                    root_path = None
                roi_difference = pixel_difference(previous, current, roi_region)
                roi_stable = (
                    not mode_changed
                    and roi_difference["mean_absolute_fraction"] <= roi_maximum_mean
                    and roi_difference["changed_pixel_fraction"] <= roi_maximum_changed
                )
                settle_pairs = settle_pairs + 1 if roi_stable else 0
                settle_comparisons.append(
                    {
                        "from": previous.name,
                        "to": current.name,
                        **roi_difference,
                        "stable": roi_stable,
                    }
                )
                previous = current
                frame_index += 1
                monitor.sample()
            if settle_pairs < roi_required:
                failed_settle = {
                    "status": "FAIL",
                    "contract_id": GUI_SETTLE_CONTRACT_ID,
                    "delay_requested_seconds": requested_settle_delay,
                    "delay_observed_seconds": round(observed_settle_delay, 3),
                    "roi": roi,
                    "required_consecutive_pairs": roi_required,
                    "observed_consecutive_pairs": settle_pairs,
                    "thresholds": {
                        "mean_absolute_fraction": roi_maximum_mean,
                        "changed_pixel_fraction": roi_maximum_changed,
                    },
                    "comparisons": settle_comparisons,
                }
                metadata["gui_settle"] = failed_settle
                atomic_write_json(metadata_file, metadata)
                raise DesktopFailure(
                    "GUI_SETTLE_TIMEOUT",
                    f"toolbar/locus stable pairs {settle_pairs}/{roi_required}; "
                    f"last={settle_comparisons[-1] if settle_comparisons else None}",
                )
            settle_ocr = verify_expected_locus_text(
                config,
                frame=previous,
                roi=roi,
                locus_roi=locus_roi,
                expected_locus=expected_locus,
                evidence_path=captures / "toolbar_locus" / "gui_settle_final.png",
                env=environment,
                control_screen_point=(
                    geometry.x + locus_roi["x"] + min(50, locus_roi["width"] // 4),
                    geometry.y + locus_roi["y"] + locus_roi["height"] // 2,
                ),
            )
            gui_settle = {
                "status": "PASS" if settle_ocr["matched"] else "FAIL",
                "contract_id": GUI_SETTLE_CONTRACT_ID,
                "delay_requested_seconds": requested_settle_delay,
                "delay_observed_seconds": round(observed_settle_delay, 3),
                "roi": roi,
                "required_consecutive_pairs": roi_required,
                "observed_consecutive_pairs": settle_pairs,
                "thresholds": {
                    "mean_absolute_fraction": roi_maximum_mean,
                    "changed_pixel_fraction": roi_maximum_changed,
                },
                "comparisons": settle_comparisons,
                "retained_chain_frames": _stable_chain_frame_names(
                    settle_comparisons, roi_required
                ),
                "final_frame": str(previous),
                "locus_ocr": settle_ocr,
            }
            metadata["gui_settle"] = gui_settle
            atomic_write_json(metadata_file, metadata)
            if not settle_ocr["matched"]:
                raise DesktopFailure(
                    "LOCUS_TEXT_NOT_DETECTED",
                    f"expected {expected_locus!r}; OCR={settle_ocr['observed_text']!r}",
                )
            if on_settle:
                on_settle(gui_settle)

            required = int(config.get("desktop.stable_consecutive_frames", 3))
            maximum_mean = float(config.get("desktop.stable_mean_absolute_fraction", 0.0015))
            maximum_changed = float(config.get("desktop.stable_changed_pixel_fraction", 0.01))
            deadline = time.monotonic() + float(config.get("timeouts.pixel_stability_seconds", 180))
            comparisons: list[dict[str, Any]] = []
            stable_pairs = 0
            while time.monotonic() < deadline and stable_pairs < required:
                if igv_process.poll() is not None:
                    raise DesktopFailure("IGV_EXIT_BEFORE_STABLE", f"IGV exited with {igv_process.returncode}")
                time.sleep(interval)
                current = stability_root / f"frame_{frame_index:03d}.png"
                current_root = stability_root / f"root_{frame_index:03d}.png"
                current_mode, current_root_path = _capture_frame(
                    config,
                    display,
                    geometry,
                    current,
                    current_root,
                    environment,
                )
                mode_changed = current_mode != mode
                if mode_changed:
                    stable_pairs = 0
                    mode = current_mode
                if current_root_path:
                    root_path = _retain_root_evidence(current_root_path, root_evidence)
                else:
                    root_evidence.unlink(missing_ok=True)
                    root_path = None
                difference = pixel_difference(previous, current)
                roi_difference = pixel_difference(previous, current, roi_region)
                full_frame_stable = (
                    difference["mean_absolute_fraction"] <= maximum_mean
                    and difference["changed_pixel_fraction"] <= maximum_changed
                )
                toolbar_locus_stable = (
                    roi_difference["mean_absolute_fraction"] <= roi_maximum_mean
                    and roi_difference["changed_pixel_fraction"] <= roi_maximum_changed
                )
                is_stable = not mode_changed and full_frame_stable and toolbar_locus_stable
                stable_pairs = stable_pairs + 1 if is_stable else 0
                comparisons.append(
                    {
                        "from": previous.name,
                        "to": current.name,
                        **difference,
                        "full_frame_stable": full_frame_stable,
                        "toolbar_locus": {**roi_difference, "stable": toolbar_locus_stable},
                        "stable": is_stable,
                    }
                )
                previous = current
                frame_index += 1
                monitor.sample()
            if stable_pairs < required:
                failed_stability = {
                    "status": "FAIL",
                    "required_consecutive_pairs": required,
                    "observed_consecutive_pairs": stable_pairs,
                    "comparisons": comparisons,
                    "toolbar_locus_guard": {
                        "status": "FAIL",
                        "roi": roi,
                        "thresholds": {
                            "mean_absolute_fraction": roi_maximum_mean,
                            "changed_pixel_fraction": roi_maximum_changed,
                        },
                    },
                }
                metadata["pixel_stability"] = failed_stability
                atomic_write_json(metadata_file, metadata)
                raise DesktopFailure(
                    "PIXEL_STABILITY_TIMEOUT",
                    f"stable pairs {stable_pairs}/{required}; last={comparisons[-1] if comparisons else None}",
                )
            final_ocr = verify_expected_locus_text(
                config,
                frame=previous,
                roi=roi,
                locus_roi=locus_roi,
                expected_locus=expected_locus,
                evidence_path=captures / "toolbar_locus" / "render_stable_final.png",
                env=environment,
                control_screen_point=(
                    geometry.x + locus_roi["x"] + min(50, locus_roi["width"] // 4),
                    geometry.y + locus_roi["y"] + locus_roi["height"] // 2,
                ),
            )
            if not final_ocr["matched"]:
                metadata["pixel_stability"] = {
                    "status": "FAIL",
                    "required_consecutive_pairs": required,
                    "observed_consecutive_pairs": stable_pairs,
                    "comparisons": comparisons,
                    "toolbar_locus_guard": {
                        "status": "FAIL",
                        "roi": roi,
                        "final_locus_ocr": final_ocr,
                    },
                }
                atomic_write_json(metadata_file, metadata)
                raise DesktopFailure(
                    "LOCUS_TEXT_NOT_DETECTED",
                    f"final frame expected {expected_locus!r}; OCR={final_ocr['observed_text']!r}",
                )
            shutil.copy2(previous, output)
            final_root_evidence = None
            if mode == "root_fallback_crop":
                if root_path != str(root_evidence) or not root_evidence.is_file():
                    raise DesktopFailure(
                        "ROOT_FALLBACK_EVIDENCE_MISSING",
                        f"final root evidence is not retained for {previous}",
                    )
                final_root_evidence = _verify_final_root_evidence(
                    root_evidence,
                    output,
                    geometry,
                )
            else:
                root_evidence.unlink(missing_ok=True)
                root_path = None
            render_chain_frames = _stable_chain_frame_names(comparisons, required)
            stability = {
                "status": "PASS",
                "required_consecutive_pairs": required,
                "observed_consecutive_pairs": stable_pairs,
                "frame_count": frame_index,
                "comparisons": comparisons,
                "retained_chain_frames": render_chain_frames,
                "final_frame": str(previous),
                "output": str(output),
                "toolbar_locus_guard": {
                    "status": "PASS",
                    "contract_id": GUI_SETTLE_CONTRACT_ID,
                    "roi": roi,
                    "thresholds": {
                        "mean_absolute_fraction": roi_maximum_mean,
                        "changed_pixel_fraction": roi_maximum_changed,
                    },
                    "final_locus_ocr": final_ocr,
                },
            }
            stability["evidence_retention"] = _prune_stability_evidence(
                stability_root,
                int(config.get("desktop.retained_stability_frames", 4)),
                successful_chains={
                    "gui_settle": list(gui_settle["retained_chain_frames"]),
                    "render_stable": render_chain_frames,
                },
            )
            evidence_pruned = True
            metadata.update(
                {
                    "capture_mode": mode,
                    "root_fallback_path": root_path,
                    "root_fallback_evidence": final_root_evidence,
                    "pixel_stability": stability,
                    "screenshot": str(output),
                    "xvfb_argv": xvfb_argv,
                    "igv_argv": igv_argv,
                }
            )
            atomic_write_json(metadata_file, metadata)
            if on_stable:
                on_stable(stability)
        finally:
            if stability_root is not None and not evidence_pruned:
                retention = _prune_stability_evidence(
                    stability_root,
                    int(config.get("desktop.retained_stability_frames", 4)),
                    timeout=True,
                )
                metadata["evidence_retention"] = retention
                atomic_write_json(metadata_file, metadata)
            monitor.sample()
            _terminate_group(igv_process)
            _terminate_group(xvfb_process)
            monitor.sample()

    wall_time = time.monotonic() - started
    ended_epoch = time.time()
    metadata["started_at_epoch"] = started_epoch
    metadata["ended_at_epoch"] = ended_epoch
    metadata["wall_time_seconds"] = round(wall_time, 3)
    metadata["peak_rss_gb"] = round(monitor.peak_kb / (1024.0 * 1024.0), 4)
    atomic_write_json(metadata_file, metadata)
    return DesktopResult(
        screenshot=output,
        metadata=metadata,
        started_at_epoch=started_epoch,
        ended_at_epoch=ended_epoch,
        wall_time_seconds=wall_time,
        peak_rss_gb=monitor.peak_kb / (1024.0 * 1024.0),
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
