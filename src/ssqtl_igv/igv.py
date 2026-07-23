from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .parsing import locus
from .selection import representative_order_key


GENOTYPE_TOKENS = {"0/0": "geno00", "0/1": "geno01", "1/1": "geno11"}
GENOTYPE_ORDER = tuple(GENOTYPE_TOKENS)


def _batch_path(path: str | Path) -> str:
    raw = str(path)
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*://", raw):
        raise ValueError(f"remote IGV resources are prohibited: {raw}")
    value = str(Path(raw).resolve(strict=False))
    if any(char.isspace() for char in value):
        raise ValueError(f"IGV batch paths cannot contain whitespace: {value}")
    return value


def _batch_preamble(
    case: dict[str, Any],
    *,
    snapshot_root: Path,
    config: WorkflowConfig,
    view: str,
) -> list[str]:
    fallback_height = int(config.get("render.max_panel_height", 2160))
    panel_height = int(config.get(f"render.{view}_max_panel_height", fallback_height))
    return [
        # IGV 2.16.2 batch startup examines the first executable command.  If
        # it is not ``genome``, IGV loads DEFAULT_GENOME_KEY before executing
        # the batch.  Keep the local hg38 definition first so a fresh profile
        # can never fall back to the packaged hg19/RefSeq default.
        f"genome {_batch_path(case['genome']['definition'])}",
        "new",
        f"setSleepInterval {int(config.get('render.sleep_interval_ms', 500))}",
        f"maxPanelHeight {panel_height}",
        f"snapshotDirectory {_batch_path(snapshot_root)}",
    ]


def _validate_generated_batch(text: str) -> None:
    if "rename " in text or re.search(r"(?:https?|ftp|s3|gs)://", text, re.IGNORECASE):
        raise ValueError("generated batch violates portability/offline contract")
    physical_lines = text.splitlines()
    first_line = physical_lines[0] if physical_lines else ""
    if (
        not first_line.startswith("genome /")
        or first_line != first_line.strip()
        or len(first_line.split(" ", 1)) != 2
        or not Path(first_line.split(" ", 1)[1]).is_absolute()
    ):
        raise ValueError("generated batch must bind the local genome before every other command")


def build_desktop_batch(
    case: dict[str, Any],
    *,
    control_directory: str | Path,
    config: WorkflowConfig,
) -> tuple[str, dict[str, Any]]:
    """Build the single local-only batch used by the native desktop capture.

    The ``batch_ready.png`` snapshot is a control-plane sentinel only.  It is
    never supplied to the compositor or publication layer; the evidence image
    is captured later from the verified IGV client window.
    """

    control_root = Path(control_directory).resolve(strict=False)
    control_root.mkdir(parents=True, exist_ok=True)
    ready_marker = control_root / "batch_ready.png"
    lines = _batch_preamble(case, snapshot_root=control_root, config=config, view="overview")
    lines.extend(
        [
            "preference SAM.SHOW_COV_TRACK true",
            "preference SAM.SHOW_ALIGNMENT_TRACK true",
            "preference SAM.SHOW_JUNCTION_TRACK true",
        ]
    )
    ordered_samples: list[dict[str, Any]] = []
    for genotype in GENOTYPE_ORDER:
        samples = sorted(
            case.get("genotypes", {}).get(genotype, []),
            key=representative_order_key,
        )
        ordered_samples.extend(samples)
        for sample in samples:
            lines.append(f"load {_batch_path(sample['bam'])} index={_batch_path(sample['bai'])}")
    if not ordered_samples:
        raise ValueError("no eligible samples across all genotype groups")
    lines.extend(
        [
            # Collapse is an IGV-native view action.  It keeps alignment tracks
            # enabled while preventing expanded read stacks from hiding the
            # sequence and configured annotation tracks in the desktop viewport.
            "collapse",
            f"load {_batch_path(case['genome']['annotation'])}",
            f"setSequenceStrand {case['strand']}",
            f"goto {locus(case['windows']['overview'])}",
            f"snapshot {ready_marker.name}",
        ]
    )
    text = "\n".join(lines) + "\n"
    _validate_generated_batch(text)
    if "preference SAM.SHOW_ALIGNMENT_TRACK false" in text:
        raise ValueError("alignment rows must be collapsed/hidden, not disabled")
    return text, {
        "ready_marker": ready_marker,
        "alignment_policy": "collapse_hide",
        "native_default_track_style": True,
        "custom_event_annotation_loaded": False,
        "sample_order": [
            {
                "genotype": sample["genotype"],
                "sample_id": sample.get("sample_id", ""),
                "bam": sample["bam"],
            }
            for sample in ordered_samples
        ],
        "control_snapshot_publishable": False,
    }
