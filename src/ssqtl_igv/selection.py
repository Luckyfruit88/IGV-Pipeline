from __future__ import annotations

import math
import statistics
from typing import Any, Iterable


TARGETS = ("min", "max", "median", "q1", "q3", "mean")
_TARGET_RANK = {label: index for index, label in enumerate(TARGETS)}


def representative_order_key(row: dict[str, Any]) -> tuple[int, str]:
    """Return the deterministic within-dosage IGV track order.

    Groups with more than six eligible samples retain the order in which the
    representative targets are selected by ``prepare_cases.R``.  Groups with
    six or fewer samples are labelled ``all`` and are ordered by sample ID.
    """

    label = str(row.get("selection_label", "")).strip().lower().replace("_", "-")
    if label == "all":
        rank = 0
    else:
        rank = _TARGET_RANK.get("mean" if label == "mean-nearest" else label, 99)
    return rank, str(row.get("sample_id", ""))


def _quantile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * probability
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def select_representatives(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    clean = []
    for row in rows:
        ratio = float(row["ratio"])
        if math.isfinite(ratio) and ratio >= 0:
            clean.append({**row, "ratio": ratio, "sample_id": str(row["sample_id"])})
    clean.sort(key=lambda row: (row["sample_id"], row["ratio"]))
    if len(clean) <= 6:
        return [{**row, "selection_label": "all"} for row in clean]
    values = [row["ratio"] for row in clean]
    targets = {
        "min": min(values),
        "max": max(values),
        "median": statistics.median(values),
        "q1": _quantile(values, 0.25),
        "q3": _quantile(values, 0.75),
        "mean": statistics.fmean(values),
    }
    remaining = clean.copy()
    selected: list[dict[str, Any]] = []
    for label in TARGETS:
        target = targets[label]
        best = min(remaining, key=lambda row: (abs(row["ratio"] - target), row["sample_id"]))
        selected.append({**best, "selection_label": label})
        remaining.remove(best)
    return selected
