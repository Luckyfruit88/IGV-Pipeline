from __future__ import annotations

import subprocess
import re
from pathlib import Path
from typing import Any

from .parsing import parse_ag_site, parse_snp
from .utils import command_prefix


class ViolinMatchError(ValueError):
    pass


def pdf_pages(pdf: str | Path, pdftotext: Any = "pdftotext", timeout: int = 900) -> list[str]:
    source = Path(pdf)
    completed = subprocess.run(
        command_prefix(pdftotext) + ["-layout", str(source), "-"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if completed.returncode != 0:
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ViolinMatchError(f"pdftotext failed for {source}: {message}")
    text = completed.stdout.decode("utf-8", errors="replace")
    pages = text.split("\f")
    if pages and not pages[-1].strip():
        pages.pop()
    if not pages:
        raise ViolinMatchError(f"no text pages extracted from {source}")
    return pages


def unique_page_for_pair(pages: list[str], ag_site: str, snp: str) -> int:
    ag_pattern, snp_pattern = _pair_patterns(ag_site, snp)
    matches = [
        index
        for index, page in enumerate(pages, 1)
        if ag_pattern.search(_normalize_page(page)) and snp_pattern.search(_normalize_page(page))
    ]
    if not matches:
        raise ViolinMatchError(f"zero violin pages match AG={ag_site} SNP={snp}")
    if len(matches) > 1:
        raise ViolinMatchError(f"multiple violin pages match AG={ag_site} SNP={snp}: {matches}")
    return matches[0]


def unique_pages_for_pairs(
    pages: list[str], pairs: list[tuple[str, str]]
) -> dict[tuple[str, str], int | ViolinMatchError]:
    normalized_pages = [_normalize_page(page) for page in pages]
    result: dict[tuple[str, str], int | ViolinMatchError] = {}
    for pair in pairs:
        ag_site, snp = pair
        ag_pattern, snp_pattern = _pair_patterns(ag_site, snp)
        matches = [
            index
            for index, page in enumerate(normalized_pages, 1)
            if ag_pattern.search(page) and snp_pattern.search(page)
        ]
        if not matches:
            result[pair] = ViolinMatchError(f"zero violin pages match AG={ag_site} SNP={snp}")
        elif len(matches) > 1:
            result[pair] = ViolinMatchError(f"multiple violin pages match AG={ag_site} SNP={snp}: {matches}")
        else:
            result[pair] = matches[0]
    return result


def _normalize_page(text: str) -> str:
    return text.lower().replace("−", "-").replace("–", "-").replace("—", "-")


def _pair_patterns(ag_site: str, snp_value: str) -> tuple[re.Pattern[str], re.Pattern[str]]:
    ag = parse_ag_site(ag_site)
    snp = parse_snp(snp_value)
    chrom_ag = re.escape(ag.chrom.lower())
    chrom_snp = re.escape(snp.chrom.lower())
    ag_pattern = re.compile(
        rf"{chrom_ag}\s*:\s*{ag.source_start}(?!\d)\s*-\s*{ag.source_end}(?!\d)",
        re.IGNORECASE,
    )
    snp_pattern = re.compile(
        rf"{chrom_snp}\s*[.:]\s*{snp.position}(?!\d)\s*[._:\s-]+"
        rf"{re.escape(snp.ref.lower())}(?![a-z])\s*[._:>/\s-]+{re.escape(snp.alt.lower())}(?![a-z])",
        re.IGNORECASE,
    )
    return ag_pattern, snp_pattern


def render_pdf_page(
    pdf: str | Path,
    page: int,
    output_png: str | Path,
    *,
    pdftoppm: Any = "pdftoppm",
    dpi: int = 180,
    timeout: int = 300,
) -> None:
    output = Path(output_png)
    output.parent.mkdir(parents=True, exist_ok=True)
    prefix = output.with_suffix("")
    completed = subprocess.run(
        command_prefix(pdftoppm)
        + [
            "-f",
            str(page),
            "-l",
            str(page),
            "-singlefile",
            "-r",
            str(dpi),
            "-png",
            str(pdf),
            str(prefix),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if completed.returncode != 0 or not output.is_file():
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"pdftoppm failed for page {page} of {pdf}: {message}")
