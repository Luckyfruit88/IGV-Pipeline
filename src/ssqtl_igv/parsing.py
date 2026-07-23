from __future__ import annotations

import re
from dataclasses import dataclass


_AG_RE = re.compile(r"^(chr[^:]+):(\d+)-(\d+)$", re.IGNORECASE)
_SNP_RE = re.compile(r"^(chr[^.:_]+)[.:](\d+)[._]([A-Za-z]+)[._]([A-Za-z]+)$", re.IGNORECASE)


@dataclass(frozen=True)
class AGSite:
    chrom: str
    source_start: int
    source_end: int

    @property
    def start(self) -> int:
        return min(self.source_start, self.source_end)

    @property
    def end(self) -> int:
        return max(self.source_start, self.source_end)

    @property
    def canonical(self) -> str:
        return f"{self.chrom}:{self.source_start}-{self.source_end}"


@dataclass(frozen=True)
class SNP:
    chrom: str
    position: int
    ref: str
    alt: str

    @property
    def canonical(self) -> str:
        return f"{self.chrom}.{self.position}_{self.ref}.{self.alt}"


def normalize_chrom(chrom: str) -> str:
    suffix = chrom.strip()
    if suffix.lower().startswith("chr"):
        suffix = suffix[3:]
    if suffix.upper() in {"X", "Y", "M", "MT"}:
        suffix = suffix.upper()
    return f"chr{suffix}"


def normalize_strand(value: str) -> str:
    normalized = value.strip().replace("−", "-").replace("–", "-")
    if normalized in {"+", "pos", "positive", "1", "+1"}:
        return "+"
    if normalized in {"-", "neg", "negative", "-1"}:
        return "-"
    raise ValueError(f"unsupported strand: {value!r}")


def strand_token(strand: str) -> str:
    return "pos" if normalize_strand(strand) == "+" else "neg"


def parse_ag_site(value: str) -> AGSite:
    match = _AG_RE.fullmatch(value.strip())
    if not match:
        raise ValueError(f"invalid AG_site: {value!r}")
    return AGSite(normalize_chrom(match.group(1)), int(match.group(2)), int(match.group(3)))


def parse_snp(value: str) -> SNP:
    match = _SNP_RE.fullmatch(value.strip())
    if not match:
        raise ValueError(f"invalid SNP: {value!r}")
    return SNP(
        normalize_chrom(match.group(1)),
        int(match.group(2)),
        match.group(3).upper(),
        match.group(4).upper(),
    )


def make_case_id(ag: AGSite, snp: SNP) -> str:
    if ag.chrom != snp.chrom:
        raise ValueError(f"AG/SNP chromosome mismatch: {ag.chrom} vs {snp.chrom}")
    ag_token = ag.canonical.replace(":", "_").replace("-", "_")
    snp_token = snp.canonical.replace(".", "_")
    return f"AG_{ag_token}__SNP_{snp_token}"


def make_windows(ag: AGSite, snp: SNP, overview_padding: int = 55, detail_padding: int = 12) -> dict[str, dict[str, int | str]]:
    if ag.chrom != snp.chrom:
        raise ValueError("AG and SNP must use the same chromosome")
    overview_start = max(1, ag.start - overview_padding)
    overview_end = ag.end + overview_padding
    detail_start = max(1, min(ag.start, snp.position) - detail_padding)
    detail_end = max(ag.end, snp.position) + detail_padding
    return {
        "overview": {"chrom": ag.chrom, "start": overview_start, "end": overview_end},
        "detail": {"chrom": ag.chrom, "start": detail_start, "end": detail_end},
    }


def locus(window: dict[str, int | str]) -> str:
    return f"{window['chrom']}:{window['start']}-{window['end']}"


def event_bed_rows(ag: AGSite, snp: SNP, strand: str) -> list[tuple[str, int, int, str, int, str, int, int, str]]:
    direction = normalize_strand(strand)
    return [
        (ag.chrom, ag.start - 1, ag.end, f"AG {ag.canonical}", 1000, direction, ag.start - 1, ag.end, "0,112,192"),
        (
            snp.chrom,
            snp.position - 1,
            snp.position,
            f"SNP {snp.position} {snp.ref}>{snp.alt}",
            1000,
            direction,
            snp.position - 1,
            snp.position,
            "220,38,38",
        ),
    ]


def compact_match_key(value: str) -> str:
    return "".join(char.lower() for char in value if char.isalnum())
