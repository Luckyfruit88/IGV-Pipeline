from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from importlib.resources import files
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .parsing import make_case_id, make_windows, normalize_strand, parse_ag_site, parse_snp, strand_token
from .selection import representative_order_key
from .utils import (
    atomic_write_json,
    command_prefix,
    optional_text,
    sha256_file,
    sha256_json,
    utc_now,
    write_jsonl,
    write_tsv,
)


_DEFAULT_LOCUS_SAMPLE_COLUMNS = ["sample_id"]
_DEFAULT_BAM_ID_COLUMNS = ["sample_id"]
_DEFAULT_BAM_PATH_COLUMNS = ["directory", "bam", "bam_path", "path"]
_DEFAULT_BAM_SUFFIXES = [
    ".accepted_hits.merged.markeddups.recal.bam",
    ".accepted_hits.merged.markdups.recal.bam",
    ".accepted_hits.merged.nodups.recal.bam",
    ".accepted_hits.merged.dedups.recal.bam",
    ".accepted_hits.bam",
    "_Aligned.sortedByCoord.out.bam",
    ".Aligned.sortedByCoord.out.bam",
    ".bam",
]
from .violin import ViolinMatchError, pdf_pages, unique_pages_for_pairs


def _read_fai_index(fai: Path) -> dict[str, tuple[int, int, int, int]]:
    index: dict[str, tuple[int, int, int, int]] = {}
    with fai.open(encoding="utf-8") as handle:
        for line in handle:
            fields = line.rstrip("\n").split("\t")
            if len(fields) >= 5:
                index[fields[0]] = tuple(int(value) for value in fields[1:5])
    return index


def _reference_context(
    fasta: Path,
    fai: Path,
    *,
    chrom: str,
    start: int,
    end: int,
    strand: str,
    fai_index: dict[str, tuple[int, int, int, int]] | Exception | None = None,
) -> dict[str, Any]:
    """Read the two AG-site bases by indexed FASTA byte offsets."""

    try:
        if isinstance(fai_index, Exception):
            raise ValueError(f"FAI index unavailable: {fai_index}")
        index = fai_index if fai_index is not None else _read_fai_index(fai)
        length, sequence_offset, line_bases, line_width = index[chrom]
        left, right = sorted((int(start), int(end)))
        if left < 1 or right > length:
            raise ValueError(f"interval {chrom}:{left}-{right} exceeds reference length {length}")
        bases: list[str] = []
        with fasta.open("rb") as handle:
            for position in range(left, right + 1):
                zero_based = position - 1
                byte_offset = sequence_offset + (zero_based // line_bases) * line_width + zero_based % line_bases
                handle.seek(byte_offset)
                bases.append(handle.read(1).decode("ascii").upper())
        genomic = "".join(bases)
        complement = str.maketrans("ACGTN", "TGCAN")
        transcript = genomic if strand == "+" else genomic.translate(complement)[::-1]
        return {
            "available": True,
            "chrom": chrom,
            "start": left,
            "end": right,
            "strand": strand,
            "genomic_sequence": genomic,
            "transcript_sequence": transcript,
            "expected_transcript_sequence": "AG",
            "canonical_ag": transcript == "AG",
        }
    except (OSError, KeyError, UnicodeDecodeError, ValueError, ZeroDivisionError) as exc:
        return {
            "available": False,
            "chrom": chrom,
            "start": min(start, end),
            "end": max(start, end),
            "strand": strand,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle, delimiter="\t"))


def _run_r_prepare(
    config: WorkflowConfig,
    associations: Path,
    cases_path: Path,
    samples_path: Path,
) -> None:
    script = files("ssqtl_igv.resources").joinpath("prepare_cases.R")
    association_columns = config.get("inputs.association_columns", {})
    if not isinstance(association_columns, dict):
        raise ValueError("inputs.association_columns must be a mapping")
    command = [
        *command_prefix(config.get("binaries.rscript"), default="Rscript"),
        str(script),
        f"--associations={associations}",
        f"--rds_dir={config.path_value('paths.rds_dir')}",
        f"--bam_lookup={config.path_value('paths.bam_lookup')}",
        f"--cases_out={cases_path}",
        f"--samples_out={samples_path}",
        f"--ag_column={association_columns.get('ag_site', 'AG_site')}",
        f"--snp_column={association_columns.get('snp', 'SNP')}",
        f"--strand_column={association_columns.get('strand', 'strand')}",
        f"--rds_filename_template={config.get('inputs.rds_filename_template', 'AGratio_SNPgeno_{strand_token}_{chrom}_list.rds')}",
        f"--locus_sample_columns={','.join(config.get('inputs.locus_sample_columns', _DEFAULT_LOCUS_SAMPLE_COLUMNS))}",
        f"--ratio_column={config.get('inputs.ratio_column', 'ratio')}",
        f"--bam_lookup_id_columns={','.join(config.get('inputs.bam_lookup_id_columns', _DEFAULT_BAM_ID_COLUMNS))}",
        f"--bam_lookup_path_columns={','.join(config.get('inputs.bam_lookup_path_columns', _DEFAULT_BAM_PATH_COLUMNS))}",
        f"--bam_suffixes={','.join(config.get('inputs.bam_suffixes', _DEFAULT_BAM_SUFFIXES))}",
    ]
    completed = subprocess.run(
        command,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=int(config.get("timeouts.r_prepare_seconds", 129600)),
    )
    if completed.returncode != 0:
        raise RuntimeError(f"R preparation failed ({completed.returncode}): {completed.stderr.strip()}")


def _as_number(value: str) -> int | float | None:
    if value == "" or value is None:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _group_samples(rows: list[dict[str, str]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    result: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        genotype = row["genotype"]
        bam = Path(row["bam"])
        bai = Path(row["bai"])
        result[row["case_id"]][genotype].append(
            {
                "sample_id": row["sample_id"],
                "genotype": genotype,
                "dosage": int(row["dosage"]),
                "ratio": float(row["ratio"]),
                "selection_label": row["selection_label"],
                "bam": row["bam"],
                "bai": row["bai"],
                "bam_identity": {
                    "size": bam.stat().st_size,
                    "mtime_ns": bam.stat().st_mtime_ns,
                }
                if bam.is_file()
                else None,
                "bai_identity": {
                    "size": bai.stat().st_size,
                    "mtime_ns": bai.stat().st_mtime_ns,
                }
                if bai.is_file()
                else None,
                "bai_fresh": None if row["bai_fresh"] == "" else row["bai_fresh"].lower() == "true",
            }
        )
    for groups in result.values():
        for samples in groups.values():
            samples.sort(key=representative_order_key)
    return result


def _violin_pdf(config: WorkflowConfig, chrom: str, strand: str) -> Path:
    template = config.get("paths.violin_pdf_template", "violin_plots_{strand_token}_{chrom}.pdf")
    name = str(template).format(chrom=chrom, strand=strand, strand_token=strand_token(strand))
    return config.path_value("paths.violin_dir") / name


def _snapshot_association_input(source: Path, snapshot: Path, expected_sha256: str) -> Path:
    """Create one immutable run-local input and detect source changes during copy."""

    snapshot.parent.mkdir(parents=True, exist_ok=True)
    if snapshot.is_file():
        if sha256_file(snapshot) != expected_sha256:
            raise ValueError("existing association snapshot differs from the pinned input")
    else:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{snapshot.name}.", dir=snapshot.parent
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            shutil.copy2(source, temporary)
            if sha256_file(temporary) != expected_sha256:
                raise ValueError("association input changed while creating the run snapshot")
            temporary.chmod(0o444)
            os.replace(temporary, snapshot)
        finally:
            temporary.unlink(missing_ok=True)
    if sha256_file(source) != expected_sha256:
        raise ValueError("association input changed after initial checksum verification")
    if (snapshot.stat().st_mode & 0o777) != 0o444:
        raise ValueError("association snapshot must be read-only (0444)")
    return snapshot


def prepare_run(
    config: WorkflowConfig,
    run_root: str | Path,
    *,
    associations: str | Path | None = None,
    prepared_cases: str | Path | None = None,
    prepared_samples: str | Path | None = None,
) -> dict[str, Any]:
    root = config.validate_run_root(run_root)
    work = root / ".work"
    association_path = Path(associations) if associations else config.path_value("paths.associations")
    association_path = association_path.expanduser().resolve(strict=False)
    association_sha = sha256_file(association_path)
    expected_association_sha = optional_text(
        config.get("paths.associations_sha256")
    ).lower()
    if expected_association_sha and association_sha != expected_association_sha:
        raise ValueError(
            "association input checksum mismatch: "
            f"expected {expected_association_sha}, observed {association_sha}"
        )
    snapshot_sha = expected_association_sha or association_sha
    config_fingerprint = sha256_json(config.data)
    existing_report_path = work / "prepare_report.json"
    if existing_report_path.is_file():
        existing = json.loads(existing_report_path.read_text(encoding="utf-8"))
        same_inputs = (
            existing.get("associations_sha256") == association_sha
            and existing.get("config_fingerprint") == config_fingerprint
        )
        manifest = Path(existing.get("manifest", ""))
        shards = Path(existing.get("shards", ""))
        if same_inputs and manifest.is_file() and shards.is_file():
            return {**existing, "action": "SKIP_IDENTICAL_PREPARE"}
        raise ValueError("run root already contains a prepare report for different or incomplete inputs")
    public_entries = [entry for entry in root.iterdir() if entry.name != ".work"] if root.is_dir() else []
    if public_entries:
        raise ValueError("refusing to prepare inside a non-fresh run root: " + ", ".join(entry.name for entry in public_entries))
    if bool(prepared_cases) != bool(prepared_samples):
        raise ValueError("prepared_cases and prepared_samples must be supplied together")
    if prepared_cases or prepared_samples:
        prepared_case_path = Path(prepared_cases).expanduser().resolve(strict=True)
        prepared_sample_path = Path(prepared_samples).expanduser().resolve(strict=True)
        if not prepared_case_path.is_file() or not prepared_sample_path.is_file():
            raise ValueError("prepared case and sample inputs must be files")
    if (work / "prepared").exists() or (work / "manifests").exists():
        abandoned = work / f"abandoned_prepare_{utc_now().replace(':', '').replace('+', '_')}"
        abandoned.mkdir(parents=True, exist_ok=False)
        for name in ("prepared", "manifests"):
            source = work / name
            if source.exists():
                source.rename(abandoned / name)
    prepared = work / "prepared"
    manifests = work / "manifests"
    inputs = work / "inputs"
    prepared.mkdir(parents=True, exist_ok=True)
    manifests.mkdir(parents=True, exist_ok=True)
    inputs.mkdir(parents=True, exist_ok=True)

    association_snapshot = inputs / association_path.name
    _snapshot_association_input(association_path, association_snapshot, snapshot_sha)

    cases_path = Path(prepared_cases) if prepared_cases else prepared / "cases.tsv"
    samples_path = Path(prepared_samples) if prepared_samples else prepared / "samples.tsv"
    if not prepared_cases or not prepared_samples:
        _run_r_prepare(config, association_snapshot, cases_path, samples_path)

    case_rows = _read_tsv(cases_path)
    expected_case_count = config.get("inputs.expected_case_count")
    if expected_case_count not in (None, "") and len(case_rows) != int(expected_case_count):
        raise ValueError(
            f"prepared case count mismatch: expected {expected_case_count}, observed {len(case_rows)}"
        )
    if not case_rows:
        raise ValueError("preparation produced no cases")
    sample_groups = _group_samples(_read_tsv(samples_path))
    case_ids = [row["case_id"] for row in case_rows]
    duplicates = sorted(case_id for case_id, count in Counter(case_ids).items() if count > 1)
    if duplicates:
        raise ValueError("duplicate normalized case_id values: " + ", ".join(duplicates[:10]))

    targets_by_pdf: dict[Path, list[tuple[str, str]]] = defaultdict(list)
    for row in case_rows:
        try:
            target_ag = parse_ag_site(row["ag_site"])
            target_snp = parse_snp(row["snp"])
            target_strand = normalize_strand(row["strand"])
        except ValueError:
            continue
        targets_by_pdf[_violin_pdf(config, target_ag.chrom, target_strand)].append(
            (target_ag.canonical, target_snp.canonical)
        )
    page_index_cache: dict[Path, dict[tuple[str, str], int | ViolinMatchError] | Exception] = {}
    pdf_sha_cache: dict[Path, str] = {}
    reference_fai = config.path_value("genome.fai")
    try:
        reference_fai_index: dict[str, tuple[int, int, int, int]] | Exception = _read_fai_index(
            reference_fai
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        reference_fai_index = exc
    records: list[dict[str, Any]] = []
    shard_counts: Counter[str] = Counter()
    failed = 0
    for row in case_rows:
        errors: list[dict[str, str]] = []
        warnings: list[dict[str, str]] = []
        if row.get("error_code"):
            errors.append({"code": row["error_code"], "message": row.get("error_message", "")})
        try:
            ag = parse_ag_site(row["ag_site"])
            snp = parse_snp(row["snp"])
            strand = normalize_strand(row["strand"])
            expected_case_id = make_case_id(ag, snp)
            windows = make_windows(
                ag,
                snp,
                int(config.get("render.overview_padding", 55)),
                int(config.get("render.detail_padding", 12)),
            )
        except ValueError as exc:
            errors.append({"code": "COORDINATE_PARSE_FAILED", "message": str(exc)})
            shard = row.get("shard") or "invalid"
            record = {
                "schema_version": "1.0",
                "run_id": root.name,
                "workflow_config_fingerprint": config_fingerprint,
                "associations_sha256": association_sha,
                "association_row": int(row["association_row"]),
                "case_id": row["case_id"],
                "ag": {"raw": row.get("ag_site", "")},
                "snp": {"raw": row.get("snp", "")},
                "strand": row.get("strand", ""),
                "shard": shard,
                "statistics": {},
                "windows": {},
                "genotypes": {"0/0": [], "0/1": [], "1/1": []},
                "violin": {"pdf": None, "page": None},
                "genome": {},
                "preflight_warnings": warnings,
                "preflight_errors": errors,
            }
            record["input_fingerprint"] = sha256_json(record)
            records.append(record)
            shard_counts[shard] += 1
            failed += 1
            continue

        groups = sample_groups.get(row["case_id"], {})
        normalized_groups = {genotype: groups.get(genotype, []) for genotype in ("0/0", "0/1", "1/1")}
        for genotype, samples in normalized_groups.items():
            if not samples:
                warnings.append(
                    {
                        "code": "EMPTY_GENOTYPE_GROUP",
                        "message": f"no eligible representative samples for {genotype}; retain the AG-SNP pair and render available groups",
                    }
                )
                continue
            for sample in samples:
                bam_value = str(sample.get("bam", "")).strip()
                bai_value = str(sample.get("bai", "")).strip()
                if not bam_value:
                    errors.append(
                        {
                            "code": "BAM_PATH_UNRESOLVED",
                            "message": f"{genotype} {sample['sample_id']}",
                        }
                    )
                elif not Path(bam_value).is_file():
                    errors.append(
                        {
                            "code": "BAM_MISSING",
                            "message": f"{genotype} {sample['sample_id']}: {bam_value}",
                        }
                    )
                if not bai_value:
                    errors.append(
                        {
                            "code": "BAI_PATH_UNRESOLVED",
                            "message": f"{genotype} {sample['sample_id']}",
                        }
                    )
                elif not Path(bai_value).is_file():
                    errors.append(
                        {
                            "code": "BAI_MISSING",
                            "message": f"{genotype} {sample['sample_id']}: {bai_value}",
                        }
                    )

        pdf = _violin_pdf(config, ag.chrom, strand)
        page: int | None = None
        if not pdf.is_file():
            errors.append({"code": "VIOLIN_PDF_MISSING", "message": str(pdf)})
        else:
            if pdf not in pdf_sha_cache:
                pdf_sha_cache[pdf] = sha256_file(pdf)
            if pdf not in page_index_cache:
                try:
                    extracted_pages = pdf_pages(
                        pdf,
                        config.get("binaries.pdftotext", "pdftotext"),
                        int(config.get("timeouts.pdftotext_seconds", 900)),
                    )
                    page_index_cache[pdf] = unique_pages_for_pairs(
                        extracted_pages,
                        sorted(set(targets_by_pdf[pdf])),
                    )
                except Exception as exc:  # case-level mapping evidence
                    page_index_cache[pdf] = exc
            cached = page_index_cache[pdf]
            if isinstance(cached, Exception):
                errors.append({"code": "VIOLIN_TEXT_FAILED", "message": str(cached)})
            else:
                match = cached.get((ag.canonical, snp.canonical))
                if isinstance(match, ViolinMatchError):
                    errors.append({"code": "VIOLIN_NOT_UNIQUE", "message": str(match)})
                elif isinstance(match, int):
                    page = match
                else:
                    errors.append({"code": "VIOLIN_NOT_UNIQUE", "message": "pair was not indexed"})

        shard = f"{ag.chrom}_{strand_token(strand)}"
        shard_counts[shard] += 1
        record: dict[str, Any] = {
            "schema_version": "1.0",
            "run_id": root.name,
            "figure_contract_id": str(config.get("workflow.figure_contract_id")),
            "gui_settle_contract_id": str(config.get("workflow.gui_settle_contract_id")),
            "workflow_config_fingerprint": config_fingerprint,
            "associations_sha256": association_sha,
            "association_row": int(row["association_row"]),
            "case_id": expected_case_id,
            "ag": {
                "raw": row["ag_site"],
                "chrom": ag.chrom,
                "source_start": ag.source_start,
                "source_end": ag.source_end,
                "start": ag.start,
                "end": ag.end,
            },
            "snp": {"raw": row["snp"], "chrom": snp.chrom, "position": snp.position, "ref": snp.ref, "alt": snp.alt},
            "strand": strand,
            "shard": shard,
            "statistics": {
                "n_total": _as_number(row.get("n_total", "")),
                "n_0": _as_number(row.get("n_0", "")),
                "n_1": _as_number(row.get("n_1", "")),
                "n_2": _as_number(row.get("n_2", "")),
                "eligible_n_0": _as_number(row.get("eligible_n_0", "")),
                "eligible_n_1": _as_number(row.get("eligible_n_1", "")),
                "eligible_n_2": _as_number(row.get("eligible_n_2", "")),
                "beta": _as_number(row.get("beta", "")),
                "abs_tvalue": _as_number(row.get("abs_tvalue", "")),
            },
            "windows": windows,
            "genotypes": normalized_groups,
            "violin": {
                "pdf": str(pdf),
                "page": page,
                "match_key": {"ag_site": ag.canonical, "snp": snp.canonical},
                "pdf_identity": (
                    {
                        "size": pdf.stat().st_size,
                        "mtime_ns": pdf.stat().st_mtime_ns,
                        "sha256": pdf_sha_cache[pdf],
                    }
                    if pdf.is_file()
                    else None
                ),
            },
            "genome": {
                "id": config.get("genome.id"),
                "display_name": str(config.get("genome.display_name")),
                "definition": str(config.path_value("genome.definition")),
                "definition_sha256": str(config.get("genome.definition_sha256") or ""),
                "fasta": str(config.path_value("genome.fasta")),
                "fai": str(config.path_value("genome.fai")),
                "cytoband": str(config.path_value("genome.cytoband")),
                "cytoband_sha256": str(config.get("genome.cytoband_sha256") or ""),
                "annotation": str(config.path_value("genome.annotation")),
                "annotation_version": str(config.get("genome.annotation_version", "unspecified")),
                "annotation_sha256": str(config.get("genome.annotation_sha256") or ""),
            },
            "preflight_warnings": warnings,
            "preflight_errors": errors,
        }
        resource_identity = {}
        for key in ("definition", "fasta", "fai", "cytoband", "annotation"):
            resource_path = Path(record["genome"][key])
            resource_identity[key] = (
                {"size": resource_path.stat().st_size, "mtime_ns": resource_path.stat().st_mtime_ns}
                if resource_path.is_file()
                else None
            )
        record["genome"]["resource_identity"] = resource_identity
        record["genome"]["resource_fingerprint"] = sha256_json(resource_identity)
        record["reference_context"] = _reference_context(
            Path(record["genome"]["fasta"]),
            Path(record["genome"]["fai"]),
            chrom=ag.chrom,
            start=ag.start,
            end=ag.end,
            strand=strand,
            fai_index=reference_fai_index,
        )
        record["input_fingerprint"] = sha256_json({key: value for key, value in record.items() if key != "input_fingerprint"})
        records.append(record)
        if errors:
            failed += 1

    records.sort(key=lambda item: item["association_row"])
    canonical_ids = [record["case_id"] for record in records]
    canonical_duplicates = sorted(case_id for case_id, count in Counter(canonical_ids).items() if count > 1)
    if canonical_duplicates:
        raise ValueError("duplicate canonical case_id values: " + ", ".join(canonical_duplicates[:10]))
    manifest_path = manifests / "case_manifest.jsonl"
    write_jsonl(manifest_path, records)
    shards = sorted(shard_counts)
    shard_path = manifests / "shards.tsv"
    write_tsv(
        shard_path,
        ["task_id", "shard", "case_count"],
        ({"task_id": index, "shard": shard, "case_count": shard_counts[shard]} for index, shard in enumerate(shards, 1)),
    )
    report = {
        "created_at": utc_now(),
        "run_root": str(root),
        "associations": str(association_path),
        "associations_sha256": association_sha,
        "associations_expected_sha256": expected_association_sha,
        "associations_snapshot": str(association_snapshot),
        "associations_snapshot_sha256": sha256_file(association_snapshot),
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "shards": str(shard_path),
        "case_count": len(records),
        "failed_preparation_count": failed,
        "shard_count": len(shards),
        "config": str(config.path),
        "config_fingerprint": config_fingerprint,
    }
    atomic_write_json(work / "prepare_report.json", report)
    return report
