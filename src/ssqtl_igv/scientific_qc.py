from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .config import WorkflowConfig
from .contracts import (
    DEFAULT_CLIENT_HEIGHT,
    DEFAULT_CLIENT_WIDTH,
    DEFAULT_LOCUS_FIELD_OCR_PSM,
    DEFAULT_LOCUS_FIELD_OCR_SCALE,
    DEFAULT_LOCUS_FIELD_OCR_WHITELIST,
    DESKTOP_LAYOUT_SCHEMA,
    FIGURE_CONTRACT_ID,
    GUI_SETTLE_CONTRACT_ID,
    IGV_LOCAL_ONLY_STARTUP_CONTRACT_ID,
    LOCUS_OCR_CONTRACT_ID,
    LOCUS_OCR_RESAMPLING,
    SCIENTIFIC_QC_SCHEMA,
)
from .parsing import parse_ag_site, parse_snp
from .selection import representative_order_key
from .utils import optional_text, sha256_file


EXPECTED_DOSAGE = {"0/0": 0, "0/1": 1, "1/1": 2}
SELECTION_LABELS = {"min", "q1", "median", "mean", "mean-nearest", "mean_nearest", "q3", "max", "all"}


def scientific_qc(
    case: dict[str, Any],
    *,
    batch_text: str,
    capture: dict[str, Any],
    layout: dict[str, Any],
    violin_qc: dict[str, Any],
    final_png_qc: dict[str, Any],
    config: WorkflowConfig,
) -> dict[str, Any]:
    """Validate scientific identity and control-plane evidence for one case.

    This layer deliberately does not claim that an association is biologically
    convincing from pixels alone.  It proves that the correct local resources,
    samples, locus, genotype ordering, and violin page reached the native IGV
    evidence figure, then enumerates the visual judgments that still require a
    human reviewer.
    """

    checks: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    def add(code: str, passed: bool, evidence: Any, *, fatal: bool = True) -> None:
        row = {"code": code, "status": "PASS" if passed else "FAIL", "fatal": fatal, "evidence": evidence}
        checks.append(row)

    ag = parse_ag_site(case["ag"]["raw"])
    snp = parse_snp(case["snp"]["raw"])
    window = case["windows"]["overview"]
    start, end = int(window["start"]), int(window["end"])
    anchors = [ag.start, ag.end, snp.position]
    add(
        "LOCUS_IDENTITY",
        ag.chrom == snp.chrom == window["chrom"] and all(start <= value <= end for value in anchors),
        {"ag": ag.canonical, "snp": snp.canonical, "window": window},
    )
    add("STRAND_VALID", case.get("strand") in {"+", "-"}, case.get("strand"))

    all_samples: list[dict[str, Any]] = []
    genotype_evidence: dict[str, Any] = {}
    genotype_pass = True
    for genotype, dosage in EXPECTED_DOSAGE.items():
        samples = sorted(
            case.get("genotypes", {}).get(genotype, []),
            key=representative_order_key,
        )
        all_samples.extend(samples)
        group_pass = 0 <= len(samples) <= 6
        labels = []
        for sample in samples:
            label = str(sample.get("selection_label", "")).strip().lower()
            labels.append(label)
            group_pass = group_pass and int(sample.get("dosage", -1)) == dosage
            group_pass = group_pass and float(sample.get("ratio", -1.0)) >= 0.0
            group_pass = group_pass and label in SELECTION_LABELS
        reported_count = case.get("statistics", {}).get(f"n_{dosage}")
        eligible_count = case.get("statistics", {}).get(f"eligible_n_{dosage}")
        if reported_count is not None:
            group_pass = group_pass and int(reported_count) >= len(samples)
        if eligible_count is not None:
            group_pass = group_pass and int(eligible_count) >= len(samples)
            group_pass = group_pass and len(samples) == min(6, int(eligible_count))
        if not samples:
            group_pass = group_pass and int(eligible_count or 0) == 0
        genotype_pass = genotype_pass and group_pass
        genotype_evidence[genotype] = {
            "dosage": dosage,
            "selected": len(samples),
            "reported": reported_count,
            "eligible": eligible_count,
            "selection_labels": labels,
        }
    sample_ids = [str(sample.get("sample_id", "")) for sample in all_samples]
    genotype_pass = (
        genotype_pass
        and bool(sample_ids)
        and len(sample_ids) == len(set(sample_ids))
        and all(sample_ids)
    )
    add("GENOTYPE_SAMPLE_CONTRACT", genotype_pass, genotype_evidence)
    empty_groups = [group for group, evidence in genotype_evidence.items() if evidence["selected"] == 0]
    if empty_groups:
        warnings.append(
            {
                "code": "EMPTY_GENOTYPE_GROUPS_RETAINED",
                "message": "the case is retained; IGV shows only genotype groups with eligible samples",
                "evidence": empty_groups,
            }
        )
    out_of_range = [
        {"sample_id": sample.get("sample_id"), "ratio": sample.get("ratio")}
        for sample in all_samples
        if float(sample.get("ratio", 0.0)) > 1.0
    ]
    if out_of_range:
        warnings.append(
            {
                "code": "RATIO_ABOVE_ONE",
                "message": "ratios are retained because the README hard filter is ratio >= 0",
                "evidence": out_of_range,
            }
        )

    expected_loads = [
        f"load {Path(sample['bam']).resolve(strict=False)} index={Path(sample['bai']).resolve(strict=False)}"
        for sample in all_samples
    ]
    observed_loads = [
        line
        for line in batch_text.splitlines()
        if line.startswith("load ") and " index=" in line
    ]
    add(
        "BAM_LOAD_IDENTITY",
        observed_loads == expected_loads and all(batch_text.count(line) == 1 for line in expected_loads),
        {
            "expected": expected_loads,
            "observed": observed_loads,
            "exact_order_match": observed_loads == expected_loads,
        },
    )
    default_style_forbidden = (
        "setTrackHeight",
        "colorBy ",
        "setDataRange",
        "preference CHART.SHOW_DATA_RANGE",
    )
    alignment_pass = (
        "preference SAM.SHOW_ALIGNMENT_TRACK true" in batch_text
        and "preference SAM.SHOW_ALIGNMENT_TRACK false" not in batch_text
        and re.search(r"(?m)^collapse\s*$", batch_text) is not None
        and all(command not in batch_text for command in default_style_forbidden)
    )
    add(
        "NATIVE_ALIGNMENT_COLLAPSE_HIDE_POLICY",
        alignment_pass,
        {
            "policy": capture.get("alignment_policy"),
            "alignment_disabled": "SAM.SHOW_ALIGNMENT_TRACK false" in batch_text,
            "forbidden_visual_commands": [command for command in default_style_forbidden if command in batch_text],
        },
    )

    annotation = str(Path(case["genome"]["annotation"]).resolve(strict=False))
    configured_annotation = str(config.path_value("genome.annotation").resolve(strict=False))
    annotation_version = str(case["genome"].get("annotation_version", ""))
    annotation_sha = optional_text(case["genome"].get("annotation_sha256")).lower()
    configured_sha = optional_text(config.get("genome.annotation_sha256")).lower()
    definition = str(Path(case["genome"]["definition"]).resolve(strict=False))
    display_name = str(case["genome"].get("display_name", ""))
    figure_contract = str(case.get("figure_contract_id", ""))
    igv_argv = [str(value) for value in capture.get("igv_argv", [])]
    batch_commands = [
        line.strip()
        for line in batch_text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    startup_preferences = capture.get("startup_preferences", {})
    startup_preferences_path = Path(str(capture.get("startup_preferences_path", ""))).resolve(
        strict=False
    )
    genome_server_registry = Path(
        str(capture.get("local_genome_server_registry", ""))
    ).resolve(strict=False)
    data_server_registry = Path(
        str(capture.get("local_data_server_registry", ""))
    ).resolve(strict=False)
    expected_startup_preferences = {
        "IGV.Bounds": (
            f"0,0,{int(config.get('desktop.screen_width', 1920))},"
            f"{int(config.get('desktop.screen_height', 2160))}"
        ),
        "DEFAULT_GENOME_KEY": definition,
        "AUTO_UPDATE_GENOMES": "false",
        "SHOW_GENOME_SERVER_WARNING": "false",
        "IGV.genome.sequence.dir": str(genome_server_registry),
        "MASTER_RESOURCE_FILE_KEY": str(data_server_registry),
    }
    startup_preferences_file_pass = (
        startup_preferences_path.is_file()
        and not startup_preferences_path.is_symlink()
        and sha256_file(startup_preferences_path)
        == capture.get("startup_preferences_sha256")
    )
    def argv_binds(flag: str, value: str) -> bool:
        return any(
            token == flag and index + 1 < len(igv_argv) and igv_argv[index + 1] == value
            for index, token in enumerate(igv_argv)
        )

    genome_id = str(config.get("genome.id"))
    configured_display_name = str(config.get("genome.display_name"))
    expected_registry = f"{configured_display_name}\t{definition}\t{genome_id}\n"
    local_registry_pass = (
        genome_server_registry.is_file()
        and not genome_server_registry.is_symlink()
        and sha256_file(genome_server_registry)
        == capture.get("local_genome_server_registry_sha256")
        and genome_server_registry.read_text(encoding="utf-8") == expected_registry
        and data_server_registry.is_file()
        and not data_server_registry.is_symlink()
        and sha256_file(data_server_registry)
        == capture.get("local_data_server_registry_sha256")
        and data_server_registry.read_text(encoding="utf-8").startswith("# local-only:")
        and argv_binds("--genomeServerURL", str(genome_server_registry))
        and argv_binds("--dataServerURL", str(data_server_registry))
    )
    runtime_startup_text = "\n".join(
        [batch_text, *igv_argv, *(str(value) for value in startup_preferences.values())]
    )
    remote_runtime_match = re.search(
        r"(?:https?|ftp|s3|gs)://",
        runtime_startup_text,
        re.IGNORECASE,
    )
    local_only_startup_pass = (
        capture.get("igv_local_only_startup_contract_id")
        == IGV_LOCAL_ONLY_STARTUP_CONTRACT_ID
        and startup_preferences == expected_startup_preferences
        and startup_preferences_file_pass
        and local_registry_pass
        and bool(batch_commands)
        and batch_commands[0] == f"genome {definition}"
        and remote_runtime_match is None
    )
    add(
        "IGV_LOCAL_ONLY_STARTUP",
        local_only_startup_pass,
        {
            "contract_id": capture.get("igv_local_only_startup_contract_id"),
            "batch_first_command": batch_commands[0] if batch_commands else None,
            "startup_preferences": startup_preferences,
            "startup_preferences_path": str(startup_preferences_path),
            "startup_preferences_sha256": capture.get("startup_preferences_sha256"),
            "startup_preferences_file_pass": startup_preferences_file_pass,
            "local_genome_server_registry": str(genome_server_registry),
            "local_data_server_registry": str(data_server_registry),
            "local_registry_pass": local_registry_pass,
            "remote_runtime_token_present": remote_runtime_match is not None,
        },
    )
    local_only = local_only_startup_pass
    cli_genome_bound = any(
        value == "--genome" and index + 1 < len(igv_argv) and igv_argv[index + 1] == definition
        for index, value in enumerate(igv_argv)
    )
    collapse_position = batch_text.find("collapse\n")
    annotation_position = batch_text.find(f"load {annotation}\n")
    genome_identity_pass = (
        figure_contract == FIGURE_CONTRACT_ID
        and str(case["genome"].get("id")) == genome_id
        and display_name == configured_display_name
        and f"genome {definition}" in batch_text
        and bool(batch_commands)
        and batch_commands[0] == f"genome {definition}"
        and Path(definition).suffix.lower() in {".json", ".genome"}
        and cli_genome_bound
    )
    add(
        "GENOME_GUI_IDENTITY",
        genome_identity_pass,
        {
            "id": case["genome"].get("id"),
            "display_name": display_name,
            "definition": definition,
            "cli_genome_bound": cli_genome_bound,
            "figure_contract_id": figure_contract,
        },
    )
    configured_version = str(config.get("genome.annotation_version"))
    annotation_identity_pass = (
        annotation == configured_annotation
        and annotation_version == configured_version
        and (not configured_sha or annotation_sha == configured_sha)
        and annotation_position >= 0
        and local_only
        and 0 <= collapse_position < annotation_position
    )
    add(
        "ANNOTATION_LOCAL_IDENTITY",
        annotation_identity_pass,
        {
            "genome": case["genome"].get("id"),
            "annotation": annotation,
            "version": annotation_version,
            "sha256": annotation_sha,
            "runtime_remote_url": not local_only,
        },
    )

    custom_annotation_tokens = ("events.bed", "ag_event.bed", "snp_event.bed", "AG event", "SNP event")
    add(
        "NO_CUSTOM_ANNOTATION_STRIP",
        all(token not in batch_text for token in custom_annotation_tokens),
        {"matched": [token for token in custom_annotation_tokens if token in batch_text]},
    )
    sequence_window_bp = end - start + 1
    sequence_pass = (
        sequence_window_bp <= int(config.get("scientific.max_sequence_window_bp", 200))
        and f"setSequenceStrand {case['strand']}" in batch_text
        and all(start <= value <= end for value in anchors)
    )
    add(
        "AG_SITE_REFERENCE_SEQUENCE_VIEW",
        sequence_pass,
        {
            "ag": ag.canonical,
            "strand": case["strand"],
            "window": window,
            "window_bp": sequence_window_bp,
        },
    )
    reference_context = case.get("reference_context", {})
    add(
        "AG_REFERENCE_CONTEXT_FETCHED",
        reference_context.get("available") is True,
        reference_context,
    )
    canonical_ag = reference_context.get("transcript_sequence") == "AG"
    add(
        "CANONICAL_AG_MOTIF",
        canonical_ag,
        reference_context,
        fatal=False,
    )
    if reference_context.get("available") is True and not canonical_ag:
        warnings.append(
            {
                "code": "NONCANONICAL_AG_REFERENCE_CONTEXT",
                "message": "the reference context is retained for visual review rather than treated as a rerun fault",
                "evidence": reference_context,
            }
        )

    expected_match = {"ag_site": ag.canonical, "snp": snp.canonical}
    match_key = case.get("violin", {}).get("match_key")
    add(
        "VIOLIN_EXACT_PAIR",
        match_key == expected_match and case.get("violin", {}).get("page") is not None and violin_qc.get("status") == "PASS",
        {"expected": expected_match, "observed": match_key, "page": case.get("violin", {}).get("page")},
    )

    window_value = capture.get("window", {})
    window_text = f"{window_value.get('title', '')}\n{window_value.get('wm_class', '')}"
    canvas = capture.get("canvas", {})
    screen_pass = (
        re.search(str(config.get("desktop.window_title_regex", r"\bIGV\b")), window_text, re.IGNORECASE)
        is not None
        and int(canvas.get("width", 0)) == int(config.get("desktop.screen_width", 1920))
        and int(canvas.get("height", 0)) == int(config.get("desktop.screen_height", 2160))
        and capture.get("geometry_verified") is True
    )
    add("NATIVE_IGV_WINDOW", screen_pass, {"window": window_value, "canvas": canvas})
    capture_mode = capture.get("capture_mode")
    root_evidence = capture.get("root_fallback_evidence")
    fallback_pass = (
        capture.get("root_screenshot_publishable") is False
        and capture_mode in {"window", "root_fallback_crop"}
        and (capture_mode != "root_fallback_crop" or capture.get("geometry_verified") is True)
        and (
            capture_mode != "root_fallback_crop"
            or (
                isinstance(root_evidence, dict)
                and root_evidence.get("status") == "PASS"
                and root_evidence.get("recomputable") is True
                and root_evidence.get("root_path") == capture.get("root_fallback_path")
                and root_evidence.get("cropped_client_sha256")
                == root_evidence.get("recomputed_crop_sha256")
            )
        )
    )
    add(
        "ROOT_FALLBACK_ONLY",
        fallback_pass,
        {
            "capture_mode": capture_mode,
            "root_path": capture.get("root_fallback_path"),
            "publishable": capture.get("root_screenshot_publishable"),
            "root_fallback_evidence": root_evidence,
        },
    )
    expected_locus = f"{window['chrom']}:{start}-{end}"

    def locus_ocr_pass(evidence: dict[str, Any]) -> bool:
        expected_normalized = re.sub(
            r"[\s,]+",
            "",
            expected_locus.lower().replace("−", "-").replace("–", "-").replace("—", "-"),
        )
        return (
            evidence.get("contract_id") == LOCUS_OCR_CONTRACT_ID
            and evidence.get("resampling") == LOCUS_OCR_RESAMPLING
            and int(evidence.get("scale", 0))
            == int(config.get("desktop.locus_field_ocr_scale", DEFAULT_LOCUS_FIELD_OCR_SCALE))
            and int(evidence.get("psm", 0))
            == int(config.get("desktop.locus_field_ocr_psm", DEFAULT_LOCUS_FIELD_OCR_PSM))
            and evidence.get("language") == str(config.get("desktop.locus_ocr_language", "eng"))
            and evidence.get("whitelist")
            == str(
                config.get(
                    "desktop.locus_field_ocr_whitelist",
                    DEFAULT_LOCUS_FIELD_OCR_WHITELIST,
                )
            )
            and evidence.get("match_mode") == "exact_normalized"
            and evidence.get("expected_locus") == expected_locus
            and evidence.get("expected_normalized") == expected_normalized
            and evidence.get("observed_normalized") == expected_normalized
            and evidence.get("matched") is True
        )

    gui_settle = capture.get("gui_settle", {})
    settle_required = int(config.get("desktop.toolbar_locus_stable_consecutive_frames", 3))
    settle_comparisons = list(gui_settle.get("comparisons", []))
    settle_tail = settle_comparisons[-settle_required:]
    settle_ocr = gui_settle.get("locus_ocr", {})
    gui_settle_pass = (
        case.get("gui_settle_contract_id") == GUI_SETTLE_CONTRACT_ID
        and capture.get("gui_settle_contract_id") == GUI_SETTLE_CONTRACT_ID
        and capture.get("expected_locus") == expected_locus
        and gui_settle.get("contract_id") == GUI_SETTLE_CONTRACT_ID
        and gui_settle.get("status") == "PASS"
        and float(gui_settle.get("delay_observed_seconds", 0.0))
        >= float(config.get("desktop.gui_settle_delay_seconds", 5.0))
        and int(gui_settle.get("observed_consecutive_pairs", 0)) >= settle_required
        and len(settle_tail) == settle_required
        and all(item.get("stable") is True for item in settle_tail)
        and all(float(item.get("mean_absolute_fraction", 1.0)) == 0.0 for item in settle_tail)
        and all(float(item.get("changed_pixel_fraction", 1.0)) == 0.0 for item in settle_tail)
        and locus_ocr_pass(settle_ocr)
    )
    add(
        "GUI_SETTLE_TOOLBAR_LOCUS_STABLE_READABLE",
        gui_settle_pass,
        gui_settle,
    )
    pixel_stability = capture.get("pixel_stability", {})
    toolbar_guard = pixel_stability.get("toolbar_locus_guard", {})
    render_required = int(config.get("desktop.stable_consecutive_frames", 3))
    render_comparisons = list(pixel_stability.get("comparisons", []))
    render_tail = render_comparisons[-render_required:]
    final_locus_ocr = toolbar_guard.get("final_locus_ocr", {})
    guarded_render_pass = (
        pixel_stability.get("status") == "PASS"
        and toolbar_guard.get("status") == "PASS"
        and toolbar_guard.get("contract_id") == GUI_SETTLE_CONTRACT_ID
        and int(pixel_stability.get("observed_consecutive_pairs", 0)) >= render_required
        and len(render_tail) == render_required
        and all(item.get("stable") is True for item in render_tail)
        and all(item.get("toolbar_locus", {}).get("stable") is True for item in render_tail)
        and all(
            float(item.get("toolbar_locus", {}).get("mean_absolute_fraction", 1.0)) == 0.0
            for item in render_tail
        )
        and all(
            float(item.get("toolbar_locus", {}).get("changed_pixel_fraction", 1.0)) == 0.0
            for item in render_tail
        )
        and locus_ocr_pass(final_locus_ocr)
    )
    add(
        "PIXEL_RENDER_STABLE",
        guarded_render_pass,
        pixel_stability,
    )

    layout_evidence = layout.get("evidence", {})
    client_width = int(config.get("desktop.screen_width", DEFAULT_CLIENT_WIDTH))
    client_height = int(config.get("desktop.screen_height", DEFAULT_CLIENT_HEIGHT))
    expected_final_width = client_width + int(config.get("compose.violin_panel_width", 720))
    layout_pass = (
        layout.get("schema_version") == DESKTOP_LAYOUT_SCHEMA
        and layout.get("figure_contract_id") == FIGURE_CONTRACT_ID
        and layout_evidence.get("native_igv_gui_preserved") is True
        and layout_evidence.get("client_resize_count") == 0
        and layout_evidence.get("client_crop_applied") is False
        and layout_evidence.get("client_post_capture_overlays") == 0
        and layout_evidence.get("external_header_present") is False
        and layout_evidence.get("divider_present") is False
        and layout_evidence.get("dosage_badges_present") is False
        and layout_evidence.get("left_origin") == [0, 0]
        and layout_evidence.get("left_size") == [client_width, client_height]
        and layout_evidence.get("left_pixel_identity") is True
        and layout_evidence.get("source_client_pixel_sha256")
        == layout_evidence.get("final_left_pixel_sha256")
        and int(layout_evidence.get("final_png_width", 0)) == expected_final_width
        and int(layout_evidence.get("final_png_height", 0)) == client_height
    )
    add("NATIVE_PIXEL_COMPOSITION", layout_pass, layout_evidence)
    add("FINAL_PNG_MECHANICAL_QC", final_png_qc.get("status") == "PASS", final_png_qc)

    failed = [check for check in checks if check["fatal"] and check["status"] != "PASS"]
    return {
        "schema_version": SCIENTIFIC_QC_SCHEMA,
        "case_id": case["case_id"],
        "status": "PASS" if not failed else "FAIL",
        "checks": checks,
        "warnings": warnings,
        "failed_codes": [check["code"] for check in failed],
        "manual_review_required": [
            "the untouched left region visibly contains the complete IGV menu, toolbar, configured genome, chromosome and locus controls; automated ROI OCR has already matched the expected locus",
            "the ideogram, coordinate ruler, native coverage/read-alignment/junction structure, and sequence bases are clear at original pixels",
            "the configured annotation track and transcript model are visibly present, and the reviewer has independently judged transcript/strand appropriateness; automation does not infer this from track loading alone",
            "the AG-site coordinates from the matched violin page can be located on the IGV ruler and the reference sequence visibly contains the expected splice-site context",
            "splice or junction evidence around the AG site is visible or its absence is directly judgeable",
            "all available dosage-group sample ordering matches the manifest and sample TSV without post-capture badges; any zero-sample group is explicitly recorded in scientific QC",
            "the violin page visibly names the same AG site and SNP",
            "no title banner, explanatory label, divider, annotation strip, overlay, crop, or resampling has altered the native IGV client",
        ],
        "decision": "REVIEW_PENDING" if not failed else "RERUN",
    }
