args <- commandArgs(trailingOnly = TRUE)

parse_args <- function(values) {
  result <- list()
  for (value in values) {
    if (!startsWith(value, "--") || !grepl("=", value, fixed = TRUE)) {
      stop(paste("arguments must use --name=value:", value))
    }
    parts <- strsplit(sub("^--", "", value), "=", fixed = TRUE)[[1]]
    result[[parts[[1]]]] <- paste(parts[-1], collapse = "=")
  }
  result
}

opt <- parse_args(args)
required <- c("associations", "rds_dir", "bam_lookup", "cases_out", "samples_out")
missing <- required[!required %in% names(opt)]
if (length(missing) > 0) stop(paste("missing arguments:", paste(missing, collapse = ", ")))
option <- function(name, default) if (name %in% names(opt)) opt[[name]] else default
csv_option <- function(name, default) {
  value <- option(name, paste(default, collapse = ","))
  trimws(strsplit(value, ",", fixed = TRUE)[[1]])
}
ag_column <- option("ag_column", "AG_site")
snp_column <- option("snp_column", "SNP")
strand_column <- option("strand_column", "strand")
rds_filename_template <- option("rds_filename_template", "AGratio_SNPgeno_{strand_token}_{chrom}_list.rds")
locus_sample_columns <- csv_option("locus_sample_columns", c("sample_id"))
ratio_column <- option("ratio_column", "ratio")
bam_lookup_id_columns <- csv_option("bam_lookup_id_columns", c("sample_id"))
bam_lookup_path_columns <- csv_option("bam_lookup_path_columns", c("directory", "bam", "bam_path", "path"))
bam_suffixes <- csv_option("bam_suffixes", c(".bam"))

normalize_strand <- function(value) {
  value <- trimws(gsub("−|–", "-", as.character(value)))
  if (value %in% c("+", "pos", "positive", "1", "+1")) return("+")
  if (value %in% c("-", "neg", "negative", "-1")) return("-")
  stop(paste("unsupported strand:", value))
}

strand_token <- function(value) if (normalize_strand(value) == "+") "pos" else "neg"

case_id_from <- function(ag, snp) {
  ag_token <- gsub("[:-]", "_", ag)
  snp_token <- gsub("[.]", "_", snp)
  paste0("AG_", ag_token, "__SNP_", snp_token)
}

normalize_genotype <- function(value) {
  value <- trimws(as.character(value))
  value <- gsub("\\|", "/", value)
  if (value %in% c("0", "0.0", "0/0")) return(c("0/0", "0"))
  if (value %in% c("1", "1.0", "0/1", "1/0")) return(c("0/1", "1"))
  if (value %in% c("2", "2.0", "1/1")) return(c("1/1", "2"))
  c(NA_character_, NA_character_)
}

regex_escape <- function(value) gsub("([][{}()+*^$|\\?.])", "\\\\\\1", value)

resolve_bam <- function(sample_id, lookup_path) {
  if (is.na(lookup_path) || lookup_path == "") return(NA_character_)
  lookup_path <- path.expand(lookup_path)
  if (grepl("[.]bam$", lookup_path, ignore.case = TRUE)) return(lookup_path)
  direct <- file.path(lookup_path, paste0(sample_id, bam_suffixes))
  hits <- direct[file.exists(direct)]
  if (length(hits) > 0) return(hits[[1]])
  if (dir.exists(lookup_path)) {
    candidates <- list.files(
      lookup_path,
      pattern = paste0(regex_escape(sample_id), ".*[.]bam$"),
      full.names = TRUE,
      recursive = FALSE,
      ignore.case = TRUE
    )
    candidates <- sort(unique(candidates))
    if (length(candidates) == 1) return(candidates[[1]])
    if (length(candidates) > 1) return(NA_character_)
  }
  direct[[1]]
}

resolve_bai <- function(bam) {
  if (is.na(bam) || bam == "") return(NA_character_)
  candidates <- c(paste0(bam, ".bai"), sub("[.]bam$", ".bai", bam, ignore.case = TRUE))
  hits <- candidates[file.exists(candidates)]
  if (length(hits) > 0) return(hits[[1]])
  candidates[[1]]
}

clean_group <- function(group) {
  group <- group[is.finite(group$ratio) & group$ratio >= 0, , drop = FALSE]
  if (nrow(group) == 0) return(group)
  group <- group[order(group$sample_id, group$ratio), , drop = FALSE]
  duplicate_keys <- duplicated(group[, c("sample_id", "ratio", "genotype")])
  group <- group[!duplicate_keys, , drop = FALSE]
  split_rows <- split(group, group$sample_id)
  conflicts <- vapply(
    split_rows,
    function(item) length(unique(item$ratio)) > 1 || length(unique(item$genotype)) > 1,
    logical(1)
  )
  if (any(conflicts)) {
    stop("sample ID has conflicting ratio or genotype rows")
  }
  group <- group[!duplicated(group$sample_id), , drop = FALSE]
  group
}

select_group <- function(group) {
  group <- clean_group(group)
  if (nrow(group) == 0) return(group)
  if (nrow(group) <= 6) {
    group$selection_label <- "all"
    return(group)
  }
  values <- group$ratio
  targets <- c(
    min = min(values),
    max = max(values),
    median = median(values),
    q1 = unname(quantile(values, 0.25, type = 7)),
    q3 = unname(quantile(values, 0.75, type = 7)),
    mean = mean(values)
  )
  remaining <- seq_len(nrow(group))
  chosen <- integer()
  labels <- character()
  for (label in names(targets)) {
    ordered <- remaining[order(abs(group$ratio[remaining] - targets[[label]]), group$sample_id[remaining])]
    pick <- ordered[[1]]
    chosen <- c(chosen, pick)
    labels <- c(labels, label)
    remaining <- setdiff(remaining, pick)
  }
  result <- group[chosen, , drop = FALSE]
  result$selection_label <- labels
  result
}

associations <- read.csv(opt$associations, stringsAsFactors = FALSE, check.names = FALSE)
required_columns <- c(ag_column, snp_column, strand_column)
if (!all(required_columns %in% names(associations))) {
  stop(paste("association table missing:", paste(setdiff(required_columns, names(associations)), collapse = ", ")))
}

bam_lookup <- read.csv(opt$bam_lookup, stringsAsFactors = FALSE, check.names = FALSE)
id_column <- intersect(bam_lookup_id_columns, names(bam_lookup))
path_column <- intersect(bam_lookup_path_columns, names(bam_lookup))
if (length(id_column) == 0 || length(path_column) == 0) stop("BAM lookup lacks an ID or path column")
id_column <- id_column[[1]]
path_column <- path_column[[1]]
lookup_split <- split(as.character(bam_lookup[[path_column]]), as.character(bam_lookup[[id_column]]))
lookup_conflicts <- vapply(lookup_split, function(paths) length(unique(paths)) > 1, logical(1))
if (any(lookup_conflicts)) stop("BAM lookup contains sample IDs mapped to multiple paths")
bam_lookup <- bam_lookup[!duplicated(as.character(bam_lookup[[id_column]])), , drop = FALSE]
bam_map <- setNames(as.character(bam_lookup[[path_column]]), as.character(bam_lookup[[id_column]]))

case_rows <- vector("list", nrow(associations))
sample_rows <- list()
sample_cursor <- 1L
current_cache_key <- ""
current_locus_list <- NULL
sort_chrom <- sub(":.*$", "", as.character(associations[[ag_column]]))
sort_strand <- vapply(
  associations[[strand_column]],
  function(value) tryCatch(strand_token(value), error = function(e) "invalid"),
  character(1)
)
processing_order <- order(sort_chrom, sort_strand, seq_len(nrow(associations)))

get_column <- function(row, name, default = "") {
  if (name %in% names(row)) as.character(row[[name]][[1]]) else default
}

for (index in processing_order) {
  row <- associations[index, , drop = FALSE]
  ag_site <- as.character(row[[ag_column]][[1]])
  snp <- as.character(row[[snp_column]][[1]])
  strand <- tryCatch(normalize_strand(row[[strand_column]][[1]]), error = function(e) NA_character_)
  chrom <- sub(":.*$", "", ag_site)
  token <- if (!is.na(strand)) strand_token(strand) else "invalid"
  shard <- paste0(chrom, "_", token)
  case_id <- case_id_from(ag_site, snp)
  error_code <- ""
  error_message <- ""
  eligible_counts <- c("0/0" = 0L, "0/1" = 0L, "1/1" = 0L)

  tryCatch({
    if (is.na(strand)) stop("invalid strand")
    rds_name <- gsub("{strand_token}", token, rds_filename_template, fixed = TRUE)
    rds_name <- gsub("{strand}", strand, rds_name, fixed = TRUE)
    rds_name <- gsub("{chrom}", chrom, rds_name, fixed = TRUE)
    rds_path <- file.path(opt$rds_dir, rds_name)
    cache_key <- paste0(token, "_", chrom)
    if (cache_key != current_cache_key) {
      if (!file.exists(rds_path)) stop(paste("RDS missing:", rds_path))
      current_locus_list <- readRDS(rds_path)
      current_cache_key <- cache_key
    }
    locus_list <- current_locus_list
    if (sum(names(locus_list) == ag_site) != 1) stop("AG site must match exactly one RDS entry")
    locus <- locus_list[[ag_site]]
    if (sum(names(locus) == snp) != 1) stop("target SNP column must match exactly one AG-site column")
    sample_col <- intersect(locus_sample_columns, names(locus))
    if (length(sample_col) == 0 || !ratio_column %in% names(locus)) stop("locus lacks configured sample ID or ratio column")
    sample_col <- sample_col[[1]]
    normalized <- lapply(locus[[snp]], normalize_genotype)
    genotype <- vapply(normalized, function(x) x[[1]], character(1))
    dosage <- vapply(normalized, function(x) x[[2]], character(1))
    working <- data.frame(
      sample_id = as.character(locus[[sample_col]]),
      ratio = suppressWarnings(as.numeric(locus[[ratio_column]])),
      genotype = genotype,
      dosage = dosage,
      stringsAsFactors = FALSE
    )
    working <- working[!is.na(working$sample_id) & nzchar(working$sample_id), , drop = FALSE]
    working <- working[!is.na(working$genotype), , drop = FALSE]
    split_working <- split(working, working$sample_id)
    cross_conflicts <- vapply(
      split_working,
      function(item) length(unique(item$ratio)) > 1 || length(unique(item$genotype)) > 1,
      logical(1)
    )
    if (any(cross_conflicts)) stop("sample ID has conflicting ratio or genotype rows across the case")
    for (group_name in c("0/0", "0/1", "1/1")) {
      cleaned <- clean_group(working[working$genotype == group_name, , drop = FALSE])
      eligible_counts[[group_name]] <- nrow(cleaned)
      selected <- select_group(cleaned)
      if (nrow(selected) == 0) next
      selected$case_id <- case_id
      selected$bam <- vapply(selected$sample_id, function(id) {
        lookup_path <- if (id %in% names(bam_map)) bam_map[[id]] else NA_character_
        resolve_bam(id, lookup_path)
      }, character(1))
      selected$bai <- vapply(selected$bam, resolve_bai, character(1))
      selected$bai_fresh <- vapply(seq_len(nrow(selected)), function(i) {
        if (!file.exists(selected$bam[[i]]) || !file.exists(selected$bai[[i]])) return(NA_character_)
        if (file.info(selected$bai[[i]])$mtime >= file.info(selected$bam[[i]])$mtime) "true" else "false"
      }, character(1))
      sample_rows[[sample_cursor]] <- selected[, c(
        "case_id", "genotype", "dosage", "sample_id", "ratio", "selection_label", "bam", "bai", "bai_fresh"
      )]
      sample_cursor <- sample_cursor + 1L
    }
  }, error = function(e) {
    error_code <<- "R_PREPARE_FAILED"
    error_message <<- conditionMessage(e)
  })

  case_rows[[index]] <- data.frame(
    association_row = index,
    case_id = case_id,
    ag_site = ag_site,
    snp = snp,
    strand = ifelse(is.na(strand), as.character(row[[strand_column]][[1]]), strand),
    chrom = chrom,
    shard = shard,
    n_total = get_column(row, "n_total"),
    n_0 = get_column(row, "n_0"),
    n_1 = get_column(row, "n_1"),
    n_2 = get_column(row, "n_2"),
    eligible_n_0 = eligible_counts[["0/0"]],
    eligible_n_1 = eligible_counts[["0/1"]],
    eligible_n_2 = eligible_counts[["1/1"]],
    beta = get_column(row, "Beta"),
    abs_tvalue = get_column(row, "abs_Tvalue"),
    error_code = error_code,
    error_message = gsub("[\t\r\n]", " ", error_message),
    stringsAsFactors = FALSE
  )
}

cases <- do.call(rbind, case_rows)
if (length(sample_rows) > 0) {
  samples <- do.call(rbind, sample_rows)
} else {
  samples <- data.frame(
    case_id = character(), genotype = character(), dosage = character(), sample_id = character(),
    ratio = numeric(), selection_label = character(), bam = character(), bai = character(), bai_fresh = character(),
    stringsAsFactors = FALSE
  )
}

dir.create(dirname(opt$cases_out), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(opt$samples_out), recursive = TRUE, showWarnings = FALSE)
write.table(cases, opt$cases_out, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
write.table(samples, opt$samples_out, sep = "\t", row.names = FALSE, quote = FALSE, na = "")
