args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) stop("usage: build_ssqtl_assets_v3.R PROJECT_ROOT")

project <- normalizePath(args[[1]], mustWork = TRUE)
locus <- data.frame(
  sample_id = "sample-1",
  ratio = 0.25,
  stringsAsFactors = FALSE,
  check.names = FALSE
)
locus[["chrA.4_T.C"]] <- 0
loci <- list()
loci[["chrA:2-3"]] <- locus
saveRDS(loci, file.path(project, "rds", "AGratio_SNPgeno_pos_chrA_list.rds"))

pdf(
  file.path(project, "violin", "violin_plots_pos_chrA.pdf"),
  width = 8,
  height = 6,
  paper = "special"
)
plot.new()
text(0.5, 0.62, "chrA:2-3")
text(0.5, 0.48, "chrA.4_T.C")
text(0.5, 0.34, "Synthetic ssQTL violin fixture")
dev.off()
