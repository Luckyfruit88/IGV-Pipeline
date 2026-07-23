args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) stop("usage: build_r_fixture.R OUTPUT_RDS")

output <- normalizePath(dirname(args[[1]]), mustWork = TRUE)
output <- file.path(output, basename(args[[1]]))
locus <- data.frame(
  sample_id = "sample-1",
  ratio = 0.25,
  stringsAsFactors = FALSE,
  check.names = FALSE
)
locus[["chrA.4_T.C"]] <- 0
loci <- list()
loci[["chrA:2-3"]] <- locus
saveRDS(loci, output)
