#!/usr/bin/env Rscript

full_args <- commandArgs(trailingOnly = FALSE)
file_args <- grep("^--file=", full_args, value = TRUE)
if (length(file_args) != 1) stop("cannot identify fixed ssQTL R wrapper location")
wrapper <- normalizePath(sub("^--file=", "", file_args[[1]]), mustWork = TRUE)
implementation <- file.path(dirname(wrapper), "prepare_cases_implementation.R")
if (!file.exists(implementation) || file.info(implementation)$isdir) {
  stop(paste("fixed R preparation implementation is missing:", implementation))
}
source(implementation, local = globalenv(), chdir = FALSE)
