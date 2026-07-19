#!/usr/bin/env Rscript

suppressPackageStartupMessages({
    library(dada2)
    library(Biostrings)
})

print_usage <- function() {
    cat(
        paste0(
            "\nUsage:\n",
            "  Rscript amplicon16s_taxonomy.R \\\n",
            "    --asv-fasta <representative_sequences.fasta> \\\n",
            "    --silva-train-set <silva_train_set.fa.gz> \\\n",
            "    --output-file <taxonomy.tsv> \\\n",
            "    [--threads <integer>]\n\n",

            "Required arguments:\n",
            "  --asv-fasta        Representative ASV FASTA file\n",
            "  --silva-train-set   SILVA DADA2 taxonomy training set\n",
            "  --output-file       Output taxonomy TSV file\n\n",

            "Optional arguments:\n",
            "  --threads           Number of threads, default: 1\n",
            "  --help              Show this help message\n\n"
        )
    )
}

parse_arguments <- function(command_line) {

    config <- list(
        asv_fasta = NULL,
        silva_train_set = NULL,
        output_file = NULL,
        threads = 1L
    )

    index <- 1L

    while (index <= length(command_line)) {

        argument_name <- command_line[[index]]

        if (argument_name %in% c("--help", "-h")) {
            print_usage()
            quit(status = 0)
        }

        if (index == length(command_line)) {
            stop("Missing value for argument: ", argument_name)
        }

        argument_value <- command_line[[index + 1L]]

        if (argument_name == "--asv-fasta") {
            config$asv_fasta <- argument_value

        } else if (argument_name == "--silva-train-set") {
            config$silva_train_set <- argument_value

        } else if (argument_name == "--output-file") {
            config$output_file <- argument_value

        } else if (argument_name == "--threads") {
            config$threads <- suppressWarnings(
                as.integer(argument_value)
            )

        } else {
            stop("Unknown argument: ", argument_name)
        }

        index <- index + 2L
    }

    config
}

validate_arguments <- function(config) {

    if (
        is.null(config$asv_fasta) ||
        !nzchar(config$asv_fasta)
    ) {
        stop("Required argument is missing: --asv-fasta")
    }

    if (
        is.null(config$silva_train_set) ||
        !nzchar(config$silva_train_set)
    ) {
        stop("Required argument is missing: --silva-train-set")
    }

    if (
        is.null(config$output_file) ||
        !nzchar(config$output_file)
    ) {
        stop("Required argument is missing: --output-file")
    }

    if (!file.exists(config$asv_fasta)) {
        stop(
            "ASV FASTA file does not exist: ",
            config$asv_fasta
        )
    }

    if (!file.exists(config$silva_train_set)) {
        stop(
            "SILVA training set does not exist: ",
            config$silva_train_set
        )
    }

    if (
        is.na(config$threads) ||
        config$threads < 1L
    ) {
        stop("--threads must be a positive integer")
    }
}

command_line <- commandArgs(trailingOnly = TRUE)

if (length(command_line) == 0L) {
    print_usage()
    quit(status = 1)
}

config <- parse_arguments(command_line)
validate_arguments(config)

asv_fasta_path <- normalizePath(
    config$asv_fasta,
    mustWork = TRUE
)

silva_train_set_path <- normalizePath(
    config$silva_train_set,
    mustWork = TRUE
)

output_parent <- dirname(config$output_file)

dir.create(
    output_parent,
    recursive = TRUE,
    showWarnings = FALSE
)

output_file_path <- normalizePath(
    config$output_file,
    mustWork = FALSE
)

message("16S taxonomy assignment started")
message("ASV FASTA: ", asv_fasta_path)
message("SILVA training set: ", silva_train_set_path)
message("Output file: ", output_file_path)
message("Threads: ", config$threads)
message("DADA2 version: ", packageVersion("dada2"))

asv_dna_set <- readDNAStringSet(
    asv_fasta_path
)

if (length(asv_dna_set) == 0L) {
    stop("No ASV sequences were found in the FASTA file")
}

asv_ids <- names(asv_dna_set)
asv_sequences <- as.character(asv_dna_set)

if (
    is.null(asv_ids) ||
    any(!nzchar(asv_ids))
) {
    stop("One or more ASV sequences do not have a FASTA identifier")
}

taxonomy_matrix <- assignTaxonomy(
    seqs = asv_sequences,
    refFasta = silva_train_set_path,
    multithread = config$threads,
    tryRC = TRUE,
    verbose = TRUE
)

taxonomy_table <- data.frame(
    asv_id = asv_ids,
    taxonomy_matrix,
    check.names = FALSE,
    stringsAsFactors = FALSE
)

write.table(
    taxonomy_table,
    file = output_file_path,
    sep = "\t",
    quote = FALSE,
    row.names = FALSE,
    col.names = TRUE,
    na = ""
)

message("16S taxonomy assignment completed")
message("ASV count: ", nrow(taxonomy_table))
message("Output: ", output_file_path)