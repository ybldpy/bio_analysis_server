#!/usr/bin/env Rscript

suppressPackageStartupMessages({
    library(dada2)
})

print_usage <- function() {
    cat(
        paste0(
            "\nUsage:\n",
            "  Rscript amplicon16s_dada2.R \\\n",
            "    --r1 <R1.fastq.gz> \\\n",
            "    [--r2 <R2.fastq.gz>] \\\n",
            "    --sample-name <sample_name> \\\n",
            "    --output-dir <output_directory> \\\n",
            "    [--threads <integer>]\n\n",

            "Required arguments:\n",
            "  --r1            Forward FASTQ file\n",
            "  --sample-name   Sample name\n",
            "  --output-dir    Output directory\n\n",

            "Optional arguments:\n",
            "  --r2            Reverse FASTQ file; omit for single-end data\n",
            "  --threads       Number of threads, default: 1\n",
            "  --help          Show this help message\n\n"
        )
    )
}

parse_arguments <- function(args) {

    result <- list(
        r1 = NULL,
        r2 = NULL,
        sample_name = NULL,
        output_dir = NULL,
        threads = 1L
    )

    index <- 1L

    while (index <= length(args)) {

        argument <- args[[index]]

        if (argument %in% c("--help", "-h")) {
            print_usage()
            quit(status = 0)
        }

        if (index == length(args)) {
            stop("Missing value for argument: ", argument)
        }

        value <- args[[index + 1L]]

        if (argument == "--r1") {
            result$r1 <- value

        } else if (argument == "--r2") {
            result$r2 <- value

        } else if (argument == "--sample-name") {
            result$sample_name <- value

        } else if (argument == "--output-dir") {
            result$output_dir <- value

        } else if (argument == "--threads") {
            result$threads <- suppressWarnings(as.integer(value))

        } else {
            stop("Unknown argument: ", argument)
        }

        index <- index + 2L
    }

    result
}

validate_arguments <- function(config) {

    if (is.null(config$r1) || !nzchar(config$r1)) {
        stop("Required argument is missing: --r1")
    }

    if (is.null(config$sample_name) || !nzchar(config$sample_name)) {
        stop("Required argument is missing: --sample-name")
    }

    if (is.null(config$output_dir) || !nzchar(config$output_dir)) {
        stop("Required argument is missing: --output-dir")
    }

    if (
        is.na(config$threads) ||
        config$threads < 1L
    ) {
        stop("--threads must be a positive integer")
    }

    if (!file.exists(config$r1)) {
        stop("R1 FASTQ file does not exist: ", config$r1)
    }

    if (
        !is.null(config$r2) &&
        nzchar(config$r2) &&
        !file.exists(config$r2)
    ) {
        stop("R2 FASTQ file does not exist: ", config$r2)
    }
}

args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0L) {
    print_usage()
    quit(status = 1)
}

config <- parse_arguments(args)
validate_arguments(config)

r1_path <- normalizePath(
    config$r1,
    mustWork = TRUE
)

paired_end <- (
    !is.null(config$r2) &&
    nzchar(config$r2) &&
    config$r2 != "-"
)

r2_path <- NULL

if (paired_end) {
    r2_path <- normalizePath(
        config$r2,
        mustWork = TRUE
    )
}

sample_name <- config$sample_name
threads <- config$threads

safe_sample_name <- gsub(
    "[^A-Za-z0-9._-]",
    "_",
    sample_name
)

dir.create(
    config$output_dir,
    recursive = TRUE,
    showWarnings = FALSE
)

output_dir <- normalizePath(
    config$output_dir,
    mustWork = TRUE
)

filtered_dir <- file.path(
    output_dir,
    "filtered"
)

dir.create(
    filtered_dir,
    recursive = TRUE,
    showWarnings = FALSE
)

filtered_r1 <- file.path(
    filtered_dir,
    paste0(
        safe_sample_name,
        "_R1.filtered.fastq.gz"
    )
)

filtered_r2 <- file.path(
    filtered_dir,
    paste0(
        safe_sample_name,
        "_R2.filtered.fastq.gz"
    )
)

get_read_count <- function(dada_result) {
    sum(getUniques(dada_result))
}

set.seed(1)

message("DADA2 analysis started")
message("Sample name: ", sample_name)
message("R1: ", r1_path)

if (paired_end) {
    message("R2: ", r2_path)
}

message("Paired-end: ", paired_end)
message("Output directory: ", output_dir)
message("Threads: ", threads)
message(
    "DADA2 version: ",
    as.character(packageVersion("dada2"))
)

if (paired_end) {

    filter_result <- filterAndTrim(
        fwd = r1_path,
        filt = filtered_r1,
        rev = r2_path,
        filt.rev = filtered_r2,
        truncLen = c(0, 0),
        maxN = 0,
        maxEE = c(2, 2),
        truncQ = 2,
        rm.phix = TRUE,
        compress = TRUE,
        multithread = threads,
        verbose = TRUE
    )

    input_count <- as.numeric(
        filter_result[1, "reads.in"]
    )

    filtered_count <- as.numeric(
        filter_result[1, "reads.out"]
    )

    if (filtered_count <= 0) {
        stop("No reads remained after quality filtering")
    }

    error_forward <- learnErrors(
        filtered_r1,
        multithread = threads,
        randomize = TRUE,
        verbose = TRUE
    )

    error_reverse <- learnErrors(
        filtered_r2,
        multithread = threads,
        randomize = TRUE,
        verbose = TRUE
    )

    derep_forward <- derepFastq(
        filtered_r1,
        verbose = TRUE
    )

    derep_reverse <- derepFastq(
        filtered_r2,
        verbose = TRUE
    )

    dada_forward <- dada(
        derep_forward,
        err = error_forward,
        multithread = threads,
        verbose = TRUE
    )

    dada_reverse <- dada(
        derep_reverse,
        err = error_reverse,
        multithread = threads,
        verbose = TRUE
    )

    denoised_forward_count <- get_read_count(
        dada_forward
    )

    denoised_reverse_count <- get_read_count(
        dada_reverse
    )

    merged <- mergePairs(
        dada_forward,
        derep_forward,
        dada_reverse,
        derep_reverse,
        minOverlap = 12,
        maxMismatch = 0,
        verbose = TRUE
    )

    if (nrow(merged) == 0) {
        stop(
            paste(
                "No paired reads could be merged.",
                "Check read orientation, overlap length",
                "and preprocessing."
            )
        )
    }

    merged_count <- sum(
        merged$abundance
    )

    sequence_table <- makeSequenceTable(
        setNames(
            list(merged),
            sample_name
        )
    )

} else {

    filter_result <- filterAndTrim(
        fwd = r1_path,
        filt = filtered_r1,
        truncLen = 0,
        maxN = 0,
        maxEE = 2,
        truncQ = 2,
        rm.phix = TRUE,
        compress = TRUE,
        multithread = threads,
        verbose = TRUE
    )

    input_count <- as.numeric(
        filter_result[1, "reads.in"]
    )

    filtered_count <- as.numeric(
        filter_result[1, "reads.out"]
    )

    if (filtered_count <= 0) {
        stop("No reads remained after quality filtering")
    }

    error_forward <- learnErrors(
        filtered_r1,
        multithread = threads,
        randomize = TRUE,
        verbose = TRUE
    )

    derep_forward <- derepFastq(
        filtered_r1,
        verbose = TRUE
    )

    dada_forward <- dada(
        derep_forward,
        err = error_forward,
        multithread = threads,
        verbose = TRUE
    )

    denoised_forward_count <- get_read_count(
        dada_forward
    )

    denoised_reverse_count <- NA
    merged_count <- NA

    sequence_table <- makeSequenceTable(
        setNames(
            list(dada_forward),
            sample_name
        )
    )
}

if (ncol(sequence_table) == 0) {
    stop("No ASVs were inferred")
}

sequence_table_nochim <- removeBimeraDenovo(
    sequence_table,
    method = "consensus",
    multithread = threads,
    verbose = TRUE
)

if (
    ncol(sequence_table_nochim) == 0 ||
    sum(sequence_table_nochim) == 0
) {
    stop("No ASVs remained after chimera removal")
}

saveRDS(
    sequence_table_nochim,
    file.path(
        output_dir,
        "sequence_table.rds"
    )
)

asv_sequences <- colnames(
    sequence_table_nochim
)

asv_ids <- sprintf(
    "ASV%06d",
    seq_along(asv_sequences)
)

asv_table <- as.data.frame(
    sequence_table_nochim,
    check.names = FALSE
)

colnames(asv_table) <- asv_ids

asv_table <- cbind(
    sample = rownames(asv_table),
    asv_table
)

rownames(asv_table) <- NULL

write.table(
    asv_table,
    file = file.path(
        output_dir,
        "asv_table.tsv"
    ),
    sep = "\t",
    quote = FALSE,
    row.names = FALSE,
    col.names = TRUE
)

asv_sequence_table <- data.frame(
    asv_id = asv_ids,
    sequence = asv_sequences,
    stringsAsFactors = FALSE
)

write.table(
    asv_sequence_table,
    file = file.path(
        output_dir,
        "asv_sequences.tsv"
    ),
    sep = "\t",
    quote = FALSE,
    row.names = FALSE,
    col.names = TRUE
)

fasta_path <- file.path(
    output_dir,
    "representative_sequences.fasta"
)

fasta_connection <- file(
    fasta_path,
    open = "wt"
)

for (index in seq_along(asv_sequences)) {
    writeLines(
        c(
            paste0(">", asv_ids[[index]]),
            asv_sequences[[index]]
        ),
        fasta_connection
    )
}

close(fasta_connection)

track_table <- data.frame(
    sample = sample_name,
    input = input_count,
    filtered = filtered_count,
    denoised_forward = denoised_forward_count,
    denoised_reverse = denoised_reverse_count,
    merged = merged_count,
    non_chimeric = sum(sequence_table_nochim),
    asv_count = ncol(sequence_table_nochim),
    stringsAsFactors = FALSE
)

write.table(
    track_table,
    file = file.path(
        output_dir,
        "dada2_track.tsv"
    ),
    sep = "\t",
    quote = FALSE,
    row.names = FALSE,
    col.names = TRUE,
    na = ""
)

message("DADA2 analysis completed")
message("Input reads: ", input_count)
message("Filtered reads: ", filtered_count)
message(
    "Non-chimeric reads: ",
    sum(sequence_table_nochim)
)
message(
    "ASV count: ",
    ncol(sequence_table_nochim)
)