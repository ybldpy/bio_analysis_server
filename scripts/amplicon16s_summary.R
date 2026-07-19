suppressPackageStartupMessages({
    library(vegan)
})

options(warn = 1)

# ============================================================
# Command-line arguments
# ============================================================

print_usage <- function() {
    cat(
        paste0(
            "\nUsage:\n",
            "  Rscript amplicon16s_summary.R \\\n",
            "    --asv-table <asv_table.tsv> \\\n",
            "    --taxonomy-file <taxonomy.tsv> \\\n",
            "    --output-dir <output_directory>\n\n",

            "Required arguments:\n",
            "  --asv-table       DADA2 ASV count table\n",
            "  --taxonomy-file   ASV taxonomy table\n",
            "  --output-dir      Directory for summary output files\n\n",

            "Optional arguments:\n",
            "  --help, -h        Show this help message\n\n",

            "Expected ASV table format:\n",
            "  sample\\tASV000001\\tASV000002\\t...\n",
            "  sample01\\t1250\\t720\\t...\n\n",

            "Expected taxonomy table format:\n",
            "  asv_id\\tKingdom\\tPhylum\\tClass\\tOrder\\tFamily\\tGenus\n\n"
        )
    )
}

parse_arguments <- function(command_line) {
    config <- list(
        asv_table = NULL,
        taxonomy_file = NULL,
        output_dir = NULL
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

        if (argument_name == "--asv-table") {
            config$asv_table <- argument_value

        } else if (argument_name == "--taxonomy-file") {
            config$taxonomy_file <- argument_value

        } else if (argument_name == "--output-dir") {
            config$output_dir <- argument_value

        } else {
            stop("Unknown argument: ", argument_name)
        }

        index <- index + 2L
    }

    config
}

validate_arguments <- function(config) {
    required_arguments <- c(
        asv_table = "--asv-table",
        taxonomy_file = "--taxonomy-file",
        output_dir = "--output-dir"
    )

    for (field_name in names(required_arguments)) {
        value <- config[[field_name]]

        if (is.null(value) || !nzchar(value)) {
            stop(
                "Required argument is missing: ",
                required_arguments[[field_name]]
            )
        }
    }

    if (!file.exists(config$asv_table)) {
        stop(
            "ASV table does not exist: ",
            config$asv_table
        )
    }

    if (!file.exists(config$taxonomy_file)) {
        stop(
            "Taxonomy file does not exist: ",
            config$taxonomy_file
        )
    }
}

command_line <- commandArgs(trailingOnly = TRUE)

if (length(command_line) == 0L) {
    print_usage()
    quit(status = 1)
}

config <- parse_arguments(command_line)
validate_arguments(config)

asv_table_path <- normalizePath(
    config$asv_table,
    mustWork = TRUE
)

taxonomy_file_path <- normalizePath(
    config$taxonomy_file,
    mustWork = TRUE
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

message("16S summary analysis started")
message("ASV table: ", asv_table_path)
message("Taxonomy file: ", taxonomy_file_path)
message("Output directory: ", output_dir)
message("vegan version: ", as.character(packageVersion("vegan")))

# ============================================================
# Read and validate input
# ============================================================

asv_table <- read.delim(
    asv_table_path,
    header = TRUE,
    sep = "\t",
    check.names = FALSE,
    stringsAsFactors = FALSE,
    quote = "",
    comment.char = ""
)

taxonomy_table <- read.delim(
    taxonomy_file_path,
    header = TRUE,
    sep = "\t",
    check.names = FALSE,
    stringsAsFactors = FALSE,
    quote = "",
    comment.char = ""
)

if (nrow(asv_table) == 0L) {
    stop("ASV table contains no samples")
}

if (ncol(asv_table) < 2L) {
    stop(
        "ASV table must contain a sample column ",
        "and at least one ASV column"
    )
}

if (!"sample" %in% colnames(asv_table)) {
    stop(
        "ASV table must contain a column named: sample"
    )
}

if (!"asv_id" %in% colnames(taxonomy_table)) {
    stop(
        "Taxonomy table must contain a column named: asv_id"
    )
}

if (anyDuplicated(asv_table$sample)) {
    duplicate_samples <- unique(
        asv_table$sample[duplicated(asv_table$sample)]
    )

    stop(
        "Duplicate sample names in ASV table: ",
        paste(duplicate_samples, collapse = ", ")
    )
}

if (anyDuplicated(taxonomy_table$asv_id)) {
    duplicate_asvs <- unique(
        taxonomy_table$asv_id[
            duplicated(taxonomy_table$asv_id)
        ]
    )

    stop(
        "Duplicate ASV identifiers in taxonomy table: ",
        paste(duplicate_asvs, collapse = ", ")
    )
}

asv_columns <- setdiff(
    colnames(asv_table),
    "sample"
)

missing_taxonomy_asvs <- setdiff(
    asv_columns,
    taxonomy_table$asv_id
)

if (length(missing_taxonomy_asvs) > 0L) {
    warning(
        length(missing_taxonomy_asvs),
        " ASVs do not have taxonomy records; ",
        "they will be marked as unclassified"
    )
}

# Convert all ASV count columns to numeric.
count_matrix <- as.matrix(
    asv_table[, asv_columns, drop = FALSE]
)

storage.mode(count_matrix) <- "numeric"

if (anyNA(count_matrix)) {
    stop(
        "ASV table contains non-numeric or missing count values"
    )
}

if (any(!is.finite(count_matrix))) {
    stop(
        "ASV table contains non-finite count values"
    )
}

if (any(count_matrix < 0)) {
    stop(
        "ASV table contains negative count values"
    )
}

if (!isTRUE(all.equal(
    count_matrix,
    round(count_matrix)
))) {
    stop(
        "ASV counts must be integers"
    )
}

count_matrix <- round(count_matrix)

rownames(count_matrix) <- asv_table$sample

empty_samples <- rownames(count_matrix)[
    rowSums(count_matrix) == 0
]

if (length(empty_samples) > 0L) {
    stop(
        "Samples contain no ASV reads: ",
        paste(empty_samples, collapse = ", ")
    )
}

# ============================================================
# Helper functions
# ============================================================

write_tsv <- function(data, filename) {
    output_path <- file.path(
        output_dir,
        filename
    )

    write.table(
        data,
        file = output_path,
        sep = "\t",
        quote = FALSE,
        row.names = FALSE,
        col.names = TRUE,
        na = "",
        fileEncoding = "UTF-8"
    )

    message("Written: ", output_path)
}

normalize_taxonomy_value <- function(value) {
    value <- trimws(as.character(value))

    invalid <- (
        is.na(value) |
        value == "" |
        tolower(value) %in% c(
            "na",
            "unknown",
            "unassigned",
            "unclassified"
        )
    )

    value[invalid] <- NA_character_
    value
}

taxonomy_ranks <- c(
    "Kingdom",
    "Phylum",
    "Class",
    "Order",
    "Family",
    "Genus"
)

available_ranks <- taxonomy_ranks[
    taxonomy_ranks %in% colnames(taxonomy_table)
]

if (length(available_ranks) == 0L) {
    stop(
        "Taxonomy table contains none of the supported ranks: ",
        paste(taxonomy_ranks, collapse = ", ")
    )
}

for (rank_name in available_ranks) {
    taxonomy_table[[rank_name]] <-
        normalize_taxonomy_value(
            taxonomy_table[[rank_name]]
        )
}

# For missing lower ranks, preserve the nearest known parent.
# Example: missing Genus with known Family becomes
# "Unclassified_Lactobacillaceae".
build_taxon_label <- function(data, target_rank) {
    target_index <- match(
        target_rank,
        taxonomy_ranks
    )

    labels <- rep(
        NA_character_,
        nrow(data)
    )

    if (target_rank %in% colnames(data)) {
        labels <- normalize_taxonomy_value(
            data[[target_rank]]
        )
    }

    unresolved <- is.na(labels)

    if (
        any(unresolved) &&
        target_index > 1L
    ) {
        parent_ranks <- rev(
            taxonomy_ranks[
                seq_len(target_index - 1L)
            ]
        )

        for (parent_rank in parent_ranks) {
            if (!parent_rank %in% colnames(data)) {
                next
            }

            parent_value <- normalize_taxonomy_value(
                data[[parent_rank]]
            )

            usable <- (
                unresolved &
                !is.na(parent_value)
            )

            labels[usable] <- paste0(
                "Unclassified_",
                parent_value[usable]
            )

            unresolved <- is.na(labels)

            if (!any(unresolved)) {
                break
            }
        }
    }

    labels[is.na(labels)] <- "Unclassified"
    labels
}

# ============================================================
# Convert ASV count matrix to long format
# ============================================================

asv_long_list <- vector(
    "list",
    nrow(count_matrix)
)

for (sample_index in seq_len(nrow(count_matrix))) {
    sample_name <- rownames(count_matrix)[sample_index]
    sample_counts <- count_matrix[sample_index, ]

    sample_table <- data.frame(
        sample = sample_name,
        asv_id = colnames(count_matrix),
        count = as.numeric(sample_counts),
        stringsAsFactors = FALSE,
        check.names = FALSE
    )

    # Zero-count ASVs are not useful in single-sample abundance output.
    sample_table <- sample_table[
        sample_table$count > 0,
        ,
        drop = FALSE
    ]

    sample_total <- sum(sample_table$count)

    sample_table$relative_abundance <-
        sample_table$count / sample_total

    asv_long_list[[sample_index]] <- sample_table
}

asv_long <- do.call(
    rbind,
    asv_long_list
)

rownames(asv_long) <- NULL

# Add taxonomy without dropping ASVs that failed classification.
asv_abundance <- merge(
    asv_long,
    taxonomy_table,
    by = "asv_id",
    all.x = TRUE,
    sort = FALSE
)

# Restore a predictable column order.
taxonomy_output_columns <- intersect(
    taxonomy_ranks,
    colnames(asv_abundance)
)

asv_abundance <- asv_abundance[
    ,
    c(
        "sample",
        "asv_id",
        "count",
        "relative_abundance",
        taxonomy_output_columns
    ),
    drop = FALSE
]

asv_abundance <- asv_abundance[
    order(
        asv_abundance$sample,
        -asv_abundance$count,
        asv_abundance$asv_id
    ),
    ,
    drop = FALSE
]

write_tsv(
    asv_abundance,
    "asv_abundance.tsv"
)

# ============================================================
# Aggregate abundance at each taxonomy rank
# ============================================================

for (rank_name in available_ranks) {
    working_table <- asv_abundance

    working_table$taxon <- build_taxon_label(
        working_table,
        rank_name
    )

    abundance_table <- aggregate(
        count ~ sample + taxon,
        data = working_table,
        FUN = sum
    )

    total_by_sample <- aggregate(
        count ~ sample,
        data = abundance_table,
        FUN = sum
    )

    colnames(total_by_sample)[
        colnames(total_by_sample) == "count"
    ] <- "sample_total"

    abundance_table <- merge(
        abundance_table,
        total_by_sample,
        by = "sample",
        all.x = TRUE,
        sort = FALSE
    )

    abundance_table$relative_abundance <-
        abundance_table$count /
        abundance_table$sample_total

    abundance_table$sample_total <- NULL

    abundance_table <- abundance_table[
        order(
            abundance_table$sample,
            -abundance_table$count,
            abundance_table$taxon
        ),
        ,
        drop = FALSE
    ]

    output_filename <- paste0(
        tolower(rank_name),
        "_abundance.tsv"
    )

    write_tsv(
        abundance_table,
        output_filename
    )
}

# ============================================================
# Alpha diversity
# ============================================================

alpha_rows <- vector(
    "list",
    nrow(count_matrix)
)

for (sample_index in seq_len(nrow(count_matrix))) {
    sample_name <- rownames(count_matrix)[sample_index]

    counts <- as.numeric(
        count_matrix[sample_index, ]
    )

    counts <- counts[
        counts > 0
    ]

    total_reads <- sum(counts)

    observed_asvs <- unname(
        vegan::specnumber(counts)
    )

    shannon <- unname(
        vegan::diversity(
            counts,
            index = "shannon"
        )
    )

    simpson <- unname(
        vegan::diversity(
            counts,
            index = "simpson"
        )
    )

    inverse_simpson <- unname(
        vegan::diversity(
            counts,
            index = "invsimpson"
        )
    )

    richness_estimates <- vegan::estimateR(
        as.integer(counts)
    )

    chao1 <- unname(
        richness_estimates[["S.chao1"]]
    )

    ace <- unname(
        richness_estimates[["S.ACE"]]
    )

    pielou_evenness <- if (observed_asvs > 1L) {
        shannon / log(observed_asvs)
    } else {
        0
    }

    alpha_rows[[sample_index]] <- data.frame(
        sample = sample_name,
        total_reads = total_reads,
        observed_asvs = observed_asvs,
        chao1 = chao1,
        ace = ace,
        shannon = shannon,
        simpson = simpson,
        inverse_simpson = inverse_simpson,
        pielou_evenness = pielou_evenness,
        stringsAsFactors = FALSE
    )
}

alpha_diversity <- do.call(
    rbind,
    alpha_rows
)

rownames(alpha_diversity) <- NULL

write_tsv(
    alpha_diversity,
    "alpha_diversity.tsv"
)

# ============================================================
# Classification-rate summary
# ============================================================

classification_rows <- list()
classification_index <- 1L

for (sample_name in unique(asv_abundance$sample)) {
    sample_table <- asv_abundance[
        asv_abundance$sample == sample_name,
        ,
        drop = FALSE
    ]

    total_reads <- sum(
        sample_table$count
    )

    for (rank_name in available_ranks) {
        rank_values <- normalize_taxonomy_value(
            sample_table[[rank_name]]
        )

        classified_reads <- sum(
            sample_table$count[
                !is.na(rank_values)
            ]
        )

        classification_rows[[classification_index]] <-
            data.frame(
                sample = sample_name,
                rank = rank_name,
                total_reads = total_reads,
                classified_reads = classified_reads,
                unclassified_reads =
                    total_reads - classified_reads,
                classification_rate =
                    classified_reads / total_reads,
                stringsAsFactors = FALSE
            )

        classification_index <-
            classification_index + 1L
    }
}

classification_summary <- do.call(
    rbind,
    classification_rows
)

rownames(classification_summary) <- NULL

write_tsv(
    classification_summary,
    "classification_summary.tsv"
)

message("16S summary analysis completed")
message("Sample count: ", nrow(count_matrix))
message("ASV count: ", ncol(count_matrix))
message("Output directory: ", output_dir)