package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics.TaxonomicBinMetrics;

/**
 * Converts Kraken2 per-sequence classifications for an assembly into
 * base-weighted taxonomic bins.
 */
public final class TaxonomicContigBinner {

    public static final String ASSIGNMENT_REPORT_FILE_NAME = "taxonomy.contig_assignments.tsv";
    public static final String MAIN_BIN_FASTA_FILE_NAME = "taxonomy.main_bin.fasta";

    private static final String RANK_SPECIES = "S";
    private static final String RANK_FAMILY = "F";
    private static final String RANK_UNCLASSIFIED = "U";
    private static final Pattern NAMED_TAX_ID_PATTERN = Pattern.compile(".*\\(taxid\\s+(\\d+)\\)\\s*$");

    private TaxonomicContigBinner() {
    }

    public static Result bin(
            Path assemblyFasta,
            Path krakenSequenceOutput,
            Path krakenReport,
            Path outputDirectory) throws IOException {
        requireRegularFile(assemblyFasta, "Assembly FASTA");
        requireRegularFile(krakenSequenceOutput, "Kraken2 sequence output");
        requireRegularFile(krakenReport, "Kraken2 report");
        if (outputDirectory == null) {
            throw new IOException("Taxonomy binning output directory must not be null");
        }

        Files.createDirectories(outputDirectory);

        Map<Integer, TaxonNode> taxonomy = parseReportTaxonomy(krakenReport);
        Map<String, SequenceAssignment> assignments = parseSequenceAssignments(krakenSequenceOutput);
        LinkedHashMap<String, FastaRecord> records = readFastaRecords(assemblyFasta);

        validateAssignmentIds(records, assignments, assemblyFasta, krakenSequenceOutput);

        Map<Integer, MutableAggregate> finalBins = new HashMap<>();
        Map<Integer, MutableAggregate> speciesBins = new HashMap<>();
        Map<Integer, MutableAggregate> familyBins = new HashMap<>();
        Map<String, ResolvedAssignment> resolvedByContig = new LinkedHashMap<>();

        long totalBases = 0L;
        long speciesAssignedBases = 0L;
        long speciesAssignedContigs = 0L;
        long unresolvedBases = 0L;
        long unresolvedContigs = 0L;
        long unclassifiedBases = 0L;
        long unclassifiedContigs = 0L;

        for (FastaRecord record : records.values()) {
            SequenceAssignment assignment = assignments.get(record.id);
            if (assignment.sequenceLength != record.length) {
                throw new IOException(
                        "Kraken2 sequence length does not match FASTA for " + record.id
                                + ": output=" + assignment.sequenceLength
                                + ", fasta=" + record.length);
            }

            totalBases = addExact(totalBases, record.length, "total assembly length");

            ResolvedAssignment resolved;
            if (!assignment.classified || assignment.taxId == 0) {
                TaxonRef unclassified = new TaxonRef(0, "Unclassified", RANK_UNCLASSIFIED);
                resolved = new ResolvedAssignment(assignment.taxId, null, unclassified, null);
                unclassifiedBases = addExact(unclassifiedBases, record.length, "unclassified length");
                unclassifiedContigs++;
            } else {
                TaxonNode assignedTaxon = taxonomy.get(assignment.taxId);
                if (assignedTaxon == null) {
                    throw new IOException(
                            "Kraken2 assigned taxid " + assignment.taxId
                                    + " to " + record.id
                                    + " but that taxid is absent from the report hierarchy");
                }

                TaxonRef finalBin = assignedTaxon.species == null
                        ? assignedTaxon.self
                        : assignedTaxon.species;
                resolved = new ResolvedAssignment(
                        assignment.taxId,
                        assignedTaxon.self,
                        finalBin,
                        assignedTaxon.family);

                if (assignedTaxon.species != null) {
                    speciesAssignedBases = addExact(
                            speciesAssignedBases,
                            record.length,
                            "species-assigned length");
                    speciesAssignedContigs++;
                    aggregate(speciesBins, assignedTaxon.species, record.length);
                } else {
                    unresolvedBases = addExact(unresolvedBases, record.length, "unresolved length");
                    unresolvedContigs++;
                }

                if (assignedTaxon.family != null) {
                    aggregate(familyBins, assignedTaxon.family, record.length);
                }
            }

            aggregate(finalBins, resolved.finalBin, record.length);
            resolvedByContig.put(record.id, resolved);
        }

        List<TaxonSummary> speciesSummaries = summaries(speciesBins, totalBases);
        List<TaxonSummary> familySummaries = summaries(familyBins, totalBases);
        List<TaxonSummary> allBinSummaries = summaries(finalBins, totalBases);
        TaxonSummary dominantSpecies = speciesSummaries.isEmpty() ? null : speciesSummaries.get(0);

        Path assignmentReportPath = outputDirectory.resolve(ASSIGNMENT_REPORT_FILE_NAME);
        writeAssignmentReport(assignmentReportPath, records, resolvedByContig);

        Path mainBinFastaPath = null;
        Integer dominantSpeciesFamilyTaxId = null;
        if (dominantSpecies != null) {
            Set<String> selectedContigIds = new HashSet<>();
            for (Map.Entry<String, ResolvedAssignment> entry : resolvedByContig.entrySet()) {
                if (entry.getValue().finalBin.taxId == dominantSpecies.taxId
                        && RANK_SPECIES.equals(entry.getValue().finalBin.rankCode)) {
                    selectedContigIds.add(entry.getKey());
                    TaxonRef family = entry.getValue().family;
                    if (family != null) {
                        if (dominantSpeciesFamilyTaxId != null
                                && dominantSpeciesFamilyTaxId.intValue() != family.taxId) {
                            throw new IOException(
                                    "Dominant species " + dominantSpecies.taxId
                                            + " resolves to conflicting family taxids");
                        }
                        dominantSpeciesFamilyTaxId = family.taxId;
                    }
                }
            }
            mainBinFastaPath = outputDirectory.resolve(MAIN_BIN_FASTA_FILE_NAME);
            writeSelectedFasta(assemblyFasta, mainBinFastaPath, selectedContigIds);
        }

        TaxonomyBinningMetrics metrics = new TaxonomyBinningMetrics();
        metrics.setTotalContigs(records.size());
        metrics.setTotalAssemblyBases(totalBases);
        metrics.setSpeciesAssignedContigs(speciesAssignedContigs);
        metrics.setSpeciesAssignedBases(speciesAssignedBases);
        metrics.setUnresolvedContigs(unresolvedContigs);
        metrics.setUnresolvedBases(unresolvedBases);
        metrics.setUnclassifiedContigs(unclassifiedContigs);
        metrics.setUnclassifiedBases(unclassifiedBases);
        metrics.setBins(allBinSummaries.stream().map(TaxonSummary::toMetrics).toList());
        if (dominantSpecies != null) {
            metrics.setDominantSpeciesTaxId(dominantSpecies.taxId);
            metrics.setDominantSpeciesName(dominantSpecies.name);
            metrics.setDominantSpeciesAssemblyFraction(dominantSpecies.assemblyFraction);
        }

        return new Result(
                metrics,
                speciesSummaries,
                familySummaries,
                mainBinFastaPath,
                assignmentReportPath,
                dominantSpeciesFamilyTaxId);
    }

    private static void requireRegularFile(Path path, String label) throws IOException {
        if (path == null || !Files.isRegularFile(path)) {
            throw new IOException(label + " does not exist or is not a regular file: " + path);
        }
    }

    private static Map<Integer, TaxonNode> parseReportTaxonomy(Path report) throws IOException {
        Map<Integer, TaxonNode> taxonomy = new HashMap<>();
        Deque<IndentedTaxon> ancestors = new ArrayDeque<>();

        try (BufferedReader reader = Files.newBufferedReader(report, StandardCharsets.UTF_8)) {
            String line;
            long lineNumber = 0L;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }

                String[] columns = line.split("\\t", 6);
                if (columns.length < 6) {
                    throw new IOException("Malformed Kraken2 report at " + report + ":" + lineNumber);
                }

                String rankCode = columns[3].trim();
                int taxId = parseInteger(columns[4], "taxid", report, lineNumber);
                String rawName = columns[5];
                int indentation = leadingWhitespace(rawName);
                String name = rawName.trim();
                if (name.isEmpty()) {
                    throw new IOException("Blank taxon name in Kraken2 report at " + report + ":" + lineNumber);
                }

                while (!ancestors.isEmpty() && ancestors.peek().indentation >= indentation) {
                    ancestors.pop();
                }

                TaxonNode parent = ancestors.isEmpty() ? null : ancestors.peek().node;
                TaxonRef self = new TaxonRef(taxId, name, rankCode);
                TaxonRef species = RANK_SPECIES.equals(rankCode)
                        ? self
                        : parent == null ? null : parent.species;
                TaxonRef family = RANK_FAMILY.equals(rankCode)
                        ? self
                        : parent == null ? null : parent.family;
                TaxonNode node = new TaxonNode(self, species, family);

                if (taxonomy.putIfAbsent(taxId, node) != null) {
                    throw new IOException(
                            "Duplicate taxid " + taxId + " in Kraken2 report " + report + ":" + lineNumber);
                }
                ancestors.push(new IndentedTaxon(indentation, node));
            }
        }

        if (taxonomy.isEmpty()) {
            throw new IOException("Kraken2 report contains no taxonomy rows: " + report);
        }
        return taxonomy;
    }

    private static Map<String, SequenceAssignment> parseSequenceAssignments(Path output) throws IOException {
        Map<String, SequenceAssignment> assignments = new LinkedHashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(output, StandardCharsets.UTF_8)) {
            String line;
            long lineNumber = 0L;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }

                String[] columns = firstFourColumns(line);
                if (columns == null) {
                    throw new IOException("Malformed Kraken2 sequence output at " + output + ":" + lineNumber);
                }

                boolean classified;
                if ("C".equals(columns[0])) {
                    classified = true;
                } else if ("U".equals(columns[0])) {
                    classified = false;
                } else {
                    throw new IOException(
                            "Invalid Kraken2 classification code at " + output + ":" + lineNumber
                                    + ": " + columns[0]);
                }

                String sequenceId = columns[1].trim();
                if (sequenceId.isEmpty()) {
                    throw new IOException("Blank sequence ID in Kraken2 output at " + output + ":" + lineNumber);
                }
                int taxId = parseTaxId(columns[2], output, lineNumber);
                long sequenceLength = parseSequenceLength(columns[3], output, lineNumber);

                SequenceAssignment previous = assignments.putIfAbsent(
                        sequenceId,
                        new SequenceAssignment(classified, taxId, sequenceLength));
                if (previous != null) {
                    throw new IOException(
                            "Duplicate sequence ID " + sequenceId
                                    + " in Kraken2 output " + output + ":" + lineNumber);
                }
            }
        }

        if (assignments.isEmpty()) {
            throw new IOException("Kraken2 sequence output contains no classifications: " + output);
        }
        return assignments;
    }

    /**
     * Extracts only the fields needed for binning. Kraken2's fifth field may be
     * extremely large for a long contig, so it must not be copied by a regular
     * expression split.
     */
    private static String[] firstFourColumns(String line) {
        int first = line.indexOf('\t');
        if (first < 0) {
            return null;
        }
        int second = line.indexOf('\t', first + 1);
        if (second < 0) {
            return null;
        }
        int third = line.indexOf('\t', second + 1);
        if (third < 0) {
            return null;
        }
        int fourth = line.indexOf('\t', third + 1);
        int fourthFieldEnd = fourth < 0 ? line.length() : fourth;
        return new String[] {
                line.substring(0, first),
                line.substring(first + 1, second),
                line.substring(second + 1, third),
                line.substring(third + 1, fourthFieldEnd)
        };
    }

    private static LinkedHashMap<String, FastaRecord> readFastaRecords(Path fasta) throws IOException {
        LinkedHashMap<String, FastaRecord> records = new LinkedHashMap<>();

        try (BufferedReader reader = Files.newBufferedReader(fasta, StandardCharsets.UTF_8)) {
            String line;
            String currentId = null;
            long currentLength = 0L;
            long lineNumber = 0L;

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.startsWith(">")) {
                    if (currentId != null) {
                        addFastaRecord(records, currentId, currentLength, fasta);
                    }
                    currentId = fastaId(line, fasta, lineNumber);
                    currentLength = 0L;
                } else {
                    if (line.isBlank()) {
                        continue;
                    }
                    if (currentId == null) {
                        throw new IOException(
                                "FASTA sequence occurs before the first header at " + fasta + ":" + lineNumber);
                    }
                    currentLength = addExact(
                            currentLength,
                            countSequenceCharacters(line),
                            "FASTA record length for " + currentId);
                }
            }

            if (currentId != null) {
                addFastaRecord(records, currentId, currentLength, fasta);
            }
        }

        if (records.isEmpty()) {
            throw new IOException("Assembly FASTA contains no records: " + fasta);
        }
        return records;
    }

    private static void addFastaRecord(
            Map<String, FastaRecord> records,
            String id,
            long length,
            Path fasta) throws IOException {
        if (length <= 0L) {
            throw new IOException("FASTA record is empty: " + id + " in " + fasta);
        }
        if (records.putIfAbsent(id, new FastaRecord(id, length)) != null) {
            throw new IOException("Duplicate FASTA record ID: " + id + " in " + fasta);
        }
    }

    private static String fastaId(String header, Path fasta, long lineNumber) throws IOException {
        String value = header.substring(1).trim();
        if (value.isEmpty()) {
            throw new IOException("Blank FASTA header at " + fasta + ":" + lineNumber);
        }
        int end = 0;
        while (end < value.length() && !Character.isWhitespace(value.charAt(end))) {
            end++;
        }
        return value.substring(0, end);
    }

    private static long countSequenceCharacters(String line) {
        long length = 0L;
        for (int i = 0; i < line.length(); i++) {
            if (!Character.isWhitespace(line.charAt(i))) {
                length++;
            }
        }
        return length;
    }

    private static void validateAssignmentIds(
            Map<String, FastaRecord> records,
            Map<String, SequenceAssignment> assignments,
            Path fasta,
            Path output) throws IOException {
        for (String recordId : records.keySet()) {
            if (!assignments.containsKey(recordId)) {
                throw new IOException(
                        "Assembly contig " + recordId + " from " + fasta
                                + " is absent from Kraken2 output " + output);
            }
        }
        for (String assignmentId : assignments.keySet()) {
            if (!records.containsKey(assignmentId)) {
                throw new IOException(
                        "Kraken2 output sequence " + assignmentId + " from " + output
                                + " is absent from assembly FASTA " + fasta);
            }
        }
    }

    private static void aggregate(Map<Integer, MutableAggregate> aggregates, TaxonRef taxon, long length)
            throws IOException {
        MutableAggregate aggregate = aggregates.computeIfAbsent(
                taxon.taxId,
                ignored -> new MutableAggregate(taxon));
        if (!aggregate.taxon.equals(taxon)) {
            throw new IOException("Conflicting taxonomy metadata for taxid " + taxon.taxId);
        }
        aggregate.contigCount++;
        aggregate.totalBases = addExact(aggregate.totalBases, length, "taxonomic bin length");
    }

    private static List<TaxonSummary> summaries(Map<Integer, MutableAggregate> aggregates, long totalBases) {
        return aggregates.values().stream()
                .map(aggregate -> new TaxonSummary(
                        aggregate.taxon.taxId,
                        aggregate.taxon.name,
                        aggregate.taxon.rankCode,
                        aggregate.contigCount,
                        aggregate.totalBases,
                        fraction(aggregate.totalBases, totalBases)))
                .sorted(Comparator
                        .comparingLong(TaxonSummary::getTotalBases).reversed()
                        .thenComparing(Comparator.comparingLong(TaxonSummary::getContigCount).reversed())
                        .thenComparingInt(TaxonSummary::getTaxId))
                .toList();
    }

    private static void writeAssignmentReport(
            Path output,
            Map<String, FastaRecord> records,
            Map<String, ResolvedAssignment> assignments) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                output,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writer.write("contig_id\tlength\tassigned_tax_id\tassigned_name\tassigned_rank"
                    + "\tbin_tax_id\tbin_name\tbin_rank");
            writer.newLine();
            for (FastaRecord record : records.values()) {
                ResolvedAssignment assignment = assignments.get(record.id);
                writer.write(record.id);
                writer.write('\t');
                writer.write(Long.toString(record.length));
                writer.write('\t');
                writer.write(Integer.toString(assignment.assignedTaxId));
                writer.write('\t');
                writer.write(assignment.assignedTaxon == null ? "Unclassified" : assignment.assignedTaxon.name);
                writer.write('\t');
                writer.write(assignment.assignedTaxon == null ? RANK_UNCLASSIFIED : assignment.assignedTaxon.rankCode);
                writer.write('\t');
                writer.write(Integer.toString(assignment.finalBin.taxId));
                writer.write('\t');
                writer.write(assignment.finalBin.name);
                writer.write('\t');
                writer.write(assignment.finalBin.rankCode);
                writer.newLine();
            }
        }
    }

    private static void writeSelectedFasta(Path input, Path output, Set<String> selectedIds) throws IOException {
        if (selectedIds.isEmpty()) {
            throw new IOException("Dominant taxonomy bin contains no contigs");
        }

        Set<String> writtenIds = new HashSet<>();
        try (BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8);
                BufferedWriter writer = Files.newBufferedWriter(
                        output,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING)) {
            String line;
            boolean writeCurrentRecord = false;
            long lineNumber = 0L;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.startsWith(">")) {
                    String id = fastaId(line, input, lineNumber);
                    writeCurrentRecord = selectedIds.contains(id);
                    if (writeCurrentRecord) {
                        writtenIds.add(id);
                    }
                }
                if (writeCurrentRecord) {
                    writer.write(line);
                    writer.newLine();
                }
            }
        }

        if (!writtenIds.equals(selectedIds)) {
            Set<String> missing = new HashSet<>(selectedIds);
            missing.removeAll(writtenIds);
            throw new IOException("Failed to materialize dominant taxonomy bin; missing contigs: " + missing);
        }
        if (Files.size(output) == 0L) {
            throw new IOException("Dominant taxonomy bin FASTA is empty: " + output);
        }
    }

    private static int parseTaxId(String value, Path path, long lineNumber) throws IOException {
        String trimmed = value.trim();
        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException ignored) {
            Matcher matcher = NAMED_TAX_ID_PATTERN.matcher(trimmed);
            if (matcher.matches()) {
                try {
                    return Integer.parseInt(matcher.group(1));
                } catch (NumberFormatException exception) {
                    throw new IOException("Invalid taxid at " + path + ":" + lineNumber + ": " + value, exception);
                }
            }
            throw new IOException("Invalid taxid at " + path + ":" + lineNumber + ": " + value);
        }
    }

    private static int parseInteger(String value, String label, Path path, long lineNumber) throws IOException {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException exception) {
            throw new IOException("Invalid " + label + " at " + path + ":" + lineNumber + ": " + value, exception);
        }
    }

    private static long parseSequenceLength(String value, Path path, long lineNumber) throws IOException {
        String[] parts = value.trim().split("\\|");
        long total = 0L;
        for (String part : parts) {
            try {
                long length = Long.parseLong(part);
                if (length < 0L) {
                    throw new NumberFormatException("negative length");
                }
                total = Math.addExact(total, length);
            } catch (ArithmeticException | NumberFormatException exception) {
                throw new IOException(
                        "Invalid sequence length at " + path + ":" + lineNumber + ": " + value,
                        exception);
            }
        }
        return total;
    }

    private static int leadingWhitespace(String value) {
        int count = 0;
        while (count < value.length() && Character.isWhitespace(value.charAt(count))) {
            count++;
        }
        return count;
    }

    private static long addExact(long left, long right, String label) throws IOException {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException exception) {
            throw new IOException(label + " overflow", exception);
        }
    }

    private static double fraction(long numerator, long denominator) {
        return denominator == 0L ? 0.0d : (double) numerator / denominator;
    }

    public static final class Result {

        private final TaxonomyBinningMetrics metrics;
        private final List<TaxonSummary> speciesBins;
        private final List<TaxonSummary> familyBins;
        private final Path mainBinFastaPath;
        private final Path assignmentReportPath;
        private final Integer dominantSpeciesFamilyTaxId;

        private Result(
                TaxonomyBinningMetrics metrics,
                List<TaxonSummary> speciesBins,
                List<TaxonSummary> familyBins,
                Path mainBinFastaPath,
                Path assignmentReportPath,
                Integer dominantSpeciesFamilyTaxId) {
            this.metrics = metrics;
            this.speciesBins = speciesBins;
            this.familyBins = familyBins;
            this.mainBinFastaPath = mainBinFastaPath;
            this.assignmentReportPath = assignmentReportPath;
            this.dominantSpeciesFamilyTaxId = dominantSpeciesFamilyTaxId;
        }

        public TaxonomyBinningMetrics getMetrics() {
            return metrics;
        }

        public List<TaxonSummary> getSpeciesBins() {
            return speciesBins;
        }

        public List<TaxonSummary> getFamilyBins() {
            return familyBins;
        }

        public Path getMainBinFastaPath() {
            return mainBinFastaPath;
        }

        public Path getAssignmentReportPath() {
            return assignmentReportPath;
        }

        public Integer getDominantSpeciesFamilyTaxId() {
            return dominantSpeciesFamilyTaxId;
        }
    }

    public static final class TaxonSummary {

        private final int taxId;
        private final String name;
        private final String rankCode;
        private final long contigCount;
        private final long totalBases;
        private final double assemblyFraction;

        private TaxonSummary(
                int taxId,
                String name,
                String rankCode,
                long contigCount,
                long totalBases,
                double assemblyFraction) {
            this.taxId = taxId;
            this.name = name;
            this.rankCode = rankCode;
            this.contigCount = contigCount;
            this.totalBases = totalBases;
            this.assemblyFraction = assemblyFraction;
        }

        public int getTaxId() {
            return taxId;
        }

        public String getName() {
            return name;
        }

        public String getRankCode() {
            return rankCode;
        }

        public long getContigCount() {
            return contigCount;
        }

        public long getTotalBases() {
            return totalBases;
        }

        public double getAssemblyFraction() {
            return assemblyFraction;
        }

        private TaxonomicBinMetrics toMetrics() {
            return new TaxonomicBinMetrics(
                    taxId,
                    name,
                    rankCode,
                    contigCount,
                    totalBases,
                    assemblyFraction);
        }
    }

    private static final class FastaRecord {

        private final String id;
        private final long length;

        private FastaRecord(String id, long length) {
            this.id = id;
            this.length = length;
        }
    }

    private static final class SequenceAssignment {

        private final boolean classified;
        private final int taxId;
        private final long sequenceLength;

        private SequenceAssignment(boolean classified, int taxId, long sequenceLength) {
            this.classified = classified;
            this.taxId = taxId;
            this.sequenceLength = sequenceLength;
        }
    }

    private static final class TaxonNode {

        private final TaxonRef self;
        private final TaxonRef species;
        private final TaxonRef family;

        private TaxonNode(TaxonRef self, TaxonRef species, TaxonRef family) {
            this.self = self;
            this.species = species;
            this.family = family;
        }
    }

    private static final class TaxonRef {

        private final int taxId;
        private final String name;
        private final String rankCode;

        private TaxonRef(int taxId, String name, String rankCode) {
            this.taxId = taxId;
            this.name = name;
            this.rankCode = rankCode;
        }

        @Override
        public boolean equals(Object value) {
            if (this == value) {
                return true;
            }
            if (!(value instanceof TaxonRef other)) {
                return false;
            }
            return taxId == other.taxId
                    && name.equals(other.name)
                    && rankCode.equals(other.rankCode);
        }

        @Override
        public int hashCode() {
            int result = Integer.hashCode(taxId);
            result = 31 * result + name.hashCode();
            result = 31 * result + rankCode.hashCode();
            return result;
        }
    }

    private static final class IndentedTaxon {

        private final int indentation;
        private final TaxonNode node;

        private IndentedTaxon(int indentation, TaxonNode node) {
            this.indentation = indentation;
            this.node = node;
        }
    }

    private static final class MutableAggregate {

        private final TaxonRef taxon;
        private long contigCount;
        private long totalBases;

        private MutableAggregate(TaxonRef taxon) {
            this.taxon = taxon;
        }
    }

    private static final class ResolvedAssignment {

        private final int assignedTaxId;
        private final TaxonRef assignedTaxon;
        private final TaxonRef finalBin;
        private final TaxonRef family;

        private ResolvedAssignment(
                int assignedTaxId,
                TaxonRef assignedTaxon,
                TaxonRef finalBin,
                TaxonRef family) {
            this.assignedTaxId = assignedTaxId;
            this.assignedTaxon = assignedTaxon;
            this.finalBin = finalBin;
            this.family = family;
        }
    }
}
