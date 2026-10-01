package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parses minimap2 PAF output into base-level statistics for reference
 * selection.
 */
public final class PafAlignmentStatsParser {

    public record AlignmentStats(
            double sequenceIdentity,
            double referenceCoverage,
            long alignedReferenceBases,
            long referenceBases,
            long matchingBases,
            long alignmentBlockBases,
            int alignmentCount) {
    }

    private record Interval(long start, long end) {
    }

    private PafAlignmentStatsParser() {
    }

    public static AlignmentStats parse(Path pafPath, Path referenceFastaPath) throws IOException {
        Map<String, Long> referenceLengths = readFastaRecordLengths(referenceFastaPath);
        long referenceBases = sumReferenceBases(referenceLengths, referenceFastaPath);

        if (!Files.isRegularFile(pafPath)) {
            throw new IOException("PAF output does not exist: " + pafPath);
        }

        Map<String, List<Interval>> intervalsByReference = new LinkedHashMap<>();
        long matchingBases = 0L;
        long alignmentBlockBases = 0L;
        int alignmentCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(pafPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }

                String[] fields = line.split("\\t", -1);
                if (fields.length < 12) {
                    throw invalidPafRow(pafPath, lineNumber, "expected at least 12 columns");
                }
                if (isSecondaryAlignment(fields)) {
                    continue;
                }

                long queryLength = parseLong(fields[1], pafPath, lineNumber, "query length");
                long queryStart = parseLong(fields[2], pafPath, lineNumber, "query start");
                long queryEnd = parseLong(fields[3], pafPath, lineNumber, "query end");
                validateInterval(queryStart, queryEnd, queryLength, pafPath, lineNumber, "query");

                if (!"+".equals(fields[4]) && !"-".equals(fields[4])) {
                    throw invalidPafRow(pafPath, lineNumber, "invalid strand: " + fields[4]);
                }

                String referenceName = fields[5];
                Long fastaReferenceLength = referenceLengths.get(referenceName);
                if (fastaReferenceLength == null) {
                    throw invalidPafRow(
                            pafPath, lineNumber, "unknown target sequence: " + referenceName);
                }

                long pafReferenceLength = parseLong(
                        fields[6], pafPath, lineNumber, "target length");
                if (pafReferenceLength != fastaReferenceLength) {
                    throw invalidPafRow(
                            pafPath,
                            lineNumber,
                            "target length does not match FASTA for " + referenceName);
                }

                long referenceStart = parseLong(
                        fields[7], pafPath, lineNumber, "target start");
                long referenceEnd = parseLong(
                        fields[8], pafPath, lineNumber, "target end");
                validateInterval(
                        referenceStart,
                        referenceEnd,
                        pafReferenceLength,
                        pafPath,
                        lineNumber,
                        "target");

                long rowMatchingBases = parseLong(
                        fields[9], pafPath, lineNumber, "matching bases");
                long rowAlignmentBlockBases = parseLong(
                        fields[10], pafPath, lineNumber, "alignment block length");
                if (rowAlignmentBlockBases <= 0L
                        || rowMatchingBases < 0L
                        || rowMatchingBases > rowAlignmentBlockBases) {
                    throw invalidPafRow(pafPath, lineNumber, "invalid alignment base counts");
                }

                long mappingQuality = parseLong(
                        fields[11], pafPath, lineNumber, "mapping quality");
                if (mappingQuality < 0L || mappingQuality > 255L) {
                    throw invalidPafRow(pafPath, lineNumber, "invalid mapping quality");
                }

                intervalsByReference
                        .computeIfAbsent(referenceName, ignored -> new ArrayList<>())
                        .add(new Interval(referenceStart, referenceEnd));
                matchingBases += rowMatchingBases;
                alignmentBlockBases += rowAlignmentBlockBases;
                alignmentCount++;
            }
        }

        long alignedReferenceBases = intervalsByReference.values().stream()
                .mapToLong(PafAlignmentStatsParser::mergedIntervalLength)
                .sum();
        double referenceCoverage = (double) alignedReferenceBases / referenceBases;
        double sequenceIdentity = alignmentBlockBases == 0L
                ? 0.0d
                : 100.0d * matchingBases / alignmentBlockBases;

        return new AlignmentStats(
                sequenceIdentity,
                referenceCoverage,
                alignedReferenceBases,
                referenceBases,
                matchingBases,
                alignmentBlockBases,
                alignmentCount);
    }

    private static Map<String, Long> readFastaRecordLengths(Path fastaPath) throws IOException {
        if (!Files.isRegularFile(fastaPath)) {
            throw new IOException("Reference FASTA does not exist: " + fastaPath);
        }

        Map<String, Long> lengths = new LinkedHashMap<>();
        String currentName = null;
        long currentLength = 0L;

        try (BufferedReader reader = Files.newBufferedReader(fastaPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.startsWith(">")) {
                    if (currentName != null) {
                        addFastaRecord(lengths, currentName, currentLength, fastaPath);
                    }
                    currentName = fastaRecordName(line, fastaPath, lineNumber);
                    currentLength = 0L;
                    continue;
                }

                if (line.isBlank()) {
                    continue;
                }
                if (currentName == null) {
                    throw new IOException(
                            "FASTA sequence occurs before the first header at "
                                    + fastaPath + ":" + lineNumber);
                }
                for (int i = 0; i < line.length(); i++) {
                    if (!Character.isWhitespace(line.charAt(i))) {
                        currentLength++;
                    }
                }
            }
        }

        if (currentName != null) {
            addFastaRecord(lengths, currentName, currentLength, fastaPath);
        }
        if (lengths.isEmpty()) {
            throw new IOException("Reference FASTA contains no records: " + fastaPath);
        }
        return lengths;
    }

    private static String fastaRecordName(String header, Path fastaPath, int lineNumber)
            throws IOException {
        String value = header.substring(1).trim();
        if (value.isEmpty()) {
            throw new IOException("Blank FASTA header at " + fastaPath + ":" + lineNumber);
        }
        int firstWhitespace = -1;
        for (int i = 0; i < value.length(); i++) {
            if (Character.isWhitespace(value.charAt(i))) {
                firstWhitespace = i;
                break;
            }
        }
        return firstWhitespace < 0 ? value : value.substring(0, firstWhitespace);
    }

    private static void addFastaRecord(
            Map<String, Long> lengths, String name, long length, Path fastaPath) throws IOException {
        if (length <= 0L) {
            throw new IOException("Reference FASTA record is empty: " + name + " in " + fastaPath);
        }
        if (lengths.putIfAbsent(name, length) != null) {
            throw new IOException("Duplicate FASTA record name: " + name + " in " + fastaPath);
        }
    }

    private static long sumReferenceBases(Map<String, Long> referenceLengths, Path fastaPath)
            throws IOException {
        long total = 0L;
        try {
            for (long length : referenceLengths.values()) {
                total = Math.addExact(total, length);
            }
        } catch (ArithmeticException e) {
            throw new IOException("Reference FASTA length overflow: " + fastaPath, e);
        }
        return total;
    }

    private static boolean isSecondaryAlignment(String[] fields) {
        for (int i = 12; i < fields.length; i++) {
            if ("tp:A:S".equals(fields[i]) || "tp:A:i".equals(fields[i])) {
                return true;
            }
        }
        return false;
    }

    private static long parseLong(
            String value, Path pafPath, int lineNumber, String fieldName) throws IOException {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw invalidPafRow(pafPath, lineNumber, "invalid " + fieldName + ": " + value, e);
        }
    }

    private static void validateInterval(
            long start,
            long end,
            long sequenceLength,
            Path pafPath,
            int lineNumber,
            String intervalType) throws IOException {
        if (sequenceLength <= 0L || start < 0L || start >= end || end > sequenceLength) {
            throw invalidPafRow(pafPath, lineNumber, "invalid " + intervalType + " interval");
        }
    }

    private static IOException invalidPafRow(Path pafPath, int lineNumber, String message) {
        return new IOException("Invalid PAF row at " + pafPath + ":" + lineNumber + ": " + message);
    }

    private static IOException invalidPafRow(
            Path pafPath, int lineNumber, String message, Exception cause) {
        return new IOException(
                "Invalid PAF row at " + pafPath + ":" + lineNumber + ": " + message,
                cause);
    }

    private static long mergedIntervalLength(List<Interval> intervals) {
        if (intervals.isEmpty()) {
            return 0L;
        }
        intervals.sort(Comparator.comparingLong(Interval::start).thenComparingLong(Interval::end));

        long total = 0L;
        long currentStart = intervals.get(0).start();
        long currentEnd = intervals.get(0).end();
        for (int i = 1; i < intervals.size(); i++) {
            Interval next = intervals.get(i);
            if (next.start() <= currentEnd) {
                currentEnd = Math.max(currentEnd, next.end());
            } else {
                total += currentEnd - currentStart;
                currentStart = next.start();
                currentEnd = next.end();
            }
        }
        return total + currentEnd - currentStart;
    }
}
