package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics.SequenceMetrics;

/**
 * Converts a complete {@code samtools depth -aa} table into a zero-based,
 * half-open BED mask for positions below the configured depth threshold.
 */
public final class ConsensusMaskBuilder {

    private ConsensusMaskBuilder() {
    }

    public static ConsensusResultMetrics build(
            Path coverageDepthPath,
            Path referenceFaiPath,
            Path maskBedPath,
            int minimumDepth) throws IOException {
        if (minimumDepth < 1) {
            throw new IllegalArgumentException("minimumConsensusDepth must be at least 1");
        }
        if (!Files.isRegularFile(coverageDepthPath)) {
            throw new IOException("Coverage depth file does not exist: " + coverageDepthPath);
        }
        if (!Files.isRegularFile(referenceFaiPath)) {
            throw new IOException("Reference FAI file does not exist: " + referenceFaiPath);
        }

        LinkedHashMap<String, Long> referenceLengths = readReferenceLengths(referenceFaiPath);
        Path parentPath = maskBedPath.getParent();
        if (parentPath != null) {
            Files.createDirectories(parentPath);
        }

        List<SequenceMetrics> sequenceMetrics = new ArrayList<>();
        long totalBases = 0L;
        long maskedBases = 0L;

        try (LineNumberReader reader = new LineNumberReader(Files.newBufferedReader(
                    coverageDepthPath, StandardCharsets.UTF_8));
                BufferedWriter writer = Files.newBufferedWriter(
                    maskBedPath, StandardCharsets.UTF_8)) {
            String pendingLine = nextNonBlankLine(reader);
            int pendingLineNumber = reader.getLineNumber();

            for (Map.Entry<String, Long> reference : referenceLengths.entrySet()) {
                String sequenceName = reference.getKey();
                long sequenceLength = reference.getValue();
                long sequenceMaskedBases = 0L;
                long maskStart = -1L;

                for (long expectedPosition = 1L;
                        expectedPosition <= sequenceLength;
                        expectedPosition++) {
                    if (pendingLine == null) {
                        throw invalidDepth(
                                coverageDepthPath,
                                reader.getLineNumber() + 1,
                                "missing depth row for " + sequenceName + ":" + expectedPosition);
                    }

                    DepthRow row = parseDepthRow(
                            pendingLine, coverageDepthPath, pendingLineNumber);
                    if (!sequenceName.equals(row.sequenceName())
                            || row.position() != expectedPosition) {
                        throw invalidDepth(
                                coverageDepthPath,
                                pendingLineNumber,
                                "expected " + sequenceName + ":" + expectedPosition
                                        + " but found " + row.sequenceName() + ":" + row.position());
                    }

                    boolean masked = row.depth() < minimumDepth;
                    if (masked) {
                        sequenceMaskedBases++;
                        if (maskStart < 0L) {
                            maskStart = expectedPosition - 1L;
                        }
                    } else if (maskStart >= 0L) {
                        writeBedInterval(writer, sequenceName, maskStart, expectedPosition - 1L);
                        maskStart = -1L;
                    }

                    pendingLine = nextNonBlankLine(reader);
                    pendingLineNumber = reader.getLineNumber();
                }

                if (maskStart >= 0L) {
                    writeBedInterval(writer, sequenceName, maskStart, sequenceLength);
                }

                long unmaskedBases = sequenceLength - sequenceMaskedBases;
                sequenceMetrics.add(new SequenceMetrics(
                        sequenceName,
                        sequenceLength,
                        unmaskedBases,
                        sequenceMaskedBases));
                totalBases = Math.addExact(totalBases, sequenceLength);
                maskedBases = Math.addExact(maskedBases, sequenceMaskedBases);
            }

            if (pendingLine != null) {
                DepthRow row = parseDepthRow(
                        pendingLine, coverageDepthPath, pendingLineNumber);
                throw invalidDepth(
                        coverageDepthPath,
                        pendingLineNumber,
                        "unexpected extra depth row " + row.sequenceName() + ":" + row.position());
            }
        } catch (ArithmeticException e) {
            throw new IOException("Coverage base count overflow: " + coverageDepthPath, e);
        }

        return new ConsensusResultMetrics(
                minimumDepth,
                totalBases,
                totalBases - maskedBases,
                maskedBases,
                sequenceMetrics);
    }

    private static LinkedHashMap<String, Long> readReferenceLengths(Path referenceFaiPath)
            throws IOException {
        LinkedHashMap<String, Long> referenceLengths = new LinkedHashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(
                referenceFaiPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                if (fields.length < 2 || fields[0].isBlank()) {
                    throw new IOException(
                            "Invalid FAI row at " + referenceFaiPath + ":" + lineNumber);
                }
                long length = parseLong(
                        fields[1], referenceFaiPath, lineNumber, "reference length");
                if (length <= 0L) {
                    throw new IOException(
                            "Reference length must be positive at "
                                    + referenceFaiPath + ":" + lineNumber);
                }
                if (referenceLengths.putIfAbsent(fields[0], length) != null) {
                    throw new IOException(
                            "Duplicate reference name in FAI: " + fields[0]);
                }
            }
        }
        if (referenceLengths.isEmpty()) {
            throw new IOException("Reference FAI contains no records: " + referenceFaiPath);
        }
        return referenceLengths;
    }

    private static DepthRow parseDepthRow(
            String line, Path coverageDepthPath, int lineNumber) throws IOException {
        String[] fields = line.split("\\t", -1);
        if (fields.length < 3 || fields[0].isBlank()) {
            throw invalidDepth(coverageDepthPath, lineNumber, "expected at least three columns");
        }
        long position = parseLong(
                fields[1], coverageDepthPath, lineNumber, "position");
        long depth = parseLong(
                fields[2], coverageDepthPath, lineNumber, "depth");
        if (position < 1L) {
            throw invalidDepth(coverageDepthPath, lineNumber, "position must be positive");
        }
        if (depth < 0L) {
            throw invalidDepth(coverageDepthPath, lineNumber, "depth must not be negative");
        }
        return new DepthRow(fields[0], position, depth);
    }

    private static long parseLong(
            String value, Path path, int lineNumber, String fieldName) throws IOException {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IOException(
                    "Invalid " + fieldName + " at " + path + ":" + lineNumber
                            + ": " + value,
                    e);
        }
    }

    private static String nextNonBlankLine(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.isBlank()) {
                return line;
            }
        }
        return null;
    }

    private static void writeBedInterval(
            BufferedWriter writer, String sequenceName, long start, long end) throws IOException {
        writer.write(sequenceName);
        writer.write('\t');
        writer.write(Long.toString(start));
        writer.write('\t');
        writer.write(Long.toString(end));
        writer.newLine();
    }

    private static IOException invalidDepth(
            Path coverageDepthPath, int lineNumber, String reason) {
        return new IOException(
                "Invalid complete coverage depth table at "
                        + coverageDepthPath + ":" + lineNumber + ": " + reason);
    }

    private record DepthRow(String sequenceName, long position, long depth) {
    }
}
