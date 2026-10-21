package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics.ReferenceMetrics;

/**
 * Calculates mapping result metrics from complete {@code samtools depth -aa} and
 * {@code samtools idxstats} output.
 */
public final class MappingMetricsCalculator {

    private MappingMetricsCalculator() {
    }

    public static MappingResultMetrics calculate(Path coverageDepthPath, Path idxstatsPath)
            throws IOException {
        if (!Files.isRegularFile(coverageDepthPath)) {
            throw new IOException("Coverage depth file does not exist: " + coverageDepthPath);
        }
        if (!Files.isRegularFile(idxstatsPath)) {
            throw new IOException("Idxstats file does not exist: " + idxstatsPath);
        }

        IdxstatsData idxstats = readIdxstats(idxstatsPath);
        List<ReferenceMetrics> references = new ArrayList<>();
        long totalReferenceBases = 0L;
        long totalCoveredBases = 0L;
        long totalDepth = 0L;
        long overallMaxDepth = 0L;
        long totalMappedReadSegments = 0L;

        try (LineNumberReader reader = new LineNumberReader(Files.newBufferedReader(
                coverageDepthPath, StandardCharsets.UTF_8))) {
            String pendingLine = nextNonBlankLine(reader);
            int pendingLineNumber = reader.getLineNumber();

            for (IdxstatsReference reference : idxstats.references().values()) {
                long coveredBases = 0L;
                long depthSum = 0L;
                long maxDepth = 0L;

                for (long expectedPosition = 1L;
                        expectedPosition <= reference.referenceLength();
                        expectedPosition++) {
                    if (pendingLine == null) {
                        throw invalidDepth(
                                coverageDepthPath,
                                reader.getLineNumber() + 1,
                                "missing depth row for " + reference.referenceName()
                                        + ":" + expectedPosition);
                    }

                    DepthRow row = parseDepthRow(
                            pendingLine, coverageDepthPath, pendingLineNumber);
                    if (!reference.referenceName().equals(row.referenceName())
                            || row.position() != expectedPosition) {
                        throw invalidDepth(
                                coverageDepthPath,
                                pendingLineNumber,
                                "expected " + reference.referenceName() + ":" + expectedPosition
                                        + " but found " + row.referenceName() + ":" + row.position());
                    }

                    if (row.depth() > 0L) {
                        coveredBases = Math.addExact(coveredBases, 1L);
                    }
                    depthSum = Math.addExact(depthSum, row.depth());
                    maxDepth = Math.max(maxDepth, row.depth());

                    pendingLine = nextNonBlankLine(reader);
                    pendingLineNumber = reader.getLineNumber();
                }

                references.add(new ReferenceMetrics(
                        reference.referenceName(),
                        reference.referenceLength(),
                        coveredBases,
                        depthSum,
                        maxDepth,
                        reference.mappedReadSegments(),
                        reference.unmappedReadSegments()));
                totalReferenceBases = Math.addExact(
                        totalReferenceBases, reference.referenceLength());
                totalCoveredBases = Math.addExact(totalCoveredBases, coveredBases);
                totalDepth = Math.addExact(totalDepth, depthSum);
                overallMaxDepth = Math.max(overallMaxDepth, maxDepth);
                totalMappedReadSegments = Math.addExact(
                        totalMappedReadSegments, reference.mappedReadSegments());
            }

            if (pendingLine != null) {
                DepthRow row = parseDepthRow(
                        pendingLine, coverageDepthPath, pendingLineNumber);
                throw invalidDepth(
                        coverageDepthPath,
                        pendingLineNumber,
                        "unexpected extra depth row " + row.referenceName() + ":" + row.position());
            }
        } catch (ArithmeticException e) {
            throw new IOException("Mapping metrics count overflow", e);
        }

        try {
            return new MappingResultMetrics(
                    totalReferenceBases,
                    totalCoveredBases,
                    totalDepth,
                    overallMaxDepth,
                    totalMappedReadSegments,
                    idxstats.totalUnmappedReadSegments(),
                    references);
        } catch (ArithmeticException e) {
            throw new IOException("Mapping metrics count overflow", e);
        }
    }

    private static IdxstatsData readIdxstats(Path idxstatsPath) throws IOException {
        LinkedHashMap<String, IdxstatsReference> references = new LinkedHashMap<>();
        long totalUnmappedReadSegments = 0L;
        boolean foundNoCoordinateRow = false;

        try (BufferedReader reader = Files.newBufferedReader(idxstatsPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                String[] fields = line.split("\\t", -1);
                if (fields.length < 4 || fields[0].isBlank()) {
                    throw invalidIdxstats(idxstatsPath, lineNumber, "expected four columns");
                }

                long referenceLength = parseNonNegativeLong(
                        fields[1], idxstatsPath, lineNumber, "reference length");
                long mappedReadSegments = parseNonNegativeLong(
                        fields[2], idxstatsPath, lineNumber, "mapped read segments");
                long unmappedReadSegments = parseNonNegativeLong(
                        fields[3], idxstatsPath, lineNumber, "unmapped read segments");
                totalUnmappedReadSegments = addExact(
                        totalUnmappedReadSegments,
                        unmappedReadSegments,
                        idxstatsPath,
                        "unmapped read segment count");

                if ("*".equals(fields[0])) {
                    if (foundNoCoordinateRow) {
                        throw invalidIdxstats(
                                idxstatsPath, lineNumber, "duplicate '*' no-coordinate row");
                    }
                    if (referenceLength != 0L || mappedReadSegments != 0L) {
                        throw invalidIdxstats(
                                idxstatsPath,
                                lineNumber,
                                "'*' row must have zero reference length and mapped count");
                    }
                    foundNoCoordinateRow = true;
                    continue;
                }

                if (referenceLength == 0L) {
                    throw invalidIdxstats(
                            idxstatsPath, lineNumber, "reference length must be positive");
                }
                IdxstatsReference reference = new IdxstatsReference(
                        fields[0], referenceLength, mappedReadSegments, unmappedReadSegments);
                if (references.putIfAbsent(fields[0], reference) != null) {
                    throw invalidIdxstats(
                            idxstatsPath, lineNumber, "duplicate reference name " + fields[0]);
                }
            }
        }

        if (references.isEmpty()) {
            throw new IOException("Idxstats contains no reference records: " + idxstatsPath);
        }
        return new IdxstatsData(references, totalUnmappedReadSegments);
    }

    private static DepthRow parseDepthRow(String line, Path path, int lineNumber)
            throws IOException {
        String[] fields = line.split("\\t", -1);
        if (fields.length < 3 || fields[0].isBlank()) {
            throw invalidDepth(path, lineNumber, "expected at least three columns");
        }
        long position = parseNonNegativeLong(fields[1], path, lineNumber, "position");
        long depth = parseNonNegativeLong(fields[2], path, lineNumber, "depth");
        if (position == 0L) {
            throw invalidDepth(path, lineNumber, "position must be positive");
        }
        return new DepthRow(fields[0], position, depth);
    }

    private static long parseNonNegativeLong(
            String value, Path path, int lineNumber, String fieldName) throws IOException {
        final long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IOException(
                    "Invalid " + fieldName + " at " + path + ":" + lineNumber
                            + ": " + value,
                    e);
        }
        if (parsed < 0L) {
            throw new IOException(
                    "Invalid " + fieldName + " at " + path + ":" + lineNumber
                            + ": value must not be negative");
        }
        return parsed;
    }

    private static long addExact(long left, long right, Path path, String fieldName)
            throws IOException {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw new IOException("Overflow in " + fieldName + " at " + path, e);
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

    private static IOException invalidDepth(Path path, int lineNumber, String reason) {
        return new IOException(
                "Invalid complete coverage depth table at " + path + ":" + lineNumber
                        + ": " + reason);
    }

    private static IOException invalidIdxstats(Path path, int lineNumber, String reason) {
        return new IOException(
                "Invalid samtools idxstats output at " + path + ":" + lineNumber
                        + ": " + reason);
    }

    private record IdxstatsData(
            Map<String, IdxstatsReference> references,
            long totalUnmappedReadSegments) {
    }

    private record IdxstatsReference(
            String referenceName,
            long referenceLength,
            long mappedReadSegments,
            long unmappedReadSegments) {
    }

    private record DepthRow(String referenceName, long position, long depth) {
    }
}
