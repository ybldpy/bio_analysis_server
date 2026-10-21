package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics.ReferenceMetrics;

class MappingMetricsCalculatorTest {

    @TempDir
    Path tempDir;

    @Test
    void calculatesPerReferenceAndAggregateMetrics() throws Exception {
        Path idxstats = write("mapping_idxstats.tsv", """
                NC_000001.1\t4\t5\t1
                NC_000002.1\t2\t2\t0
                *\t0\t0\t7
                """);
        Path depth = write("coverage_depth.tsv", """
                NC_000001.1\t1\t0
                NC_000001.1\t2\t1
                NC_000001.1\t3\t3
                NC_000001.1\t4\t2
                NC_000002.1\t1\t10
                NC_000002.1\t2\t0
                """);

        MappingResultMetrics metrics = MappingMetricsCalculator.calculate(depth, idxstats);

        assertEquals(6L, metrics.getTotalReferenceBases());
        assertEquals(4L, metrics.getCoveredBases());
        assertEquals(2L, metrics.getZeroDepthBases());
        assertEquals(4.0d / 6.0d, metrics.getCoverageFractionAt1x(), 0.0d);
        assertEquals(16L, metrics.getDepthSum());
        assertEquals(16.0d / 6.0d, metrics.getMeanDepth(), 0.0d);
        assertEquals(10L, metrics.getMaxDepth());
        assertEquals(7L, metrics.getMappedReadSegments());
        assertEquals(8L, metrics.getUnmappedReadSegments());
        assertEquals(15L, metrics.getTotalReadSegments());
        assertEquals(7.0d / 15.0d, metrics.getMappingFraction(), 0.0d);
        assertEquals(2, metrics.getReferences().size());

        ReferenceMetrics first = metrics.getReferences().get(0);
        assertEquals("NC_000001.1", first.getReferenceName());
        assertEquals(4L, first.getReferenceLength());
        assertEquals(3L, first.getCoveredBases());
        assertEquals(1L, first.getZeroDepthBases());
        assertEquals(0.75d, first.getCoverageFractionAt1x(), 0.0d);
        assertEquals(6L, first.getDepthSum());
        assertEquals(1.5d, first.getMeanDepth(), 0.0d);
        assertEquals(3L, first.getMaxDepth());
        assertEquals(5L, first.getMappedReadSegments());
        assertEquals(1L, first.getUnmappedReadSegments());

        ReferenceMetrics second = metrics.getReferences().get(1);
        assertEquals("NC_000002.1", second.getReferenceName());
        assertEquals(0.5d, second.getCoverageFractionAt1x(), 0.0d);
        assertEquals(5.0d, second.getMeanDepth(), 0.0d);
    }

    @Test
    void rejectsIncompleteDepthOutput() throws Exception {
        Path idxstats = write("mapping_idxstats.tsv", """
                ref\t3\t1\t0
                *\t0\t0\t0
                """);
        Path depth = write("coverage_depth.tsv", """
                ref\t1\t1
                ref\t3\t1
                """);

        IOException exception = assertThrows(
                IOException.class,
                () -> MappingMetricsCalculator.calculate(depth, idxstats));

        assertTrue(exception.getMessage().contains("expected ref:2"));
    }

    @Test
    void rejectsDepthReferenceNotDeclaredByIdxstats() throws Exception {
        Path idxstats = write("mapping_idxstats.tsv", """
                ref\t1\t1\t0
                *\t0\t0\t0
                """);
        Path depth = write("coverage_depth.tsv", "other\t1\t1\n");

        IOException exception = assertThrows(
                IOException.class,
                () -> MappingMetricsCalculator.calculate(depth, idxstats));

        assertTrue(exception.getMessage().contains("expected ref:1 but found other:1"));
    }

    @Test
    void rejectsMalformedIdxstatsCounts() throws Exception {
        Path idxstats = write("mapping_idxstats.tsv", "ref\t2\t-1\t0\n");
        Path depth = write("coverage_depth.tsv", """
                ref\t1\t0
                ref\t2\t0
                """);

        IOException exception = assertThrows(
                IOException.class,
                () -> MappingMetricsCalculator.calculate(depth, idxstats));

        assertTrue(exception.getMessage().contains("mapped read segments"));
        assertTrue(exception.getMessage().contains("must not be negative"));
    }

    private Path write(String fileName, String content) throws IOException {
        Path path = tempDir.resolve(fileName);
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }
}
