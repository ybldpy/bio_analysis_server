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

import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;

class ConsensusMaskBuilderTest {

    @TempDir
    Path tempDir;

    @Test
    void buildsMergedZeroBasedBedMaskAndResultMetrics() throws Exception {
        Path fai = write("reference.fasta.fai", """
                chr1\t5\t0\t5\t6
                chr2\t3\t10\t3\t4
                """);
        Path depth = write("coverage_depth.tsv", """
                chr1\t1\t0
                chr1\t2\t3
                chr1\t3\t10
                chr1\t4\t9
                chr1\t5\t12
                chr2\t1\t20
                chr2\t2\t1
                chr2\t3\t0
                """);
        Path mask = tempDir.resolve("consensus.mask.bed");

        ConsensusResultMetrics metrics = ConsensusMaskBuilder.build(
                depth, fai, mask, 10);

        assertEquals("""
                chr1\t0\t2
                chr1\t3\t4
                chr2\t1\t3
                """, Files.readString(mask, StandardCharsets.UTF_8));
        assertEquals(10, metrics.getMinimumDepth());
        assertEquals(8L, metrics.getTotalBases());
        assertEquals(3L, metrics.getUnmaskedBases());
        assertEquals(5L, metrics.getMaskedBases());
        assertEquals(0.375d, metrics.getUnmaskedFraction(), 0.0d);
        assertEquals(0.625d, metrics.getMaskedFraction(), 0.0d);
        assertEquals(2, metrics.getSequences().size());
        assertEquals("chr1", metrics.getSequences().get(0).getSequenceName());
        assertEquals(3L, metrics.getSequences().get(0).getMaskedBases());
        assertEquals("chr2", metrics.getSequences().get(1).getSequenceName());
        assertEquals(2L, metrics.getSequences().get(1).getMaskedBases());
    }

    @Test
    void createsEmptyMaskWhenEveryPositionMeetsThreshold() throws Exception {
        Path fai = write("reference.fasta.fai", "chr1\t2\t0\t2\t3\n");
        Path depth = write("coverage_depth.tsv", """
                chr1\t1\t10
                chr1\t2\t11
                """);
        Path mask = tempDir.resolve("consensus.mask.bed");

        ConsensusResultMetrics metrics = ConsensusMaskBuilder.build(
                depth, fai, mask, 10);

        assertTrue(Files.isRegularFile(mask));
        assertEquals(0L, Files.size(mask));
        assertEquals(2L, metrics.getUnmaskedBases());
        assertEquals(0L, metrics.getMaskedBases());
    }

    @Test
    void rejectsIncompleteDepthTable() throws Exception {
        Path fai = write("reference.fasta.fai", "chr1\t3\t0\t3\t4\n");
        Path depth = write("coverage_depth.tsv", """
                chr1\t1\t10
                chr1\t3\t10
                """);

        IOException exception = assertThrows(IOException.class,
                () -> ConsensusMaskBuilder.build(
                        depth,
                        fai,
                        tempDir.resolve("consensus.mask.bed"),
                        10));

        assertTrue(exception.getMessage().contains("expected chr1:2"));
    }

    @Test
    void rejectsNonPositiveMinimumDepth() throws Exception {
        Path fai = write("reference.fasta.fai", "chr1\t1\t0\t1\t2\n");
        Path depth = write("coverage_depth.tsv", "chr1\t1\t0\n");

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ConsensusMaskBuilder.build(
                        depth,
                        fai,
                        tempDir.resolve("consensus.mask.bed"),
                        0));

        assertEquals(
                "minimumConsensusDepth must be at least 1",
                exception.getMessage());
    }

    private Path write(String fileName, String content) throws IOException {
        Path path = tempDir.resolve(fileName);
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }
}
