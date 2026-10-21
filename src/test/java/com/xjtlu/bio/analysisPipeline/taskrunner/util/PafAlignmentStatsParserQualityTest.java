package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser.AlignmentStats;

class PafAlignmentStatsParserQualityTest {

    @TempDir
    Path tempDir;

    @Test
    void filtersLowAndUnavailableQualityAndMergesAcceptedIntervals() throws Exception {
        Path fasta = referenceFasta();
        Path paf = paf(
                "first\t10\t0\t6\t+\tref\t10\t0\t6\t5\t6\t20\ttp:A:P",
                "overlap\t10\t0\t4\t-\tref\t10\t4\t8\t4\t4\t60\ttp:A:P",
                "low\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t19\ttp:A:P",
                "unavailable\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t255\ttp:A:P",
                "secondary\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t60\ttp:A:S",
                "secondaryInversion\t10\t0\t10\t-\tref\t10\t0\t10\t10\t10\t60\ttp:A:i");

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, fasta, 20);

        assertEquals(10L, stats.referenceBases());
        assertEquals(8L, stats.alignedReferenceBases());
        assertEquals(0.8d, stats.referenceCoverage(), 1.0e-12);
        assertEquals(9L, stats.matchingBases());
        assertEquals(10L, stats.alignmentBlockBases());
        assertEquals(90.0d, stats.sequenceIdentity(), 1.0e-12);
        assertEquals(2, stats.alignmentCount());
    }

    @Test
    void zeroThresholdIncludesZeroQualityButStillExcludesUnavailableQuality() throws Exception {
        Path fasta = referenceFasta();
        Path paf = paf(
                "zero\t10\t0\t3\t+\tref\t10\t0\t3\t3\t3\t0",
                "unavailable\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t255");

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, fasta, 0);

        assertEquals(1, stats.alignmentCount());
        assertEquals(3L, stats.alignedReferenceBases());
        assertEquals(3L, stats.matchingBases());
    }

    @Test
    void upperThresholdIncludes254ButExcludes255() throws Exception {
        Path fasta = referenceFasta();
        Path paf = paf(
                "highest\t10\t0\t3\t+\tref\t10\t0\t3\t3\t3\t254",
                "unavailable\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t255");

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, fasta, 254);

        assertEquals(1, stats.alignmentCount());
        assertEquals(3L, stats.alignedReferenceBases());
    }

    @Test
    void legacyOverloadContinuesToIncludeZeroAndUnavailableQuality() throws Exception {
        Path fasta = referenceFasta();
        Path paf = paf(
                "zero\t10\t0\t3\t+\tref\t10\t0\t3\t3\t3\t0",
                "unavailable\t10\t0\t7\t+\tref\t10\t3\t10\t7\t7\t255");

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, fasta);

        assertEquals(2, stats.alignmentCount());
        assertEquals(10L, stats.alignedReferenceBases());
        assertEquals(1.0d, stats.referenceCoverage());
    }

    @Test
    void returnsZeroStatisticsWhenAllRowsAreFiltered() throws Exception {
        Path fasta = referenceFasta();
        Path paf = paf(
                "low\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t19",
                "unavailable\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t255");

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, fasta, 20);

        assertEquals(10L, stats.referenceBases());
        assertEquals(0L, stats.alignedReferenceBases());
        assertEquals(0L, stats.matchingBases());
        assertEquals(0L, stats.alignmentBlockBases());
        assertEquals(0, stats.alignmentCount());
        assertEquals(0.0d, stats.referenceCoverage());
        assertEquals(0.0d, stats.sequenceIdentity());
    }

    @Test
    void rejectsMalformedRowsEvenWhenTheirQualityWouldBeFiltered() throws Exception {
        Path fasta = referenceFasta();
        for (String malformed : new String[] {
                "query\t10\t0\t10",
                "query\t10\t0\t11\t+\tref\t10\t0\t10\t10\t10\t0",
                "query\t10\t0\t10\t+\tmissing\t10\t0\t10\t10\t10\t0",
                "query\t10\t0\t10\t+\tref\t10\t0\t10\t11\t10\t255",
                "query\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t-1",
                "query\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\t256",
                "query\t10\t0\t10\t+\tref\t10\t0\t10\t10\t10\tunknown"
        }) {
            Path paf = paf(malformed);
            IOException error = assertThrows(
                    IOException.class, () -> PafAlignmentStatsParser.parse(paf, fasta, 20));
            assertTrue(error.getMessage().contains("Invalid PAF row"));
            assertTrue(error.getMessage().contains(":1:"));
        }
    }

    @Test
    void rejectsThresholdsOutsideSupportedRange() {
        Path missing = tempDir.resolve("not-created");
        for (int threshold : new int[] {-1, 255, Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class,
                    () -> PafAlignmentStatsParser.parse(missing, missing, threshold));
        }
    }

    private Path referenceFasta() throws IOException {
        Path fasta = tempDir.resolve("reference.fasta");
        Files.writeString(fasta, ">ref\nAAAAAAAAAA\n");
        return fasta;
    }

    private Path paf(String... rows) throws IOException {
        Path paf = tempDir.resolve("alignment.paf");
        Files.writeString(paf, String.join("\n", rows) + "\n");
        return paf;
    }
}
