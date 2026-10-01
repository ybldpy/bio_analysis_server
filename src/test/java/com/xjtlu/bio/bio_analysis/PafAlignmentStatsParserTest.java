package com.xjtlu.bio.bio_analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser.AlignmentStats;

public class PafAlignmentStatsParserTest {

    @TempDir
    Path tempDir;

    @Test
    public void mergesReferenceIntervalsAndIgnoresSecondaryAlignments() throws Exception {
        Path referenceFasta = tempDir.resolve("reference.fasta");
        Files.writeString(referenceFasta, """
                >ref1 description
                AAAAAAAAAA
                >ref2
                CCCCC
                """);

        Path paf = tempDir.resolve("alignment.paf");
        Files.writeString(paf, String.join("\n",
                "query1\t12\t0\t8\t+\tref1\t10\t0\t8\t7\t8\t60\ttp:A:P",
                "query2\t10\t0\t5\t+\tref1\t10\t5\t10\t5\t5\t60\ttp:A:P",
                "query3\t5\t0\t3\t-\tref2\t5\t1\t4\t3\t3\t20\ttp:A:I",
                "secondary\t5\t0\t5\t+\tref2\t5\t0\t5\t5\t5\t0\ttp:A:S",
                ""));

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, referenceFasta);

        assertEquals(15L, stats.referenceBases());
        assertEquals(13L, stats.alignedReferenceBases());
        assertEquals(13.0d / 15.0d, stats.referenceCoverage(), 1.0e-12);
        assertEquals(15L, stats.matchingBases());
        assertEquals(16L, stats.alignmentBlockBases());
        assertEquals(93.75d, stats.sequenceIdentity(), 1.0e-12);
        assertEquals(3, stats.alignmentCount());
    }

    @Test
    public void treatsEmptyPafAsZeroAlignment() throws Exception {
        Path referenceFasta = tempDir.resolve("reference.fasta");
        Files.writeString(referenceFasta, ">ref\nACGT\n");
        Path paf = tempDir.resolve("empty.paf");
        Files.createFile(paf);

        AlignmentStats stats = PafAlignmentStatsParser.parse(paf, referenceFasta);

        assertEquals(4L, stats.referenceBases());
        assertEquals(0L, stats.alignedReferenceBases());
        assertEquals(0.0d, stats.referenceCoverage());
        assertEquals(0.0d, stats.sequenceIdentity());
        assertEquals(0, stats.alignmentCount());
    }

    @Test
    public void rejectsUnknownReferenceName() throws Exception {
        Path referenceFasta = tempDir.resolve("reference.fasta");
        Files.writeString(referenceFasta, ">ref\nACGT\n");
        Path paf = tempDir.resolve("alignment.paf");
        Files.writeString(
                paf,
                "query\t4\t0\t4\t+\tunknown\t4\t0\t4\t4\t4\t60\ttp:A:P\n");

        assertThrows(
                IOException.class,
                () -> PafAlignmentStatsParser.parse(paf, referenceFasta));
    }
}
