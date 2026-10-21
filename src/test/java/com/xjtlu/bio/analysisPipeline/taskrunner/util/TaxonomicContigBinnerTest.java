package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics.TaxonomicBinMetrics;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.TaxonomicContigBinner.Result;

class TaxonomicContigBinnerTest {

    @TempDir
    Path tempDir;

    @Test
    void groupsStrainsAtSpeciesAndSelectsDominantBinByBases() throws Exception {
        Path fasta = write("assembly.fasta", """
                >ecoli_long full header is preserved
                AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC
                >staph_1
                GGGGGGGGGGGGGGGGGGGGGGGGGGGGGG
                >staph_2 description
                TTTTTTTTTTTTTTTTTTTTTTTTTTTTTT
                >genus_only
                ACGTACGTAC
                >unknown
                TGCATGCATG
                """);
        Path krakenOutput = write("taxonomy.output", """
                C\tecoli_long\t83334\t100\t83334:20 562:5 A:3 0:1
                C\tstaph_1\t1280\t30\t1280:10
                C\tstaph_2\t1280\t30\t1280:10
                C\tgenus_only\t561\t10\t561:3
                U\tunknown\t0\t10\t0:6
                """);
        Path report = write("taxonomy.report", """
                5.56\t1\t1\tU\t0\tunclassified
                94.44\t4\t0\tR\t1\troot
                94.44\t4\t0\tD\t2\t  Bacteria
                47.22\t2\t0\tF\t543\t    Enterobacteriaceae
                47.22\t2\t1\tG\t561\t      Escherichia
                41.67\t1\t0\tS\t562\t        Escherichia coli
                41.67\t1\t1\tS1\t83334\t          Escherichia coli K-12
                47.22\t2\t0\tF\t90964\t    Staphylococcaceae
                47.22\t2\t0\tG\t1279\t      Staphylococcus
                47.22\t2\t2\tS\t1280\t        Staphylococcus aureus
                """);

        Result result = TaxonomicContigBinner.bin(fasta, krakenOutput, report, tempDir.resolve("bins"));

        TaxonomyBinningMetrics metrics = result.getMetrics();
        assertEquals(5L, metrics.getTotalContigs());
        assertEquals(180L, metrics.getTotalAssemblyBases());
        assertEquals(3L, metrics.getSpeciesAssignedContigs());
        assertEquals(160L, metrics.getSpeciesAssignedBases());
        assertEquals(1L, metrics.getUnresolvedContigs());
        assertEquals(10L, metrics.getUnresolvedBases());
        assertEquals(1L, metrics.getUnclassifiedContigs());
        assertEquals(10L, metrics.getUnclassifiedBases());
        assertEquals(562, metrics.getDominantSpeciesTaxId());
        assertEquals("Escherichia coli", metrics.getDominantSpeciesName());
        assertEquals(100.0d / 180.0d, metrics.getDominantSpeciesAssemblyFraction(), 0.0d);

        TaxonomicBinMetrics ecoli = bin(metrics, 562);
        TaxonomicBinMetrics staph = bin(metrics, 1280);
        TaxonomicBinMetrics genusOnly = bin(metrics, 561);
        TaxonomicBinMetrics unclassified = bin(metrics, 0);
        assertEquals(1L, ecoli.getContigCount());
        assertEquals(100L, ecoli.getTotalBases());
        assertEquals(100.0d / 180.0d, ecoli.getAssemblyFraction(), 0.0d);
        assertEquals(2L, staph.getContigCount());
        assertEquals(60L, staph.getTotalBases());
        assertEquals("G", genusOnly.getRankCode());
        assertEquals(10L, genusOnly.getTotalBases());
        assertEquals("U", unclassified.getRankCode());

        assertEquals(2, result.getSpeciesBins().size());
        assertEquals(562, result.getSpeciesBins().get(0).getTaxId());
        assertEquals(2, result.getFamilyBins().size());
        assertEquals(543, result.getFamilyBins().get(0).getTaxId());
        assertEquals(110L, result.getFamilyBins().get(0).getTotalBases());
        assertEquals(543, result.getDominantSpeciesFamilyTaxId());

        assertEquals(
                """
                >ecoli_long full header is preserved
                AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
                CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC
                """,
                Files.readString(result.getMainBinFastaPath(), StandardCharsets.UTF_8));

        String assignmentReport = Files.readString(result.getAssignmentReportPath(), StandardCharsets.UTF_8);
        assertTrue(assignmentReport.contains(
                "ecoli_long\t100\t83334\tEscherichia coli K-12\tS1\t562\tEscherichia coli\tS"));
        assertTrue(assignmentReport.contains("unknown\t10\t0\tUnclassified\tU\t0\tUnclassified\tU"));
        assertFalse(assignmentReport.contains("staph_1\t30\t1280\tStaphylococcus aureus\tS\t562"));
    }

    @Test
    void rejectsKrakenSequenceThatIsAbsentFromAssembly() throws Exception {
        Path fasta = write("assembly.fasta", ">contig_1\nAAAA\n");
        Path krakenOutput = write("taxonomy.output", "C\tother_contig\t562\t4\t562:1\n");
        Path report = write("taxonomy.report", "100\t1\t1\tS\t562\tEscherichia coli\n");

        IOException exception = assertThrows(
                IOException.class,
                () -> TaxonomicContigBinner.bin(fasta, krakenOutput, report, tempDir.resolve("bins")));

        assertTrue(exception.getMessage().contains("contig_1"));
        assertTrue(exception.getMessage().contains("absent from Kraken2 output"));
    }

    @Test
    void rejectsKrakenLengthThatDoesNotMatchFasta() throws Exception {
        Path fasta = write("assembly.fasta", ">contig_1\nAAAA\n");
        Path krakenOutput = write("taxonomy.output", "C\tcontig_1\t562\t5\t562:1\n");
        Path report = write("taxonomy.report", "100\t1\t1\tS\t562\tEscherichia coli\n");

        IOException exception = assertThrows(
                IOException.class,
                () -> TaxonomicContigBinner.bin(fasta, krakenOutput, report, tempDir.resolve("bins")));

        assertTrue(exception.getMessage().contains("does not match FASTA"));
    }

    @Test
    void keepsGenusOnlyAssignmentsUnresolvedAndAcceptsNamedTaxIds() throws Exception {
        Path fasta = write("assembly.fasta", """
                >genus_only
                AAAAAA
                >unknown
                TTTT
                """);
        Path krakenOutput = write("taxonomy.output", """
                C\tgenus_only\tEscherichia (taxid 561)\t6\t561:2
                U\tunknown\tunclassified (taxid 0)\t4\t0:1
                """);
        Path report = write("taxonomy.report", """
                40.00\t1\t1\tU\t0\tunclassified
                60.00\t1\t0\tR\t1\troot
                60.00\t1\t0\tD\t2\t  Bacteria
                60.00\t1\t0\tF\t543\t    Enterobacteriaceae
                60.00\t1\t1\tG\t561\t      Escherichia
                """);

        Result result = TaxonomicContigBinner.bin(fasta, krakenOutput, report, tempDir.resolve("bins"));

        assertTrue(result.getSpeciesBins().isEmpty());
        assertEquals(1, result.getFamilyBins().size());
        assertEquals(543, result.getFamilyBins().get(0).getTaxId());
        assertEquals(6L, result.getFamilyBins().get(0).getTotalBases());
        assertNull(result.getMainBinFastaPath());
        assertNull(result.getDominantSpeciesFamilyTaxId());
        assertNull(result.getMetrics().getDominantSpeciesTaxId());
        assertEquals(1L, result.getMetrics().getUnresolvedContigs());
        assertEquals(6L, result.getMetrics().getUnresolvedBases());
        assertEquals(1L, result.getMetrics().getUnclassifiedContigs());
        assertEquals(4L, result.getMetrics().getUnclassifiedBases());
    }

    private TaxonomicBinMetrics bin(TaxonomyBinningMetrics metrics, int taxId) {
        return metrics.getBins().stream()
                .filter(candidate -> candidate.getTaxId() == taxId)
                .findFirst()
                .orElseThrow();
    }

    private Path write(String fileName, String content) throws IOException {
        Path path = tempDir.resolve(fileName);
        Files.writeString(path, content, StandardCharsets.UTF_8);
        return path;
    }
}
