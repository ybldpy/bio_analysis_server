package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class AssemblyQualityTestSupportTest {

    @TempDir
    Path tempDir;

    @Test
    void readsMultilineFastaWithDescriptionsAndPreservesIupacBases() throws IOException {
        Path fasta = text("multiline.fa", "\n>first description\nacgt\nnryswkmbdhv\n\n>second\nTTA\n");

        assertEquals(Map.of("first", "ACGTNRYSWKMBDHV", "second", "TTA"),
                AssemblyQualityTestSupport.readFasta(fasta));
    }

    @Test
    void detectsGzipByContentRatherThanFileSuffix() throws IOException {
        Path gzip = tempDir.resolve("compressed.data");
        try (OutputStream output = new GZIPOutputStream(Files.newOutputStream(gzip))) {
            output.write(">gzip description\nac\ngtn\n".getBytes(StandardCharsets.UTF_8));
        }
        Path plain = text("actually-plain.fa.gz", ">plain\nACGTN\n");

        assertEquals(Map.of("gzip", "ACGTN"), AssemblyQualityTestSupport.readFasta(gzip));
        assertEquals(Map.of("plain", "ACGTN"), AssemblyQualityTestSupport.readFasta(plain));
        assertEquals(AssemblyQualityTestSupport.canonicalSequences(plain),
                AssemblyQualityTestSupport.canonicalSequences(gzip));
    }

    @Test
    void rejectsMissingHeadersAndEmptyInput() throws IOException {
        for (String content : List.of("", "\n \n", "ACGT\n", "> \nACGT\n")) {
            Path fasta = text("missing-header.fa", content);
            assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readFasta(fasta), content);
        }
    }

    @Test
    void rejectsEmptyRecordsIncludingTheFinalRecord() throws IOException {
        for (String content : List.of(">empty\n", ">empty\n>valid\nACGT\n", ">valid\nACGT\n>empty\n")) {
            Path fasta = text("empty-record.fa", content);
            assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readFasta(fasta), content);
        }
    }

    @Test
    void rejectsDuplicateIdsEvenWhenDescriptionsDiffer() throws IOException {
        Path fasta = text("duplicate.fa", ">same first description\nACGT\n>same other description\nTTAA\n");

        assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readFasta(fasta));
    }

    @Test
    void rejectsNonDnaAndNonIupacCharacters() throws IOException {
        for (String sequence : List.of("ACGU", "ACGX", "ACG-", "ACG?", "ACG1", "AC G", "ACG\u017f")) {
            Path fasta = text("invalid.fa", ">record\n" + sequence + "\n");
            assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readFasta(fasta), sequence);
        }
    }

    @Test
    void canonicalizationIgnoresNamesOrderAndStrandButRetainsDuplicates() throws IOException {
        Path forward = text("forward.fa", ">ambiguity\nACGTRYSWKMBDHVN\n>repeat1\nAAGC\n>repeat2\nAAGC\n");
        Path reversed = text("reversed.fa", ">renamed2\nGCTT\n>renamed1\nGCTT\n>ambiguity-rc\nNBDHVKMWSRYACGT\n");
        Path withoutDuplicate = text("one-repeat.fa", ">ambiguity\nACGTRYSWKMBDHVN\n>repeat\nAAGC\n");

        assertEquals(List.of("AAGC", "AAGC", "ACGTRYSWKMBDHVN"),
                AssemblyQualityTestSupport.canonicalSequences(forward));
        assertEquals(AssemblyQualityTestSupport.canonicalSequences(forward),
                AssemblyQualityTestSupport.canonicalSequences(reversed));
        assertEquals(AssemblyQualityTestSupport.fastaMetrics(forward).get("canonicalDigest"),
                AssemblyQualityTestSupport.fastaMetrics(reversed).get("canonicalDigest"));
        assertNotEquals(AssemblyQualityTestSupport.canonicalSequences(forward),
                AssemblyQualityTestSupport.canonicalSequences(withoutDuplicate));
        assertNotEquals(AssemblyQualityTestSupport.fastaMetrics(forward).get("canonicalDigest"),
                AssemblyQualityTestSupport.fastaMetrics(withoutDuplicate).get("canonicalDigest"));
    }

    @Test
    void computesBaseWeightedN50AndCountsOnlyNAsNBases() throws IOException {
        Path fasta = text("metrics.fa", ">ten\nACGTNNACGT\n>six\nAAAAAA\n>four\nCGTR\n");

        Map<String, Object> metrics = AssemblyQualityTestSupport.fastaMetrics(fasta);

        assertEquals(3L, metrics.get("contigs"));
        assertEquals(20L, metrics.get("totalBases"));
        assertEquals(10L, metrics.get("largestContig"));
        // Lengths 10, 6, 4 reach half of 20 at the first contig, not at the median length.
        assertEquals(10L, metrics.get("n50"));
        assertEquals(2L, metrics.get("nBases"));
        assertTrue(((String) metrics.get("canonicalDigest")).matches("[0-9a-f]{64}"));
    }

    @Test
    void n50RequiresAtLeastHalfTheBasesForOddTotals() throws IOException {
        Path fasta = text("odd-total.fa", ">six\nAAAAAA\n>five\nCCCCC\n>two\nGG\n");

        assertEquals(5L, AssemblyQualityTestSupport.fastaMetrics(fasta).get("n50"));
    }

    @Test
    void canonicalDigestPreservesRecordBoundaries() throws IOException {
        Path split = text("split.fa", ">one\nAC\n>two\nGT\n");
        Path joined = text("joined.fa", ">one\nACAC\n");

        assertNotEquals(AssemblyQualityTestSupport.fastaMetrics(split).get("canonicalDigest"),
                AssemblyQualityTestSupport.fastaMetrics(joined).get("canonicalDigest"));
    }

    @Test
    void hashesRawFileBytesUsingSha256() throws IOException {
        Path input = text("abc.txt", "abc");

        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                AssemblyQualityTestSupport.sha256(input));
    }

    @Test
    void transposesQuastReportsWithoutTurningMissingMetricsIntoZero() throws IOException {
        Path report = text("report.tsv", "Assembly\tbaseline\tcandidate\n"
                + "# contigs\t1\t2\n"
                + "Genome fraction (%)\t-\t\n"
                + "# misassemblies\t0\t-\n"
                + "N50\t1000\t900\n");

        Map<String, Map<String, String>> parsed = AssemblyQualityTestSupport.readQuastReport(report);

        assertEquals(List.of("baseline", "candidate"), List.copyOf(parsed.keySet()));
        assertEquals("1", parsed.get("baseline").get("# contigs"));
        assertEquals("900", parsed.get("candidate").get("N50"));
        assertEquals("-", parsed.get("baseline").get("Genome fraction (%)"));
        assertEquals("-", parsed.get("candidate").get("Genome fraction (%)"));
        assertEquals("0", parsed.get("baseline").get("# misassemblies"));
        assertEquals("-", parsed.get("candidate").get("# misassemblies"));
        assertFalse(parsed.get("candidate").containsKey("# mismatches per 100 kbp"));
    }

    @Test
    void rejectsDuplicateQuastLabelsAndMetrics() throws IOException {
        Path duplicateLabels = text("labels.tsv", "Assembly\tsame\t same \nN50\t1\t2\n");
        Path duplicateMetrics = text("metrics.tsv", "Assembly\ta\nN50\t1\n N50 \t2\n");

        assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readQuastReport(duplicateLabels));
        assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readQuastReport(duplicateMetrics));
    }

    @Test
    void rejectsBadQuastColumnCountsHeadersAndEmptyNames() throws IOException {
        for (String content : List.of(
                "", "Assembly\n", "Other\ta\n", "Assembly\t\n",
                "Assembly\ta\tb\nN50\t1\n", "Assembly\ta\nN50\t1\t2\n", "Assembly\ta\n\t1\n",
                "Assembly\ta\tb\n\t\t\t\n")) {
            Path report = text("invalid.tsv", content);
            assertThrows(IOException.class, () -> AssemblyQualityTestSupport.readQuastReport(report), content);
        }
    }

    @Test
    void allowsAbsentQuastRowsWithoutInventingMetrics() throws IOException {
        Path report = text("header-only.tsv", "Assembly\ta\tb\n");

        assertEquals(Map.of("a", Map.of(), "b", Map.of()), AssemblyQualityTestSupport.readQuastReport(report));
    }

    @Test
    void writesJsonUsingJacksonAndCreatesReportDirectory() throws IOException {
        Path report = tempDir.resolve("nested/report.json");
        AssemblyQualityTestSupport.writeJson(report, Map.of("contigs", 3L, "missing", "-", "labels", List.of("a", "b")));

        JsonNode parsed = new ObjectMapper().readTree(report.toFile());
        assertEquals(3L, parsed.get("contigs").longValue());
        assertEquals("-", parsed.get("missing").textValue());
        assertEquals("b", parsed.get("labels").get(1).textValue());
    }

    private Path text(String name, String content) throws IOException {
        return Files.writeString(tempDir.resolve(name), content, StandardCharsets.UTF_8);
    }
}
