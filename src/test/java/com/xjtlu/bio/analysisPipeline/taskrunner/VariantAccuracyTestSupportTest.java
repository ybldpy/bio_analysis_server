package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.Accuracy;
import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.Allele;
import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.FilterPolicy;
import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.Interval;

class VariantAccuracyTestSupportTest {

    private static final String HEADER = "##fileformat=VCFv4.2\n"
            + "#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tsample\n";

    @TempDir
    Path tempDir;

    @Test
    void calculatesTrueFalseAndMissedAllelesAgainstIndependentTruth() {
        Allele first = allele(1, "C");
        Allele second = allele(2, "G");
        Allele missed = allele(3, "T");
        Allele spurious = allele(4, "C");

        Accuracy accuracy = VariantAccuracyTestSupport.compare(
                List.of(first, second, missed), List.of(first, second, spurious));

        assertEquals(2, accuracy.truePositives());
        assertEquals(1, accuracy.falsePositives());
        assertEquals(1, accuracy.falseNegatives());
        assertEquals(2.0 / 3, accuracy.precision(), 1e-12);
        assertEquals(2.0 / 3, accuracy.recall(), 1e-12);
        assertEquals(1.0 / 3, accuracy.falseDiscoveryRate(), 1e-12);
        assertEquals(1.0 / 3, accuracy.falseNegativeRate(), 1e-12);
    }

    @Test
    void returnsUndefinedRatiosForEmptyTruthAndPredictions() {
        Accuracy accuracy = VariantAccuracyTestSupport.compare(List.of(), List.of());

        assertEquals(0, accuracy.truePositives());
        assertEquals(0, accuracy.falsePositives());
        assertEquals(0, accuracy.falseNegatives());
        assertNull(accuracy.precision());
        assertNull(accuracy.recall());
        assertNull(accuracy.falseDiscoveryRate());
        assertNull(accuracy.falseNegativeRate());
    }

    @Test
    void missingPredictionsRemainFalseNegativesWithoutCoverageExclusion() {
        Accuracy accuracy = VariantAccuracyTestSupport.compare(List.of(allele(4, "G")), List.of());

        assertEquals(1, accuracy.falseNegatives());
        assertEquals(0.0, accuracy.recall());
        assertEquals(1.0, accuracy.falseNegativeRate());
        assertNull(accuracy.precision());
        assertNull(accuracy.falseDiscoveryRate());
    }

    @Test
    void predictionsAgainstEmptyTruthAreFalsePositivesNotPerfectRecall() {
        Accuracy accuracy = VariantAccuracyTestSupport.compare(List.of(), List.of(allele(4, "G")));

        assertEquals(1, accuracy.falsePositives());
        assertEquals(0.0, accuracy.precision());
        assertEquals(1.0, accuracy.falseDiscoveryRate());
        assertNull(accuracy.recall());
        assertNull(accuracy.falseNegativeRate());
    }

    @Test
    void duplicateTruthAndPredictionsDoNotInflateCounts() {
        Allele matched = allele(5, "C");
        Accuracy accuracy = VariantAccuracyTestSupport.compare(
                List.of(matched, matched), List.of(matched, matched, matched));

        assertEquals(1, accuracy.truePositives());
        assertEquals(0, accuracy.falsePositives());
        assertEquals(0, accuracy.falseNegatives());
        assertEquals(1.0, accuracy.precision());
        assertEquals(1.0, accuracy.recall());
    }

    @Test
    void chromosomePositionReferenceAndAlternateAreAllPartOfTheIdentity() {
        Allele truth = allele(5, "C");
        Accuracy accuracy = VariantAccuracyTestSupport.compare(List.of(truth), List.of(
                new Allele("other", 5, "A", "C"), allele(6, "C"),
                new Allele("chr", 5, "G", "C"), allele(5, "T")));

        assertEquals(0, accuracy.truePositives());
        assertEquals(4, accuracy.falsePositives());
        assertEquals(1, accuracy.falseNegatives());
    }

    @Test
    void doesNotPretendDifferentComplexRepresentationsAreEquivalent() {
        Accuracy accuracy = VariantAccuracyTestSupport.compare(
                List.of(new Allele("chr", 10, "AA", "A")),
                List.of(new Allele("chr", 11, "AA", "A")));

        assertEquals(0, accuracy.truePositives());
        assertEquals(1, accuracy.falsePositives());
        assertEquals(1, accuracy.falseNegatives());
    }

    @Test
    void explicitBedUsesZeroBasedHalfOpenBoundariesForBothSets() {
        List<Allele> truth = List.of(allele(10, "C"), allele(11, "C"),
                allele(20, "G"), allele(21, "T"), new Allele("other", 11, "A", "G"));
        List<Allele> calls = List.of(allele(10, "C"), allele(11, "C"), allele(15, "T"),
                allele(21, "T"), new Allele("other", 11, "A", "G"));

        Accuracy accuracy = VariantAccuracyTestSupport.compare(truth, calls,
                List.of(new Interval("chr", 10, 20), new Interval("chr", 10, 15)));

        assertEquals(1, accuracy.truePositives());
        assertEquals(1, accuracy.falsePositives());
        assertEquals(1, accuracy.falseNegatives());
    }

    @Test
    void bedScopeIsExplicitlyBasedOnAnchorNotDeletionSpan() {
        Allele deletion = new Allele("chr", 20, "AAA", "A");
        Accuracy accuracy = VariantAccuracyTestSupport.compare(List.of(deletion), List.of(),
                List.of(new Interval("chr", 19, 20)));

        assertEquals(1, accuracy.falseNegatives());
    }

    @Test
    void parsesOnlyGtSelectedAlternativesAndDeduplicatesDosageAndRecords() throws Exception {
        Path vcf = vcf(row(1, "C,G", "PASS", "GT", "2")
                + row(2, "C,G", "PASS", "GT", "1|2")
                + row(3, "C", "PASS", "GT", "1/1")
                + row(3, "C", "PASS", "GT", "1")
                + row(4, "C", "PASS", "GT", "0/0")
                + row(5, ".", "PASS", "GT", "0"));

        assertEquals(Set.of(allele(1, "G"), allele(2, "C"), allele(2, "G"), allele(3, "C")),
                VariantAccuracyTestSupport.readCalledAlleles(vcf, FilterPolicy.PASS_ONLY));
    }

    @Test
    void explicitNoCallsAreEmptyWhileKnownAlleleOfPartialNoCallIsRetained() throws Exception {
        Path vcf = vcf(row(1, "C", "PASS", "GT", ".")
                + row(2, "G", "PASS", "GT", "./.")
                + row(3, "T", "PASS", "GT", "1/.")
                + row(4, "C", "PASS", "GT", ".|1")
                + row(5, "C", "PASS", "DP:GT", "5:1"));

        assertEquals(Set.of(allele(3, "T"), allele(4, "C"), allele(5, "C")),
                VariantAccuracyTestSupport.readCalledAlleles(vcf, FilterPolicy.ALL));
    }

    @Test
    void noCallAtKnownTruthPositionCountsAsMissedVariant() throws Exception {
        Set<Allele> calls = VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, "C", "PASS", "GT", "./.")), FilterPolicy.PASS_ONLY);

        Accuracy accuracy = VariantAccuracyTestSupport.compare(List.of(allele(1, "C")), calls);

        assertEquals(1, accuracy.falseNegatives());
        assertEquals(0.0, accuracy.recall());
    }

    @Test
    void makesPassUnfilteredAndRejectedRecordInclusionExplicit() throws Exception {
        Path vcf = vcf(row(1, "C", "PASS", "GT", "1")
                + row(2, "G", ".", "GT", "1")
                + row(3, "T", "LowQual;q10", "GT", "1"));

        assertEquals(Set.of(allele(1, "C")),
                VariantAccuracyTestSupport.readCalledAlleles(vcf, FilterPolicy.PASS_ONLY));
        assertEquals(Set.of(allele(1, "C"), allele(2, "G")),
                VariantAccuracyTestSupport.readCalledAlleles(vcf, FilterPolicy.PASS_OR_UNFILTERED));
        assertEquals(Set.of(allele(1, "C"), allele(2, "G"), allele(3, "T")),
                VariantAccuracyTestSupport.readCalledAlleles(vcf, FilterPolicy.ALL));
    }

    @Test
    void detectsGzipFromMagicNotFileExtension() throws Exception {
        Path gzip = tempDir.resolve("compressed.data");
        try (OutputStream output = new GZIPOutputStream(Files.newOutputStream(gzip))) {
            output.write((HEADER + row(2, "G", "PASS", "GT", "1")).getBytes(StandardCharsets.UTF_8));
        }

        assertEquals(Set.of(allele(2, "G")),
                VariantAccuracyTestSupport.readCalledAlleles(gzip, FilterPolicy.PASS_ONLY));
    }

    @Test
    void distinguishesStandaloneSymbolicVariantsFromNonVariantPlaceholders() throws Exception {
        Path vcf = vcf(row(1, "C,*", "PASS", "GT", "2")
                + row(2, "<NON_REF>", "PASS", "GT", "1")
                + row(3, "<*>", "PASS", "GT", "1")
                + row(4, "<DEL>", "PASS", "GT", "1"));

        assertEquals(Set.of(new Allele("chr", 4, "A", "<DEL>")),
                VariantAccuracyTestSupport.readCalledAlleles(vcf, FilterPolicy.PASS_ONLY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "-1", "x", "1/", "/1", "1//0", "1.0", "2", "999999999999999999"})
    void rejectsMalformedOrOutOfRangeGenotypes(String gt) throws Exception {
        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, "C", "PASS", "GT", gt)), FilterPolicy.ALL));
    }

    @Test
    void rejectsMissingGtFieldAndMissingGtValue() throws Exception {
        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, "C", "PASS", "DP", "9")), FilterPolicy.ALL));
        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, "C", "PASS", "DP:GT", "9")), FilterPolicy.ALL));
        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, "C", "PASS", "GT:GT", "1:1")), FilterPolicy.ALL));
    }

    @Test
    void rejectsInvalidGtEvenWhenRecordWouldBeExcludedByFilter() throws Exception {
        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, "C", "LowQual", "GT", "2")), FilterPolicy.PASS_ONLY));
    }

    @Test
    void missingColumnHeaderSampleOrExtraSamplesAreErrors() throws Exception {
        for (String text : List.of("##fileformat=VCFv4.2\n",
                HEADER.replace("\tFORMAT\tsample", "\tFORMAT"),
                HEADER.replace("\tsample\n", "\t\n"),
                HEADER.replace("\tsample\n", "\tsample\tsecond\n"),
                HEADER.substring(HEADER.indexOf('#', 2)),
                row(1, "C", "PASS", "GT", "1"))) {
            Path file = tempDir.resolve("invalid-header.vcf");
            Files.writeString(file, text);
            assertThrows(IOException.class,
                    () -> VariantAccuracyTestSupport.readCalledAlleles(file, FilterPolicy.ALL));
        }
    }

    @Test
    void validHeaderWithNoRecordsIsAValidEmptyPredictionSet() throws Exception {
        assertTrue(VariantAccuracyTestSupport.readCalledAlleles(vcf(""), FilterPolicy.ALL).isEmpty());
    }

    @Test
    void rejectsMissingSampleInDataAndInconsistentNoAlternateGenotype() throws Exception {
        Path missingSample = vcf("chr\t1\t.\tA\tC\t30\tPASS\t.\tGT\n");
        assertThrows(IOException.class,
                () -> VariantAccuracyTestSupport.readCalledAlleles(missingSample, FilterPolicy.ALL));
        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readCalledAlleles(
                vcf(row(1, ".", "PASS", "GT", "1")), FilterPolicy.ALL));
    }

    @Test
    void readsBedCommentsAdditionalColumnsAndBounds() throws Exception {
        Path bed = tempDir.resolve("regions.bed");
        Files.writeString(bed, "# explicit evaluation regions\ntrack name=truth\n"
                + "browser position chr:1-20\nchr\t0\t10\tfirst\nchr\t15\t20\n");

        assertEquals(List.of(new Interval("chr", 0, 10), new Interval("chr", 15, 20)),
                VariantAccuracyTestSupport.readBed(bed));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "# no regions\n", "chr\t0\n", "chr\t-1\t10\n",
            "chr\t10\t10\n", "chr\t20\t10\n", "chr\ta\t10\n"})
    void rejectsEmptyOrMalformedExplicitBedInsteadOfSilentlyChangingScope(String content) throws Exception {
        Path bed = tempDir.resolve("invalid.bed");
        Files.writeString(bed, content);

        assertThrows(IOException.class, () -> VariantAccuracyTestSupport.readBed(bed));
    }

    private Path vcf(String rows) throws IOException {
        Path path = Files.createTempFile(tempDir, "case-", ".vcf");
        Files.writeString(path, HEADER + rows, StandardCharsets.UTF_8);
        return path;
    }

    private static String row(long position, String alt, String filter, String format, String gt) {
        return "chr\t" + position + "\t.\tA\t" + alt + "\t30\t" + filter + "\t.\t"
                + format + "\t" + gt + "\n";
    }

    private static Allele allele(long position, String alternative) {
        return new Allele("chr", position, "A", alternative);
    }
}
