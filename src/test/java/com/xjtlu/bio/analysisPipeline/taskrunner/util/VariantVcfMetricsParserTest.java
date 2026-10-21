package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics.ReferenceMetrics;

class VariantVcfMetricsParserTest {

    private static final String VCF = """
            ##fileformat=VCFv4.2
            ##contig=<ID=segment-a,length=10>
            ##contig=<ID=segment-b,length=20>
            ##contig=<ID=segment-with-no-variants,length=30>
            #CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	sample
            segment-a	1	.	A	.	.	PASS	.	GT	0
            segment-a	2	.	A	*	.	PASS	.	GT	0
            segment-a	3	.	A	C	.	PASS	.	GT	1
            segment-a	4	.	A	AT	.	.	.	GT	1
            segment-a	5	.	A	C,AT	.	q10	.	GT	1/2
            segment-b	1	.	AT	GC	.	PASS	.	GT	1
            segment-b	2	.	AT	A,ATT	.	LowQual	.	GT	1
            segment-b	3	.	A	<DEL>	.	.	.	GT	1
            segment-b	4	.	A	C,G	.	PASS	.	GT	1
            segment-b	5	.	A	C,*	.	PASS	.	GT	1
            """;

    @TempDir
    Path tempDir;

    @Test
    void separatesReferenceRowsAndPartitionsMultiAllelicVariants() throws Exception {
        Path vcf = tempDir.resolve("variants.vcf");
        Files.writeString(vcf, VCF, StandardCharsets.UTF_8);

        VariantCallResultMetrics metrics = VariantVcfMetricsParser.parse(vcf);

        assertEquals(10L, metrics.getTotalVcfRecords());
        assertEquals(8L, metrics.getVariantRecords());
        assertEquals(3L, metrics.getSnpCount());
        assertEquals(2L, metrics.getIndelCount());
        assertEquals(3L, metrics.getOtherCount());
        assertEquals(4L, metrics.getExplicitPassCount());
        assertEquals(2L, metrics.getFilteredCount());
        assertEquals(2L, metrics.getUnfilteredCount());

        ReferenceMetrics segmentA = reference(metrics, "segment-a");
        assertEquals(10L, segmentA.getReferenceLength());
        assertEquals(5L, segmentA.getTotalVcfRecords());
        assertEquals(3L, segmentA.getVariantRecords());
        assertEquals(1L, segmentA.getSnpCount());
        assertEquals(1L, segmentA.getIndelCount());
        assertEquals(1L, segmentA.getOtherCount());

        ReferenceMetrics segmentB = reference(metrics, "segment-b");
        assertEquals(5L, segmentB.getTotalVcfRecords());
        assertEquals(5L, segmentB.getVariantRecords());
        assertEquals(2L, segmentB.getSnpCount());
        assertEquals(1L, segmentB.getIndelCount());
        assertEquals(2L, segmentB.getOtherCount());

        ReferenceMetrics emptySegment = reference(
                metrics, "segment-with-no-variants");
        assertEquals(30L, emptySegment.getReferenceLength());
        assertEquals(0L, emptySegment.getTotalVcfRecords());
        assertEquals(0L, emptySegment.getVariantRecords());
        assertEquals(0L, emptySegment.getExplicitPassCount());
    }

    @Test
    void detectsGzipFromContentAndProducesTheSameMetrics() throws Exception {
        Path gzipVcfWithoutGzipExtension = tempDir.resolve("variants.data");
        writeGzip(gzipVcfWithoutGzipExtension, VCF);

        VariantCallResultMetrics metrics = VariantVcfMetricsParser.parse(
                gzipVcfWithoutGzipExtension);

        assertEquals(10L, metrics.getTotalVcfRecords());
        assertEquals(8L, metrics.getVariantRecords());
        assertEquals(3, metrics.getReferences().size());
        assertEquals("segment-with-no-variants",
                metrics.getReferences().get(2).getReferenceName());
    }

    @Test
    void rejectsTruncatedVcfWithoutColumnHeader() throws Exception {
        Path vcf = tempDir.resolve("truncated.vcf");
        Files.writeString(vcf, """
                ##fileformat=VCFv4.2
                ##contig=<ID=segment-a,length=10>
                """, StandardCharsets.UTF_8);

        IOException exception = assertThrows(
                IOException.class,
                () -> VariantVcfMetricsParser.parse(vcf));

        assertTrue(exception.getMessage().contains("missing #CHROM"));
    }

    @Test
    void rejectsDataBeforeRequiredVcfHeaders() throws Exception {
        Path vcf = tempDir.resolve("headerless.vcf");
        Files.writeString(
                vcf,
                "segment-a\t1\t.\tA\tC\t.\t.\t.\n",
                StandardCharsets.UTF_8);

        IOException exception = assertThrows(
                IOException.class,
                () -> VariantVcfMetricsParser.parse(vcf));

        assertTrue(exception.getMessage().contains("before required VCF headers"));
    }

    private static ReferenceMetrics reference(
            VariantCallResultMetrics metrics, String referenceName) {
        ReferenceMetrics result = metrics.getReferences().stream()
                .filter(reference -> referenceName.equals(reference.getReferenceName()))
                .findFirst()
                .orElse(null);
        assertNotNull(result);
        return result;
    }

    private static void writeGzip(Path path, String value) throws IOException {
        try (OutputStream output = new GZIPOutputStream(Files.newOutputStream(path))) {
            output.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
