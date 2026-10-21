package com.xjtlu.bio.analysisPipeline.stageResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;
import com.xjtlu.bio.utils.JsonUtil;

class StageResultMetricsCompatibilityTest {

    @Test
    void readsLegacyMappingMetricsPropertiesButWritesOnlyMetricsUrl() throws Exception {
        MappingResult result = JsonUtil.toObject("""
                {
                  "qcUrl": "legacy/mapping.qc.json",
                  "qcMetrics": {"totalReferenceBases": 10}
                }
                """, MappingResult.class);

        assertEquals("legacy/mapping.qc.json", result.getMetricsUrl());
        assertNotNull(result.getMetrics());
        assertEquals(10L, result.getMetrics().getTotalReferenceBases());
        assertContainsOnlyMetricsUrl(JsonUtil.toJson(result), "legacy/mapping.qc.json");
    }

    @Test
    void readsLegacyVariantMetricsPropertiesButWritesOnlyMetricsUrl() throws Exception {
        VarientCallStageResult result = JsonUtil.toObject("""
                {
                  "qcUrl": "legacy/variant.qc.json",
                  "qcMetrics": {"variantRecords": 2}
                }
                """, VarientCallStageResult.class);

        assertEquals("legacy/variant.qc.json", result.getMetricsUrl());
        assertNotNull(result.getMetrics());
        assertEquals(2L, result.getMetrics().getVariantRecords());
        assertContainsOnlyMetricsUrl(JsonUtil.toJson(result), "legacy/variant.qc.json");
    }

    @Test
    void readsLegacyConsensusMetricsPropertiesButWritesOnlyMetricsUrl() throws Exception {
        ConsensusStageResult result = JsonUtil.toObject("""
                {
                  "qcUrl": "legacy/consensus.qc.json",
                  "qcMetrics": {"minimumDepth": 10}
                }
                """, ConsensusStageResult.class);

        assertEquals("legacy/consensus.qc.json", result.getMetricsUrl());
        assertNotNull(result.getMetrics());
        assertEquals(10, result.getMetrics().getMinimumDepth());
        assertContainsOnlyMetricsUrl(JsonUtil.toJson(result), "legacy/consensus.qc.json");
    }

    @Test
    void readsPreviouslyEmbeddedMetricsWithoutWritingThemBack() throws Exception {
        MappingResult mapping = JsonUtil.toObject("""
                {"metricsUrl":"saved/mapping.metrics.json","metrics":{"totalReferenceBases":10}}
                """, MappingResult.class);
        VarientCallStageResult variant = JsonUtil.toObject("""
                {"metricsUrl":"saved/variant.metrics.json","metrics":{"variantRecords":2}}
                """, VarientCallStageResult.class);
        ConsensusStageResult consensus = JsonUtil.toObject("""
                {"metricsUrl":"saved/consensus.metrics.json","metrics":{"minimumDepth":10}}
                """, ConsensusStageResult.class);

        assertNotNull(mapping.getMetrics());
        assertEquals(10L, mapping.getMetrics().getTotalReferenceBases());
        assertNotNull(variant.getMetrics());
        assertEquals(2L, variant.getMetrics().getVariantRecords());
        assertNotNull(consensus.getMetrics());
        assertEquals(10, consensus.getMetrics().getMinimumDepth());
        assertContainsOnlyMetricsUrl(JsonUtil.toJson(mapping), "saved/mapping.metrics.json");
        assertContainsOnlyMetricsUrl(JsonUtil.toJson(variant), "saved/variant.metrics.json");
        assertContainsOnlyMetricsUrl(JsonUtil.toJson(consensus), "saved/consensus.metrics.json");
    }

    @Test
    void mappingResultRoundTripsUsingOnlyArtifactUrls() throws Exception {
        MappingResult result = new MappingResult(
                "mapping/result.bam", "mapping/result.bam.bai", "mapping/depth.tsv",
                "mapping/metrics.json");

        String json = JsonUtil.toJson(result);
        MappingResult restored = JsonUtil.toObject(json, MappingResult.class);

        assertEquals("mapping/result.bam", restored.getBamUrl());
        assertEquals("mapping/result.bam.bai", restored.getBamIndexUrl());
        assertEquals("mapping/depth.tsv", restored.getCoverageDepthUrl());
        assertEquals("mapping/metrics.json", restored.getMetricsUrl());
        assertNull(result.getMetrics());
        assertNull(restored.getMetrics());
        assertContainsOnlyMetricsUrl(json, "mapping/metrics.json");
        assertEquals(json, JsonUtil.toJson(restored));
    }

    @Test
    void variantResultRoundTripsUsingOnlyArtifactUrls() throws Exception {
        VarientCallStageResult result = new VarientCallStageResult(
                "variant/raw.vcf.gz", "variant/raw.vcf.gz.tbi",
                "variant/filtered.vcf.gz", "variant/filtered.vcf.gz.tbi",
                "variant/metrics.json");

        String json = JsonUtil.toJson(result);
        VarientCallStageResult restored = JsonUtil.toObject(json, VarientCallStageResult.class);

        assertEquals("variant/raw.vcf.gz", restored.getRawVcfGzUrl());
        assertEquals("variant/raw.vcf.gz.tbi", restored.getRawVcfTbiUrl());
        assertEquals("variant/filtered.vcf.gz", restored.getVcfGzUrl());
        assertEquals("variant/filtered.vcf.gz.tbi", restored.getVcfTbiUrl());
        assertEquals("variant/metrics.json", restored.getMetricsUrl());
        assertNull(result.getMetrics());
        assertNull(restored.getMetrics());
        assertContainsOnlyMetricsUrl(json, "variant/metrics.json");
        assertEquals(json, JsonUtil.toJson(restored));
    }

    @Test
    void consensusResultRoundTripsUsingOnlyArtifactUrls() throws Exception {
        ConsensusStageResult result = new ConsensusStageResult(
                "consensus/result.fasta", "consensus/mask.bed", "consensus/metrics.json");

        String json = JsonUtil.toJson(result);
        ConsensusStageResult restored = JsonUtil.toObject(json, ConsensusStageResult.class);

        assertEquals("consensus/result.fasta", restored.getConsensusUrl());
        assertEquals("consensus/mask.bed", restored.getMaskBedUrl());
        assertEquals("consensus/metrics.json", restored.getMetricsUrl());
        assertNull(result.getMetrics());
        assertNull(restored.getMetrics());
        assertContainsOnlyMetricsUrl(json, "consensus/metrics.json");
        assertEquals(json, JsonUtil.toJson(restored));
    }

    @Test
    void mappingResultDoesNotSerializeLargeMetrics() throws Exception {
        MappingResult result = new MappingResult(
                "mapping/result.bam", "mapping/result.bam.bai", "mapping/depth.tsv",
                "mapping/metrics.json");
        String urlOnlyJson = JsonUtil.toJson(result);
        MappingResultMetrics metrics = new MappingResultMetrics();
        metrics.setReferences(IntStream.range(0, 1_000)
                .mapToObj(index -> new MappingResultMetrics.ReferenceMetrics(
                        "mapping-reference-" + index, 1_000L, 900L, 9_000L, 15L, 80L, 20L))
                .toList());
        assertTrue(JsonUtil.toJson(metrics).length() > 50_000);

        result.setMetrics(metrics);
        String json = JsonUtil.toJson(result);

        assertNotNull(result.getMetrics());
        assertContainsOnlyMetricsUrl(json, "mapping/metrics.json");
        assertEquals(urlOnlyJson, json);
    }

    @Test
    void variantResultDoesNotSerializeLargeMetrics() throws Exception {
        VarientCallStageResult result = new VarientCallStageResult(
                "variant/raw.vcf.gz", "variant/raw.vcf.gz.tbi",
                "variant/filtered.vcf.gz", "variant/filtered.vcf.gz.tbi",
                "variant/metrics.json");
        String urlOnlyJson = JsonUtil.toJson(result);
        VariantCallResultMetrics metrics = new VariantCallResultMetrics();
        metrics.setReferences(IntStream.range(0, 1_000)
                .mapToObj(index -> {
                    VariantCallResultMetrics.ReferenceMetrics reference =
                            new VariantCallResultMetrics.ReferenceMetrics();
                    reference.setReferenceName("variant-reference-" + index);
                    reference.setVariantRecords(2L);
                    return reference;
                })
                .toList());
        assertTrue(JsonUtil.toJson(metrics).length() > 50_000);

        result.setMetrics(metrics);
        String json = JsonUtil.toJson(result);

        assertNotNull(result.getMetrics());
        assertContainsOnlyMetricsUrl(json, "variant/metrics.json");
        assertEquals(urlOnlyJson, json);
    }

    @Test
    void consensusResultDoesNotSerializeLargeMetrics() throws Exception {
        ConsensusStageResult result = new ConsensusStageResult(
                "consensus/result.fasta", "consensus/mask.bed", "consensus/metrics.json");
        String urlOnlyJson = JsonUtil.toJson(result);
        ConsensusResultMetrics metrics = new ConsensusResultMetrics();
        metrics.setSequences(IntStream.range(0, 1_000)
                .mapToObj(index -> new ConsensusResultMetrics.SequenceMetrics(
                        "consensus-sequence-" + index, 1_000L, 900L, 100L))
                .toList());
        assertTrue(JsonUtil.toJson(metrics).length() > 50_000);

        result.setMetrics(metrics);
        String json = JsonUtil.toJson(result);

        assertNotNull(result.getMetrics());
        assertContainsOnlyMetricsUrl(json, "consensus/metrics.json");
        assertEquals(urlOnlyJson, json);
    }

    private static void assertContainsOnlyMetricsUrl(String json, String expectedMetricsUrl)
            throws Exception {
        Map<String, Object> properties = JsonUtil.toMap(json);
        assertEquals(expectedMetricsUrl, properties.get("metricsUrl"));
        assertFalse(properties.containsKey("metrics"));
        assertFalse(properties.containsKey("qcUrl"));
        assertFalse(properties.containsKey("qcMetrics"));
    }
}
