package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.VarientCallStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;
import com.xjtlu.bio.utils.JsonUtil;

class VarientStageDoneHandlerTest {

    private TestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TestHandler();
        ReflectionTestUtils.setField(handler, "vcfFileName", "variants.vcf.gz");
        ReflectionTestUtils.setField(handler, "vcfFileIndexFileName", "variants.vcf.gz.tbi");
        ReflectionTestUtils.setField(handler, "rawVcfFileName", "variants.raw.vcf.gz");
        ReflectionTestUtils.setField(handler, "rawVcfFileIndexFileName", "variants.raw.vcf.gz.tbi");
        ReflectionTestUtils.setField(handler, "metricsFileName", "variant.metrics.json");
    }

    @Test
    void includesMetricsArtifactUrlWithoutInlineMetricsInCompletionPlan() throws Exception {
        VariantCallResultMetrics metrics = new VariantCallResultMetrics(
                2L, 1L, 1L, 0L, 0L, 1L, 0L, 0L, List.of());
        VariantStageOutput output = new VariantStageOutput(
                "/work/variants.raw.vcf.gz",
                "/work/variants.raw.vcf.gz.tbi",
                "/work/variants.vcf.gz",
                "/work/variants.vcf.gz.tbi",
                "/work/variant.metrics.json",
                metrics);

        Pair<Map<String, String>, VarientCallStageResult> plan = handler.plan(output);

        assertEquals(5, plan.getLeft().size());
        assertEquals("stored/variants.raw.vcf.gz",
                plan.getLeft().get("/work/variants.raw.vcf.gz"));
        assertEquals("stored/variants.raw.vcf.gz.tbi",
                plan.getLeft().get("/work/variants.raw.vcf.gz.tbi"));
        assertEquals("stored/variants.raw.vcf.gz",
                plan.getRight().getRawVcfGzUrl());
        assertEquals("stored/variants.raw.vcf.gz.tbi",
                plan.getRight().getRawVcfTbiUrl());
        assertEquals("stored/variant.metrics.json",
                plan.getLeft().get("/work/variant.metrics.json"));
        assertEquals("stored/variant.metrics.json", plan.getRight().getMetricsUrl());
        assertEquals("stored/variants.vcf.gz", plan.getRight().getVcfGzUrl());
        assertEquals("stored/variants.vcf.gz.tbi", plan.getRight().getVcfTbiUrl());
        assertNull(plan.getRight().getMetrics());
        String serializedResult = JsonUtil.toJson(plan.getRight());
        assertFalse(serializedResult.contains("\"metrics\""));
        assertFalse(serializedResult.contains("\"qcMetrics\""));
        assertTrue(serializedResult.length() <= 500);
    }

    @Test
    void legacyOutputConstructorStillBuildsTheOriginalTwoFilePlan() {
        VariantStageOutput output = new VariantStageOutput(
                "/work/variants.vcf.gz", "/work/variants.vcf.gz.tbi");

        Pair<Map<String, String>, VarientCallStageResult> plan = handler.plan(output);

        assertEquals(2, plan.getLeft().size());
        assertNull(plan.getRight().getRawVcfGzUrl());
        assertNull(plan.getRight().getRawVcfTbiUrl());
        assertNull(plan.getRight().getMetricsUrl());
        assertNull(plan.getRight().getMetrics());
    }

    private static final class TestHandler extends VarientStageDoneHandler {

        private Pair<Map<String, String>, VarientCallStageResult> plan(
                VariantStageOutput output) {
            StageRunResult<VariantStageOutput> result = StageRunResult.OK(
                    output, new StageContext(1L, 0, getType(), 1), Path.of("/work"));
            return buildUploadConfigAndOutputUrlMap(result);
        }

        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }
    }
}
