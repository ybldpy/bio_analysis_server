package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

class MappingStageDoneHandlerTest {

    private TestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TestHandler();
        ReflectionTestUtils.setField(handler, "bamFileName", "result.bam");
        ReflectionTestUtils.setField(handler, "bamIndexFileName", "result.bam.bai");
    }

    @Test
    void includesMetricsArtifactUrlWithoutInlineMetricsInCompletionPlan() throws Exception {
        MappingResultMetrics metrics = new MappingResultMetrics(
                10L, 8L, 20L, 5L, 7L, 3L,
                IntStream.range(0, 100)
                        .mapToObj(index -> new MappingResultMetrics.ReferenceMetrics(
                                "reference_" + index, 10L, 8L, 20L, 5L, 7L, 3L))
                        .toList());
        MappingStageOutput output = new MappingStageOutput(
                Path.of("/work/result.bam"),
                Path.of("/work/result.bam.bai"),
                Path.of("/work/coverage_depth.tsv"),
                Path.of("/work/mapping.metrics.json"),
                metrics);

        Pair<Map<String, String>, MappingResult> plan = handler.plan(output);

        assertEquals(4, plan.getLeft().size());
        assertEquals("stored/mapping.metrics.json",
                plan.getLeft().get("/work/mapping.metrics.json"));
        assertEquals("stored/mapping.metrics.json", plan.getRight().getMetricsUrl());
        assertEquals("stored/result.bam", plan.getRight().getBamUrl());
        assertEquals("stored/result.bam.bai", plan.getRight().getBamIndexUrl());
        assertEquals("stored/coverage_depth.tsv", plan.getRight().getCoverageDepthUrl());
        assertNull(plan.getRight().getMetrics());
        String serializedResult = JsonUtil.toJson(plan.getRight());
        assertFalse(serializedResult.contains("\"metrics\""));
        assertFalse(serializedResult.contains("\"qcMetrics\""));
        assertTrue(serializedResult.length() <= 500);
        assertTrue(JsonUtil.toJson(metrics).length() > 500);
    }

    @Test
    void legacyOutputConstructorStillBuildsTheOriginalThreeFilePlan() {
        MappingStageOutput output = new MappingStageOutput(
                Path.of("/work/result.bam"),
                Path.of("/work/result.bam.bai"),
                Path.of("/work/coverage_depth.tsv"));

        Pair<Map<String, String>, MappingResult> plan = handler.plan(output);

        assertEquals(3, plan.getLeft().size());
        assertNull(plan.getRight().getMetricsUrl());
        assertNull(plan.getRight().getMetrics());
    }

    private static final class TestHandler extends MappingStageDoneHandler {

        private Pair<Map<String, String>, MappingResult> plan(
                MappingStageOutput output) {
            StageRunResult<MappingStageOutput> result = StageRunResult.OK(
                    output, new StageContext(1L, 0, getType(), 1), Path.of("/work"));
            return buildUploadConfigAndOutputUrlMap(result);
        }

        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }
    }
}
