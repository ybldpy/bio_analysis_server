package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.ConsensusStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

class ConsensusStageDoneHandlerTest {

    @Test
    void uploadsConsensusMaskAndMetricsButOnlyStoresTheirUrls() throws Exception {
        TestHandler handler = new TestHandler();
        ReflectionTestUtils.setField(handler, "consensusOutputFileName", "consensus.fasta");
        ReflectionTestUtils.setField(handler, "maskBedFileName", "consensus.mask.bed");
        ReflectionTestUtils.setField(handler, "metricsFileName", "consensus.metrics.json");
        ConsensusResultMetrics metrics = new ConsensusResultMetrics(
                10, 100L, 80L, 20L,
                IntStream.range(0, 100)
                        .mapToObj(index -> new ConsensusResultMetrics.SequenceMetrics(
                                "reference_" + index, 100L, 80L, 20L))
                        .toList());
        ConsensusStageOutput output = new ConsensusStageOutput(
                "/work/consensus.fasta", "/work/consensus.mask.bed",
                "/work/consensus.metrics.json", metrics);

        Pair<Map<String, String>, ConsensusStageResult> plan = handler.plan(output);

        assertEquals(3, plan.getLeft().size());
        assertEquals("stored/consensus.fasta", plan.getLeft().get("/work/consensus.fasta"));
        assertEquals("stored/consensus.mask.bed", plan.getLeft().get("/work/consensus.mask.bed"));
        assertEquals("stored/consensus.metrics.json", plan.getLeft().get("/work/consensus.metrics.json"));
        assertEquals("stored/consensus.fasta", plan.getRight().getConsensusUrl());
        assertEquals("stored/consensus.mask.bed", plan.getRight().getMaskBedUrl());
        assertEquals("stored/consensus.metrics.json", plan.getRight().getMetricsUrl());
        assertNull(plan.getRight().getMetrics());
        String serializedResult = JsonUtil.toJson(plan.getRight());
        assertFalse(serializedResult.contains("\"metrics\""));
        assertFalse(serializedResult.contains("\"qcMetrics\""));
        assertTrue(serializedResult.length() <= 500);
        assertTrue(JsonUtil.toJson(metrics).length() > 500);
    }

    private static final class TestHandler extends ConsensusStageDoneHandler {

        private Pair<Map<String, String>, ConsensusStageResult> plan(ConsensusStageOutput output) {
            return buildUploadConfigAndOutputUrlMap(StageRunResult.OK(
                    output, new StageContext(1L, 0, getType(), 1), Path.of("/work")));
        }

        @Override
        protected String createStoreObjectName(StageContext context, String name) {
            return "stored/" + name;
        }
    }
}
