package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.QCStageOutput;

class QcStageDoneHandlerTest {

    private TestHandler handler;

    @BeforeEach
    void setUp() {
        handler = new TestHandler();
        ReflectionTestUtils.setField(handler, "jsonFileName", "cleaned.json");
        ReflectionTestUtils.setField(handler, "htmlFileName", "cleaned.html");
    }

    @Test
    void dottedDirectoriesAndSampleNamesDoNotEnterPairedReadObjectNames() {
        QCStageOutput output = new QCStageOutput(
                "/work/run.v1/sample.v2_R1.fastq.gz",
                "/work/run.v1/sample.v2_R2.fastq.gz",
                "/work/run.v1/cleaned.json",
                "/work/run.v1/cleaned.html", true);

        Pair<Map<String, String>, QcResult> plan = handler.plan(output);

        assertEquals(4, plan.getLeft().size());
        assertEquals("stored/trimmed_r1.fastq.gz",
                plan.getLeft().get(output.getR1Path()));
        assertEquals("stored/trimmed_r2.fastq.gz",
                plan.getLeft().get(output.getR2Path()));
        assertEquals("stored/trimmed_r1.fastq.gz", plan.getRight().getCleanedR1());
        assertEquals("stored/trimmed_r2.fastq.gz", plan.getRight().getCleanedR2());
    }

    @Test
    void uncompressedSingleReadKeepsItsFileExtension() {
        QCStageOutput output = new QCStageOutput(
                "/work/run.v1/sample.v2.fastq", null,
                "/work/run.v1/cleaned.json",
                "/work/run.v1/cleaned.html", false);

        Pair<Map<String, String>, QcResult> plan = handler.plan(output);

        assertEquals(3, plan.getLeft().size());
        assertEquals("stored/trimmed_r1.fastq",
                plan.getLeft().get(output.getR1Path()));
        assertEquals("stored/trimmed_r1.fastq", plan.getRight().getCleanedR1());
        assertNull(plan.getRight().getCleanedR2());
    }

    private static final class TestHandler extends QcStageDoneHandler {

        private Pair<Map<String, String>, QcResult> plan(QCStageOutput output) {
            StageRunResult<QCStageOutput> result = StageRunResult.OK(
                    output, new StageContext(1L, 0, getType(), 1), Path.of("/work/run.v1"));
            return buildUploadConfigAndOutputUrlMap(result);
        }

        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }
    }
}
