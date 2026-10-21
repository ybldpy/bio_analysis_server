package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.CovidWastewaterStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.CovidWastewaterStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

class CovidWastewaterStageDoneHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void importsFourRequiredArtifactsWithoutBamOrInlineMetrics() throws Exception {
        CovidWastewaterStageOutput output = output();
        TestHandler handler = new TestHandler();

        Pair<Map<String, String>, CovidWastewaterStageResult> plan = handler.plan(output);

        assertEquals(tempDir, output.getParentPath());
        assertEquals(Constants.StageType.PIPELINE_STAGE_COVID_WASTEWATER, handler.getType());
        assertEquals(4, plan.getLeft().size());
        assertEquals("stored/freyja.demix.tsv", plan.getRight().getReportUrl());
        assertEquals("stored/covid-wastewater.metrics.json", plan.getRight().getMetricsUrl());
        assertEquals("stored/provenance.json", plan.getRight().getProvenanceUrl());
        assertEquals("stored/execution.log", plan.getRight().getExecutionLogUrl());
        assertNull(plan.getRight().getBamUrl());
        assertNull(plan.getRight().getBamIndexUrl());
        String json = JsonUtil.toJson(plan.getRight());
        assertFalse(json.contains("\"lineages\""));
        assertFalse(json.contains("\"metrics\":"));
    }

    @Test
    void publishesOptionalBamAndIndexOnlyWhenBothFilesAreUsable() throws Exception {
        CovidWastewaterStageOutput output = output();
        output.setBamPath(artifact("source.bam"));
        output.setBamIndexPath(artifact("source.bam.bai"));

        Pair<Map<String, String>, CovidWastewaterStageResult> plan = new TestHandler().plan(output);

        assertEquals(6, plan.getLeft().size());
        assertEquals("stored/aligned.bam", plan.getRight().getBamUrl());
        assertEquals("stored/aligned.bam.bai", plan.getRight().getBamIndexUrl());
        assertEquals("stored/aligned.bam", plan.getLeft().get(output.getBamPath().toAbsolutePath().toString()));
    }

    @Test
    void incompleteOptionalPairsAreOmittedWithoutNullPointerFailures() throws Exception {
        CovidWastewaterStageOutput output = output();
        TestHandler handler = new TestHandler();
        Path bam = artifact("source.bam");
        Path index = artifact("source.bam.bai");
        Path empty = Files.createFile(tempDir.resolve("empty.bai"));
        Path missing = tempDir.resolve("missing.bai");
        for (Path[] pair : new Path[][] {
                { bam, null }, { null, index }, { bam, missing }, { bam, empty }, { tempDir, index } }) {
            output.setBamPath(pair[0]);
            output.setBamIndexPath(pair[1]);
            Pair<Map<String, String>, CovidWastewaterStageResult> plan = handler.plan(output);
            assertEquals(4, plan.getLeft().size());
            assertNull(plan.getRight().getBamUrl());
            assertNull(plan.getRight().getBamIndexUrl());
        }
    }

    @ParameterizedTest
    @MethodSource("requiredArtifactSetters")
    void everyRequiredArtifactMustBeANonemptyRegularFile(BiConsumer<CovidWastewaterStageOutput, Path> setter)
            throws Exception {
        CovidWastewaterStageOutput output = output();
        Path empty = Files.createFile(tempDir.resolve("empty"));
        for (Path invalid : new Path[] { null, tempDir.resolve("missing"), empty, tempDir }) {
            setter.accept(output, invalid);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                    () -> new TestHandler().plan(output));
            assertTrue(exception.getMessage().contains("nonempty regular file"));
        }
    }

    @Test
    void nullOutputAndAliasedArtifactPathsAreRejectedClearly() throws Exception {
        TestHandler handler = new TestHandler();
        assertThrows(IllegalArgumentException.class, () -> handler.plan(null));
        assertNull(new CovidWastewaterStageOutput().getParentPath());

        CovidWastewaterStageOutput output = output();
        output.setMetricsPath(output.getReportPath());
        assertTrue(assertThrows(IllegalArgumentException.class, () -> handler.plan(output))
                .getMessage().contains("distinct source files"));
    }

    @Test
    void inheritedCompletionUploadsThenStoresOnlyArtifactUrls() throws Exception {
        CovidWastewaterStageOutput output = output();
        TestHandler handler = new TestHandler();

        handler.handleStageDone(handler.runResult(output));

        assertEquals(4, handler.uploaded.size());
        assertNotNull(handler.saved);
        assertEquals(Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED, handler.saved.getStatus());
        assertNotNull(handler.saved.getOutputUrl());
        assertNull(handler.saved.getOutputInline());
        CovidWastewaterStageResult stored = JsonUtil.toObject(
                handler.saved.getOutputUrl(), CovidWastewaterStageResult.class);
        assertEquals("stored/covid-wastewater.metrics.json", stored.getMetricsUrl());
        assertTrue(handler.notifiedSuccess);
        assertNull(handler.failure);
    }

    @Test
    void invalidCompletionIsRoutedThroughTheParentFailureBoundaryBeforeUploading() throws Exception {
        CovidWastewaterStageOutput output = output();
        output.setProvenancePath(null);
        TestHandler handler = new TestHandler();

        handler.handleStageDone(handler.runResult(output));

        assertNull(handler.uploaded);
        assertNull(handler.saved);
        assertNotNull(handler.failure);
        assertTrue(handler.failure.contains("provenance"));
        assertFalse(handler.notifiedSuccess);
    }

    private static Stream<BiConsumer<CovidWastewaterStageOutput, Path>> requiredArtifactSetters() {
        return Stream.of(CovidWastewaterStageOutput::setReportPath, CovidWastewaterStageOutput::setMetricsPath,
                CovidWastewaterStageOutput::setProvenancePath, CovidWastewaterStageOutput::setExecutionLogPath);
    }

    private CovidWastewaterStageOutput output() throws IOException {
        CovidWastewaterStageOutput output = new CovidWastewaterStageOutput();
        output.setReportPath(artifact("report.tsv"));
        output.setMetricsPath(artifact("metrics.json"));
        output.setProvenancePath(artifact("provenance.json"));
        output.setExecutionLogPath(artifact("execution.log"));
        return output;
    }

    private Path artifact(String filename) throws IOException {
        // Content is a synthetic marker, not real sequencing data or tool output.
        return Files.writeString(tempDir.resolve(filename), "fixture\n");
    }

    private static final class TestHandler extends CovidWastewaterStageDoneHandler {

        private Map<String, String> uploaded;
        private BioPipelineStage saved;
        private String failure;
        private boolean notifiedSuccess;

        private Pair<Map<String, String>, CovidWastewaterStageResult> plan(CovidWastewaterStageOutput output) {
            return buildUploadConfigAndOutputUrlMap(runResult(output));
        }

        private StageRunResult<CovidWastewaterStageOutput> runResult(CovidWastewaterStageOutput output) {
            return StageRunResult.OK(output, new StageContext(1L, 0, getType(), 1), null);
        }

        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }

        @Override
        protected boolean batchUploadObjectsFromLocal(Map<String, String> params) {
            uploaded = Map.copyOf(params);
            return true;
        }

        @Override
        protected int updateStageFromVersion(BioPipelineStage stage, long stageId, int version) {
            saved = stage;
            return 1;
        }

        @Override
        protected void cleanupAfterCompletion(String deleteDir) {
            // Test fixtures remain managed by JUnit; no production directories are touched.
        }

        @Override
        protected void notifyStageDone(StageContext context, boolean success) {
            notifiedSuccess = success;
        }

        @Override
        protected void handleFail(StageContext context, String deleteDir, String failMessage) {
            failure = failMessage;
        }
    }
}
