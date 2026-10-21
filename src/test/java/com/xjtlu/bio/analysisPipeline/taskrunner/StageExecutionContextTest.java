package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.StageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

/** Exercises the real executor lifecycle without processes, storage, Spring or database access. */
class StageExecutionContextTest {

    private static final long PIPELINE_ID = 3_000_000_000L;
    private static final long STAGE_ID = 4_000_000_000L;
    private static final int VERSION = 7;
    private static final int STAGE_TYPE = 30;

    @TempDir
    Path tempDir;

    @Test
    void explicitFailurePreservesPipelineAndExecutionIdentityForTheDoneHandler() {
        StubExecutor executor = new StubExecutor(tempDir);

        StageRunResult<StageOutput> result = executor.execute(stage(PIPELINE_ID));

        assertFalse(result.isSuccess());
        assertEquals("fixture execution failure", result.getFailReason());
        assertContext(result, PIPELINE_ID);
        assertSame(executor.preContext, result.getStageContext());
        assertSame(executor.executionContext, result.getStageContext());
        assertSame(executor.postContext, result.getStageContext());
    }

    @Test
    void preExecutionFailureAlreadyHasThePipelineAndExecutionIdentity() {
        StubExecutor executor = new StubExecutor(tempDir);
        executor.preFailure = new IOException("fixture preparation failure");

        StageRunResult<StageOutput> result = executor.execute(stage(PIPELINE_ID));

        assertFalse(result.isSuccess());
        assertSame(executor.preFailure, result.getE());
        assertContext(result, PIPELINE_ID);
        assertSame(executor.preContext, result.getStageContext());
        assertSame(executor.postContext, result.getStageContext());
        assertNull(executor.executionContext);
    }

    @Test
    void unexpectedExecutionFailureRetainsItsPipelineAssociation() {
        StubExecutor executor = new StubExecutor(tempDir);
        executor.executionFailure = new IllegalStateException("fixture execution exception");

        StageRunResult<StageOutput> result = executor.execute(stage(PIPELINE_ID));

        assertFalse(result.isSuccess());
        assertTrue(result.getFailReason().contains("fixture execution exception"));
        assertContext(result, PIPELINE_ID);
        assertSame(executor.executionContext, result.getStageContext());
    }

    @Test
    void standaloneStageWithoutPipelineRetainsTheLegacyZeroContext() {
        StubExecutor executor = new StubExecutor(tempDir);

        StageRunResult<StageOutput> result = executor.execute(stage(null));

        assertFalse(result.isSuccess());
        assertContext(result, 0L);
        assertEquals("fixture execution failure", result.getFailReason());
    }

    private void assertContext(StageRunResult<StageOutput> result, long expectedPipelineId) {
        StageContext context = result.getStageContext();
        assertEquals(expectedPipelineId, context.getPipelineId());
        assertEquals(STAGE_ID, context.getRunStageId());
        assertEquals(VERSION, context.getVersion());
        assertEquals(STAGE_TYPE, context.getStageType());
        assertEquals(tempDir.resolve("work"), result.getWorkDir());
    }

    private static BioPipelineStage stage(Long pipelineId) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(pipelineId);
        stage.setStageId(STAGE_ID);
        stage.setVersion(VERSION);
        stage.setStageType(STAGE_TYPE);
        stage.setInputUrl("{}");
        stage.setParameters("{}");
        return stage;
    }

    public static final class StubInput implements StageInputUrls {
    }

    private static final class StubExecutor
            extends AbstractPipelineStageExector<StageOutput, StubInput, BaseStageParams> {
        private final Path root;
        private StageContext preContext;
        private StageContext executionContext;
        private StageContext postContext;
        private IOException preFailure;
        private RuntimeException executionFailure;

        private StubExecutor(Path root) {
            this.root = root;
        }

        @Override
        public int id() {
            return STAGE_TYPE;
        }

        @Override
        protected Class<StubInput> stageInputType() {
            return StubInput.class;
        }

        @Override
        protected Class<BaseStageParams> stageParameterType() {
            return BaseStageParams.class;
        }

        @Override
        public Path workDirPath(StageContext context) {
            return root.resolve("work");
        }

        @Override
        public Path stageInputPath(StageContext context) {
            return root.resolve("input");
        }

        @Override
        protected void preExecute(StageContext context) throws IOException {
            preContext = context;
            if (preFailure != null) {
                throw preFailure;
            }
        }

        @Override
        protected StageRunResult<StageOutput> _execute(StageExecutionInput input) {
            executionContext = input.stageContext;
            if (executionFailure != null) {
                throw executionFailure;
            }
            return runFail(input.stageContext, "fixture execution failure", input.workDir);
        }

        @Override
        protected void postExecute(StageExecutionInput input) {
            postContext = input.stageContext;
        }
    }
}
