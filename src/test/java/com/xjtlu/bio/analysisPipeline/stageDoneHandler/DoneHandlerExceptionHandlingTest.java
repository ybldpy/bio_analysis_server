package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_ACTION_REQUIRED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.EVIDENCE_FASTANI;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;

/** Completion errors are tested without a Spring context, database or pipeline execution. */
class DoneHandlerExceptionHandlingTest {

    private static final long STAGE_ID = 42L;
    private static final int VERSION = 3;

    @TempDir
    Path tempDir;

    private Path workDir;
    private TestHandler handler;
    private PipelineService pipelineService;
    private StorageService storageService;

    @BeforeEach
    void setUp() throws IOException {
        workDir = Files.createDirectory(tempDir.resolve("work"));
        pipelineService = mock(PipelineService.class);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(1);
        storageService = mock(StorageService.class);
        handler = new TestHandler();
        handler.pipelineService = pipelineService;
        handler.storageService = storageService;
    }

    @Test
    void nullPointerWhileBuildingUploadPlanMarksTheStageFailed() {
        handler.planningFailure = new NullPointerException("missing output metadata");

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @Test
    void serializationExceptionMarksTheStageFailedInsteadOfLeavingItRunning() {
        handler.output = new UnserializableResult();

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @Test
    void unexpectedStorageExceptionMarksTheStageFailed() throws IOException {
        Path artifact = Files.writeString(workDir.resolve("artifact.txt"), "fixture");
        handler.uploadPlan = Map.of(artifact.toString(), "stored/artifact.txt");
        when(storageService.putObject(anyString(), any(InputStream.class)))
                .thenThrow(new IllegalStateException("storage unavailable"));

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        assertFailedUpdate();
        verify(storageService).putObject(anyString(), any(InputStream.class));
        assertFalse(Files.exists(workDir));
    }

    @Test
    void missingStageOutputUsesWorkDirectoryToRecordFailureAndCleanUp() {
        StageRunResult<StageOutput> result = StageRunResult.OK(null, stageContext(), workDir);

        assertDoesNotThrow(() -> handler.handleStageDone(result));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @Test
    void cleanupExceptionDoesNotPreventFailurePersistenceOrNotification() {
        handler.planningFailure = new IllegalArgumentException("invalid upload plan");
        handler.cleanupFailure = new SecurityException("cleanup denied");

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        assertFailedUpdate();
        assertTrue(Files.exists(workDir));
    }

    @ParameterizedTest
    @MethodSource("databaseFailures")
    void databaseExceptionsEscapeWithoutAnAttemptToRewriteTheStageAsFailed(RuntimeException failure) {
        handler.planningFailure = failure;

        assertSame(failure, assertThrows(RuntimeException.class, () -> handler.handleStageDone(result())));

        verify(pipelineService, never()).updateStageFromVersion(any(UpdateStageCommand.class));
        verify(pipelineService, never()).pipelineStageDone(anyLong(), anyBoolean());
        assertTrue(Files.exists(workDir));
    }

    @Test
    void failurePersistenceExceptionIsNotRecursivelyRetried() {
        handler.planningFailure = new NullPointerException("invalid output");
        IllegalStateException failure = new IllegalStateException("failed to persist FAIL");
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenThrow(failure);

        assertRootCause(failure, assertThrows(RuntimeException.class, () -> handler.handleStageDone(result())));

        assertEquals(PIPELINE_STAGE_STATUS_FAIL, capturedUpdate().getUpdateStage().getStatus());
        verify(pipelineService, never()).pipelineStageDone(anyLong(), anyBoolean());
    }

    @Test
    void databaseExceptionWhileFinishingDoesNotTriggerASecondFailedUpdate() {
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("database unavailable");
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenThrow(failure);

        assertRootCause(failure, assertThrows(RuntimeException.class,
                () -> handler.handleStageDone(result())));

        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, capturedUpdate().getUpdateStage().getStatus());
        verify(pipelineService, never()).pipelineStageDone(anyLong(), anyBoolean());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1 })
    void rejectedFinishUpdatePreservesArtifactsWithoutTryingToFailTheStage(int updateResult) {
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(updateResult);

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, capturedUpdate().getUpdateStage().getStatus());
        assertTrue(Files.exists(workDir));
        verify(pipelineService).pipelineStageDone(STAGE_ID, false);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, true);
    }

    @Test
    void successfulCompletionNotificationExceptionDoesNotRewriteFinishedState() {
        IllegalStateException failure = new IllegalStateException("notification unavailable");
        doThrow(failure).when(pipelineService).pipelineStageDone(STAGE_ID, true);

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        verify(pipelineService).pipelineStageDone(STAGE_ID, true);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, false);
    }

    @Test
    void failureNotificationExceptionDoesNotRetryFailurePersistence() {
        handler.planningFailure = new IllegalArgumentException("invalid output");
        IllegalStateException failure = new IllegalStateException("notification unavailable");
        doThrow(failure).when(pipelineService).pipelineStageDone(STAGE_ID, false);

        assertDoesNotThrow(() -> handler.handleStageDone(result()));

        assertFailedUpdate();
    }

    @Test
    void readInspectMalformedStoredInputJsonMarksFailureOnceAndDeletesTheActualWorkDirectory() {
        ReadInspectStageDoneHandler readInspect = readInspectHandler();
        BioPipelineStage stage = new BioPipelineStage();
        stage.setInputUrl("{invalid-json");
        when(pipelineService.queryStageById(STAGE_ID)).thenReturn(stage);

        assertDoesNotThrow(() -> readInspect.handleStageDone(readInspectResult()));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @Test
    void readInspectUnexpectedQueryExceptionMarksFailureWithoutFallingThroughToAnotherUpdate() {
        ReadInspectStageDoneHandler readInspect = readInspectHandler();
        when(pipelineService.queryStageById(STAGE_ID))
                .thenThrow(new IllegalStateException("unexpected lookup failure"));

        assertDoesNotThrow(() -> readInspect.handleStageDone(readInspectResult()));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @ParameterizedTest
    @MethodSource("databaseFailures")
    void readInspectDatabaseQueryExceptionEscapesWithoutRecordingFailure(RuntimeException failure) {
        ReadInspectStageDoneHandler readInspect = readInspectHandler();
        when(pipelineService.queryStageById(STAGE_ID)).thenThrow(failure);

        assertSame(failure, assertThrows(RuntimeException.class,
                () -> readInspect.handleStageDone(readInspectResult())));

        verify(pipelineService, never()).updateStageFromVersion(any(UpdateStageCommand.class));
        verify(pipelineService, never()).pipelineStageDone(anyLong(), anyBoolean());
        assertTrue(Files.exists(workDir));
    }

    @Test
    void taxonomyMissingOutputUsesTheCommonFailureBoundaryExactlyOnce() {
        TaxonomyStageDoneHandler taxonomy = taxonomyHandler();
        StageRunResult<TaxonomyStageOutput> result = StageRunResult.OK(null,
                new StageContext(STAGE_ID, VERSION, PIPELINE_STAGE_TAXONOMY, 1), workDir);

        assertDoesNotThrow(() -> taxonomy.handleStageDone(result));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @Test
    void taxonomyActionRequiredNotificationFailureDoesNotOverwriteCommittedState() {
        TaxonomyStageDoneHandler taxonomy = taxonomyHandler();
        TaxonomyStageOutput output = new TaxonomyStageOutput(null, null, STATUS_LOW_CONFIDENCE, EVIDENCE_FASTANI);
        StageRunResult<TaxonomyStageOutput> result = StageRunResult.OK(output,
                new StageContext(STAGE_ID, VERSION, PIPELINE_STAGE_TAXONOMY, 1), workDir);
        doThrow(new IllegalStateException("notification unavailable"))
                .when(pipelineService).pipelineStageDone(STAGE_ID, true);

        assertDoesNotThrow(() -> taxonomy.handleStageDone(result));

        ArgumentCaptor<UpdateStageCommand> captor = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captor.capture());
        UpdateStageCommand command = captor.getValue();
        assertEquals(STAGE_ID, command.getStageId());
        assertEquals(VERSION, command.getCurrentVersion());
        assertEquals(PIPELINE_STAGE_STATUS_ACTION_REQUIRED, command.getUpdateStage().getStatus());
        assertNotNull(command.getUpdateStage().getEndTime());
        assertNotNull(command.getUpdateStage().getOutputInline());
        verify(pipelineService).pipelineStageDone(STAGE_ID, true);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, false);
    }

    @Test
    void explicitFailureHandlerAcceptsAnExecutionFailureWithNoOutputAndPersistsFailureOnce() {
        FailStageDoneHandler fail = new FailStageDoneHandler();
        fail.pipelineService = pipelineService;
        StageRunResult<StageOutput> result = StageRunResult.fail("fixture execution failure", workDir,
                stageContext(), new NullPointerException("missing execution metadata"));
        assertNull(result.getStageOutput());

        assertDoesNotThrow(() -> fail.handleStageDone(result));

        assertFailedUpdate();
        assertFalse(Files.exists(workDir));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1 })
    void readInspectRejectedFinishUpdatePreservesArtifactsAndDoesNotReportSuccess(int updateResult)
            throws IOException {
        ReadInspectStageDoneHandler readInspect = readInspectHandler();
        BioPipelineStage stage = new BioPipelineStage();
        stage.setInputUrl("{\"read1Url\":\"stored/read1.fastq\",\"read2Url\":null}");
        when(pipelineService.queryStageById(STAGE_ID)).thenReturn(stage);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(updateResult);
        Path artifact = Files.writeString(workDir.resolve("inspect-result.txt"), "fixture");

        assertDoesNotThrow(() -> readInspect.handleStageDone(readInspectResult()));

        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, capturedUpdate().getUpdateStage().getStatus());
        assertTrue(Files.exists(workDir));
        assertEquals("fixture", Files.readString(artifact));
        verify(pipelineService).pipelineStageDone(STAGE_ID, false);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, true);
    }

    private static Stream<RuntimeException> databaseFailures() {
        return Stream.of(
                new DataAccessResourceFailureException("database unavailable"),
                new IllegalStateException("wrapped SQL failure", new SQLException("connection lost")),
                new PersistenceException("MyBatis persistence failed"));
    }

    private void assertRootCause(Throwable expected, Throwable failure) {
        while (failure.getCause() != null) {
            failure = failure.getCause();
        }
        assertSame(expected, failure);
    }

    private StageContext stageContext() {
        return new StageContext(STAGE_ID, VERSION, PIPELINE_STAGE_ASSEMBLY, 1);
    }

    private StageRunResult<StageOutput> result() {
        return StageRunResult.OK(() -> workDir, stageContext(), workDir);
    }

    private ReadInspectStageDoneHandler readInspectHandler() {
        ReadInspectStageDoneHandler readInspect = new ReadInspectStageDoneHandler();
        readInspect.pipelineService = pipelineService;
        readInspect.storageService = storageService;
        return readInspect;
    }

    private TaxonomyStageDoneHandler taxonomyHandler() {
        TaxonomyStageDoneHandler taxonomy = new TaxonomyStageDoneHandler();
        taxonomy.pipelineService = pipelineService;
        taxonomy.storageService = storageService;
        return taxonomy;
    }

    private StageRunResult<ReadInspectStageOutput> readInspectResult() {
        ReadInspectStageOutput output = new ReadInspectStageOutput(33, 0, true, null, null, null, workDir);
        return StageRunResult.OK(output,
                new StageContext(STAGE_ID, VERSION, PIPELINE_STAGE_READ_INSPECT, 1), workDir);
    }

    private void assertFailedUpdate() {
        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputUrl());
        assertNull(patch.getOutputInline());
        verify(pipelineService).pipelineStageDone(STAGE_ID, false);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, true);
    }

    private UpdateStageCommand capturedUpdate() {
        ArgumentCaptor<UpdateStageCommand> captor = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captor.capture());
        UpdateStageCommand command = captor.getValue();
        assertEquals(STAGE_ID, command.getStageId());
        assertEquals(VERSION, command.getCurrentVersion());
        assertEquals(VERSION + 1, command.getUpdateStage().getVersion());
        return command;
    }

    private static final class TestHandler extends AbstractStageDoneHandler<StageOutput> {

        private RuntimeException planningFailure;
        private RuntimeException cleanupFailure;
        private Map<String, String> uploadPlan = Map.of();
        private StageResult output = new SerializableResult();

        @Override
        public int getType() {
            return PIPELINE_STAGE_ASSEMBLY;
        }

        @Override
        protected Pair<Map<String, String>, ? extends StageResult> buildUploadConfigAndOutputUrlMap(
                StageRunResult<StageOutput> result) {
            if (planningFailure != null) {
                throw planningFailure;
            }
            result.getStageOutput().getParentPath();
            return Pair.of(uploadPlan, output);
        }

        @Override
        protected boolean deleteStageResultDir(String directory) {
            if (cleanupFailure != null) {
                throw cleanupFailure;
            }
            return super.deleteStageResultDir(directory);
        }
    }

    public static final class SerializableResult implements StageResult {

        public String getValue() {
            return "fixture";
        }
    }

    public static final class UnserializableResult implements StageResult {

        public String getValue() throws JsonProcessingException {
            throw new JsonProcessingException("injected serialization failure") { };
        }
    }
}
