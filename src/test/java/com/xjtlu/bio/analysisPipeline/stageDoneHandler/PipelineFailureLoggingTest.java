package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.entity.BioPipelineLog;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.mapper.BioPipelineLogMapper;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.service.PipelineService;

/** Real handler/service failure path; persistence is mocked, no Spring or external tools. */
class PipelineFailureLoggingTest {
    @TempDir
    Path tempDir;

    private Path workDir;
    private StageContext context;
    private PipelineService service;
    private BioPipelineStageMapper stages;
    private BioPipelineLogMapper logs;
    private FailStageDoneHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        workDir = Files.createDirectory(tempDir.resolve("work"));
        context = new StageContext(42L, 3, 30, 1);
        context.setPipelineId(3_000_000_000L);
        stages = mock(BioPipelineStageMapper.class);
        logs = mock(BioPipelineLogMapper.class);
        when(stages.updateByExampleSelective(any(), any())).thenReturn(1);
        when(logs.insert(any())).thenReturn(1);
        service = spy(new PipelineService());
        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stages);
        ReflectionTestUtils.setField(service, "bioPipelineLogMapper", logs);
        doNothing().when(service).pipelineStageDone(anyLong(), anyBoolean());
        handler = new FailStageDoneHandler();
        handler.pipelineService = service;
    }

    @Test
    void preservesLongPlainTextFailureAndExecutionVersion() {
        String message = "工具执行失败：\"missing output\"\n路径：C:\\input\\sample\n" + "detail\n".repeat(3000);
        handler.handleStageDone(failure(message));

        BioPipelineLog log = capturedLog();
        assertEquals(context.getPipelineId(), log.getPipelineId());
        assertEquals(42L, log.getStageId());
        assertEquals(3, log.getStageVersion());
        assertEquals("ERROR", log.getLevel());
        assertEquals("STAGE_FAILED", log.getEventType());
        assertEquals(message, log.getContent());
        assertNotNull(log.getCreatedAt());
        assertEquals(4, capturedStage().getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, capturedStage().getStatus());
        assertFalse(Files.exists(workDir));
        var order = inOrder(stages, logs, service);
        order.verify(stages).updateByExampleSelective(any(), any());
        order.verify(logs).insert(any());
        order.verify(service).pipelineStageDone(42L, false);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectedFailureUpdateDoesNotAppendLogOrDeleteArtifacts(int rows) {
        when(stages.updateByExampleSelective(any(), any())).thenReturn(rows);

        handler.handleStageDone(failure("failed"));

        verifyNoInteractions(logs);
        assertTrue(Files.exists(workDir));
        verify(service).pipelineStageDone(42L, false);
    }

    @Test
    void duplicateCompletionOnlyLogsTheAcceptedUpdate() {
        when(stages.updateByExampleSelective(any(), any())).thenReturn(1, 0);
        StageRunResult<StageOutput> result = failure("same execution");

        handler.handleStageDone(result);
        handler.handleStageDone(result);

        verify(logs).insert(any());
    }

    @Test
    void stageDatabaseFailureDoesNotProduceAFalsePersistedFailureLog() {
        when(stages.updateByExampleSelective(any(), any()))
                .thenThrow(new DataAccessResourceFailureException("stage database unavailable"));

        assertDoesNotThrow(() -> handler.handleStageDone(failure("failed")));

        verifyNoInteractions(logs);
        verify(stages).updateByExampleSelective(any(), any());
        assertTrue(Files.exists(workDir));
    }

    @Test
    void logDatabaseFailureDoesNotRetryStatusOrPreventCleanupAndNotification() {
        when(logs.insert(any())).thenThrow(new DataAccessResourceFailureException("log write unavailable"));

        assertDoesNotThrow(() -> handler.handleStageDone(failure("original failure")));

        assertEquals(PIPELINE_STAGE_STATUS_FAIL, capturedStage().getStatus());
        verify(logs).insert(any());
        assertFalse(Files.exists(workDir));
        verify(service).pipelineStageDone(42L, false);
    }

    @Test
    void unsuccessfulLogInsertDoesNotChangeTheCommittedFailure() {
        when(logs.insert(any())).thenReturn(0);

        assertDoesNotThrow(() -> handler.handleStageDone(failure("original failure")));

        assertEquals(PIPELINE_STAGE_STATUS_FAIL, capturedStage().getStatus());
        assertFalse(Files.exists(workDir));
        verify(service).pipelineStageDone(42L, false);
    }

    @Test
    void directServiceCallContainsDatabaseExceptionWithoutTheHandlerBoundary() {
        when(logs.insert(any())).thenThrow(new DataAccessResourceFailureException("log database unavailable"));

        assertDoesNotThrow(() -> service.recordStageFailure(context, "original failure"));

        verify(logs).insert(any());
        verifyNoInteractions(stages);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void directServiceCallContainsUnsuccessfulInsertWithoutTheHandlerBoundary(int rows) {
        when(logs.insert(any())).thenReturn(rows);

        assertDoesNotThrow(() -> service.recordStageFailure(context, "original failure"));

        verify(logs).insert(any());
        verifyNoInteractions(stages);
    }

    @Test
    void missingContextDoesNotThrowAgainWhileReportingTheLoggingFailure() {
        assertDoesNotThrow(() -> service.recordStageFailure(null, "original failure"));

        verifyNoInteractions(logs, stages);
    }

    @Test
    void retryAppendsAnotherMessageWithItsOwnExecutionVersion() {
        handler.handleStageDone(failure("first attempt"));
        StageContext retry = new StageContext(42L, 7, 30, 1);
        retry.setPipelineId(context.getPipelineId());
        handler.handleStageDone(StageRunResult.fail("second attempt", null, retry, null));

        ArgumentCaptor<BioPipelineLog> captor = ArgumentCaptor.forClass(BioPipelineLog.class);
        verify(logs, times(2)).insert(captor.capture());
        assertEquals(List.of(3, 7), captor.getAllValues().stream().map(BioPipelineLog::getStageVersion).toList());
        assertEquals(List.of("first attempt", "second attempt"),
                captor.getAllValues().stream().map(BioPipelineLog::getContent).toList());
    }

    @Test
    void missingFailureMessageGetsAPlainTextFallback() {
        handler.handleStageDone(failure(null));
        assertEquals("Stage failed without a failure message", capturedLog().getContent());
    }

    @Test
    void completionExceptionRecordsItsCause() {
        CompletionHandler completion = completionHandler();
        completion.exception = new IllegalStateException("missing output metadata");

        completion.handleStageDone(StageRunResult.OK(() -> workDir, context, workDir));

        assertEquals("Stage completion failed: IllegalStateException: missing output metadata",
                capturedLog().getContent());
    }

    @Test
    void unsuccessfulUploadRecordsAnExplicitReason() {
        CompletionHandler completion = completionHandler();
        completion.uploadFails = true;

        completion.handleStageDone(StageRunResult.OK(() -> workDir, context, workDir));

        assertEquals("Failed to upload stage outputs", capturedLog().getContent());
    }

    @Test
    void successfulCompletionDoesNotProduceAFailureEvent() {
        completionHandler().handleStageDone(StageRunResult.OK(() -> workDir, context, workDir));

        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, capturedStage().getStatus());
        verifyNoInteractions(logs);
        verify(service).pipelineStageDone(42L, true);
    }

    private StageRunResult<StageOutput> failure(String message) {
        return StageRunResult.fail(message, workDir, context, null);
    }

    private BioPipelineLog capturedLog() {
        ArgumentCaptor<BioPipelineLog> captor = ArgumentCaptor.forClass(BioPipelineLog.class);
        verify(logs).insert(captor.capture());
        return captor.getValue();
    }

    private BioPipelineStage capturedStage() {
        ArgumentCaptor<BioPipelineStage> captor = ArgumentCaptor.forClass(BioPipelineStage.class);
        verify(stages).updateByExampleSelective(captor.capture(), any(BioPipelineStageExample.class));
        return captor.getValue();
    }

    private CompletionHandler completionHandler() {
        CompletionHandler completion = new CompletionHandler();
        completion.pipelineService = service;
        return completion;
    }

    private static final class CompletionHandler extends AbstractStageDoneHandler<StageOutput> {
        RuntimeException exception;
        boolean uploadFails;

        @Override public int getType() { return 30; }

        @Override
        protected Pair<Map<String, String>, ? extends StageResult> buildUploadConfigAndOutputUrlMap(
                StageRunResult<StageOutput> result) {
            if (exception != null) throw exception;
            return Pair.of(uploadFails ? Map.of("artifact", "stored/artifact") : Map.of(), new Output());
        }

        @Override
        protected boolean batchUploadObjectsFromLocal(Map<String, String> paths) {
            return !uploadFails;
        }
    }

    public static final class Output implements StageResult {
        public String getValue() { return "fixture"; }
    }
}
