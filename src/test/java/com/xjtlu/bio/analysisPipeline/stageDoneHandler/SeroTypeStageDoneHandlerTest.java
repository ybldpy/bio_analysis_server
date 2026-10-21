package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.SeroTypeResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Persistence/upload contracts with temporary text files; no database or analysis tools. */
class SeroTypeStageDoneHandlerTest {

    private static final long STAGE_ID = 42L;
    private static final String PRIMARY_OBJECT = "stageOutput/42/serotypeResult.tsv";
    private static final String K_OBJECT = "stageOutput/42/kaptive_k.tsv";
    private static final String O_OBJECT = "stageOutput/42/kaptive_o.tsv";
    private static final String PRIMARY_CONTENT = "locus_type\tfixture\nK\tk-result\nO\to-result\n";
    private static final String K_CONTENT = "fixture\noriginal-k-result\n";
    private static final String O_CONTENT = "fixture\noriginal-o-result\n";

    @TempDir
    Path tempDir;
    private Path workDir;
    private Path storageRoot;
    private SeroTypeStageDoneHandler handler;
    private PipelineService pipelineService;

    @BeforeEach
    void setUp() throws IOException {
        workDir = Files.createDirectories(tempDir.resolve("work"));
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        pipelineService = mock(PipelineService.class);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(1);
        handler = new SeroTypeStageDoneHandler();
        handler.storageService = storage;
        handler.pipelineService = pipelineService;
        handler.bioStageUtil = new BioStageUtil();
    }

    @Test
    void existingSingleReportKeepsItsPrimaryUrlAndSuccessfulLifecycle() throws Exception {
        StageRunResult<SeroTypingStageOutput> execution = result(false);
        Pair<Map<String, String>, SeroTypeResult> plan = handler.buildUploadConfigAndOutputUrlMap(execution);
        assertEquals(Map.of(execution.getStageOutput().getResultPath().toString(), PRIMARY_OBJECT), plan.getLeft());
        assertEquals(PRIMARY_OBJECT, plan.getRight().getSerotypeResultUrl());
        assertNull(plan.getRight().getKaptiveKResultUrl());
        assertNull(plan.getRight().getKaptiveOResultUrl());

        handler.handleStageDone(execution);

        SeroTypeResult persisted = successfulResult();
        assertEquals(PRIMARY_OBJECT, persisted.getSerotypeResultUrl());
        assertNull(persisted.getKaptiveKResultUrl());
        assertNull(persisted.getKaptiveOResultUrl());
        assertEquals(PRIMARY_CONTENT, Files.readString(storageRoot.resolve(PRIMARY_OBJECT)));
        assertFalse(Files.exists(storageRoot.resolve(K_OBJECT)));
        assertFalse(Files.exists(storageRoot.resolve(O_OBJECT)));
    }

    @Test
    void dualKaptiveReportsAreUploadedSeparatelyAndPersistAlongsideTheCombinedPrimaryReport() throws Exception {
        StageRunResult<SeroTypingStageOutput> execution = result(true);
        SeroTypingStageOutput output = execution.getStageOutput();
        Pair<Map<String, String>, SeroTypeResult> plan = handler.buildUploadConfigAndOutputUrlMap(execution);
        assertEquals(Map.of(
                output.getResultPath().toString(), PRIMARY_OBJECT,
                output.getKaptiveKResultPath().toString(), K_OBJECT,
                output.getKaptiveOResultPath().toString(), O_OBJECT), plan.getLeft());

        handler.handleStageDone(execution);

        SeroTypeResult persisted = successfulResult();
        assertEquals(PRIMARY_OBJECT, persisted.getSerotypeResultUrl());
        assertEquals(K_OBJECT, persisted.getKaptiveKResultUrl());
        assertEquals(O_OBJECT, persisted.getKaptiveOResultUrl());
        assertEquals(PRIMARY_CONTENT, Files.readString(storageRoot.resolve(PRIMARY_OBJECT)));
        assertEquals(K_CONTENT, Files.readString(storageRoot.resolve(K_OBJECT)));
        assertEquals(O_CONTENT, Files.readString(storageRoot.resolve(O_OBJECT)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "K", "O" })
    void partialKaptivePairFailsBeforeUploadingAnyReport(String omitted) throws Exception {
        StageRunResult<SeroTypingStageOutput> execution = result(true);
        if (omitted.equals("K")) {
            execution.getStageOutput().setKaptiveKResultPath(null);
        } else {
            execution.getStageOutput().setKaptiveOResultPath(null);
        }
        StorageService storage = mock(StorageService.class);
        handler.storageService = storage;

        assertDoesNotThrow(() -> handler.handleStageDone(execution));

        assertFailed();
        verifyNoInteractions(storage);
    }

    @Test
    void failureUploadingAnOriginalReportDoesNotPersistFinishedStateOrAnyResultUrls() throws Exception {
        StageRunResult<SeroTypingStageOutput> execution = result(true);
        StorageService storage = mock(StorageService.class);
        when(storage.putObject(anyString(), any(InputStream.class)))
                .thenReturn(new StorageService.PutResult(true, null));
        when(storage.putObject(eq(O_OBJECT), any(InputStream.class)))
                .thenReturn(new StorageService.PutResult(false, new IOException("injected original report upload failure")));
        handler.storageService = storage;

        handler.handleStageDone(execution);

        assertFailed();
        verify(storage).putObject(eq(PRIMARY_OBJECT), any(InputStream.class));
        verify(storage).putObject(eq(K_OBJECT), any(InputStream.class));
        verify(storage).putObject(eq(O_OBJECT), any(InputStream.class));
    }

    @ParameterizedTest
    @ValueSource(strings = { "primary", "K", "O" })
    void missingArtifactCannotProduceSuccessfulCompletion(String missing) throws Exception {
        StageRunResult<SeroTypingStageOutput> execution = result(true);
        SeroTypingStageOutput output = execution.getStageOutput();
        Path missingPath = switch (missing) {
            case "K" -> output.getKaptiveKResultPath();
            case "O" -> output.getKaptiveOResultPath();
            default -> output.getResultPath();
        };
        Files.delete(missingPath);

        assertDoesNotThrow(() -> handler.handleStageDone(execution));

        assertFailed();
    }

    @Test
    void oldStoredResultJsonStillDeserializesWithoutOptionalKaptiveUrls() throws Exception {
        SeroTypeResult restored = JsonUtil.toObject(
                "{\"serotypeResultUrl\":\"legacy/report.txt\"}", SeroTypeResult.class);

        assertEquals("legacy/report.txt", restored.getSerotypeResultUrl());
        assertNull(restored.getKaptiveKResultUrl());
        assertNull(restored.getKaptiveOResultUrl());
    }

    private StageRunResult<SeroTypingStageOutput> result(boolean dual) throws IOException {
        Path primary = Files.writeString(workDir.resolve("local-primary.tsv"), PRIMARY_CONTENT);
        SeroTypingStageOutput output;
        if (dual) {
            Path kResult = Files.writeString(workDir.resolve("local-k.tsv"), K_CONTENT);
            Path oResult = Files.writeString(workDir.resolve("local-o.tsv"), O_CONTENT);
            output = new SeroTypingStageOutput(primary, kResult, oResult);
        } else {
            output = new SeroTypingStageOutput(primary);
        }
        return StageRunResult.OK(output, new StageContext(STAGE_ID, 3, PIPELINE_STAGE_SEROTYPE, 1), workDir);
    }

    private SeroTypeResult successfulResult() throws Exception {
        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputInline());
        assertFalse(Files.exists(workDir));
        verify(pipelineService).pipelineStageDone(STAGE_ID, true);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, false);
        return JsonUtil.toObject(patch.getOutputUrl(), SeroTypeResult.class);
    }

    private void assertFailed() {
        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputUrl());
        assertNull(patch.getOutputInline());
        verify(pipelineService).pipelineStageDone(STAGE_ID, false);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, true);
    }

    private UpdateStageCommand capturedUpdate() {
        ArgumentCaptor<UpdateStageCommand> captured = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captured.capture());
        UpdateStageCommand command = captured.getValue();
        assertEquals(STAGE_ID, command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, command.getUpdateStage().getVersion());
        return command;
    }
}
