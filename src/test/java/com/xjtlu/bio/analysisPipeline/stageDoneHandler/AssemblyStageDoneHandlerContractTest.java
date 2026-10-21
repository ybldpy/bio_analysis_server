package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
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
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Completion/upload contracts; no Spring context, database or external analysis tool. */
class AssemblyStageDoneHandlerContractTest {

    private static final String CONTIG_OBJECT = "stageOutput/42/contigs.fasta";
    private static final String SCAFFOLD_OBJECT = "stageOutput/42/scaffolds.fasta";
    private static final String CONTIG_CONTENT = ">fixture_contig\nACGT\n";
    private static final String SCAFFOLD_CONTENT = ">fixture_scaffold\nACGTNNACGT\n";

    @TempDir
    Path tempDir;
    private Path storageRoot;
    private Path workDir;
    private AssemblyStageDoneHandler handler;
    private PipelineService pipelineService;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        workDir = Files.createDirectories(tempDir.resolve("work"));
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        pipelineService = mock(PipelineService.class);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(1);
        handler = new AssemblyStageDoneHandler();
        handler.storageService = storage;
        handler.pipelineService = pipelineService;
        handler.bioStageUtil = new BioStageUtil();
        ReflectionTestUtils.setField(handler, "contigsFileName", "contigs.fasta");
        ReflectionTestUtils.setField(handler, "scaffoldFileName", "scaffolds.fasta");
    }

    @Test
    void uploadsBothArtifactsAndPersistsTheirUrlsAndSuccessfulLifecycle() throws Exception {
        StageRunResult<AssemblyStageOutput> result = result(true);
        Pair<Map<String, String>, AssemblyResult> plan = handler.buildUploadConfigAndOutputUrlMap(result);
        assertEquals(Map.of(
                result.getStageOutput().getContigPath(), CONTIG_OBJECT,
                result.getStageOutput().getScaffoldPath(), SCAFFOLD_OBJECT), plan.getLeft());
        assertEquals(CONTIG_OBJECT, plan.getRight().getContigsUrl());
        assertEquals(SCAFFOLD_OBJECT, plan.getRight().getScaffoldUrl());

        handler.handleStageDone(result);

        assertEquals(CONTIG_CONTENT, Files.readString(storageRoot.resolve(CONTIG_OBJECT)));
        assertEquals(SCAFFOLD_CONTENT, Files.readString(storageRoot.resolve(SCAFFOLD_OBJECT)));
        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputInline());
        AssemblyResult restored = JsonUtil.toObject(patch.getOutputUrl(), AssemblyResult.class);
        assertEquals(CONTIG_OBJECT, restored.getContigsUrl());
        assertEquals(SCAFFOLD_OBJECT, restored.getScaffoldUrl());
        assertFalse(Files.exists(workDir));
        verify(pipelineService).pipelineStageDone(42L, true);
        verify(pipelineService, never()).pipelineStageDone(42L, false);
    }

    @Test
    void uploadFailureMarksTheStageFailedAndDoesNotPublishSuccessfulUrls() throws Exception {
        StorageService failingStorage = mock(StorageService.class);
        when(failingStorage.putObject(anyString(), any(InputStream.class)))
                .thenReturn(new StorageService.PutResult(false, new IOException("Injected upload failure")));
        handler.storageService = failingStorage;

        handler.handleStageDone(result(true));

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, patch.getStatus());
        assertNull(patch.getOutputUrl());
        assertNull(patch.getOutputInline());
        verify(failingStorage).putObject(anyString(), any(InputStream.class));
        verify(pipelineService).pipelineStageDone(42L, false);
        verify(pipelineService, never()).pipelineStageDone(42L, true);
    }

    @Test
    void missingLocalArtifactDoesNotProduceSuccessfulCompletion() throws Exception {
        StageRunResult<AssemblyStageOutput> result = result(true);
        Files.delete(Path.of(result.getStageOutput().getScaffoldPath()));

        handler.handleStageDone(result);

        assertEquals(PIPELINE_STAGE_STATUS_FAIL, capturedUpdate().getUpdateStage().getStatus());
        verify(pipelineService).pipelineStageDone(42L, false);
        verify(pipelineService, never()).pipelineStageDone(42L, true);
    }

    @Test
    void failedOptimisticUpdateDoesNotSignalSuccessfulPersistence() throws Exception {
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(0);

        handler.handleStageDone(result(true));

        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, capturedUpdate().getUpdateStage().getStatus());
        verify(pipelineService).pipelineStageDone(42L, false);
        verify(pipelineService, never()).pipelineStageDone(42L, true);
    }

    @Test
    void completesWithOnlyContigsWhenTheExecutorOmitsOptionalScaffolds() throws Exception {
        StageRunResult<AssemblyStageOutput> result = result(false);
        Pair<Map<String, String>, AssemblyResult> plan = handler.buildUploadConfigAndOutputUrlMap(result);
        assertEquals(Map.of(result.getStageOutput().getContigPath(), CONTIG_OBJECT), plan.getLeft());
        assertEquals(CONTIG_OBJECT, plan.getRight().getContigsUrl());
        assertNull(plan.getRight().getScaffoldUrl());

        assertDoesNotThrow(() -> handler.handleStageDone(result),
                "The completion handler must support the executor's optional scaffold output");

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputInline());
        AssemblyResult restored = JsonUtil.toObject(patch.getOutputUrl(), AssemblyResult.class);
        assertEquals(CONTIG_OBJECT, restored.getContigsUrl());
        assertNull(restored.getScaffoldUrl());
        assertEquals(CONTIG_CONTENT, Files.readString(storageRoot.resolve(CONTIG_OBJECT)));
        assertFalse(Files.exists(storageRoot.resolve(SCAFFOLD_OBJECT)));
        assertFalse(Files.exists(workDir));
        verify(pipelineService).pipelineStageDone(42L, true);
        verify(pipelineService, never()).pipelineStageDone(42L, false);
    }

    @Test
    void uploadsFlyeAssemblyAsCanonicalContigsWithoutInventingScaffolds() throws Exception {
        Path flyeAssembly = workDir.resolve("assembly.fasta");
        Files.writeString(flyeAssembly, CONTIG_CONTENT);
        StageRunResult<AssemblyStageOutput> result = StageRunResult.OK(
                new AssemblyStageOutput(flyeAssembly.toString(), null),
                new StageContext(42L, 3, PIPELINE_STAGE_ASSEMBLY, 1), workDir);

        Pair<Map<String, String>, AssemblyResult> plan = handler.buildUploadConfigAndOutputUrlMap(result);
        assertEquals(Map.of(flyeAssembly.toString(), CONTIG_OBJECT), plan.getLeft());
        assertEquals(CONTIG_OBJECT, plan.getRight().getContigsUrl());
        assertNull(plan.getRight().getScaffoldUrl());

        handler.handleStageDone(result);

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputInline());
        AssemblyResult restored = JsonUtil.toObject(patch.getOutputUrl(), AssemblyResult.class);
        assertEquals(CONTIG_OBJECT, restored.getContigsUrl());
        assertNull(restored.getScaffoldUrl());
        assertEquals(CONTIG_CONTENT, Files.readString(storageRoot.resolve(CONTIG_OBJECT)));
        assertFalse(Files.exists(storageRoot.resolve(SCAFFOLD_OBJECT)));
        assertFalse(Files.exists(storageRoot.resolve("stageOutput/42/assembly.fasta")));
        assertFalse(Files.exists(workDir));
        verify(pipelineService).pipelineStageDone(42L, true);
        verify(pipelineService, never()).pipelineStageDone(42L, false);
    }

    @Test
    void missingRequiredContigsStillFailsWhenScaffoldsAreAbsent() throws Exception {
        StageRunResult<AssemblyStageOutput> result = result(false);
        Files.delete(Path.of(result.getStageOutput().getContigPath()));

        handler.handleStageDone(result);

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, patch.getStatus());
        assertNull(patch.getOutputUrl());
        assertFalse(Files.exists(storageRoot.resolve(CONTIG_OBJECT)));
        verify(pipelineService).pipelineStageDone(42L, false);
        verify(pipelineService, never()).pipelineStageDone(42L, true);
    }

    private StageRunResult<AssemblyStageOutput> result(boolean includeScaffolds) throws IOException {
        Path contigs = workDir.resolve("local-contigs.fasta");
        Path scaffolds = workDir.resolve("local-scaffolds.fasta");
        Files.writeString(contigs, CONTIG_CONTENT);
        if (includeScaffolds) {
            Files.writeString(scaffolds, SCAFFOLD_CONTENT);
        }
        AssemblyStageOutput output = new AssemblyStageOutput(
                contigs.toString(), includeScaffolds ? scaffolds.toString() : null);
        return StageRunResult.OK(output, new StageContext(42L, 3, PIPELINE_STAGE_ASSEMBLY, 1), workDir);
    }

    private UpdateStageCommand capturedUpdate() {
        ArgumentCaptor<UpdateStageCommand> captor = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captor.capture());
        UpdateStageCommand command = captor.getValue();
        assertEquals(42L, command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, command.getUpdateStage().getVersion());
        return command;
    }
}
