package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls.SequenceInputMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Persist the executor's resolved metadata; do not reconstruct it from stage parameters. */
class ReadInspectSequenceMetaDoneHandlerTest {

    private static final long STAGE_ID = 42L;
    private static final String ORIGINAL_R1 = "original/r1.fastq";
    private static final String ORIGINAL_R2 = "original/r2.fastq";

    @TempDir
    Path tempDir;
    private Path workDir;
    private PipelineService pipelineService;
    private StorageService storageService;
    private TestHandler handler;

    @BeforeEach
    void setUp() throws IOException {
        workDir = Files.createDirectories(tempDir.resolve("work"));
        pipelineService = mock(PipelineService.class);
        storageService = mock(StorageService.class);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(1);
        handler = new TestHandler();
        handler.pipelineService = pipelineService;
        handler.storageService = storageService;
    }

    @ParameterizedTest
    @ValueSource(ints = {Constants.SequencingPlatform.UNKNOWN, Constants.SequencingPlatform.ILLUMINA,
            Constants.SequencingPlatform.ONT, Constants.SequencingPlatform.PACBIO})
    void originalFileCompletionPersistsEntireOutputMetadataWithoutReReadingParameters(int platform)
            throws Exception {
        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls();
        input.setRead1Url(ORIGINAL_R1);
        input.setRead2Url(ORIGINAL_R2);
        SequenceInputMeta staleDeclaration = new SequenceInputMeta();
        staleDeclaration.setSequencingPlatform(Constants.SequencingPlatform.SOLID);
        input.setSequenceInputMeta(staleDeclaration);
        BioPipelineStage storedStage = new BioPipelineStage();
        storedStage.setInputUrl(JsonUtil.toJson(input));
        storedStage.setParameters("{invalid-and-unneeded-parameters");
        when(pipelineService.queryStageById(STAGE_ID)).thenReturn(storedStage);

        ReadInspectStageOutput output = output(platform, null, null);
        handler.handleStageDone(result(output));

        ReadInspectStageResult restored = capturedSuccessfulResult();
        assertMetadata(output.getReadMeta(), restored.getReadMeta());
        assertEquals(ORIGINAL_R1, restored.getR1Url());
        assertEquals(ORIGINAL_R2, restored.getR2Url());
        verify(pipelineService).queryStageById(STAGE_ID);
        verifyNoInteractions(storageService);
    }

    @Test
    void splitUploadCompletionPersistsEntireMetadataWithoutLoadingStoredStage() throws Exception {
        String r1Content = "@synthetic/1\nAAAA\n+\nIIII\n";
        String r2Content = "@synthetic/2\nTTTT\n+\nIIII\n";
        Path r1 = Files.writeString(workDir.resolve("r1.fastq"), r1Content);
        Path r2 = Files.writeString(workDir.resolve("r2.fastq"), r2Content);
        Map<String, String> uploadedContents = captureUploads();

        ReadInspectStageOutput output = output(Constants.SequencingPlatform.PACBIO, r1, r2);
        handler.handleStageDone(result(output));

        ReadInspectStageResult restored = capturedSuccessfulResult();
        assertMetadata(output.getReadMeta(), restored.getReadMeta());
        assertEquals("stored/r1.fastq", restored.getR1Url());
        assertEquals("stored/r2.fastq", restored.getR2Url());
        assertEquals(Map.of("stored/r1.fastq", r1Content, "stored/r2.fastq", r2Content), uploadedContents);
        verify(pipelineService, never()).queryStageById(anyLong());
    }

    @Test
    void singleMaterializedZipReadPublishesItsOutputAndLeavesSecondReadAbsent() throws Exception {
        storedInput("original/single.fastq.zip", null);
        String content = "@synthetic\nACGT\n+\nIIII\n";
        Path r1 = Files.writeString(workDir.resolve("r1.fastq"), content);
        Map<String, String> uploadedContents = captureUploads();

        ReadInspectStageOutput output = output(Constants.SequencingPlatform.ONT, r1, null);
        assertFalse(output.isUseOriginalSequence());
        handler.handleStageDone(result(output));

        ReadInspectStageResult restored = capturedSuccessfulResult();
        assertEquals("stored/r1.fastq", restored.getR1Url());
        assertNull(restored.getR2Url());
        assertMetadata(output.getReadMeta(), restored.getReadMeta());
        assertEquals(Map.of("stored/r1.fastq", content), uploadedContents);
        verify(pipelineService).queryStageById(STAGE_ID);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void mixedZipAndGzipPairReplacesOnlyTheMaterializedSlot(boolean materializedR1) throws Exception {
        String originalR1 = "original/r1.fastq." + (materializedR1 ? "zip" : "gz");
        String originalR2 = "original/r2.fastq." + (materializedR1 ? "gz" : "zip");
        storedInput(originalR1, originalR2);
        String content = "@synthetic/" + (materializedR1 ? "1" : "2") + "\nACGT\n+\nIIII\n";
        String fileName = materializedR1 ? "r1.fastq" : "r2.fastq";
        Path materialized = Files.writeString(workDir.resolve(fileName), content);
        Map<String, String> uploadedContents = captureUploads();

        ReadInspectStageOutput output = output(Constants.SequencingPlatform.ILLUMINA,
                materializedR1 ? materialized : null, materializedR1 ? null : materialized);
        assertFalse(output.isUseOriginalSequence());
        handler.handleStageDone(result(output));

        ReadInspectStageResult restored = capturedSuccessfulResult();
        assertEquals(materializedR1 ? "stored/r1.fastq" : originalR1, restored.getR1Url());
        assertEquals(materializedR1 ? originalR2 : "stored/r2.fastq", restored.getR2Url());
        assertMetadata(output.getReadMeta(), restored.getReadMeta());
        assertEquals(Map.of("stored/" + fileName, content), uploadedContents);
        verify(pipelineService).queryStageById(STAGE_ID);
    }

    private void storedInput(String r1, String r2) throws Exception {
        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls();
        input.setRead1Url(r1);
        input.setRead2Url(r2);
        SequenceInputMeta staleDeclaration = new SequenceInputMeta();
        staleDeclaration.setSequencingPlatform(Constants.SequencingPlatform.SOLID);
        input.setSequenceInputMeta(staleDeclaration);
        BioPipelineStage storedStage = new BioPipelineStage();
        storedStage.setInputUrl(JsonUtil.toJson(input));
        storedStage.setParameters("{invalid-and-unneeded-parameters");
        when(pipelineService.queryStageById(STAGE_ID)).thenReturn(storedStage);
    }

    private Map<String, String> captureUploads() throws IOException {
        Map<String, String> uploadedContents = new HashMap<>();
        when(storageService.putObject(anyString(), any(InputStream.class))).thenAnswer(invocation -> {
            InputStream stream = invocation.getArgument(1);
            uploadedContents.put(invocation.getArgument(0),
                    new String(stream.readAllBytes(), StandardCharsets.UTF_8));
            return new StorageService.PutResult(true, null);
        });
        return uploadedContents;
    }

    private ReadInspectStageOutput output(int platform, Path r1, Path r2) {
        ReadInspectStageOutput output = new ReadInspectStageOutput(
                Constants.SequenceInput.QUALITY_ENCODING_64, Constants.SequenceInput.READ_LEN_TYPE_LONG,
                r1 == null && r2 == null, ORIGINAL_R1, r1, r2, workDir);
        SequenceMeta meta = new SequenceMeta();
        meta.setQualityEncoding(Constants.SequenceInput.QUALITY_ENCODING_64);
        meta.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_LONG);
        meta.setSequencingPlatform(platform);
        meta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_READ);
        output.setReadMeta(meta);
        return output;
    }

    private StageRunResult<ReadInspectStageOutput> result(ReadInspectStageOutput output) {
        return StageRunResult.OK(output,
                new StageContext(STAGE_ID, 3, Constants.StageType.PIPELINE_STAGE_READ_INSPECT, 1), workDir);
    }

    private ReadInspectStageResult capturedSuccessfulResult() throws Exception {
        ArgumentCaptor<UpdateStageCommand> captor = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captor.capture());
        BioPipelineStage updated = captor.getValue().getUpdateStage();
        assertEquals(Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED, updated.getStatus());
        assertNotNull(updated.getOutputUrl());
        assertNull(updated.getOutputInline());
        verify(pipelineService).pipelineStageDone(STAGE_ID, true);
        verify(pipelineService, never()).pipelineStageDone(STAGE_ID, false);
        assertFalse(Files.exists(workDir));
        return JsonUtil.toObject(updated.getOutputUrl(), ReadInspectStageResult.class);
    }

    private static void assertMetadata(SequenceMeta expected, SequenceMeta actual) {
        assertNotNull(actual);
        assertEquals(expected.getSequencingPlatform(), actual.getSequencingPlatform());
        assertEquals(expected.getReadLenType(), actual.getReadLenType());
        assertEquals(expected.getQualityEncoding(), actual.getQualityEncoding());
        assertEquals(expected.getSequenceLevel(), actual.getSequenceLevel());
    }

    private static final class TestHandler extends ReadInspectStageDoneHandler {
        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }
    }
}
