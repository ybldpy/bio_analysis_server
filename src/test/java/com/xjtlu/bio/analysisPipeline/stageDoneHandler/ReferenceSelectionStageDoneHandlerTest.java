package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSelectionOutcome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics.Status;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class ReferenceSelectionStageDoneHandlerTest {

    @TempDir
    Path tempDir;

    private ReferenceSelectionStageDoneHandler handler;
    private PipelineService pipelineService;
    private StorageService storageService;

    @BeforeEach
    void setUp() {
        pipelineService = mock(PipelineService.class);
        storageService = mock(StorageService.class);
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(1);
        handler = new ReferenceSelectionStageDoneHandler();
        handler.pipelineService = pipelineService;
        handler.storageService = storageService;
    }

    @Test
    void persistsLongSelectionResultInlineWithoutChangingLifecycle() throws Exception {
        StageRunResult<ReferenceSelectionStageOutput> runResult = runResult();

        handler.handleStageDone(runResult);

        UpdateStageCommand command = capturedUpdate();
        BioPipelineStage patch = command.getUpdateStage();
        assertEquals(42L, command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, patch.getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputUrl());
        assertNotNull(patch.getOutputInline());
        assertTrue(patch.getOutputInline().length() > 10_000);

        ReferenceSelectionStageResult restored = JsonUtil.toObject(
                patch.getOutputInline(), ReferenceSelectionStageResult.class);
        ReferenceSequence original = runResult.getStageOutput().getSelectedReference()
                .getSequences().get(0);
        ReferenceSequence restoredSequence = restored.getSelectedReference().getSequences().get(0);
        assertEquals(original.getAccession(), restoredSequence.getAccession());
        assertEquals(original.getRawMetadata(), restoredSequence.getRawMetadata());
        assertEquals(original.getPath(), restoredSequence.getPath());
        assertEquals("SEGMENT_1", restoredSequence.getSegmentKey());
        assertEquals(1, restored.getCandidateScores().size());
        assertEquals(original.getAccession(), restored.getCandidateScores().get(0).getReferenceAccession());
        assertEquals(List.of("SEGMENT_2"), restored.getUnmatchedSegmentKeys());
        assertTrue(restored.isPartial());
        verify(pipelineService).pipelineStageDone(42L, true);
        verifyNoInteractions(storageService);
    }

    @Test
    void persistsNoMatchAsFinishedWithDiagnosticsAndSuccessfulCallback() throws Exception {
        Path workDir = Files.createDirectory(tempDir.resolve("no-match"));
        ReferenceCandidateScore score = new ReferenceCandidateScore(
                "TEST_REFERENCE_1", "SEGMENT_1", 0.0d, 0.0d,
                0L, 100L, 0L, 0L, 0, 0.0d);
        score.setRejectionReason("NO_ALIGNMENT_PASSING_MAPPING_QUALITY");
        ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                null, List.of(score), workDir.resolve("reference_selection.tsv"), List.of("SEGMENT_1"));
        output.setOutcome(ReferenceSelectionOutcome.NO_MATCH);
        ReferenceSelectionReadFallbackMetrics metrics = new ReferenceSelectionReadFallbackMetrics(
                Status.COMPLETED, "TEST_POLICY", 0.5d, 80.0d, 20, 1, 0);
        output.setReadFallbackMetrics(metrics);
        StageRunResult<ReferenceSelectionStageOutput> runResult = StageRunResult.OK(
                output, new StageContext(42L, 3, PIPELINE_STAGE_REFERENCE_SELECTION, 7), workDir);

        handler.handleStageDone(runResult);

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        assertEquals(4, patch.getVersion());
        assertNotNull(patch.getEndTime());
        assertNull(patch.getOutputUrl());
        ReferenceSelectionStageResult restored = JsonUtil.toObject(
                patch.getOutputInline(), ReferenceSelectionStageResult.class);
        assertEquals(ReferenceSelectionOutcome.NO_MATCH, restored.getOutcome());
        assertNull(restored.getSelectedReference());
        assertFalse(restored.isPartial());
        assertEquals(List.of("SEGMENT_1"), restored.getUnmatchedSegmentKeys());
        assertEquals(metrics, restored.getReadFallbackMetrics());
        assertEquals("NO_ALIGNMENT_PASSING_MAPPING_QUALITY", restored.getCandidateScores().get(0).getRejectionReason());
        assertFalse(Files.exists(workDir));
        verify(pipelineService).pipelineStageDone(42L, true);
        verify(pipelineService, never()).recordStageFailure(any(), any());
        verifyNoInteractions(storageService);
    }

    @Test
    void reportsUnsuccessfulOptimisticUpdateWithoutClaimingPersistenceSucceeded() throws Exception {
        when(pipelineService.updateStageFromVersion(any(UpdateStageCommand.class))).thenReturn(0);

        handler.handleStageDone(runResult());

        assertNotNull(capturedUpdate().getUpdateStage().getOutputInline());
        verify(pipelineService).pipelineStageDone(42L, false);
    }

    @Test
    void leavesOtherHandlersUsingTheExistingOutputUrlStorage() throws Exception {
        UrlResultHandler urlHandler = new UrlResultHandler();
        urlHandler.pipelineService = pipelineService;

        urlHandler.handleStageDone(runResult());

        BioPipelineStage patch = capturedUpdate().getUpdateStage();
        assertNull(patch.getOutputInline());
        AssemblyResult result = JsonUtil.toObject(patch.getOutputUrl(), AssemblyResult.class);
        assertEquals("outputDir/assembly/contigs.fasta", result.getContigsUrl());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, patch.getStatus());
        verify(pipelineService).pipelineStageDone(42L, true);
    }

    private UpdateStageCommand capturedUpdate() {
        ArgumentCaptor<UpdateStageCommand> captor = ArgumentCaptor.forClass(UpdateStageCommand.class);
        verify(pipelineService).updateStageFromVersion(captor.capture());
        return captor.getValue();
    }

    private StageRunResult<ReferenceSelectionStageOutput> runResult() throws Exception {
        ReferenceSequence sequence = new ReferenceSequence();
        sequence.setReferenceId(1L);
        sequence.setAccession("TEST_REFERENCE_1");
        sequence.setTaxId(123);
        sequence.setOrganismName("Test reference");
        sequence.setSegment("RNA 1");
        sequence.setSegmentOrdinal(1);
        sequence.setPath("refseq/test/reference_1.fna");
        sequence.setRawMetadata("{\"description\":\"" + "x".repeat(10_000) + "\"}");
        ReferenceCandidateScore score = new ReferenceCandidateScore(
                sequence.getAccession(), sequence.getSegmentKey(), 99.0d, 1.0d,
                100L, 100L, 99L, 100L, 1, 99.0d);
        Path workDir = Files.createDirectory(tempDir.resolve("work"));
        ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                ReferenceGenome.fromSingleSequence(sequence), List.of(score),
                workDir.resolve("reference_selection.tsv"), List.of("SEGMENT_2"));
        return StageRunResult.OK(output,
                new StageContext(42L, 3, PIPELINE_STAGE_REFERENCE_SELECTION, 7), workDir);
    }

    private static final class UrlResultHandler
            extends AbstractStageDoneHandler<ReferenceSelectionStageOutput> {

        @Override
        public int getType() {
            return PIPELINE_STAGE_ASSEMBLY;
        }

        @Override
        protected Pair<Map<String, String>, AssemblyResult> buildUploadConfigAndOutputUrlMap(
                StageRunResult<ReferenceSelectionStageOutput> result) {
            return Pair.of(Map.of(), new AssemblyResult("outputDir/assembly/contigs.fasta", null));
        }
    }
}
