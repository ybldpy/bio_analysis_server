package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_ACTION_REQUIRED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.EVIDENCE_FASTANI;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.EVIDENCE_KRAKEN2;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_AMBIGUOUS;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_OUT_PANEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput.TaxonomyClassificationOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.JsonUtil;

class TaxonomyStageDoneHandlerTest {

    private TestHandler handler;
    private PipelineService pipelineService;

    @BeforeEach
    void setUp() {
        handler = new TestHandler();
        pipelineService = mock(PipelineService.class);
        ReflectionTestUtils.setField(handler, "pipelineService", pipelineService);
    }

    @Test
    void ambiguousResultStoresSpeciesCandidatesEvidenceAndBinningArtifacts() throws Exception {
        TaxonomyClassificationOutput candidate = candidate();
        TaxonomyStageOutput output = new TaxonomyStageOutput(
                List.of(candidate), null, STATUS_AMBIGUOUS, EVIDENCE_FASTANI);
        output.setMainBinFastaPath(Path.of("/work/main_bin.fasta"));
        output.setContigAssignmentReportPath(Path.of("/work/contig_assignments.tsv"));
        TaxonomyBinningMetrics metrics = new TaxonomyBinningMetrics();
        metrics.setTotalContigs(7);
        output.setBinningMetrics(metrics);

        handler.handleStageDone(runResult(output));

        TaxonomyResult result = persistedResult();
        assertEquals(PIPELINE_STAGE_STATUS_ACTION_REQUIRED, handler.patch.getStatus());
        assertEquals(STATUS_AMBIGUOUS, result.getStatus());
        assertEquals(EVIDENCE_FASTANI, result.getEvidenceResource());
        assertNull(result.getBest());
        assertNull(result.getManualConfirmation());
        assertEquals(111, result.getCandidates().get(0).getTaxid());
        assertEquals("Candidate A", result.getCandidates().get(0).getName());
        assertEquals("S", result.getCandidates().get(0).getRank());
        assertEquals(97.5, result.getCandidates().get(0).getScore());
        assertEquals(7, result.getBinningMetrics().getTotalContigs());
        assertNotNull(result.getMainBinFastaUrl());
        assertNotNull(result.getContigAssignmentReportUrl());
        assertTrue(handler.uploadConfig.containsKey("/work/main_bin.fasta"));
        assertTrue(handler.uploadConfig.containsKey("/work/contig_assignments.tsv"));
        assertNotNull(handler.patch.getEndTime());
        verify(pipelineService).pipelineStageDone(10L, true);
    }

    @Test
    void confidentResultStillStoresItsBestAndEvidence() throws Exception {
        TaxonomyClassificationOutput candidate = candidate();
        TaxonomyStageOutput output = new TaxonomyStageOutput(
                List.of(candidate), candidate, STATUS_CONFIDENT, EVIDENCE_KRAKEN2);

        handler.handleStageDone(runResult(output));

        TaxonomyResult result = persistedResult();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, handler.patch.getStatus());
        assertEquals(STATUS_CONFIDENT, result.getStatus());
        assertEquals(EVIDENCE_KRAKEN2, result.getEvidenceResource());
        assertEquals(111, result.getBest().getTaxid());
        assertEquals("S", result.getBest().getRank());
        assertEquals(97.5, result.getBest().getScore());
        verify(pipelineService).pipelineStageDone(10L, true);
    }

    @Test
    void noCandidatesRemainAnUnresolvedEmptyResult() throws Exception {
        TaxonomyStageOutput output = new TaxonomyStageOutput(
                null, null, STATUS_LOW_CONFIDENCE, EVIDENCE_FASTANI);

        handler.handleStageDone(runResult(output));

        TaxonomyResult result = persistedResult();
        assertEquals(PIPELINE_STAGE_STATUS_ACTION_REQUIRED, handler.patch.getStatus());
        assertEquals(STATUS_LOW_CONFIDENCE, result.getStatus());
        assertEquals(EVIDENCE_FASTANI, result.getEvidenceResource());
        assertNull(result.getBest());
        assertTrue(result.getCandidates().isEmpty());
        verify(pipelineService).pipelineStageDone(10L, true);
    }

    @Test
    void outPanelWithNoCandidatesCompletesAsNotApplicable() throws Exception {
        TaxonomyStageOutput output = new TaxonomyStageOutput(
                List.of(), null, STATUS_OUT_PANEL, EVIDENCE_KRAKEN2);

        handler.handleStageDone(runResult(output));

        TaxonomyResult result = persistedResult();
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, handler.patch.getStatus());
        assertEquals(STATUS_OUT_PANEL, result.getStatus());
        assertEquals(EVIDENCE_KRAKEN2, result.getEvidenceResource());
        assertNull(result.getBest());
        assertNull(result.getManualConfirmation());
        assertTrue(result.getCandidates().isEmpty());
        assertNotNull(handler.patch.getEndTime());
        verify(pipelineService).pipelineStageDone(10L, true);
    }

    @Test
    void outPanelPreservesCandidatesEvidenceAndBinningArtifactsWhenNotApplicable() throws Exception {
        TaxonomyStageOutput output = new TaxonomyStageOutput(
                List.of(candidate()), null, STATUS_OUT_PANEL, EVIDENCE_FASTANI);
        output.setMainBinFastaPath(Path.of("/work/main_bin.fasta"));
        output.setContigAssignmentReportPath(Path.of("/work/contig_assignments.tsv"));
        TaxonomyBinningMetrics metrics = new TaxonomyBinningMetrics();
        metrics.setTotalContigs(7);
        output.setBinningMetrics(metrics);

        handler.handleStageDone(runResult(output));

        TaxonomyResult result = persistedResult();
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, handler.patch.getStatus());
        assertEquals(STATUS_OUT_PANEL, result.getStatus());
        assertEquals(EVIDENCE_FASTANI, result.getEvidenceResource());
        assertNull(result.getBest());
        assertNull(result.getManualConfirmation());
        assertEquals(1, result.getCandidates().size());
        assertEquals(111, result.getCandidates().get(0).getTaxid());
        assertEquals("Candidate A", result.getCandidates().get(0).getName());
        assertEquals("S", result.getCandidates().get(0).getRank());
        assertEquals(97.5, result.getCandidates().get(0).getScore());
        assertEquals(7, result.getBinningMetrics().getTotalContigs());
        assertEquals("stored/taxonomy.main_bin.fasta", result.getMainBinFastaUrl());
        assertEquals("stored/taxonomy.contig_assignments.tsv", result.getContigAssignmentReportUrl());
        assertEquals(Map.of(
                "/work/main_bin.fasta", result.getMainBinFastaUrl(),
                "/work/contig_assignments.tsv", result.getContigAssignmentReportUrl()), handler.uploadConfig);
        assertNotNull(handler.patch.getEndTime());
        verify(pipelineService).pipelineStageDone(10L, true);
    }

    private TaxonomyClassificationOutput candidate() {
        return new TaxonomyClassificationOutput(112, "Candidate A strain", 111, "Candidate A", 97.5);
    }

    private StageRunResult<TaxonomyStageOutput> runResult(TaxonomyStageOutput output) {
        return StageRunResult.OK(output,
                new StageContext(10L, 3, handler.getType(), 1), Path.of("/work"));
    }

    private TaxonomyResult persistedResult() throws Exception {
        assertEquals(10L, handler.updatedStageId);
        assertEquals(3, handler.expectedVersion);
        return JsonUtil.toObject(handler.patch.getOutputInline(), TaxonomyResult.class);
    }

    private static final class TestHandler extends TaxonomyStageDoneHandler {

        private BioPipelineStage patch;
        private long updatedStageId;
        private int expectedVersion;
        private Map<String, String> uploadConfig = Map.of();

        @Override
        protected int updateStageFromVersion(BioPipelineStage patch, long stageId, int currentVersion) {
            this.patch = patch;
            this.updatedStageId = stageId;
            this.expectedVersion = currentVersion;
            return 1;
        }

        @Override
        protected String createStoreObjectName(StageContext stageContext, String name) {
            return "stored/" + name;
        }

        @Override
        protected boolean batchUploadObjectsFromLocal(Map<String, String> uploadConfig) {
            this.uploadConfig = Map.copyOf(uploadConfig);
            return true;
        }
    }
}
