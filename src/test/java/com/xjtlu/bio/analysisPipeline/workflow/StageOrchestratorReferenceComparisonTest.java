package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class StageOrchestratorReferenceComparisonTest {

    private static final int INITIAL_VERSION = 3;
    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @Test
    void queuesStandaloneComparisonWithoutSelectionAndPreservesAllCandidates() throws Exception {
        BioPipelineStage comparison = standaloneComparison();
        String originalInput = comparison.getInputUrl();
        String originalParameters = comparison.getParameters();

        OrchestratePlan plan = orchestrator.makePlan(List.of(comparison), comparison.getStageId());

        assertFalse(plan.isNoNextStage());
        assertEquals(1, plan.getRunStages().size());
        assertSame(comparison, plan.getRunStages().get(0));
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, comparison.getStatus());
        assertEquals(INITIAL_VERSION + 1, comparison.getVersion());
        assertEquals(originalInput, comparison.getInputUrl());
        assertEquals(originalParameters, comparison.getParameters());
        ReferenceComparisonStageParameters parameters = JsonUtil.toObject(
                comparison.getParameters(), ReferenceComparisonStageParameters.class);
        assertEquals(List.of("FIRST", "SECOND"), parameters.getCandidateReferences().stream()
                .map(ReferenceSequence::getAccession).toList());
        assertNull(parameters.getReferenceGenome());

        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand update = plan.getUpdateStageCommands().get(0);
        assertEquals(comparison.getStageId().longValue(), update.getStageId());
        assertEquals(INITIAL_VERSION, update.getCurrentVersion());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, update.getUpdateStage().getStatus());
        assertEquals(INITIAL_VERSION + 1, update.getUpdateStage().getVersion());
        assertNull(update.getUpdateStage().getInputUrl());
        assertNull(update.getUpdateStage().getParameters());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void standaloneComparisonCompletionHasNoDownstreamWork(boolean resolveById) throws Exception {
        BioPipelineStage comparison = standaloneComparison();
        comparison.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        List<BioPipelineStage> stages = List.of(comparison);

        OrchestratePlan plan = resolveById
                ? orchestrator.makeDownstreamPlan(comparison.getStageId(), stages, PIPELINE_VIRUS)
                : orchestrator.makeDownstreamPlan(comparison, stages, PIPELINE_VIRUS);

        assertTrue(plan.isNoNextStage());
        assertTrue(plan.getRunStages().isEmpty());
        assertTrue(plan.getUpdateStageCommands().isEmpty());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, comparison.getStatus());
        assertEquals(INITIAL_VERSION, comparison.getVersion());
    }

    @Test
    void legacySelectionCompletionStillQueuesComparisonWithSelectedReference() throws Exception {
        BioPipelineStage selection = stage(PIPELINE_STAGE_REFERENCE_SELECTION, 0);
        selection.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        selection.setOutputInline(JsonUtil.toJson(new ReferenceSelectionStageResult(
                ReferenceGenome.fromSingleSequence(reference("LEGACY_SELECTED")), List.of())));
        BioPipelineStage comparison = legacyComparison();
        String originalInput = comparison.getInputUrl();

        OrchestratePlan plan = orchestrator.makeDownstreamPlan(selection,
                List.of(selection, comparison), PIPELINE_VIRUS);

        assertEquals(1, plan.getRunStages().size());
        assertSame(comparison, plan.getRunStages().get(0));
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, comparison.getStatus());
        assertEquals(originalInput, comparison.getInputUrl());
        ReferenceComparisonStageParameters parameters = JsonUtil.toObject(
                comparison.getParameters(), ReferenceComparisonStageParameters.class);
        assertNull(parameters.getCandidateReferences(), "Legacy stages retain selected-reference mode");
        assertEquals("LEGACY_SELECTED", parameters.getReferenceGenome().getSequences().get(0).getAccession());
        assertEquals(SEQUENCE_LEVEL_ASSEMBLY, parameters.getReadMeta().getSequenceLevel());
    }

    @Test
    void legacyComparisonStillWaitsForSelection() throws Exception {
        BioPipelineStage selection = stage(PIPELINE_STAGE_REFERENCE_SELECTION, 0);
        BioPipelineStage comparison = legacyComparison();

        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(List.of(selection, comparison), comparison.getStageId()));

        assertEquals(PIPELINE_STAGE_STATUS_PENDING, comparison.getStatus());
        assertEquals(INITIAL_VERSION, comparison.getVersion());
    }

    private static BioPipelineStage standaloneComparison() throws Exception {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of(reference("FIRST"), reference("SECOND")));
        PipelineSampleInput input = new PipelineSampleInput("uploads/sample.fasta.gz", null);
        input.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);
        BioPipelineStage comparison = AnalysisPipelineStagesBuilder.buildVirusStages(input, configurations).get(0);
        comparison.setStageId(4300L);
        comparison.setVersion(INITIAL_VERSION);
        return comparison;
    }

    private static BioPipelineStage legacyComparison() throws Exception {
        BioPipelineStage comparison = stage(PIPELINE_STAGE_REFERENCE_COMPARISON, -1);
        comparison.setInputUrl(JsonUtil.toJson(new ReferenceComparisonStageInputUrls("uploads/legacy.fasta")));
        ReferenceComparisonStageParameters parameters = new ReferenceComparisonStageParameters();
        AnalysisPipelineStagesBuilder.initializeParameters(parameters, SEQUENCE_LEVEL_ASSEMBLY,
                ReferenceComparisonStageParameters.ANALYSIS_TARGET_TYPE_VIRUS);
        comparison.setParameters(JsonUtil.toJson(parameters));
        return comparison;
    }

    private static BioPipelineStage stage(int stageType, int stageIndex) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId((long) stageType);
        stage.setStageType(stageType);
        stage.setStageIndex(stageIndex);
        stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stage.setVersion(INITIAL_VERSION);
        return stage;
    }

    private static ReferenceSequence reference(String accession) {
        ReferenceSequence reference = new ReferenceSequence();
        reference.setAccession(accession);
        reference.setPath("references/" + accession + ".fasta");
        reference.setTaxId(1234);
        return reference;
    }
}
