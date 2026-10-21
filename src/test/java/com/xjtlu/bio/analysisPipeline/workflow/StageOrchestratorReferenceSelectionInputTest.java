package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_SHORT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class StageOrchestratorReferenceSelectionInputTest {

    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @Test
    void queuesAssemblyEntryWithoutQcReadInspectOrAssemblyStages() throws Exception {
        BioPipelineStage selection = selectionStage();
        selection.setStageIndex(0);
        selection.setInputUrl("{\"contigsUrl\":\"uploaded.contigs.fasta\"}");
        String originalParameters = selection.getParameters();
        String originalInput = selection.getInputUrl();

        OrchestratePlan plan = orchestrator.makePlan(List.of(selection), selection.getStageId());

        assertSame(selection, plan.getRunStages().get(0));
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, selection.getStatus());
        assertEquals(originalParameters, selection.getParameters());
        assertEquals(originalInput, selection.getInputUrl());
        ReferenceSelectionStageInputUrls input = selectionInput(selection);
        assertEquals("uploaded.contigs.fasta", input.getContigsUrl());
        assertNull(input.getR1Url());
        assertNull(input.getR2Url());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"qc/cleaned-r2.fastq.gz"})
    void passesCleanedReadsAndPreservesSelectionConfiguration(String cleanedR2) throws Exception {
        Fixture fixture = readFixture(cleanedR2, READ_LEN_TYPE_SHORT);

        OrchestratePlan plan = orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId());

        ReferenceSelectionStageInputUrls input = selectionInput(fixture.selection());
        assertEquals("assembly/contigs.fasta", input.getContigsUrl());
        assertEquals("qc/cleaned-r1.fastq.gz", input.getR1Url());
        assertEquals(cleanedR2, input.getR2Url());
        ReferenceSelectionStageParameters parameters = selectionParameters(fixture.selection());
        assertEquals(READ_LEN_TYPE_SHORT, parameters.getReadMeta().getReadLenType());
        assertEquals(33, parameters.getReadMeta().getQualityEncoding());
        assertEquals(6, parameters.getThreads());
        assertEquals(0.7d, parameters.getMinimumReadReferenceCoverage());
        assertEquals(91.0d, parameters.getMinimumReadSequenceIdentity());
        assertEquals(28, parameters.getMinimumReadMappingQuality());
        assertEquals("CANDIDATE_1", parameters.getCandidateReferences().get(0).getAccession());
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, fixture.selection().getVersion());
        assertEquals(fixture.selection().getInputUrl(), command.getUpdateStage().getInputUrl());
        assertEquals(fixture.selection().getParameters(), command.getUpdateStage().getParameters());
    }

    @Test
    void preservesLongReadMetadataWithoutInferringASequencingPlatform() throws Exception {
        Fixture fixture = readFixture(null, READ_LEN_TYPE_LONG);

        orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId());

        ReferenceSelectionStageParameters parameters = selectionParameters(fixture.selection());
        assertEquals(READ_LEN_TYPE_LONG, parameters.getReadMeta().getReadLenType());
        assertEquals(0, parameters.getReadMeta().getSequencingPlatform());
        assertNull(selectionInput(fixture.selection()).getR2Url());
    }

    @Test
    void preservesLegacyContigsOnlyPlanningWhenReadStagesAreAbsent() throws Exception {
        BioPipelineStage selection = selectionStage();
        BioPipelineStage assembly = assemblyStage();

        orchestrator.makePlan(List.of(assembly, selection), selection.getStageId());

        ReferenceSelectionStageInputUrls input = selectionInput(selection);
        assertEquals("assembly/contigs.fasta", input.getContigsUrl());
        assertNull(input.getR1Url());
        assertNull(input.getR2Url());
        assertNull(selectionParameters(selection).getReadMeta());
    }

    @Test
    void explicitlyDisabledFallbackDoesNotRequireReadArtifacts() throws Exception {
        Fixture fixture = readFixture(null, READ_LEN_TYPE_SHORT);
        ReferenceSelectionStageParameters parameters = selectionParameters(fixture.selection());
        parameters.setReadFallbackEnabled(false);
        fixture.selection().setParameters(JsonUtil.toJson(parameters));
        fixture.qc().setOutputUrl("malformed unused output");
        fixture.readInspect().setOutputUrl(null);

        orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId());

        assertNull(selectionInput(fixture.selection()).getR1Url());
        assertNull(selectionInput(fixture.selection()).getR2Url());
        assertFalse(selectionParameters(fixture.selection()).isReadFallbackEnabled());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "  "})
    void rejectsMissingCleanedR1WithoutFallingBackToRawReads(String cleanedR1) throws Exception {
        Fixture fixture = readFixture("qc/cleaned-r2.fastq.gz", READ_LEN_TYPE_SHORT);
        fixture.qc().setOutputUrl(JsonUtil.toJson(new QcResult(cleanedR1, "qc/cleaned-r2.fastq.gz", null, null)));

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId()));

        assertEquals("Reference selection read fallback requires QC cleaned R1", exception.getDesc());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, fixture.selection().getStatus());
        assertEquals(3, fixture.selection().getVersion());
    }

    @Test
    void rejectsMissingReadMetadataInsteadOfAssumingShortReads() throws Exception {
        Fixture fixture = readFixture(null, READ_LEN_TYPE_SHORT);
        fixture.readInspect().setOutputUrl(JsonUtil.toJson(new ReadInspectStageResult()));

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId()));

        assertEquals("Reference selection read fallback requires inspected read metadata", exception.getDesc());
    }

    @Test
    void rejectsUnsupportedReadLengthType() throws Exception {
        Fixture fixture = readFixture(null, -1);

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId()));

        assertEquals("Reference selection read fallback received an unsupported read length type", exception.getDesc());
    }

    @Test
    void rejectsAnIncompleteReadRouteInsteadOfUsingRawReads() throws Exception {
        Fixture fixture = readFixture(null, READ_LEN_TYPE_SHORT);
        fixture.stages().remove(fixture.qc());

        MissingUpstreamException exception = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(fixture.stages(), fixture.selection().getStageId()));

        assertEquals("Reference selection read fallback requires a finished QC stage", exception.getDesc());
    }

    private static Fixture readFixture(String cleanedR2, int readLenType) throws Exception {
        SequenceMeta metadata = new SequenceMeta();
        metadata.setReadLenType(readLenType);
        metadata.setQualityEncoding(33);
        ReadInspectStageResult inspected = new ReadInspectStageResult();
        inspected.setReadMeta(metadata);
        inspected.setR1Url("raw/r1.fastq.gz");
        inspected.setR2Url(cleanedR2 == null ? null : "raw/r2.fastq.gz");
        BioPipelineStage readInspect = stage(PIPELINE_STAGE_READ_INSPECT, JsonUtil.toJson(inspected));
        BioPipelineStage qc = stage(PIPELINE_STAGE_QC, JsonUtil.toJson(
                new QcResult("qc/cleaned-r1.fastq.gz", cleanedR2, null, null)));
        BioPipelineStage selection = selectionStage();
        return new Fixture(new ArrayList<>(List.of(readInspect, qc, assemblyStage(), selection)),
                selection, qc, readInspect);
    }

    private static BioPipelineStage assemblyStage() throws Exception {
        return stage(PIPELINE_STAGE_ASSEMBLY, JsonUtil.toJson(
                new AssemblyResult("assembly/contigs.fasta", null)));
    }

    private static BioPipelineStage selectionStage() throws Exception {
        BioPipelineStage selection = stage(PIPELINE_STAGE_REFERENCE_SELECTION, null);
        selection.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        ReferenceSequence candidate = new ReferenceSequence();
        candidate.setAccession("CANDIDATE_1");
        ReferenceSelectionStageParameters parameters = new ReferenceSelectionStageParameters(List.of(candidate));
        parameters.setThreads(6);
        parameters.setMinimumReadReferenceCoverage(0.7d);
        parameters.setMinimumReadSequenceIdentity(91.0d);
        parameters.setMinimumReadMappingQuality(28);
        selection.setParameters(JsonUtil.toJson(parameters));
        return selection;
    }

    private static BioPipelineStage stage(int type, String output) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId((long) type + 1);
        stage.setStageType(type);
        stage.setPipelineId(1L);
        stage.setVersion(3);
        stage.setStageIndex(-1);
        stage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        stage.setOutputUrl(output);
        return stage;
    }

    private static ReferenceSelectionStageInputUrls selectionInput(BioPipelineStage stage) throws Exception {
        return JsonUtil.toObject(stage.getInputUrl(), ReferenceSelectionStageInputUrls.class);
    }

    private static ReferenceSelectionStageParameters selectionParameters(BioPipelineStage stage) throws Exception {
        return JsonUtil.toObject(stage.getParameters(), ReferenceSelectionStageParameters.class);
    }

    private record Fixture(List<BioPipelineStage> stages, BioPipelineStage selection,
            BioPipelineStage qc, BioPipelineStage readInspect) {
    }
}
