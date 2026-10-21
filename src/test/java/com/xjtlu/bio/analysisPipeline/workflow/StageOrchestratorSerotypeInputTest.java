package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.QUALITY_ENCODING_33;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_SHORT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor;
import com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor.InputMode;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Pure scheduling contracts; no Spring, database, files or external tools. */
class StageOrchestratorSerotypeInputTest {

    private static final int SALMONELLA = 28901;
    private static final int PNEUMOCOCCUS = 1313;
    private static final int INITIAL_VERSION = 7;
    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @Test
    void pairedReadsUseActualTaxonomyInputsAndInspectionMetadataWithoutQcStage() throws Exception {
        SequenceMeta inspected = readMeta(READ_LEN_TYPE_SHORT);
        inspected.setSequencingPlatform(17);
        Fixture fixture = fixture(SALMONELLA, "qc/clean_R1.fastq.gz", "qc/clean_R2.fastq.gz",
                "assembly/contigs.fasta", readMeta(READ_LEN_TYPE_LONG), inspected, false);

        assertQueued(fixture, plan(fixture), InputMode.READS);

        SeroTypeStageInputUrls inputs = inputs(fixture);
        assertEquals("qc/clean_R1.fastq.gz", inputs.getR1Url());
        assertEquals("qc/clean_R2.fastq.gz", inputs.getR2Url());
        assertEquals("assembly/contigs.fasta", inputs.getContigsUrl());
        SequenceMeta actual = parameters(fixture).getReadMeta();
        assertEquals(READ_LEN_TYPE_SHORT, actual.getReadLenType());
        assertEquals(SEQUENCE_LEVEL_READ, actual.getSequenceLevel());
        assertEquals(QUALITY_ENCODING_33, actual.getQualityEncoding());
        assertEquals(17, actual.getSequencingPlatform());
    }

    @Test
    void confirmedSingleReadsCanBeScheduledWithoutContigs() throws Exception {
        Fixture fixture = fixture(SALMONELLA, "clean_R1.fastq", null, null,
                null, readMeta(READ_LEN_TYPE_SHORT), false);

        assertQueued(fixture, plan(fixture), InputMode.READS);
        assertNull(inputs(fixture).getR2Url());
        assertNull(inputs(fixture).getContigsUrl());
    }

    @Test
    void pneumococcusPairedShortReadsDoNotRequireQcStagePresence() throws Exception {
        Fixture fixture = fixture(PNEUMOCOCCUS, "R1.fastq", "R2.fastq", "contigs.fasta",
                null, readMeta(READ_LEN_TYPE_SHORT), false);

        assertQueued(fixture, plan(fixture), InputMode.READS);
    }

    @Test
    void pneumococcusSingleReadsFallBackToExistingContigs() throws Exception {
        Fixture fixture = fixture(PNEUMOCOCCUS, "R1.fastq", null, "contigs.fasta",
                null, readMeta(READ_LEN_TYPE_SHORT), true);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1313, 573, 562, 28901 })
    void assemblyEntryUsesContigsAndDoesNotPretendItsR1AliasIsReads(int taxid) throws Exception {
        Fixture fixture = fixture(taxid, "uploaded/contigs.fna.gz", null, "uploaded/contigs.fna.gz",
                assemblyMeta(), null, false);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
        assertNull(inputs(fixture).getR1Url());
        assertNull(inputs(fixture).getR2Url());
        assertEquals("uploaded/contigs.fna.gz", inputs(fixture).getContigsUrl());
        assertEquals(SEQUENCE_LEVEL_ASSEMBLY, parameters(fixture).getReadMeta().getSequenceLevel());
    }

    @ParameterizedTest
    @ValueSource(ints = { 562, 573 })
    void contigOnlyImplementationsDoNotSwitchToAvailableShortReads(int taxid) throws Exception {
        Fixture fixture = fixture(taxid, "R1.fastq", "R2.fastq", "contigs.fasta",
                null, readMeta(READ_LEN_TYPE_SHORT), true);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
    }

    @ParameterizedTest
    @ValueSource(ints = { 28901, 1313 })
    void longReadsFallBackToContigsInsteadOfShortReadRoute(int taxid) throws Exception {
        Fixture fixture = fixture(taxid, "long_R1.fastq", "long_R2.fastq", "contigs.fasta",
                readMeta(READ_LEN_TYPE_SHORT), readMeta(READ_LEN_TYPE_LONG), true);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
        assertEquals(READ_LEN_TYPE_LONG, parameters(fixture).getReadMeta().getReadLenType());
    }

    @Test
    void longReadsWithoutContigsAreSkippedWithReason() throws Exception {
        Fixture fixture = fixture(SALMONELLA, "long.fastq", null, null,
                readMeta(READ_LEN_TYPE_SHORT), readMeta(READ_LEN_TYPE_LONG), true);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void builderDefaultShortMetadataIsNotTreatedAsCompletedInspection() throws Exception {
        Fixture fixture = fixture(SALMONELLA, "R1.fastq", "R2.fastq", "contigs.fasta",
                readMeta(READ_LEN_TYPE_SHORT), null, true);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
        assertNull(parameters(fixture).getReadMeta());
    }

    @Test
    void qcStageAloneDoesNotMakeUnknownReadsCompatible() throws Exception {
        Fixture fixture = fixture(SALMONELLA, "R1.fastq", "R2.fastq", null,
                readMeta(READ_LEN_TYPE_SHORT), null, true);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void failedInspectionCannotSupplyStaleShortReadMetadata() throws Exception {
        Fixture fixture = fixture(SALMONELLA, "R1.fastq", "R2.fastq", null,
                null, readMeta(READ_LEN_TYPE_SHORT), true);
        fixture.stages().stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_READ_INSPECT)
                .findFirst().orElseThrow().setStatus(PIPELINE_STAGE_STATUS_FAIL);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void assemblyMetadataPreventsEvenDifferentR1AndR2FieldsBeingUsedAsReads() throws Exception {
        Fixture fixture = fixture(PNEUMOCOCCUS, "legacy_R1.fasta", "legacy_R2.fasta", "contigs.fasta",
                assemblyMeta(), readMeta(READ_LEN_TYPE_SHORT), false);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
        assertNull(inputs(fixture).getR1Url());
        assertNull(inputs(fixture).getR2Url());
    }

    @Test
    void identicalR1AndContigAliasNeverBecomesReadsEvenWithShortReadMetadata() throws Exception {
        Fixture fixture = fixture(SALMONELLA, "contigs.fasta", "R2.fastq", "contigs.fasta",
                null, readMeta(READ_LEN_TYPE_SHORT), false);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
        assertNull(inputs(fixture).getR1Url());
        assertNull(inputs(fixture).getR2Url());
    }

    @Test
    void contigOnlySpeciesWithReadsOnlyIsSkipped() throws Exception {
        Fixture fixture = fixture(573, "R1.fastq", "R2.fastq", null,
                null, readMeta(READ_LEN_TYPE_SHORT), true);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void pneumococcusSingleReadWithoutContigsIsSkipped() throws Exception {
        Fixture fixture = fixture(PNEUMOCOCCUS, "R1.fastq", null, null,
                null, readMeta(READ_LEN_TYPE_SHORT), true);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void missingTaxonomyInputsAreSkippedInsteadOfNullPointerFailure() throws Exception {
        Fixture fixture = fixture(562, null, null, null, null, null, false);
        fixture.taxonomy().setInputUrl(null);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void unsupportedTaxonomyIsSkippedWithVersionEndTimeAndReason() throws Exception {
        Fixture fixture = fixture(999, null, null, "contigs.fasta", null, null, false);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void missingTaxonomyResultPreservesSkipContract() throws Exception {
        Fixture fixture = fixture(562, null, null, "contigs.fasta", null, null, false);
        fixture.taxonomy().setOutputInline(null);

        assertSkipped(fixture, plan(fixture));
    }

    @Test
    void declaredArtifactIsQueuedWithoutOrchestratorCheckingLocalFileExistence() throws Exception {
        Fixture fixture = fixture(562, null, null, "objects/not-downloaded-yet.fasta", null, null, false);

        assertQueued(fixture, plan(fixture), InputMode.CONTIGS);
        assertEquals("objects/not-downloaded-yet.fasta", inputs(fixture).getContigsUrl());
    }

    private OrchestratePlan plan(Fixture fixture) throws Exception {
        return orchestrator.makeDownstreamPlan(fixture.taxonomy(), fixture.stages(), PIPELINE_REGULAR_BACTERIA);
    }

    private static void assertQueued(Fixture fixture, OrchestratePlan plan, InputMode expectedMode)
            throws Exception {
        assertEquals(1, plan.getRunStages().size());
        assertSame(fixture.serotype(), plan.getRunStages().get(0));
        UpdateStageCommand update = singleUpdate(fixture, plan);
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, fixture.serotype().getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, update.getUpdateStage().getStatus());
        assertEquals(fixture.serotype().getInputUrl(), update.getUpdateStage().getInputUrl());
        assertEquals(fixture.serotype().getParameters(), update.getUpdateStage().getParameters());
        assertNull(fixture.serotype().getEndTime());
        assertNull(fixture.serotype().getOutputInline());
        SeroTypingStageParameters parameters = parameters(fixture);
        assertEquals(expectedMode, SeroTypingStageExectuor.resolveInputMode(
                parameters.getTaxonomyContext(), inputs(fixture), parameters.getReadMeta()));
    }

    private static void assertSkipped(Fixture fixture, OrchestratePlan plan) throws Exception {
        assertTrue(plan.getRunStages().isEmpty());
        UpdateStageCommand update = singleUpdate(fixture, plan);
        BioPipelineStage patch = update.getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, fixture.serotype().getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, patch.getStatus());
        assertNotNull(patch.getEndTime());
        assertEquals(patch.getEndTime(), fixture.serotype().getEndTime());
        assertEquals(patch.getOutputInline(), fixture.serotype().getOutputInline());
        Map<String, String> output = JsonUtil.toMap(patch.getOutputInline(), String.class);
        assertNotNull(output.get("skipReason"));
        assertFalse(output.get("skipReason").isBlank());
    }

    private static UpdateStageCommand singleUpdate(Fixture fixture, OrchestratePlan plan) {
        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand update = plan.getUpdateStageCommands().get(0);
        assertEquals(fixture.serotype().getStageId().longValue(), update.getStageId());
        assertEquals(INITIAL_VERSION, update.getCurrentVersion());
        assertEquals(INITIAL_VERSION + 1, update.getUpdateStage().getVersion());
        assertEquals(INITIAL_VERSION + 1, fixture.serotype().getVersion());
        return update;
    }

    private static SeroTypeStageInputUrls inputs(Fixture fixture) throws Exception {
        return JsonUtil.toObject(fixture.serotype().getInputUrl(), SeroTypeStageInputUrls.class);
    }

    private static SeroTypingStageParameters parameters(Fixture fixture) throws Exception {
        return JsonUtil.toObject(fixture.serotype().getParameters(), SeroTypingStageParameters.class);
    }

    private static Fixture fixture(int taxid, String r1, String r2, String contigs,
            SequenceMeta declaredMeta, SequenceMeta inspectedMeta, boolean withQc) throws Exception {
        TaxonomyResult.Taxon best = new TaxonomyResult.Taxon();
        best.setTaxid(taxid);
        best.setName("test taxon " + taxid);
        best.setRank("S");
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_CONFIDENT);
        result.setBest(best);
        TaxonomyStageInputUrls taxonomyInputs = new TaxonomyStageInputUrls();
        taxonomyInputs.setR1(r1);
        taxonomyInputs.setR2(r2);
        taxonomyInputs.setContigs(contigs);
        BioPipelineStage taxonomy = stage(10L, PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_STATUS_FINISHED);
        taxonomy.setInputUrl(JsonUtil.toJson(taxonomyInputs));
        taxonomy.setOutputInline(JsonUtil.toJson(result));

        SeroTypingStageParameters parameters = new SeroTypingStageParameters();
        parameters.setReadMeta(declaredMeta);
        BioPipelineStage serotype = stage(11L, PIPELINE_STAGE_SEROTYPE, PIPELINE_STAGE_STATUS_PENDING);
        serotype.setParameters(JsonUtil.toJson(parameters));
        List<BioPipelineStage> stages = new ArrayList<>(List.of(taxonomy, serotype));
        if (inspectedMeta != null) {
            ReadInspectStageResult inspected = new ReadInspectStageResult();
            inspected.setReadMeta(inspectedMeta);
            inspected.setR1Url("original/raw_R1.fastq");
            inspected.setR2Url("original/raw_R2.fastq");
            BioPipelineStage inspection = stage(12L, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_FINISHED);
            inspection.setOutputUrl(JsonUtil.toJson(inspected));
            stages.add(inspection);
        }
        if (withQc) {
            stages.add(stage(13L, PIPELINE_STAGE_QC, PIPELINE_STAGE_STATUS_FINISHED));
        }
        return new Fixture(stages, taxonomy, serotype);
    }

    private static BioPipelineStage stage(long id, int type, int status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(id);
        stage.setPipelineId(1L);
        stage.setStageType(type);
        stage.setStageIndex(-1);
        stage.setStatus(status);
        stage.setVersion(INITIAL_VERSION);
        return stage;
    }

    private static SequenceMeta readMeta(int lengthType) {
        SequenceMeta metadata = new SequenceMeta();
        metadata.setSequenceLevel(SEQUENCE_LEVEL_READ);
        metadata.setReadLenType(lengthType);
        metadata.setQualityEncoding(QUALITY_ENCODING_33);
        return metadata;
    }

    private static SequenceMeta assemblyMeta() {
        SequenceMeta metadata = new SequenceMeta();
        metadata.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);
        return metadata;
    }

    private record Fixture(List<BioPipelineStage> stages, BioPipelineStage taxonomy, BioPipelineStage serotype) {
    }
}
