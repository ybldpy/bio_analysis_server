package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.QUALITY_ENCODING_64;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ONT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AssemblyStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Metadata handoff contracts only: no Spring, database, files or external tools. */
class StageOrchestratorSequenceMetaTest {

    private static final int INITIAL_VERSION = 3;
    private static final int TEST_PLATFORM = ONT;
    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @ParameterizedTest
    @CsvSource({ "true,true", "true,false", "false,true", "false,false" })
    void assemblyUsesFinalInspectionMetadataWithOrWithoutQc(boolean withQc, boolean paired) throws Exception {
        BioPipelineStage readInspect = readInspectStage(paired);
        BioPipelineStage assembly = pendingStage(PIPELINE_STAGE_ASSEMBLY, new AssemblyStageParameters());
        List<BioPipelineStage> stages = new ArrayList<>(List.of(readInspect, assembly));
        if (withQc) {
            stages.add(qcStage(paired));
        }

        OrchestratePlan plan = orchestrator.makePlan(stages, assembly.getStageId());

        AssemblyInputUrls input = JsonUtil.toObject(assembly.getInputUrl(), AssemblyInputUrls.class);
        String prefix = withQc ? "qc/cleaned-" : "inspected/";
        assertEquals(prefix + "r1.fastq", input.getRead1Url());
        assertEquals(paired ? prefix + "r2.fastq" : null, input.getRead2Url());
        AssemblyStageParameters parameters = JsonUtil.toObject(assembly.getParameters(), AssemblyStageParameters.class);
        assertResolvedMetadata(parameters);
        assertEquals(BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, parameters.getAnalysisTargetType());
        assertPatchMatchesStage(plan, assembly);
    }

    @Test
    void assemblyWithoutExistingParametersStillReceivesInspectedMetadata() throws Exception {
        BioPipelineStage assembly = pendingStage(PIPELINE_STAGE_ASSEMBLY, null);

        orchestrator.makePlan(List.of(readInspectStage(false), assembly), assembly.getStageId());

        assertResolvedMetadata(JsonUtil.toObject(assembly.getParameters(), AssemblyStageParameters.class));
    }

    @Test
    void qcUsesInspectionOutputRatherThanItsInitialMetadata() throws Exception {
        QcParameters parameters = new QcParameters();
        parameters.getShortReadFilters().setMinimumLength(25);
        parameters.getLongReadFilters().setMeanQuality(8);
        BioPipelineStage qc = pendingStage(PIPELINE_STAGE_QC, parameters);

        OrchestratePlan plan = orchestrator.makePlan(List.of(readInspectStage(false), qc), qc.getStageId());

        QcParameters resolved = JsonUtil.toObject(qc.getParameters(), QcParameters.class);
        assertResolvedMetadata(resolved);
        assertEquals(25, resolved.getShortReadFilters().getMinimumLength());
        assertEquals(8, resolved.getLongReadFilters().getMeanQuality());
        assertEquals(15, resolved.getShortReadFilters().getQualifiedQualityPhred());
        assertEquals(10, resolved.getLongReadFilters().getNPercentLimit());
        QcStageInputUrls input = JsonUtil.toObject(qc.getInputUrl(), QcStageInputUrls.class);
        assertEquals("inspected/r1.fastq", input.getRead1());
        assertNull(input.getRead2());
        assertPatchMatchesStage(plan, qc);
    }

    @Test
    void taxonomyReceivesFinalMetadataAlongsideAssemblyAndReadInputs() throws Exception {
        BioPipelineStage taxonomy = pendingStage(PIPELINE_STAGE_TAXONOMY, new BaseStageParams());
        BioPipelineStage assembly = stage(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED);
        assembly.setOutputUrl(JsonUtil.toJson(new AssemblyResult("assembly/contigs.fasta", null)));

        OrchestratePlan plan = orchestrator.makePlan(
                List.of(readInspectStage(false), qcStage(false), assembly, taxonomy), taxonomy.getStageId());

        BaseStageParams parameters = JsonUtil.toObject(taxonomy.getParameters(), BaseStageParams.class);
        assertResolvedMetadata(parameters);
        assertEquals(BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, parameters.getAnalysisTargetType());
        TaxonomyStageInputUrls input = JsonUtil.toObject(taxonomy.getInputUrl(), TaxonomyStageInputUrls.class);
        assertEquals("assembly/contigs.fasta", input.getContigs());
        assertEquals("qc/cleaned-r1.fastq", input.getR1());
        assertPatchMatchesStage(plan, taxonomy);
    }

    @Test
    void assemblyLevelEntryPreservesItsMetadataWithoutAReadInspectionStage() throws Exception {
        BioPipelineStage taxonomy = pendingStage(PIPELINE_STAGE_TAXONOMY, new BaseStageParams());
        taxonomy.setStageIndex(0);
        taxonomy.setInputUrl("{\"contigs\":\"uploaded/assembly.fasta\"}");
        String originalParameters = taxonomy.getParameters();
        String originalInput = taxonomy.getInputUrl();

        OrchestratePlan plan = orchestrator.makePlan(List.of(taxonomy), taxonomy.getStageId());

        assertEquals(originalParameters, taxonomy.getParameters());
        assertEquals(originalInput, taxonomy.getInputUrl());
        assertEquals(SEQUENCE_LEVEL_ASSEMBLY,
                JsonUtil.toObject(taxonomy.getParameters(), BaseStageParams.class).getReadMeta().getSequenceLevel());
        assertSame(taxonomy, plan.getRunStages().get(0));
        assertNull(plan.getUpdateStageCommands().get(0).getUpdateStage().getParameters());
    }

    private static BioPipelineStage readInspectStage(boolean paired) throws Exception {
        SequenceMeta metadata = new SequenceMeta();
        metadata.setSequenceLevel(SEQUENCE_LEVEL_READ);
        metadata.setQualityEncoding(QUALITY_ENCODING_64);
        metadata.setReadLenType(READ_LEN_TYPE_LONG);
        metadata.setSequencingPlatform(TEST_PLATFORM);
        ReadInspectStageResult inspected = new ReadInspectStageResult();
        inspected.setReadMeta(metadata);
        inspected.setR1Url("inspected/r1.fastq");
        inspected.setR2Url(paired ? "inspected/r2.fastq" : null);
        BioPipelineStage stage = stage(PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_FINISHED);
        stage.setOutputUrl(JsonUtil.toJson(inspected));
        return stage;
    }

    private static BioPipelineStage qcStage(boolean paired) throws Exception {
        BioPipelineStage stage = stage(PIPELINE_STAGE_QC, PIPELINE_STAGE_STATUS_FINISHED);
        stage.setOutputUrl(JsonUtil.toJson(new QcResult("qc/cleaned-r1.fastq",
                paired ? "qc/cleaned-r2.fastq" : null, null, null)));
        return stage;
    }

    private static BioPipelineStage pendingStage(int type, BaseStageParams parameters) throws Exception {
        BioPipelineStage stage = stage(type, PIPELINE_STAGE_STATUS_PENDING);
        if (parameters != null) {
            SequenceMeta staleMetadata = new SequenceMeta();
            staleMetadata.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);
            parameters.setReadMeta(staleMetadata);
            parameters.setAnalysisTargetType(BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA);
            stage.setParameters(JsonUtil.toJson(parameters));
        }
        return stage;
    }

    private static BioPipelineStage stage(int type, int status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId((long) type + 1);
        stage.setStageType(type);
        stage.setPipelineId(1L);
        stage.setVersion(INITIAL_VERSION);
        stage.setStageIndex(-1);
        stage.setStatus(status);
        return stage;
    }

    private static void assertResolvedMetadata(BaseStageParams parameters) {
        assertNotNull(parameters);
        SequenceMeta metadata = parameters.getReadMeta();
        assertNotNull(metadata);
        assertEquals(SEQUENCE_LEVEL_READ, metadata.getSequenceLevel());
        assertEquals(QUALITY_ENCODING_64, metadata.getQualityEncoding());
        assertEquals(READ_LEN_TYPE_LONG, metadata.getReadLenType());
        assertEquals(TEST_PLATFORM, metadata.getSequencingPlatform());
    }

    private static void assertPatchMatchesStage(OrchestratePlan plan, BioPipelineStage stage) {
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, stage.getStatus());
        assertEquals(INITIAL_VERSION + 1, stage.getVersion());
        assertSame(stage, plan.getRunStages().get(0));
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        assertEquals(INITIAL_VERSION, command.getCurrentVersion());
        assertEquals(stage.getParameters(), command.getUpdateStage().getParameters());
        assertEquals(stage.getInputUrl(), command.getUpdateStage().getInputUrl());
    }
}
