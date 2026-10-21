package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID_WASTEWATER;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ILLUMINA;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_COVID_WASTEWATER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CovidWastewaterStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.CovidWastewaterStageParameters;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Pure scheduling contracts; no Spring, database or external analysis. */
class CovidWastewaterStageOrchestratorTest {

    private static final int INITIAL_VERSION = 7;
    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @Test
    void queuesStandaloneEntryWithoutReadInspectQcOrAssemblyDependencies() throws Exception {
        BioPipelineStage stage = wastewaterStage();
        String originalInput = stage.getInputUrl();
        String originalParameters = stage.getParameters();

        OrchestratePlan plan = orchestrator.makePlan(List.of(stage), stage.getStageId());

        assertNotNull(plan);
        assertFalse(plan.isNoNextStage());
        assertEquals(1, plan.getRunStages().size());
        assertSame(stage, plan.getRunStages().get(0));
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, stage.getStatus());
        assertEquals(INITIAL_VERSION + 1, stage.getVersion());
        assertEquals(originalInput, stage.getInputUrl());
        assertEquals(originalParameters, stage.getParameters());
        assertNull(stage.getEndTime());

        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand update = plan.getUpdateStageCommands().get(0);
        assertEquals(stage.getStageId().longValue(), update.getStageId());
        assertEquals(INITIAL_VERSION, update.getCurrentVersion());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, update.getUpdateStage().getStatus());
        assertEquals(INITIAL_VERSION + 1, update.getUpdateStage().getVersion());
        assertNull(update.getUpdateStage().getInputUrl(), "Entry scheduling must not overwrite persisted inputs");
        assertNull(update.getUpdateStage().getParameters(), "Entry scheduling must not overwrite persisted parameters");

        CovidWastewaterStageInputUrls input = JsonUtil.toObject(stage.getInputUrl(), CovidWastewaterStageInputUrls.class);
        assertEquals("uploads/sample_R1.fastq.gz", input.getR1Url());
        assertEquals("uploads/sample_R2.fastq.gz", input.getR2Url());
        assertEquals(ILLUMINA, input.getSequencingPlatform());
        CovidWastewaterStageParameters parameters = JsonUtil.toObject(stage.getParameters(),
                CovidWastewaterStageParameters.class);
        assertEquals(4, parameters.getThreads());
        assertEquals(1800L, parameters.getTimeoutSeconds());
        assertEquals("test-profile", parameters.getAnalysisProfile());
        assertNull(parameters.getReadMeta());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void completionReturnsTerminalPlanWithoutDownstreamWork(boolean resolveByStageId) throws Exception {
        BioPipelineStage stage = wastewaterStage();
        stage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        String originalInput = stage.getInputUrl();
        String originalParameters = stage.getParameters();
        List<BioPipelineStage> stages = List.of(stage);

        OrchestratePlan plan = resolveByStageId
                ? orchestrator.makeDownstreamPlan(stage.getStageId(), stages, PIPELINE_VIRUS_COVID_WASTEWATER)
                : orchestrator.makeDownstreamPlan(stage, stages, PIPELINE_VIRUS_COVID_WASTEWATER);

        assertNotNull(plan);
        assertTrue(plan.isNoNextStage());
        assertTrue(plan.getRunStages().isEmpty());
        assertTrue(plan.getUpdateStageCommands().isEmpty());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stage.getStatus());
        assertEquals(INITIAL_VERSION, stage.getVersion());
        assertEquals(originalInput, stage.getInputUrl());
        assertEquals(originalParameters, stage.getParameters());
    }

    private static BioPipelineStage wastewaterStage() throws Exception {
        CovidWastewaterStageInputUrls input = new CovidWastewaterStageInputUrls();
        input.setR1Url("uploads/sample_R1.fastq.gz");
        input.setR2Url("uploads/sample_R2.fastq.gz");
        input.setSequencingPlatform(ILLUMINA);
        CovidWastewaterStageParameters parameters = new CovidWastewaterStageParameters();
        parameters.setThreads(4);
        parameters.setTimeoutSeconds(1800L);
        parameters.setAnalysisProfile("test-profile");

        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(9201L);
        stage.setStageType(PIPELINE_STAGE_COVID_WASTEWATER);
        stage.setStageName("新冠污水分析");
        stage.setStageIndex(0);
        stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stage.setVersion(INITIAL_VERSION);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(parameters));
        return stage;
    }
}
