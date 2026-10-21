package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_RUNNING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_AMR;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MLST;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VIRULENCE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_OUT_PANEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.BeanUtils;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapperExtension;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Real callback/orchestrator contracts with in-memory stages; no database or analysis execution. */
class PipelineServiceTaxonomyOutPanelTerminalTest {

    private static final long PIPELINE_ID = 10L;
    private static final long TAXONOMY_ID = 2L;
    private static final long SEROTYPE_ID = 3L;
    private static final int VERSION = 3;
    private static final String SKIP_REASON = "Taxonomy is OUT_PANEL; serotyping is not applicable";

    private PipelineService service;
    private PipelineStageTaskDispatcher dispatcher;
    private BioAnalysisPipelineMapper pipelineMapper;
    private BioAnalysisStageMapperExtension stageExtension;
    private BioAnalysisPipeline pipeline;
    private String taxonomyOutput;
    private int batchResult;
    private final Map<Long, BioPipelineStage> storedStages = new LinkedHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        service = spy(new PipelineService());
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        stageExtension = mock(BioAnalysisStageMapperExtension.class);
        BioAnalysisPipelineMapperExtension pipelineExtension = mock(BioAnalysisPipelineMapperExtension.class);
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapperExtension", pipelineExtension);
        ReflectionTestUtils.setField(service, "bioAnalysisStageMapperExtension", stageExtension);
        ReflectionTestUtils.setField(service, "stageOrchestrator", new StageOrchestrator());

        pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(PIPELINE_ID);
        pipeline.setPipelineType(PIPELINE_REGULAR_BACTERIA);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_RUNNING);
        storeStage(1L, PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage taxonomy = storeStage(TAXONOMY_ID, PIPELINE_STAGE_TAXONOMY,
                PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        TaxonomyResult taxonomyResult = new TaxonomyResult();
        taxonomyResult.setStatus(STATUS_OUT_PANEL);
        taxonomyOutput = JsonUtil.toJson(taxonomyResult);
        taxonomy.setOutputInline(taxonomyOutput);
        storeStage(SEROTYPE_ID, PIPELINE_STAGE_SEROTYPE, PIPELINE_STAGE_STATUS_PENDING);
        storeStage(4L, PIPELINE_STAGE_AMR, PIPELINE_STAGE_STATUS_FINISHED);
        storeStage(5L, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_STATUS_FINISHED);
        storeStage(6L, PIPELINE_STAGE_MLST, PIPELINE_STAGE_STATUS_FINISHED);

        when(pipelineExtension.selectPipelineTypeByStageId(TAXONOMY_ID)).thenReturn(pipeline);
        when(stageExtension.selectAllPipelineStagesByStageId(TAXONOMY_ID)).thenAnswer(invocation ->
                storedStages.values().stream().map(stage -> {
                    BioPipelineStage snapshot = new BioPipelineStage();
                    BeanUtils.copyProperties(stage, snapshot);
                    return snapshot;
                }).toList());
        doAnswer(invocation -> {
            List<UpdateStageCommand> commands = invocation.getArgument(0);
            for (UpdateStageCommand command : commands) {
                assertEquals(SEROTYPE_ID, command.getStageId());
                assertEquals(VERSION, command.getCurrentVersion());
                BioPipelineStage patch = command.getUpdateStage();
                assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, patch.getStatus());
                assertEquals(VERSION + 1, patch.getVersion());
                assertNotNull(patch.getEndTime());
                assertEquals(Map.of("skipReason", SKIP_REASON), JsonUtil.toMap(patch.getOutputInline()));
                assertNull(patch.getInputUrl());
                assertNull(patch.getParameters());
                if (batchResult == 0) {
                    BioPipelineStage stored = storedStages.get(command.getStageId());
                    assertEquals(command.getCurrentVersion(), stored.getVersion());
                    stored.setStatus(patch.getStatus());
                    stored.setVersion(patch.getVersion());
                    stored.setEndTime(patch.getEndTime());
                    stored.setOutputInline(patch.getOutputInline());
                }
            }
            return batchResult;
        }).when(service).batchUpdateStages(anyList());
        when(pipelineMapper.updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class))).thenAnswer(invocation -> {
                    BioAnalysisPipeline patch = invocation.getArgument(0);
                    pipeline.setStatus(patch.getStatus());
                    return 1;
                });
    }

    @AfterEach
    void outPanelCallbacksNeverDispatchAnalysisOrModifyTaxonomy() {
        verifyNoInteractions(dispatcher);
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, storedStages.get(TAXONOMY_ID).getStatus());
        assertEquals(VERSION, storedStages.get(TAXONOMY_ID).getVersion());
        assertEquals(taxonomyOutput, storedStages.get(TAXONOMY_ID).getOutputInline());
    }

    @Test
    void callbackSkipsSerotypeAndCompletesPipelineWhenOtherStagesAreTerminal() throws Exception {
        service.pipelineStageDone(TAXONOMY_ID, true);

        assertSerotypeSkipped();
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verify(service).batchUpdateStages(argThat(updates -> updates.size() == 1));
        verify(pipelineMapper).updateByExampleSelective(
                argThat(patch -> patch.getStatus() == PipelineService.PIPELINE_STATUS_COMPELETE),
                any(BioAnalysisPipelineExample.class));
        verify(stageExtension, times(2)).selectAllPipelineStagesByStageId(TAXONOMY_ID);
    }

    @ParameterizedTest
    @ValueSource(ints = { PIPELINE_STAGE_AMR, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_MLST })
    void skippedSerotypeDoesNotCompletePipelineWithAnUnfinishedParallelBranch(int branchType) throws Exception {
        storedStages.values().stream().filter(stage -> stage.getStageType() == branchType)
                .findFirst().orElseThrow().setStatus(PIPELINE_STAGE_STATUS_RUNNING);

        service.pipelineStageDone(TAXONOMY_ID, true);

        assertSerotypeSkipped();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verify(service).batchUpdateStages(argThat(updates -> updates.size() == 1));
        verifyNoInteractions(pipelineMapper);
        verify(stageExtension, times(2)).selectAllPipelineStagesByStageId(TAXONOMY_ID);
    }

    @Test
    void repeatedCallbacksDoNotRewriteAlreadySkippedSerotype() throws Exception {
        storedStages.get(4L).setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        service.pipelineStageDone(TAXONOMY_ID, true);
        Date endTime = storedStages.get(SEROTYPE_ID).getEndTime();
        String skipOutput = storedStages.get(SEROTYPE_ID).getOutputInline();
        service.pipelineStageDone(TAXONOMY_ID, true);

        assertSerotypeSkipped();
        assertEquals(endTime, storedStages.get(SEROTYPE_ID).getEndTime());
        assertEquals(skipOutput, storedStages.get(SEROTYPE_ID).getOutputInline());
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verify(service).batchUpdateStages(argThat(updates -> updates.size() == 1));
        verify(service).batchUpdateStages(List.of());
        verifyNoInteractions(pipelineMapper);
        verify(stageExtension, times(4)).selectAllPipelineStagesByStageId(TAXONOMY_ID);
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 100 })
    void failedSkipCommitLeavesPipelineRunningAndNextCallbackRetriesPersistedPendingStage(int failedResult)
            throws Exception {
        batchResult = failedResult;

        service.pipelineStageDone(TAXONOMY_ID, true);

        assertSerotypeRemainsPending();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verifyNoInteractions(pipelineMapper);
        verify(stageExtension).selectAllPipelineStagesByStageId(TAXONOMY_ID);

        batchResult = 0;
        service.pipelineStageDone(TAXONOMY_ID, true);

        assertSerotypeSkipped();
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verify(service, times(2)).batchUpdateStages(argThat(updates -> updates.size() == 1));
        verify(pipelineMapper).updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class));
        verify(stageExtension, times(3)).selectAllPipelineStagesByStageId(TAXONOMY_ID);
    }

    private void assertSerotypeSkipped() throws Exception {
        BioPipelineStage serotype = storedStages.get(SEROTYPE_ID);
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, serotype.getStatus());
        assertEquals(VERSION + 1, serotype.getVersion());
        assertNotNull(serotype.getEndTime());
        assertEquals(Map.of("skipReason", SKIP_REASON), JsonUtil.toMap(serotype.getOutputInline()));
        assertEquals(SEROTYPE_ID, serotype.getStageId());
        assertEquals(PIPELINE_ID, serotype.getPipelineId());
        assertEquals(PIPELINE_STAGE_SEROTYPE, serotype.getStageType());
        assertEquals(-1, serotype.getStageIndex());
        assertNull(serotype.getStartTime());
        assertNull(serotype.getInputUrl());
        assertNull(serotype.getParameters());
        assertNull(serotype.getOutputUrl());
    }

    private void assertSerotypeRemainsPending() {
        BioPipelineStage serotype = storedStages.get(SEROTYPE_ID);
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, serotype.getStatus());
        assertEquals(VERSION, serotype.getVersion());
        assertNull(serotype.getEndTime());
        assertNull(serotype.getOutputInline());
        assertNull(serotype.getInputUrl());
        assertNull(serotype.getParameters());
    }

    private BioPipelineStage storeStage(long id, int type, int status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(id);
        stage.setPipelineId(PIPELINE_ID);
        stage.setStageType(type);
        stage.setStageIndex(-1);
        stage.setStatus(status);
        stage.setVersion(VERSION);
        storedStages.put(id, stage);
        return stage;
    }
}
