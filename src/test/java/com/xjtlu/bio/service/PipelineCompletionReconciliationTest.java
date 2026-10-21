package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_ACTION_REQUIRED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_RUNNING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_AMR;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MLST;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VIRULENCE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_OUT_PANEL;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STATUS_COMPELETE;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STATUS_PENDING;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STATUS_RUNNING;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapperExtension;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.utils.JsonUtil;

/** Whole-pipeline completion recovery only; no context, database or analysis execution. */
class PipelineCompletionReconciliationTest {

    private static final long PIPELINE_ID = 10L;

    private PipelineService service;
    private BioAnalysisPipelineMapper pipelineMapper;
    private BioPipelineStageMapper stageMapper;
    private PipelineStageTaskDispatcher dispatcher;
    private StageOrchestrator orchestrator;
    private BioAnalysisPipelineMapperExtension pipelineExtension;
    private BioAnalysisStageMapperExtension stageExtension;
    private BioAnalysisPipeline storedPipeline;
    private List<BioPipelineStage> storedStages;

    @BeforeEach
    void setUp() {
        service = new PipelineService();
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        stageMapper = mock(BioPipelineStageMapper.class);
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        orchestrator = mock(StageOrchestrator.class);
        pipelineExtension = mock(BioAnalysisPipelineMapperExtension.class);
        stageExtension = mock(BioAnalysisStageMapperExtension.class);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "analysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stageMapper);
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "stageOrchestrator", orchestrator);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapperExtension", pipelineExtension);
        ReflectionTestUtils.setField(service, "bioAnalysisStageMapperExtension", stageExtension);

        storedPipeline = new BioAnalysisPipeline();
        storedPipeline.setPipelineId(PIPELINE_ID);
        storedPipeline.setPipelineType(PIPELINE_REGULAR_BACTERIA);
        storedPipeline.setStatus(PIPELINE_STATUS_RUNNING);
        storedStages = List.of(stage(1L, PIPELINE_STAGE_STATUS_FINISHED),
                stage(2L, PIPELINE_STAGE_STATUS_FINISHED));
        when(pipelineMapper.selectByPrimaryKey(PIPELINE_ID)).thenAnswer(invocation -> freshPipelineSnapshot());
        when(stageMapper.selectByExample(any(BioPipelineStageExample.class)))
                .thenAnswer(invocation -> storedStages);
        when(pipelineMapper.updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class)))
                .thenAnswer(invocation -> persistPipelinePatch(invocation.getArgument(0)));
    }

    @AfterEach
    void reconciliationNeverWritesOrRerunsStages() {
        verifyNoInteractions(dispatcher, orchestrator, pipelineExtension, stageExtension);
        assertTrue(mockingDetails(stageMapper).getInvocations().stream()
                .allMatch(invocation -> invocation.getMethod().getName().equals("selectByExample")),
                "Completion reconciliation may only read stages through the non-BLOB status query");
    }

    @Test
    void lostFinalCallbackIsReconciledWhenEveryPersistedStageIsFinished() {
        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_COMPELETE, storedPipeline.getStatus());
        verify(pipelineMapper).selectByPrimaryKey(PIPELINE_ID);
        assertStageQueries(1);
        assertCompletionWrites(1);
    }

    @Test
    void finishedAndNotApplicableStagesTogetherPermitPipelineCompletion() {
        storedStages = List.of(stage(1L, PIPELINE_STAGE_STATUS_FINISHED),
                stage(2L, PIPELINE_STAGE_STATUS_NOT_APPLICABLE),
                stage(3L, PIPELINE_STAGE_STATUS_FINISHED));

        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_COMPELETE, storedPipeline.getStatus());
        assertStageQueries(1);
        assertCompletionWrites(1);
    }

    @Test
    void outPanelTaxonomyAndSkippedSerotypeWaitForEveryParallelBranchBeforeCompletion() throws Exception {
        BioPipelineStage taxonomy = typedStage(2L, PIPELINE_STAGE_TAXONOMY,
                PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_OUT_PANEL);
        taxonomy.setOutputInline(JsonUtil.toJson(result));
        BioPipelineStage serotype = typedStage(3L, PIPELINE_STAGE_SEROTYPE,
                PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        serotype.setOutputInline(JsonUtil.toJson(Map.of("skipReason", "Taxonomy is outside the supported panel")));
        BioPipelineStage amr = typedStage(4L, PIPELINE_STAGE_AMR, PIPELINE_STAGE_STATUS_RUNNING);
        BioPipelineStage virulence = typedStage(5L, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_STATUS_QUEUING);
        BioPipelineStage mlst = typedStage(6L, PIPELINE_STAGE_MLST, PIPELINE_STAGE_STATUS_PENDING);
        storedStages = List.of(typedStage(1L, PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED),
                taxonomy, serotype, amr, virulence, mlst);

        service.reconcilePipelineCompletion(PIPELINE_ID);
        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertNoCompletionWrite();

        amr.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        service.reconcilePipelineCompletion(PIPELINE_ID);
        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertNoCompletionWrite();

        virulence.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        service.reconcilePipelineCompletion(PIPELINE_ID);
        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertNoCompletionWrite();

        mlst.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_COMPELETE, storedPipeline.getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, taxonomy.getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, serotype.getStatus());
        assertStageQueries(4);
        assertCompletionWrites(1);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = { PIPELINE_STAGE_STATUS_PENDING, PIPELINE_STAGE_STATUS_QUEUING,
            PIPELINE_STAGE_STATUS_RUNNING, PIPELINE_STAGE_STATUS_FAIL, PIPELINE_STAGE_STATUS_ACTION_REQUIRED })
    void anyUnfinishedFailedActionRequiredOrUnknownStageStatusBlocksCompletion(Integer status) {
        storedStages = List.of(stage(1L, PIPELINE_STAGE_STATUS_FINISHED), stage(2L, status),
                stage(3L, PIPELINE_STAGE_STATUS_NOT_APPLICABLE));

        assertDoesNotThrow(() -> service.reconcilePipelineCompletion(PIPELINE_ID));

        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertStageQueries(1);
        assertNoCompletionWrite();
    }

    @Test
    void emptyStageListDoesNotVacuouslyCompleteThePipeline() {
        storedStages = List.of();

        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertStageQueries(1);
        assertNoCompletionWrite();
    }

    @Test
    void missingStageListDoesNotCompleteThePipeline() {
        storedStages = null;

        assertDoesNotThrow(() -> service.reconcilePipelineCompletion(PIPELINE_ID));

        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertStageQueries(1);
        assertNoCompletionWrite();
    }

    @Test
    void missingPipelineDoesNotReadStagesOrAttemptCompletion() {
        storedPipeline = null;

        assertDoesNotThrow(() -> service.reconcilePipelineCompletion(PIPELINE_ID));

        verify(pipelineMapper).selectByPrimaryKey(PIPELINE_ID);
        verifyNoInteractions(stageMapper);
        assertNoCompletionWrite();
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = { PIPELINE_STATUS_COMPELETE, PIPELINE_STATUS_PENDING, 0, -1 })
    void pipelinesNotCurrentlyRunningAreLeftUntouched(Integer status) {
        storedPipeline.setStatus(status);

        assertDoesNotThrow(() -> service.reconcilePipelineCompletion(PIPELINE_ID));

        assertEquals(status, storedPipeline.getStatus());
        verify(pipelineMapper).selectByPrimaryKey(PIPELINE_ID);
        verifyNoInteractions(stageMapper);
        assertNoCompletionWrite();
    }

    @Test
    void transientCompletionWriteExceptionIsRetriedOnTheNextReconciliation() {
        doThrow(new DataAccessResourceFailureException("temporary completion write failure"))
                .doAnswer(invocation -> persistPipelinePatch(invocation.getArgument(0)))
                .when(pipelineMapper).updateByExampleSelective(any(BioAnalysisPipeline.class),
                        any(BioAnalysisPipelineExample.class));

        assertDoesNotThrow(() -> service.reconcilePipelineCompletion(PIPELINE_ID));
        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());

        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_COMPELETE, storedPipeline.getStatus());
        verify(pipelineMapper, times(2)).selectByPrimaryKey(PIPELINE_ID);
        assertStageQueries(2);
        assertCompletionWrites(2);
    }

    @Test
    void rejectedCompletionCompareAndSetIsRetriedOnTheNextReconciliation() {
        doReturn(0).doAnswer(invocation -> persistPipelinePatch(invocation.getArgument(0)))
                .when(pipelineMapper).updateByExampleSelective(any(BioAnalysisPipeline.class),
                        any(BioAnalysisPipelineExample.class));

        service.reconcilePipelineCompletion(PIPELINE_ID);
        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());

        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_COMPELETE, storedPipeline.getStatus());
        verify(pipelineMapper, times(2)).selectByPrimaryKey(PIPELINE_ID);
        assertStageQueries(2);
        assertCompletionWrites(2);
    }

    @Test
    void repeatedReconciliationReadsFreshPipelineStateAndDoesNotRewriteComplete() {
        service.reconcilePipelineCompletion(PIPELINE_ID);
        service.reconcilePipelineCompletion(PIPELINE_ID);

        assertEquals(PIPELINE_STATUS_COMPELETE, storedPipeline.getStatus());
        verify(pipelineMapper, times(2)).selectByPrimaryKey(PIPELINE_ID);
        assertStageQueries(1);
        assertCompletionWrites(1);
    }

    @Test
    void pipelineLookupExceptionPropagatesWithoutAnyCompletionAttempt() {
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("pipeline read failed");
        when(pipelineMapper.selectByPrimaryKey(PIPELINE_ID)).thenThrow(failure);

        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> service.reconcilePipelineCompletion(PIPELINE_ID)));

        verifyNoInteractions(stageMapper);
        assertNoCompletionWrite();
    }

    @Test
    void stageLookupExceptionPropagatesWithoutCompletingThePipeline() {
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("stage read failed");
        when(stageMapper.selectByExample(any(BioPipelineStageExample.class))).thenThrow(failure);

        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> service.reconcilePipelineCompletion(PIPELINE_ID)));

        assertEquals(PIPELINE_STATUS_RUNNING, storedPipeline.getStatus());
        assertStageQueries(1);
        assertNoCompletionWrite();
    }

    private BioPipelineStage stage(long stageId, Integer status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(stageId);
        stage.setPipelineId(PIPELINE_ID);
        stage.setStatus(status);
        return stage;
    }

    private BioPipelineStage typedStage(long stageId, int type, int status) {
        BioPipelineStage stage = stage(stageId, status);
        stage.setStageType(type);
        return stage;
    }

    private BioAnalysisPipeline freshPipelineSnapshot() {
        if (storedPipeline == null) {
            return null;
        }
        BioAnalysisPipeline snapshot = new BioAnalysisPipeline();
        snapshot.setPipelineId(storedPipeline.getPipelineId());
        snapshot.setPipelineType(storedPipeline.getPipelineType());
        snapshot.setStatus(storedPipeline.getStatus());
        return snapshot;
    }

    private int persistPipelinePatch(BioAnalysisPipeline patch) {
        storedPipeline.setStatus(patch.getStatus());
        return 1;
    }

    private void assertNoCompletionWrite() {
        verify(pipelineMapper, never()).updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class));
    }

    private void assertStageQueries(int count) {
        ArgumentCaptor<BioPipelineStageExample> captor = ArgumentCaptor.forClass(BioPipelineStageExample.class);
        verify(stageMapper, times(count)).selectByExample(captor.capture());
        for (BioPipelineStageExample query : captor.getAllValues()) {
            assertEquals(1, query.getOredCriteria().size());
            Map<String, Object> criteria = new LinkedHashMap<>();
            query.getOredCriteria().get(0).getAllCriteria()
                    .forEach(criterion -> criteria.put(criterion.getCondition(), criterion.getValue()));
            assertEquals(Map.of("pipeline_id =", PIPELINE_ID), criteria,
                    "Query every stage in this pipeline, without filtering out unfinished stages");
        }
    }

    private void assertCompletionWrites(int count) {
        ArgumentCaptor<BioAnalysisPipeline> patchCaptor = ArgumentCaptor.forClass(BioAnalysisPipeline.class);
        ArgumentCaptor<BioAnalysisPipelineExample> conditionCaptor = ArgumentCaptor
                .forClass(BioAnalysisPipelineExample.class);
        verify(pipelineMapper, times(count)).updateByExampleSelective(patchCaptor.capture(),
                conditionCaptor.capture());
        for (BioAnalysisPipeline patch : patchCaptor.getAllValues()) {
            assertEquals(PIPELINE_STATUS_COMPELETE, patch.getStatus());
        }
        for (BioAnalysisPipelineExample condition : conditionCaptor.getAllValues()) {
            assertEquals(1, condition.getOredCriteria().size());
            Map<String, Object> criteria = new LinkedHashMap<>();
            condition.getOredCriteria().get(0).getAllCriteria()
                    .forEach(criterion -> criteria.put(criterion.getCondition(), criterion.getValue()));
            assertEquals(Map.of("pipeline_id =", PIPELINE_ID, "status =", PIPELINE_STATUS_RUNNING), criteria);
        }
    }
}
