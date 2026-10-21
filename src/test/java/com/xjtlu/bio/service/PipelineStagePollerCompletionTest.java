package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;

class PipelineStagePollerCompletionTest {

    private PipelineStagePoller poller;
    private PipelineService pipelineService;
    private PipelineStageTaskDispatcher dispatcher;
    private StageOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        poller = new PipelineStagePoller();
        pipelineService = mock(PipelineService.class);
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        orchestrator = mock(StageOrchestrator.class);
        ReflectionTestUtils.setField(poller, "pipelineService", pipelineService);
        ReflectionTestUtils.setField(poller, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(poller, "stageOrchestrator", orchestrator);
    }

    @Test
    void queriesOnlyRunningCandidatesAndChecksEveryPipelineWithoutDispatchingStages() {
        when(pipelineService.queryPipelines(any(BioAnalysisPipelineExample.class)))
                .thenReturn(successfulCandidates());

        poller.reconcileCompletedPipelines();

        ArgumentCaptor<BioAnalysisPipelineExample> query = ArgumentCaptor.forClass(
                BioAnalysisPipelineExample.class);
        verify(pipelineService).queryPipelines(query.capture());
        assertEquals(1, query.getValue().getOredCriteria().size());
        var criteria = query.getValue().getOredCriteria().get(0).getAllCriteria();
        assertEquals(1, criteria.size());
        assertEquals("status =", criteria.get(0).getCondition());
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, criteria.get(0).getValue());
        verify(pipelineService).reconcilePipelineCompletion(10L);
        verify(pipelineService).reconcilePipelineCompletion(20L);
        verifyNoMoreInteractions(pipelineService);
        verifyNoInteractions(dispatcher, orchestrator);
    }

    @Test
    void onePipelineFailureDoesNotSkipOtherCandidatesAndIsRetriedInTheNextRound() {
        when(pipelineService.queryPipelines(any(BioAnalysisPipelineExample.class)))
                .thenReturn(successfulCandidates());
        doThrow(new IllegalStateException("temporary completion failure"))
                .doNothing().when(pipelineService).reconcilePipelineCompletion(10L);

        assertDoesNotThrow(() -> poller.reconcileCompletedPipelines());
        assertDoesNotThrow(() -> poller.reconcileCompletedPipelines());

        InOrder calls = inOrder(pipelineService);
        calls.verify(pipelineService).queryPipelines(any(BioAnalysisPipelineExample.class));
        calls.verify(pipelineService).reconcilePipelineCompletion(10L);
        calls.verify(pipelineService).reconcilePipelineCompletion(20L);
        calls.verify(pipelineService).queryPipelines(any(BioAnalysisPipelineExample.class));
        calls.verify(pipelineService).reconcilePipelineCompletion(10L);
        calls.verify(pipelineService).reconcilePipelineCompletion(20L);
        calls.verifyNoMoreInteractions();
        verify(pipelineService, times(2)).reconcilePipelineCompletion(10L);
        verifyNoInteractions(dispatcher, orchestrator);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unavailableCandidates")
    void unsuccessfulOrEmptyCandidateQueriesDoNotAttemptReconciliation(
            String scenario, Result<List<BioAnalysisPipeline>> queryResult) {
        when(pipelineService.queryPipelines(any(BioAnalysisPipelineExample.class)))
                .thenReturn(queryResult);

        assertDoesNotThrow(() -> poller.reconcileCompletedPipelines());

        verify(pipelineService).queryPipelines(any(BioAnalysisPipelineExample.class));
        verifyNoMoreInteractions(pipelineService);
        verifyNoInteractions(dispatcher, orchestrator);
    }

    @Test
    void completionReconciliationHasItsOwnPeriodicSchedule() throws NoSuchMethodException {
        Scheduled reconciliation = PipelineStagePoller.class
                .getMethod("reconcileCompletedPipelines").getAnnotation(Scheduled.class);

        assertNotNull(reconciliation);
        assertEquals(12000L, reconciliation.fixedDelay());
        assertEquals(12000L, reconciliation.initialDelay());
        assertNotNull(PipelineStagePoller.class.getMethod("dispatchPendingStages")
                .getAnnotation(Scheduled.class));
    }

    private static Stream<Arguments> unavailableCandidates() {
        return Stream.of(
                Arguments.of("failed query even with candidate data",
                        new Result<>(Result.INTERNAL_FAIL, List.of(pipeline(10L)), "query failed")),
                Arguments.of("null result", null),
                Arguments.of("successful query with null data",
                        new Result<List<BioAnalysisPipeline>>(Result.SUCCESS, null, null)),
                Arguments.of("successful query with empty data",
                        new Result<List<BioAnalysisPipeline>>(Result.SUCCESS, List.of(), null)));
    }

    private static Result<List<BioAnalysisPipeline>> successfulCandidates() {
        return new Result<>(Result.SUCCESS, List.of(pipeline(10L), pipeline(20L)), null);
    }

    private static BioAnalysisPipeline pipeline(long pipelineId) {
        BioAnalysisPipeline pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(pipelineId);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_RUNNING);
        return pipeline;
    }
}
