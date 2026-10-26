package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_ACTION_REQUIRED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_RUNNING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_OUT_PANEL;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.beans.BeanUtils;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** Pending-plan dispatch contracts; no database, Spring context or analysis execution. */
class PipelineStagePollerPendingTest {

    private static final long PIPELINE_ID = 10L;

    private PipelineStagePoller poller;
    private PipelineService service;
    private PipelineStageTaskDispatcher dispatcher;
    private StageOrchestrator orchestrator;
    private BioPipelineStage pending;
    private List<BioPipelineStage> stages;

    @BeforeEach
    void setUp() {
        poller = new PipelineStagePoller();
        service = mock(PipelineService.class);
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        orchestrator = mock(StageOrchestrator.class);
        ReflectionTestUtils.setField(poller, "pipelineService", service);
        ReflectionTestUtils.setField(poller, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(poller, "stageOrchestrator", orchestrator);
        pending = stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_PENDING);
        stages = List.of(pending);
        when(service.queryPipelines(any(BioAnalysisPipelineExample.class)))
                .thenReturn(new Result<>(Result.SUCCESS, List.of(pipeline(PIPELINE_ID)), null));
        when(service.queryStages(any(BioPipelineStageExample.class))).thenAnswer(invocation -> stages);
        when(service.batchUpdateStages(anyList())).thenReturn(0);
        when(dispatcher.addTask(any(BioPipelineStage.class))).thenReturn(true);
    }

    @AfterEach
    void pendingDispatchUsesOnlyTheBatchCommitAndDoesNotPerformCompletionReconciliation() {
        verify(service, never()).updateStageFromVersion(any(UpdateStageCommand.class));
        verify(service, never()).reconcilePipelineCompletion(anyLong());
    }

    @Test
    void updateOnlyNotApplicablePlanIsPersistedWithoutTryingToReadAnEmptyRunList() throws Exception {
        OrchestratePlan plan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_NOT_APPLICABLE)), List.of());
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(plan);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verify(service).batchUpdateStages(same(plan.getUpdateStageCommands()));
        verifyNoInteractions(dispatcher);
    }

    @ParameterizedTest
    @MethodSource("emptyPlans")
    void nullOrEmptyUpdatePlanIsNotPersistedOrEnqueued(OrchestratePlan plan) throws Exception {
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(plan);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verify(service, never()).batchUpdateStages(anyList());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void normalRunPlanIsEnqueuedOnlyAfterItsBatchCommit() throws Exception {
        BioPipelineStage queued = stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        OrchestratePlan plan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_QUEUING)), List.of(queued));
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(plan);

        poller.dispatchPendingStages();

        InOrder order = inOrder(service, dispatcher);
        order.verify(service).batchUpdateStages(same(plan.getUpdateStageCommands()));
        order.verify(dispatcher).addTask(same(queued));
        verifyNoMoreInteractions(dispatcher);
    }

    @Test
    void commitsEveryCommandAndEnqueuesEveryRunnableStageNotJustTheFirst() throws Exception {
        BioPipelineStage second = stage(2L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_PENDING);
        BioPipelineStage skipped = stage(3L, PIPELINE_ID, PIPELINE_STAGE_SEROTYPE, PIPELINE_STAGE_STATUS_PENDING);
        BioPipelineStage firstRun = stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        BioPipelineStage secondRun = stage(2L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        OrchestratePlan plan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_QUEUING),
                update(second, PIPELINE_STAGE_STATUS_QUEUING), update(skipped, PIPELINE_STAGE_STATUS_NOT_APPLICABLE)),
                List.of(firstRun, secondRun));
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(plan);

        poller.dispatchPendingStages();

        InOrder order = inOrder(service, dispatcher);
        order.verify(service).batchUpdateStages(same(plan.getUpdateStageCommands()));
        order.verify(dispatcher).addTask(same(firstRun));
        order.verify(dispatcher).addTask(same(secondRun));
        verifyNoMoreInteractions(dispatcher);
        assertEquals(3, plan.getUpdateStageCommands().size());
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 100 })
    void failedOrConflictingBatchDoesNotEnqueueUncommittedRunStages(int batchResult) throws Exception {
        BioPipelineStage queued = stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        OrchestratePlan plan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_QUEUING)), List.of(queued));
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(plan);
        when(service.batchUpdateStages(anyList())).thenReturn(batchResult);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verify(service).batchUpdateStages(same(plan.getUpdateStageCommands()));
        verifyNoInteractions(dispatcher);
    }

    @Test
    void stateOnlySkipDoesNotPreventAnotherPendingStageFromBeingScheduled() throws Exception {
        BioPipelineStage next = stage(2L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_PENDING);
        stages = List.of(pending, next);
        BioPipelineStage queued = stage(2L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        OrchestratePlan skipPlan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_NOT_APPLICABLE)), List.of());
        OrchestratePlan runPlan = plan(List.of(update(next, PIPELINE_STAGE_STATUS_QUEUING)), List.of(queued));
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(skipPlan);
        when(orchestrator.makePlan(anyList(), eq(next.getStageId()))).thenReturn(runPlan);

        assertDoesNotThrow(poller::dispatchPendingStages);

        InOrder order = inOrder(service, dispatcher);
        order.verify(service).batchUpdateStages(same(skipPlan.getUpdateStageCommands()));
        order.verify(service).batchUpdateStages(same(runPlan.getUpdateStageCommands()));
        order.verify(dispatcher).addTask(same(queued));
        verifyNoMoreInteractions(dispatcher);
    }

    @Test
    void failedCommitDiscardsTheRemainingPipelineSnapshotButStillProcessesOtherPipelines() throws Exception {
        BioPipelineStage next = stage(2L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_PENDING);
        BioPipelineStage other = stage(3L, 20L, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_PENDING);
        stages = List.of(pending, next, other);
        when(service.queryPipelines(any(BioAnalysisPipelineExample.class)))
                .thenReturn(new Result<>(Result.SUCCESS, List.of(pipeline(PIPELINE_ID), pipeline(20L)), null));
        BioPipelineStage uncommitted = stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        BioPipelineStage otherRun = stage(3L, 20L, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        OrchestratePlan failedPlan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_QUEUING)), List.of(uncommitted));
        OrchestratePlan otherPlan = plan(List.of(update(other, PIPELINE_STAGE_STATUS_QUEUING)), List.of(otherRun));
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenAnswer(invocation -> {
            pending.setStatus(PIPELINE_STAGE_STATUS_QUEUING); // The real planner mutates its local snapshot.
            return failedPlan;
        });
        when(orchestrator.makePlan(anyList(), eq(other.getStageId()))).thenReturn(otherPlan);
        when(service.batchUpdateStages(same(failedPlan.getUpdateStageCommands()))).thenReturn(100);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verify(service).batchUpdateStages(same(failedPlan.getUpdateStageCommands()));
        verify(service).batchUpdateStages(same(otherPlan.getUpdateStageCommands()));
        verify(orchestrator, never()).makePlan(anyList(), eq(next.getStageId()));
        verify(dispatcher).addTask(same(otherRun));
        verify(dispatcher, never()).addTask(same(uncommitted));
        verifyNoMoreInteractions(dispatcher);
    }

    @Test
    void fullDispatcherDoesNotRollbackOrImmediatelyRetryTheCommittedQueuingStage() throws Exception {
        BioPipelineStage queued = stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING);
        OrchestratePlan plan = plan(List.of(update(pending, PIPELINE_STAGE_STATUS_QUEUING)), List.of(queued));
        when(orchestrator.makePlan(anyList(), eq(pending.getStageId()))).thenReturn(plan);
        when(dispatcher.addTask(same(queued))).thenReturn(false);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verify(service).batchUpdateStages(same(plan.getUpdateStageCommands()));
        verify(orchestrator).makePlan(anyList(), eq(pending.getStageId()));
        verify(dispatcher).addTask(same(queued));
        verifyNoMoreInteractions(dispatcher);
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, plan.getUpdateStageCommands().get(0).getUpdateStage().getStatus());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = { PIPELINE_STAGE_STATUS_FAIL, PIPELINE_STAGE_STATUS_QUEUING, PIPELINE_STAGE_STATUS_RUNNING,
            PIPELINE_STAGE_STATUS_FINISHED, PIPELINE_STAGE_STATUS_NOT_APPLICABLE, PIPELINE_STAGE_STATUS_ACTION_REQUIRED })
    void nonPendingStagesIncludingFailedStagesAreNotAutomaticallyRetried(Integer status) {
        pending.setStatus(status);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verifyNoInteractions(orchestrator, dispatcher);
        verify(service, never()).batchUpdateStages(anyList());
    }

    @ParameterizedTest
    @MethodSource("unavailableRunningPipelineResults")
    void missingFailedOrEmptyRunningPipelineQueryStopsBeforeReadingStages(Result<List<BioAnalysisPipeline>> result) {
        when(service.queryPipelines(any(BioAnalysisPipelineExample.class))).thenReturn(result);

        assertDoesNotThrow(poller::dispatchPendingStages);

        verify(service, never()).queryStages(any(BioPipelineStageExample.class));
        verify(service, never()).batchUpdateStages(anyList());
        verifyNoInteractions(orchestrator, dispatcher);
    }

    @ParameterizedTest
    @MethodSource("unavailableStageLists")
    void missingOrEmptyStageQueryDoesNotAttemptAPlan(List<BioPipelineStage> result) {
        stages = result;

        assertDoesNotThrow(poller::dispatchPendingStages);

        verifyNoInteractions(orchestrator, dispatcher);
        verify(service, never()).batchUpdateStages(anyList());
    }

    @Test
    void realSerotypePlannerUnsupportedSpeciesSkipIsPersistedWithoutAnalysis() throws Exception {
        ReflectionTestUtils.setField(poller, "stageOrchestrator", new StageOrchestrator());
        BioPipelineStage assembly = stage(1L, PIPELINE_ID, PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage taxonomy = stage(2L, PIPELINE_ID, PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage serotype = stage(3L, PIPELINE_ID, PIPELINE_STAGE_SEROTYPE, PIPELINE_STAGE_STATUS_PENDING);
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_CONFIDENT);
        TaxonomyResult.Taxon best = new TaxonomyResult.Taxon();
        best.setTaxid(999);
        best.setName("Unsupported fixture species");
        result.setBest(best);
        taxonomy.setOutputInline(JsonUtil.toJson(result));
        stages = List.of(assembly, taxonomy, serotype);

        assertDoesNotThrow(poller::dispatchPendingStages);

        assertSinglePersistedSkip(serotype);
        verifyNoInteractions(dispatcher);
    }

    @Test
    void realOutPanelPlannerPersistsSerotypeSkipOnceAcrossRepeatedPolls() throws Exception {
        ReflectionTestUtils.setField(poller, "stageOrchestrator", new StageOrchestrator());
        BioPipelineStage assembly = stage(1L, PIPELINE_ID, PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage taxonomy = stage(2L, PIPELINE_ID, PIPELINE_STAGE_TAXONOMY,
                PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage serotype = stage(3L, PIPELINE_ID, PIPELINE_STAGE_SEROTYPE, PIPELINE_STAGE_STATUS_PENDING);
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_OUT_PANEL);
        taxonomy.setOutputInline(JsonUtil.toJson(result));
        String taxonomyOutput = taxonomy.getOutputInline();
        stages = List.of(assembly, taxonomy, serotype);
        when(service.queryStages(any(BioPipelineStageExample.class))).thenAnswer(invocation ->
                stages.stream().map(stage -> {
                    BioPipelineStage snapshot = new BioPipelineStage();
                    BeanUtils.copyProperties(stage, snapshot);
                    return snapshot;
                }).toList());
        when(service.batchUpdateStages(anyList())).thenAnswer(invocation -> {
            List<UpdateStageCommand> commands = invocation.getArgument(0);
            assertEquals(1, commands.size());
            UpdateStageCommand command = commands.get(0);
            assertEquals(serotype.getStageId().longValue(), command.getStageId());
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, serotype.getStatus());
            assertEquals(command.getCurrentVersion(), serotype.getVersion());
            BioPipelineStage patch = command.getUpdateStage();
            serotype.setStatus(patch.getStatus());
            serotype.setVersion(patch.getVersion());
            serotype.setEndTime(patch.getEndTime());
            serotype.setOutputInline(patch.getOutputInline());
            return 0;
        });

        assertDoesNotThrow(poller::dispatchPendingStages);
        Date endTime = serotype.getEndTime();
        String skipOutput = serotype.getOutputInline();
        assertDoesNotThrow(poller::dispatchPendingStages);

        UpdateStageCommand command = assertSinglePersistedSkip(serotype);
        assertEquals(4, command.getUpdateStage().getVersion());
        assertNotNull(command.getUpdateStage().getEndTime());
        assertEquals(Map.of("skipReason", "Taxonomy is OUT_PANEL; serotyping is not applicable",
                "causeStageId", 2, "causeStatus", PIPELINE_STAGE_STATUS_NOT_APPLICABLE),
                JsonUtil.toMap(command.getUpdateStage().getOutputInline()));
        assertNull(command.getUpdateStage().getInputUrl());
        assertNull(command.getUpdateStage().getParameters());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, serotype.getStatus());
        assertEquals(4, serotype.getVersion());
        assertNotNull(endTime);
        assertEquals(endTime, serotype.getEndTime());
        assertEquals(skipOutput, serotype.getOutputInline());
        assertEquals(command.getUpdateStage().getOutputInline(), serotype.getOutputInline());
        assertEquals(3L, serotype.getStageId());
        assertEquals(PIPELINE_ID, serotype.getPipelineId());
        assertEquals(PIPELINE_STAGE_SEROTYPE, serotype.getStageType());
        assertEquals(-1, serotype.getStageIndex());
        assertNull(serotype.getStartTime());
        assertNull(serotype.getInputUrl());
        assertNull(serotype.getParameters());
        assertNull(serotype.getOutputUrl());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, taxonomy.getStatus());
        assertEquals(3, taxonomy.getVersion());
        assertEquals(taxonomyOutput, taxonomy.getOutputInline());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void realSnpPlannerMissingReferenceAnnotationSkipIsPersistedWithoutAnalysis() throws Exception {
        ReflectionTestUtils.setField(poller, "stageOrchestrator", new StageOrchestrator());
        BioPipelineStage selection = stage(1L, PIPELINE_ID, PIPELINE_STAGE_REFERENCE_SELECTION,
                PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage variants = stage(2L, PIPELINE_ID, PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage annotation = stage(3L, PIPELINE_ID, PIPELINE_STAGE_SNP_ANNOTATION,
                PIPELINE_STAGE_STATUS_PENDING);
        ReferenceSequence reference = new ReferenceSequence();
        reference.setAccession("fixture-reference");
        reference.setTaxId(1);
        reference.setPath("references/fixture.fasta");
        reference.setAnnotationFile(null);
        selection.setOutputInline(JsonUtil.toJson(new ReferenceSelectionStageResult(
                ReferenceGenome.fromSingleSequence(reference), List.of())));
        stages = List.of(selection, variants, annotation);

        assertDoesNotThrow(poller::dispatchPendingStages);

        assertSinglePersistedSkip(annotation);
        verifyNoInteractions(dispatcher);
    }

    private static Stream<OrchestratePlan> emptyPlans() {
        OrchestratePlan runWithoutUpdate = new OrchestratePlan();
        runWithoutUpdate.getRunStages()
                .add(stage(1L, PIPELINE_ID, PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_STATUS_QUEUING));
        return Stream.of(null, new OrchestratePlan(), runWithoutUpdate);
    }

    private static Stream<Arguments> unavailableRunningPipelineResults() {
        return Stream.of(Arguments.of((Object) null),
                Arguments.of(new Result<List<BioAnalysisPipeline>>(Result.INTERNAL_FAIL,
                        List.of(pipeline(PIPELINE_ID)), "query failed")),
                Arguments.of(new Result<List<BioAnalysisPipeline>>(Result.SUCCESS, null, null)),
                Arguments.of(new Result<List<BioAnalysisPipeline>>(Result.SUCCESS, List.of(), null)));
    }

    private static Stream<Arguments> unavailableStageLists() {
        return Stream.of(Arguments.of((Object) null), Arguments.of(List.of()));
    }

    private static BioAnalysisPipeline pipeline(long pipelineId) {
        BioAnalysisPipeline pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(pipelineId);
        pipeline.setPipelineType(PIPELINE_REGULAR_BACTERIA);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_RUNNING);
        return pipeline;
    }

    private static BioPipelineStage stage(long stageId, long pipelineId, int type, Integer status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(stageId);
        stage.setPipelineId(pipelineId);
        stage.setStageType(type);
        stage.setStageIndex(-1);
        stage.setVersion(3);
        stage.setStatus(status);
        return stage;
    }

    private static UpdateStageCommand update(BioPipelineStage stage, int status) {
        BioPipelineStage patch = new BioPipelineStage();
        patch.setStatus(status);
        patch.setVersion(stage.getVersion() + 1);
        return new UpdateStageCommand(patch, stage.getStageId(), stage.getVersion());
    }

    private static OrchestratePlan plan(List<UpdateStageCommand> commands, List<BioPipelineStage> runStages) {
        OrchestratePlan plan = new OrchestratePlan();
        plan.getUpdateStageCommands().addAll(commands);
        plan.getRunStages().addAll(runStages);
        return plan;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private UpdateStageCommand assertSinglePersistedSkip(BioPipelineStage stage) {
        ArgumentCaptor<List<UpdateStageCommand>> captor = ArgumentCaptor.forClass((Class) List.class);
        verify(service).batchUpdateStages(captor.capture());
        assertEquals(1, captor.getValue().size());
        UpdateStageCommand command = captor.getValue().get(0);
        assertEquals(stage.getStageId().longValue(), command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, command.getUpdateStage().getStatus());
        return command;
    }
}
