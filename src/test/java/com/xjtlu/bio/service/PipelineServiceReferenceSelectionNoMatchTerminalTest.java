package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_RUNNING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_AMR;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_CONSENSUS;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
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

import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapperExtension;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/** State-transition contracts only; mocked persistence and dispatch, no database or analysis execution. */
class PipelineServiceReferenceSelectionNoMatchTerminalTest {

    private static final long PIPELINE_ID = 10L;
    private static final long SELECTION_ID = PIPELINE_STAGE_REFERENCE_SELECTION;
    private static final int VERSION = 3;
    private static final String NO_MATCH_OUTPUT = """
            {"outcome":"NO_MATCH","selectedReference":null,"candidateScores":[],"readFallbackMetrics":null}
            """;
    private static final List<Integer> CONSUMER_TYPES = List.of(PIPELINE_STAGE_MAPPING,
            PIPELINE_STAGE_REFERENCE_COMPARISON, PIPELINE_STAGE_VARIANT_CALL,
            PIPELINE_STAGE_CONSENSUS, PIPELINE_STAGE_SNP_ANNOTATION);
    private static final List<Integer> DIRECT_CHILD_TYPES = List.of(PIPELINE_STAGE_MAPPING,
            PIPELINE_STAGE_REFERENCE_COMPARISON);

    private PipelineService service;
    private PipelineStagePoller poller;
    private PipelineStageTaskDispatcher dispatcher;
    private BioAnalysisPipelineMapper pipelineMapper;
    private BioAnalysisPipelineMapperExtension pipelineExtension;
    private BioAnalysisStageMapperExtension stageExtension;
    private BioPipelineStageMapper stageMapper;
    private BioAnalysisPipeline pipeline;
    private int batchResult;
    private Integer failOnStageType;
    private final List<List<Long>> commitAttempts = new ArrayList<>();
    private final Map<Long, BioPipelineStage> storedStages = new LinkedHashMap<>();

    @BeforeEach
    void setUp() {
        service = spy(new PipelineService());
        poller = new PipelineStagePoller();
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        pipelineExtension = mock(BioAnalysisPipelineMapperExtension.class);
        stageExtension = mock(BioAnalysisStageMapperExtension.class);
        stageMapper = mock(BioPipelineStageMapper.class);
        StageOrchestrator orchestrator = new StageOrchestrator();
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapperExtension", pipelineExtension);
        ReflectionTestUtils.setField(service, "bioAnalysisStageMapperExtension", stageExtension);
        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stageMapper);
        ReflectionTestUtils.setField(service, "stageOrchestrator", orchestrator);
        ReflectionTestUtils.setField(poller, "pipelineService", service);
        ReflectionTestUtils.setField(poller, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(poller, "stageOrchestrator", orchestrator);

        pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(PIPELINE_ID);
        pipeline.setPipelineType(PIPELINE_VIRUS);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_RUNNING);
        storeStage(PIPELINE_STAGE_REFERENCE_SELECTION, PIPELINE_STAGE_STATUS_FINISHED)
                .setOutputInline(NO_MATCH_OUTPUT);
        CONSUMER_TYPES.forEach(type -> storeStage(type, PIPELINE_STAGE_STATUS_PENDING));

        when(pipelineExtension.selectPipelineTypeByStageId(SELECTION_ID))
                .thenAnswer(invocation -> pipelineSnapshot());
        when(pipelineMapper.selectByPrimaryKey(PIPELINE_ID)).thenAnswer(invocation -> pipelineSnapshot());
        when(stageExtension.selectAllPipelineStagesByStageId(SELECTION_ID))
                .thenAnswer(invocation -> stageSnapshots(true));
        when(stageMapper.selectByExample(any(BioPipelineStageExample.class)))
                .thenAnswer(invocation -> stageSnapshots(false));
        doAnswer(invocation -> new Result<>(Result.SUCCESS,
                pipeline.getStatus() == PipelineService.PIPELINE_STATUS_RUNNING
                        ? List.of(pipelineSnapshot()) : List.of(), null))
                .when(service).queryPipelines(any(BioAnalysisPipelineExample.class));
        doAnswer(invocation -> stageSnapshots(true))
                .when(service).queryStages(any(BioPipelineStageExample.class));
        doAnswer(invocation -> persistStagePatches(invocation.getArgument(0)))
                .when(service).batchUpdateStages(anyList());
        when(pipelineMapper.updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class))).thenAnswer(invocation -> {
                    BioAnalysisPipeline patch = invocation.getArgument(0);
                    pipeline.setStatus(patch.getStatus());
                    return 1;
                });
    }

    @AfterEach
    void noMatchNeverDispatchesAnalysisOrRewritesSelection() {
        verifyNoInteractions(dispatcher);
        assertEquals(VERSION, storedStages.get(SELECTION_ID).getVersion());
    }

    @Test
    void callbackSkipsDirectChildrenAndPendingPollPropagatesToDescendants() throws Exception {
        service.pipelineStageDone(SELECTION_ID, true);

        assertDirectChildrenSkippedAndDescendantsPending();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();
        verify(service).batchUpdateStages(argThat(updates -> updates.size() == DIRECT_CHILD_TYPES.size()));
        verify(stageExtension, times(2)).selectAllPipelineStagesByStageId(SELECTION_ID);

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verify(service, times(3)).batchUpdateStages(argThat(updates -> updates.size() == 1));
        assertSingleCompletionWrite();
    }

    @Test
    void lostCallbackIsRecoveredByPendingPollAndSeparateCompletionPoll() throws Exception {
        poller.dispatchPendingStages();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();
        verify(service, times(CONSUMER_TYPES.size()))
                .batchUpdateStages(argThat(updates -> updates.size() == 1));

        poller.reconcileCompletedPipelines();

        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verify(stageMapper).selectByExample(any(BioPipelineStageExample.class));
        verify(stageMapper, never()).selectByExampleWithBLOBs(any(BioPipelineStageExample.class));
        assertSingleCompletionWrite();
    }

    @Test
    void reverseStageOrderConvergesAcrossPollsWithoutReadingSelectionFromDescendants() throws Exception {
        List<BioPipelineStage> reverseOrder = new ArrayList<>(storedStages.values());
        Collections.reverse(reverseOrder);
        storedStages.clear();
        reverseOrder.forEach(stage -> storedStages.put(stage.getStageId(), stage));

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertDirectChildrenSkippedAndDescendantsPending();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertStageSkipped(PIPELINE_STAGE_VARIANT_CALL);
        assertStageSkipped(PIPELINE_STAGE_CONSENSUS);
        assertStagePending(PIPELINE_STAGE_SNP_ANNOTATION);
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verify(service, times(CONSUMER_TYPES.size()))
                .batchUpdateStages(argThat(updates -> updates.size() == 1));
        assertSingleCompletionWrite();
    }

    @Test
    void reconciliationCompletesAlreadyPersistedNoMatchSkipsUsingOnlyStageStatuses() {
        CONSUMER_TYPES.forEach(type -> storedStages.get(type.longValue())
                .setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE));

        poller.reconcileCompletedPipelines();
        poller.reconcileCompletedPipelines();

        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        assertSelectionNoMatchUnchanged();
        verify(service, never()).batchUpdateStages(anyList());
        verify(stageMapper).selectByExample(any(BioPipelineStageExample.class));
        verify(stageMapper, never()).selectByExampleWithBLOBs(any(BioPipelineStageExample.class));
        assertSingleCompletionWrite();
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 100 })
    void failedSkipCommitDoesNotCompleteAndNextCallbackRetriesPersistedState(int failedResult) throws Exception {
        batchResult = failedResult;

        service.pipelineStageDone(SELECTION_ID, true);

        assertConsumersPending();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();

        batchResult = 0;
        service.pipelineStageDone(SELECTION_ID, true);

        assertDirectChildrenSkippedAndDescendantsPending();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();
        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verify(service, times(2)).batchUpdateStages(argThat(updates -> updates.size() == DIRECT_CHILD_TYPES.size()));
        assertSingleCompletionWrite();
    }

    @Test
    void failedPollCommitDoesNotCompleteAndNextPollRetriesPersistedPendingStages() throws Exception {
        batchResult = 100;

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersPending();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();
        assertEquals(List.of(List.of((long) PIPELINE_STAGE_MAPPING)), commitAttempts);

        batchResult = 0;
        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        assertSingleCompletionWrite();
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 100 })
    void failedDescendantCommitStopsCurrentPollAndResumesFromPersistedState(int failedResult) throws Exception {
        batchResult = failedResult;
        failOnStageType = PIPELINE_STAGE_VARIANT_CALL;

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertDirectChildrenSkippedAndDescendantsPending();
        assertEquals(List.of(List.of((long) PIPELINE_STAGE_MAPPING),
                List.of((long) PIPELINE_STAGE_REFERENCE_COMPARISON),
                List.of((long) PIPELINE_STAGE_VARIANT_CALL)), commitAttempts);
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();

        batchResult = 0;
        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        assertEquals(6, commitAttempts.size());
        assertEquals(2, commitAttempts.stream()
                .filter(ids -> ids.equals(List.of((long) PIPELINE_STAGE_VARIANT_CALL))).count());
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        assertSingleCompletionWrite();
    }

    @Test
    void noMatchSkipsWaitForAnIndependentBranchBeforeCompleting() throws Exception {
        BioPipelineStage independent = storeStage(PIPELINE_STAGE_AMR, PIPELINE_STAGE_STATUS_RUNNING);

        service.pipelineStageDone(SELECTION_ID, true);
        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersSkipped();
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        assertNoCompletionWrite();

        independent.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        poller.reconcileCompletedPipelines();

        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        assertSingleCompletionWrite();
    }

    @Test
    void repeatedCallbackAndPendingPollDoNotRewriteAlreadyPersistedSkips() throws Exception {
        storeStage(PIPELINE_STAGE_AMR, PIPELINE_STAGE_STATUS_RUNNING);
        service.pipelineStageDone(SELECTION_ID, true);
        poller.dispatchPendingStages();
        List<BioPipelineStage> firstSnapshot = stageSnapshots(true);

        service.pipelineStageDone(SELECTION_ID, true);
        poller.dispatchPendingStages();

        assertConsumersSkipped();
        assertSelectionNoMatchUnchanged();
        for (BioPipelineStage before : firstSnapshot) {
            BioPipelineStage after = storedStages.get(before.getStageId());
            assertEquals(before.getVersion(), after.getVersion());
            assertEquals(before.getEndTime(), after.getEndTime());
            assertEquals(before.getOutputInline(), after.getOutputInline());
        }
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verify(service).batchUpdateStages(argThat(updates -> updates.size() == DIRECT_CHILD_TYPES.size()));
        verify(service, times(3)).batchUpdateStages(argThat(updates -> updates.size() == 1));
        verify(service).batchUpdateStages(List.of());
        assertNoCompletionWrite();
    }

    @Test
    void trueExecutionFailureRemainsFailedAndDoesNotScheduleOrComplete() {
        BioPipelineStage selection = storedStages.get(SELECTION_ID);
        selection.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        selection.setOutputInline(null);

        service.pipelineStageDone(SELECTION_ID, false);
        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersPending();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, selection.getStatus());
        assertNull(selection.getOutputInline());
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verify(service, never()).batchUpdateStages(anyList());
        verifyNoInteractions(pipelineExtension, stageExtension);
        assertNoCompletionWrite();
    }

    @Test
    void explicitNoMatchPayloadCannotOverrideAFailedStageStatus() {
        storedStages.get(SELECTION_ID).setStatus(PIPELINE_STAGE_STATUS_FAIL);

        poller.dispatchPendingStages();
        poller.reconcileCompletedPipelines();

        assertConsumersPending();
        assertEquals(PIPELINE_STAGE_STATUS_FAIL, storedStages.get(SELECTION_ID).getStatus());
        assertEquals(NO_MATCH_OUTPUT, storedStages.get(SELECTION_ID).getOutputInline());
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verify(service, never()).batchUpdateStages(anyList());
        assertNoCompletionWrite();
    }

    private int persistStagePatches(List<UpdateStageCommand> commands) throws Exception {
        commitAttempts.add(commands.stream().map(UpdateStageCommand::getStageId).toList());
        int result = failOnStageType == null || commands.stream()
                .anyMatch(command -> storedStages.get(command.getStageId()).getStageType().equals(failOnStageType))
                ? batchResult : 0;
        for (UpdateStageCommand command : commands) {
            BioPipelineStage stored = storedStages.get(command.getStageId());
            BioPipelineStage patch = command.getUpdateStage();
            assertTrue(CONSUMER_TYPES.contains(stored.getStageType()));
            assertEquals(VERSION, command.getCurrentVersion());
            assertEquals(command.getCurrentVersion(), stored.getVersion());
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, stored.getStatus());
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, patch.getStatus());
            assertEquals(VERSION + 1, patch.getVersion());
            assertNotNull(patch.getEndTime());
            Map<String, Object> skip = JsonUtil.toMap(patch.getOutputInline());
            Object reason = skip.get("skipReason");
            assertTrue(reason instanceof String && !((String) reason).isBlank());
            assertSkipCause(stored.getStageType(), skip);
            assertNull(patch.getInputUrl());
            assertNull(patch.getParameters());
            if (result == 0) {
                stored.setStatus(patch.getStatus());
                stored.setVersion(patch.getVersion());
                stored.setEndTime(patch.getEndTime());
                stored.setOutputInline(patch.getOutputInline());
            }
        }
        return result;
    }

    private void assertSkipCause(int stageType, Map<String, Object> skip) {
        long causeStageId = ((Number) skip.get("causeStageId")).longValue();
        if (DIRECT_CHILD_TYPES.contains(stageType)) {
            assertEquals(SELECTION_ID, causeStageId);
            assertEquals("NO_MATCH", skip.get("causeOutcome"));
            assertNull(skip.get("causeStatus"));
        } else {
            List<Integer> expectedParents = stageType == PIPELINE_STAGE_CONSENSUS
                    ? List.of(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_MAPPING)
                    : List.of(stageType == PIPELINE_STAGE_VARIANT_CALL
                            ? PIPELINE_STAGE_MAPPING : PIPELINE_STAGE_VARIANT_CALL);
            assertTrue(expectedParents.contains((int) causeStageId));
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, storedStages.get(causeStageId).getStatus());
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, ((Number) skip.get("causeStatus")).intValue());
            assertEquals("Required upstream stage is NOT_APPLICABLE", skip.get("skipReason"));
            assertNull(skip.get("causeOutcome"));
        }
    }

    private void assertConsumersSkipped() throws Exception {
        for (Integer type : CONSUMER_TYPES) {
            assertStageSkipped(type);
        }
    }

    private void assertStageSkipped(int type) throws Exception {
        BioPipelineStage stage = storedStages.get((long) type);
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, stage.getStatus(), "stage type " + type);
        assertEquals(VERSION + 1, stage.getVersion());
        assertNotNull(stage.getEndTime());
        assertNotNull(stage.getOutputInline());
        assertNull(stage.getInputUrl());
        assertNull(stage.getParameters());
        assertNull(stage.getOutputUrl());
        assertSkipCause(type, JsonUtil.toMap(stage.getOutputInline()));
    }

    private void assertConsumersPending() {
        for (Integer type : CONSUMER_TYPES) {
            assertStagePending(type);
        }
    }

    private void assertStagePending(int type) {
        BioPipelineStage stage = storedStages.get((long) type);
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, stage.getStatus(), "stage type " + type);
        assertEquals(VERSION, stage.getVersion());
        assertNull(stage.getEndTime());
        assertNull(stage.getOutputInline());
    }

    private void assertDirectChildrenSkippedAndDescendantsPending() throws Exception {
        for (Integer type : CONSUMER_TYPES) {
            if (DIRECT_CHILD_TYPES.contains(type)) {
                assertStageSkipped(type);
            } else {
                assertStagePending(type);
            }
        }
    }

    private void assertSelectionNoMatchUnchanged() {
        BioPipelineStage selection = storedStages.get(SELECTION_ID);
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, selection.getStatus());
        assertEquals(NO_MATCH_OUTPUT, selection.getOutputInline());
        assertNull(selection.getOutputUrl());
    }

    private void assertNoCompletionWrite() {
        verify(pipelineMapper, never()).updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class));
    }

    private void assertSingleCompletionWrite() {
        verify(pipelineMapper).updateByExampleSelective(
                argThat(patch -> patch.getStatus() == PipelineService.PIPELINE_STATUS_COMPELETE),
                any(BioAnalysisPipelineExample.class));
    }

    private BioPipelineStage storeStage(int type, int status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId((long) type);
        stage.setPipelineId(PIPELINE_ID);
        stage.setStageType(type);
        stage.setStageIndex(-1);
        stage.setStatus(status);
        stage.setVersion(VERSION);
        storedStages.put(stage.getStageId(), stage);
        return stage;
    }

    private BioAnalysisPipeline pipelineSnapshot() {
        BioAnalysisPipeline snapshot = new BioAnalysisPipeline();
        BeanUtils.copyProperties(pipeline, snapshot);
        return snapshot;
    }

    private List<BioPipelineStage> stageSnapshots(boolean includeOutput) {
        return storedStages.values().stream().map(stored -> {
            BioPipelineStage snapshot = new BioPipelineStage();
            BeanUtils.copyProperties(stored, snapshot);
            if (!includeOutput) {
                snapshot.setOutputInline(null);
                snapshot.setOutputUrl(null);
            }
            return snapshot;
        }).toList();
    }
}
