package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class StageOrchestratorReferenceSelectionNoMatchTest {

    private static final int INITIAL_VERSION = 3;
    private static final List<Integer> DEPENDENT_TYPES = List.of(
            PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_CONSENSUS,
            PIPELINE_STAGE_SNP_ANNOTATION, PIPELINE_STAGE_REFERENCE_COMPARISON);
    private static final List<Integer> DIRECT_TYPES = List.of(
            PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_REFERENCE_COMPARISON);
    private static final String NO_MATCH_RESULT = """
            {"outcome":"NO_MATCH","selectedReference":null,"candidateScores":[],
             "readFallbackMetrics":{"status":"COMPLETED","evaluatedCandidates":2,"acceptedGroups":0}}
            """;

    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @Test
    void callbackSkipsOnlyPendingDirectChildrenWithoutPreparingInputsOrTouchingOtherBranches() throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        for (int stageType : List.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_TAXONOMY,
                PIPELINE_STAGE_AMR, PIPELINE_STAGE_MLST, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_SEROTYPE)) {
            stages.add(stage(stageType, PIPELINE_STAGE_STATUS_PENDING));
        }
        BioPipelineStage standaloneComparison = stage(PIPELINE_STAGE_REFERENCE_COMPARISON,
                PIPELINE_STAGE_STATUS_PENDING);
        standaloneComparison.setStageId(4300L);
        standaloneComparison.setStageIndex(0);
        stages.add(standaloneComparison);

        OrchestratePlan plan = orchestrator.makeDownstreamPlan(selection, stages, PIPELINE_VIRUS);

        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(DIRECT_TYPES.size(), plan.getUpdateStageCommands().size());
        for (UpdateStageCommand command : plan.getUpdateStageCommands()) {
            BioPipelineStage skipped = stages.stream()
                    .filter(stage -> stage.getStageId() == command.getStageId()).findFirst().orElseThrow();
            assertTrue(DIRECT_TYPES.contains(skipped.getStageType()));
            assertNoMatchSkip(command, skipped, selection);
        }
        for (int stageType : List.of(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_CONSENSUS,
                PIPELINE_STAGE_SNP_ANNOTATION)) {
            assertUntouched(findType(stages, stageType));
        }
        for (BioPipelineStage untouched : stages.subList(DEPENDENT_TYPES.size() + 1, stages.size())) {
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, untouched.getStatus());
            assertEquals(INITIAL_VERSION, untouched.getVersion());
            assertNull(untouched.getEndTime());
            assertNull(untouched.getOutputInline());
        }
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, selection.getStatus());
        assertEquals(INITIAL_VERSION, selection.getVersion());
        assertEquals(NO_MATCH_RESULT, selection.getOutputInline());
    }

    @ParameterizedTest
    @MethodSource("directTypes")
    void pollerRecoversDirectChildrenFromNoMatchSelection(int stageType) throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        BioPipelineStage target = findType(stages, stageType);

        OrchestratePlan plan = orchestrator.makePlan(stages, target.getStageId());

        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertNoMatchSkip(plan.getUpdateStageCommands().get(0), target, selection);
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_CONSENSUS,
            PIPELINE_STAGE_SNP_ANNOTATION})
    void deeperDescendantsWaitForTheirOwnPendingParentsInsteadOfReadingNoMatchRoot(int stageType) {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        BioPipelineStage target = findType(stages, stageType);

        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(stages, target.getStageId()));
        assertUntouched(target);
    }

    @Test
    void pollingPropagatesSkipAlongDirectDependenciesAndRecordsImmediateCause() throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        for (int stageType : DEPENDENT_TYPES) {
            BioPipelineStage target = findType(stages, stageType);
            OrchestratePlan plan = orchestrator.makePlan(stages, target.getStageId());
            assertTrue(plan.getRunStages().isEmpty());
            assertEquals(1, plan.getUpdateStageCommands().size());
            if (DIRECT_TYPES.contains(stageType)) {
                assertNoMatchSkip(plan.getUpdateStageCommands().get(0), target, selection);
            } else {
                Map<String, Object> reason = JsonUtil.toMap(target.getOutputInline());
                long causeId = ((Number) reason.get("causeStageId")).longValue();
                List<Integer> requiredTypes = switch (stageType) {
                    case PIPELINE_STAGE_VARIANT_CALL -> List.of(PIPELINE_STAGE_MAPPING);
                    case PIPELINE_STAGE_CONSENSUS -> List.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_VARIANT_CALL);
                    case PIPELINE_STAGE_SNP_ANNOTATION -> List.of(PIPELINE_STAGE_VARIANT_CALL);
                    default -> throw new AssertionError(stageType);
                };
                assertTrue(requiredTypes.stream().anyMatch(type -> type.longValue() == causeId));
                assertUpstreamSkip(plan.getUpdateStageCommands().get(0), target,
                        findType(stages, (int) causeId));
            }
        }
        for (BioPipelineStage dependent : stages.subList(1, stages.size())) {
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, dependent.getStatus());
        }
    }

    @Test
    void reversePollingOrderEventuallySkipsWholeChainWithoutPrematurelySkippingGrandchildren() throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        List<Integer> reverseOrder = List.of(PIPELINE_STAGE_SNP_ANNOTATION, PIPELINE_STAGE_CONSENSUS,
                PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_REFERENCE_COMPARISON);

        for (int round = 0; round < 3; round++) {
            for (int stageType : reverseOrder) {
                BioPipelineStage target = findType(stages, stageType);
                try {
                    OrchestratePlan plan = orchestrator.makePlan(stages, target.getStageId());
                    assertTrue(plan.getRunStages().isEmpty());
                } catch (MissingUpstreamException waiting) {
                    assertUntouched(target);
                }
            }
            if (round == 0) {
                assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE,
                        findType(stages, PIPELINE_STAGE_MAPPING).getStatus());
                assertUntouched(findType(stages, PIPELINE_STAGE_VARIANT_CALL));
                assertUntouched(findType(stages, PIPELINE_STAGE_CONSENSUS));
                assertUntouched(findType(stages, PIPELINE_STAGE_SNP_ANNOTATION));
            }
        }
        for (BioPipelineStage dependent : stages.subList(1, stages.size())) {
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, dependent.getStatus());
            assertEquals(INITIAL_VERSION + 1, dependent.getVersion());
        }
    }

    @Test
    void repeatedCallbacksAndPollingAreIdempotent() throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        orchestrator.planDownstreamReferenceSelection(stages, selection);
        for (BioPipelineStage target : stages.subList(1, stages.size())) {
            orchestrator.makePlan(stages, target.getStageId());
        }
        List<Date> endTimes = stages.stream().map(BioPipelineStage::getEndTime).toList();
        List<String> outputs = stages.stream().map(BioPipelineStage::getOutputInline).toList();

        assertNoWork(orchestrator.planDownstreamReferenceSelection(stages, selection));
        for (BioPipelineStage target : stages.subList(1, stages.size())) {
            assertNoWork(orchestrator.makePlan(stages, target.getStageId()));
            assertEquals(INITIAL_VERSION + 1, target.getVersion());
        }
        assertEquals(endTimes, stages.stream().map(BioPipelineStage::getEndTime).toList());
        assertEquals(outputs, stages.stream().map(BioPipelineStage::getOutputInline).toList());
    }

    @ParameterizedTest
    @MethodSource("nonPendingDescendants")
    void callbackAndPollerNeverSkipNonPendingDescendantsEvenWhenTheirRequiredParentIsUnavailable(
            int stageType, int status) throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        for (BioPipelineStage stage : stages.subList(1, stages.size())) {
            stage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        }
        if (stageType == PIPELINE_STAGE_VARIANT_CALL || stageType == PIPELINE_STAGE_CONSENSUS) {
            findType(stages, PIPELINE_STAGE_MAPPING).setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        } else if (stageType == PIPELINE_STAGE_SNP_ANNOTATION) {
            findType(stages, PIPELINE_STAGE_VARIANT_CALL).setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        }
        BioPipelineStage target = findType(stages, stageType);
        target.setStatus(status);
        Date endTime = new Date(1234567L);
        target.setEndTime(endTime);
        target.setOutputInline("existing output");

        assertNoWork(orchestrator.planDownstreamReferenceSelection(stages, selection));
        assertNoWork(orchestrator.makePlan(stages, target.getStageId()));

        assertEquals(status, target.getStatus());
        assertEquals(INITIAL_VERSION, target.getVersion());
        assertSame(endTime, target.getEndTime());
        assertEquals("existing output", target.getOutputInline());
    }

    @Test
    void standaloneComparisonDoesNotInheritNoMatchSelectionDependency() throws Exception {
        BioPipelineStage selection = selection();
        BioPipelineStage comparison = stage(PIPELINE_STAGE_REFERENCE_COMPARISON, PIPELINE_STAGE_STATUS_PENDING);
        comparison.setStageIndex(0);
        List<BioPipelineStage> stages = List.of(selection, comparison);

        OrchestratePlan downstream = orchestrator.planDownstreamReferenceSelection(stages, selection);
        assertNoWork(downstream);
        assertTrue(downstream.isNoNextStage());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, comparison.getStatus());

        OrchestratePlan entryPlan = orchestrator.makePlan(stages, comparison.getStageId());
        assertEquals(List.of(comparison), entryPlan.getRunStages());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, comparison.getStatus());
        assertNull(comparison.getOutputInline());
    }

    @Test
    void independentMappingEntryProtectsItsDescendantsFromUnrelatedSelection() throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        stages.remove(findType(stages, PIPELINE_STAGE_REFERENCE_COMPARISON));
        BioPipelineStage mapping = findType(stages, PIPELINE_STAGE_MAPPING);
        mapping.setStageIndex(0);
        mapping.setStatus(PIPELINE_STAGE_STATUS_RUNNING);

        OrchestratePlan callback = orchestrator.planDownstreamReferenceSelection(stages, selection);
        assertNoWork(callback);
        assertTrue(callback.isNoNextStage());

        for (int stageType : List.of(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_CONSENSUS,
                PIPELINE_STAGE_SNP_ANNOTATION)) {
            BioPipelineStage target = findType(stages, stageType);
            assertThrows(MissingUpstreamException.class,
                    () -> orchestrator.makePlan(stages, target.getStageId()));
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, target.getStatus());
            assertEquals(INITIAL_VERSION, target.getVersion());
            assertNull(target.getEndTime());
            assertNull(target.getOutputInline());
        }
        assertEquals(PIPELINE_STAGE_STATUS_RUNNING, mapping.getStatus());
        assertEquals(INITIAL_VERSION, mapping.getVersion());
    }

    @Test
    void consensusPropagatesSkippedMappingEvenWhenItsOtherParentIsAnIndependentEntry() throws Exception {
        BioPipelineStage selection = selection();
        List<BioPipelineStage> stages = descendants(selection);
        BioPipelineStage variant = findType(stages, PIPELINE_STAGE_VARIANT_CALL);
        variant.setStageIndex(0);
        BioPipelineStage snpAnnotation = findType(stages, PIPELINE_STAGE_SNP_ANNOTATION);

        OrchestratePlan callback = orchestrator.planDownstreamReferenceSelection(stages, selection);

        assertTrue(callback.getRunStages().isEmpty());
        assertEquals(2, callback.getUpdateStageCommands().size());
        for (int stageType : DIRECT_TYPES) {
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, findType(stages, stageType).getStatus());
        }
        BioPipelineStage consensus = findType(stages, PIPELINE_STAGE_CONSENSUS);
        assertUntouched(consensus);
        OrchestratePlan consensusPlan = orchestrator.makePlan(stages, consensus.getStageId());
        assertUpstreamSkip(consensusPlan.getUpdateStageCommands().get(0), consensus,
                findType(stages, PIPELINE_STAGE_MAPPING));
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, variant.getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, snpAnnotation.getStatus());
        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(stages, snpAnnotation.getStageId()));
        assertEquals(INITIAL_VERSION, snpAnnotation.getVersion());
        assertNull(snpAnnotation.getOutputInline());
    }

    @Test
    void selectionWithoutDependentStagesReturnsTerminalEmptyPlan() throws Exception {
        BioPipelineStage selection = selection();

        OrchestratePlan plan = orchestrator.planDownstreamReferenceSelection(List.of(selection), selection);

        assertNoWork(plan);
        assertTrue(plan.isNoNextStage());
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_STATUS_PENDING, PIPELINE_STAGE_STATUS_QUEUING,
            PIPELINE_STAGE_STATUS_RUNNING, PIPELINE_STAGE_STATUS_FAIL, PIPELINE_STAGE_STATUS_ACTION_REQUIRED})
    void noMatchOutputDoesNotSkipUntilSelectionHasFinished(int selectionStatus) {
        BioPipelineStage selection = selection();
        selection.setStatus(selectionStatus);
        BioPipelineStage mapping = stage(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_STATUS_PENDING);
        List<BioPipelineStage> stages = List.of(selection, mapping);

        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.planDownstreamReferenceSelection(stages, selection));
        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(stages, mapping.getStageId()));
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, mapping.getStatus());
        assertEquals(INITIAL_VERSION, mapping.getVersion());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "null", "{}", "{\"selectedReference\":null}",
            "{\"outcome\":\"MATCHED\",\"selectedReference\":null}",
            "{\"outcome\":\"PARTIAL\",\"selectedReference\":null}"})
    void missingResultOrReferenceWithoutExplicitNoMatchRemainsAnError(String result) {
        BioPipelineStage selection = selection();
        selection.setOutputInline(result);
        BioPipelineStage mapping = stage(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_STATUS_PENDING);
        List<BioPipelineStage> stages = List.of(selection, mapping);

        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.planDownstreamReferenceSelection(stages, selection));
        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(stages, mapping.getStageId()));
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, mapping.getStatus());
        assertEquals(INITIAL_VERSION, mapping.getVersion());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{malformed", "{\"outcome\":\"UNKNOWN\",\"selectedReference\":null}"})
    void malformedResultCannotBecomeNoMatchOrFallBackToLegacyOutput(String result) {
        BioPipelineStage selection = selection();
        selection.setOutputInline(result);
        selection.setOutputUrl(NO_MATCH_RESULT);
        BioPipelineStage mapping = stage(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_STATUS_PENDING);
        List<BioPipelineStage> stages = List.of(selection, mapping);

        assertThrows(JsonProcessingException.class,
                () -> orchestrator.planDownstreamReferenceSelection(stages, selection));
        assertThrows(JsonProcessingException.class,
                () -> orchestrator.makePlan(stages, mapping.getStageId()));
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, mapping.getStatus());
    }

    @ParameterizedTest
    @MethodSource("directTypes")
    void inconsistentNoMatchWithSelectedReferenceCannotSkipDirectChild(int stageType) {
        BioPipelineStage selection = selection();
        selection.setOutputInline("{\"outcome\":\"NO_MATCH\",\"selectedReference\":{}}");
        selection.setOutputUrl(NO_MATCH_RESULT);
        List<BioPipelineStage> stages = descendants(selection);
        BioPipelineStage target = findType(stages, stageType);

        MissingUpstreamException callbackError = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.planDownstreamReferenceSelection(stages, selection));
        MissingUpstreamException pollerError = assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(stages, target.getStageId()));

        assertEquals("NO_MATCH reference selection result contains a selected reference", callbackError.getDesc());
        assertEquals(callbackError.getDesc(), pollerError.getDesc());
        for (BioPipelineStage dependent : stages.subList(1, stages.size())) {
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, dependent.getStatus());
            assertEquals(INITIAL_VERSION, dependent.getVersion());
            assertNull(dependent.getOutputInline());
        }
    }

    @ParameterizedTest
    @MethodSource("requiredEdges")
    void requiredDirectUpstreamNotApplicableSkipsCurrentStageWithoutPreparingItsInputs(
            int upstreamType, int downstreamType) throws Exception {
        BioPipelineStage upstream = stage(upstreamType, PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage target = stage(downstreamType, PIPELINE_STAGE_STATUS_PENDING);

        OrchestratePlan plan = orchestrator.makePlan(List.of(upstream, target), target.getStageId());

        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertUpstreamSkip(plan.getUpdateStageCommands().get(0), target, upstream);
        assertEquals(INITIAL_VERSION, upstream.getVersion());
    }

    @ParameterizedTest
    @MethodSource("deepChildrenAndUnavailableRoots")
    void skippedDirectParentIsSufficientEvenIfSelectionIsMissingOrMalformed(
            int parentType, int childType, String rootResult) throws Exception {
        BioPipelineStage parent = stage(parentType, PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        parent.setOutputInline("malformed parent output must not need parsing");
        BioPipelineStage target = stage(childType, PIPELINE_STAGE_STATUS_PENDING);
        List<BioPipelineStage> stages = new ArrayList<>(List.of(parent, target));
        if (rootResult != null) {
            BioPipelineStage selection = selection();
            selection.setOutputInline(rootResult);
            stages.add(selection);
        }

        OrchestratePlan plan = orchestrator.makePlan(stages, target.getStageId());

        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertUpstreamSkip(plan.getUpdateStageCommands().get(0), target, parent);
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_VARIANT_CALL})
    void anySkippedRequiredParentIsEnoughEvenWhileOtherRequiredParentHasNotFinished(int skippedType)
            throws Exception {
        BioPipelineStage mapping = stage(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_STATUS_RUNNING);
        BioPipelineStage variant = stage(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_STATUS_PENDING);
        BioPipelineStage skipped = skippedType == PIPELINE_STAGE_MAPPING ? mapping : variant;
        skipped.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage consensus = stage(PIPELINE_STAGE_CONSENSUS, PIPELINE_STAGE_STATUS_PENDING);

        OrchestratePlan plan = orchestrator.makePlan(List.of(mapping, variant, consensus), consensus.getStageId());

        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertUpstreamSkip(plan.getUpdateStageCommands().get(0), consensus, skipped);
        assertEquals(INITIAL_VERSION, mapping.getVersion());
        assertEquals(INITIAL_VERSION, variant.getVersion());
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_STATUS_FAIL, PIPELINE_STAGE_STATUS_RUNNING,
            PIPELINE_STAGE_STATUS_QUEUING, PIPELINE_STAGE_STATUS_ACTION_REQUIRED})
    void unsuccessfulButNotSkippedParentDoesNotPropagateNotApplicable(int parentStatus) {
        BioPipelineStage mapping = stage(PIPELINE_STAGE_MAPPING, parentStatus);
        BioPipelineStage target = stage(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_STATUS_PENDING);

        assertThrows(MissingUpstreamException.class,
                () -> orchestrator.makePlan(List.of(mapping, target), target.getStageId()));

        assertUntouched(target);
    }

    @Test
    void unrelatedAndOptionalSkippedStagesDoNotPreventIndependentMlstFromRunning() throws Exception {
        BioPipelineStage assembly = stage(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED);
        assembly.setOutputUrl("{\"contigsUrl\":\"fixture/contigs.fasta\"}");
        BioPipelineStage taxonomy = stage(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage annotation = stage(PIPELINE_STAGE_SNP_ANNOTATION, PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage selection = selection();
        selection.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        selection.setOutputInline("malformed unrelated result");
        BioPipelineStage mlst = stage(PIPELINE_STAGE_MLST, PIPELINE_STAGE_STATUS_PENDING);
        mlst.setParameters("{}");

        OrchestratePlan plan = orchestrator.makePlan(
                List.of(assembly, taxonomy, annotation, selection, mlst), mlst.getStageId());

        assertEquals(List.of(mlst), plan.getRunStages());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, mlst.getStatus());
        assertNull(mlst.getOutputInline());
    }

    @ParameterizedTest
    @MethodSource("requiredEdges")
    void entryStageBypassesSkippedDependencies(int upstreamType, int downstreamType) throws Exception {
        BioPipelineStage upstream = stage(upstreamType, PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage entry = stage(downstreamType, PIPELINE_STAGE_STATUS_PENDING);
        entry.setStageIndex(0);

        OrchestratePlan plan = orchestrator.makePlan(List.of(upstream, entry), entry.getStageId());

        assertEquals(List.of(entry), plan.getRunStages());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, entry.getStatus());
        assertNull(entry.getOutputInline());
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_STATUS_QUEUING, PIPELINE_STAGE_STATUS_RUNNING,
            PIPELINE_STAGE_STATUS_FINISHED, PIPELINE_STAGE_STATUS_FAIL,
            PIPELINE_STAGE_STATUS_NOT_APPLICABLE, PIPELINE_STAGE_STATUS_ACTION_REQUIRED})
    void genericPropagationNeverOverwritesAlreadyProcessedChild(int childStatus) throws Exception {
        BioPipelineStage mapping = stage(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage variant = stage(PIPELINE_STAGE_VARIANT_CALL, childStatus);
        variant.setOutputInline("existing child result");
        Date endTime = new Date(1234567L);
        variant.setEndTime(endTime);

        assertNoWork(orchestrator.makePlan(List.of(mapping, variant), variant.getStageId()));

        assertEquals(childStatus, variant.getStatus());
        assertEquals(INITIAL_VERSION, variant.getVersion());
        assertEquals("existing child result", variant.getOutputInline());
        assertSame(endTime, variant.getEndTime());
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_STATUS_QUEUING, PIPELINE_STAGE_STATUS_FAIL})
    void explicitSchedulingStillAcceptsPrequeuedEntryAndFailedEntryRetry(int initialStatus) throws Exception {
        BioPipelineStage entry = stage(PIPELINE_STAGE_READ_INSPECT, initialStatus);
        entry.setStageIndex(0);

        OrchestratePlan plan = orchestrator.makePlan(List.of(entry), entry.getStageId());

        assertEquals(List.of(entry), plan.getRunStages());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, entry.getStatus());
        assertEquals(INITIAL_VERSION + 1, entry.getVersion());
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        assertEquals(INITIAL_VERSION, command.getCurrentVersion());
        assertEquals(entry.getStageId().longValue(), command.getStageId());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, command.getUpdateStage().getStatus());
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_STATUS_FAIL, PIPELINE_STAGE_STATUS_QUEUING})
    void explicitNonentryRetryOrDispatchStillRunsWithFinishedRequiredParents(int initialStatus) throws Exception {
        BioPipelineStage assembly = stage(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_STATUS_FINISHED);
        assembly.setOutputUrl("{\"contigsUrl\":\"fixture/contigs.fasta\"}");
        BioPipelineStage mlst = stage(PIPELINE_STAGE_MLST, initialStatus);
        mlst.setParameters("{}");

        OrchestratePlan plan = orchestrator.makePlan(List.of(assembly, mlst), mlst.getStageId());

        assertEquals(List.of(mlst), plan.getRunStages());
        assertEquals(1, plan.getUpdateStageCommands().size());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, mlst.getStatus());
        assertEquals(INITIAL_VERSION + 1, mlst.getVersion());
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        assertEquals(INITIAL_VERSION, command.getCurrentVersion());
        assertEquals(mlst.getStageId().longValue(), command.getStageId());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, command.getUpdateStage().getStatus());
        assertTrue(mlst.getInputUrl().contains("fixture/contigs.fasta"));
    }

    private static void assertNoMatchSkip(UpdateStageCommand command, BioPipelineStage target,
            BioPipelineStage selection) throws Exception {
        assertSkipPatch(command, target);
        Map<String, Object> reason = JsonUtil.toMap(target.getOutputInline());
        assertTrue(reason.get("skipReason").toString().contains("NO_MATCH"));
        assertEquals(selection.getStageId().longValue(), ((Number) reason.get("causeStageId")).longValue());
        assertEquals("NO_MATCH", reason.get("causeOutcome"));
    }

    private static void assertUpstreamSkip(UpdateStageCommand command, BioPipelineStage target,
            BioPipelineStage parent) throws Exception {
        assertSkipPatch(command, target);
        Map<String, Object> reason = JsonUtil.toMap(target.getOutputInline());
        assertTrue(reason.get("skipReason").toString().contains("NOT_APPLICABLE"));
        assertEquals(parent.getStageId().longValue(), ((Number) reason.get("causeStageId")).longValue());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, ((Number) reason.get("causeStatus")).intValue());
        assertFalse(reason.containsKey("causeOutcome"));
    }

    private static void assertSkipPatch(UpdateStageCommand command, BioPipelineStage target) {
        BioPipelineStage patch = command.getUpdateStage();
        assertEquals(target.getStageId().longValue(), command.getStageId());
        assertEquals(INITIAL_VERSION, command.getCurrentVersion());
        assertEquals(INITIAL_VERSION + 1, patch.getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, patch.getStatus());
        assertEquals(patch.getStatus(), target.getStatus());
        assertEquals(patch.getVersion(), target.getVersion());
        assertNotNull(patch.getEndTime());
        assertSame(patch.getEndTime(), target.getEndTime());
        assertEquals(patch.getOutputInline(), target.getOutputInline());
        assertNull(patch.getInputUrl());
        assertNull(patch.getParameters());
        assertNull(patch.getOutputUrl());
        assertNull(patch.getStartTime());
        assertEquals("unused input", target.getInputUrl());
        assertEquals("parameters must not be parsed", target.getParameters());
    }

    private static void assertNoWork(OrchestratePlan plan) {
        assertTrue(plan.getRunStages().isEmpty());
        assertTrue(plan.getUpdateStageCommands().isEmpty());
    }

    private static void assertUntouched(BioPipelineStage target) {
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, target.getStatus());
        assertEquals(INITIAL_VERSION, target.getVersion());
        assertNull(target.getEndTime());
        assertNull(target.getOutputInline());
    }

    private static Stream<Integer> directTypes() {
        return DIRECT_TYPES.stream();
    }

    private static Stream<Arguments> requiredEdges() {
        return Stream.of(
                Arguments.of(PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_QC),
                Arguments.of(PIPELINE_STAGE_QC, PIPELINE_STAGE_ASSEMBLY),
                Arguments.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_REFERENCE_SELECTION),
                Arguments.of(PIPELINE_STAGE_REFERENCE_SELECTION, PIPELINE_STAGE_MAPPING),
                Arguments.of(PIPELINE_STAGE_REFERENCE_SELECTION, PIPELINE_STAGE_REFERENCE_COMPARISON),
                Arguments.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_VARIANT_CALL),
                Arguments.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_CONSENSUS),
                Arguments.of(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_CONSENSUS),
                Arguments.of(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_SNP_ANNOTATION),
                Arguments.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_TAXONOMY),
                Arguments.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_AMR),
                Arguments.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_MLST),
                Arguments.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_VIRULENCE),
                Arguments.of(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_SEROTYPE),
                Arguments.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_SEROTYPE));
    }

    private static Stream<Arguments> deepChildrenAndUnavailableRoots() {
        return Stream.of(
                List.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_VARIANT_CALL),
                List.of(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_CONSENSUS),
                List.of(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_SNP_ANNOTATION))
                .flatMap(edge -> Stream.of(null, "null", "{malformed",
                        "{\"outcome\":\"UNKNOWN\"}",
                        "{\"outcome\":\"NO_MATCH\",\"selectedReference\":{}}")
                        .map(result -> Arguments.of(edge.get(0), edge.get(1), result)));
    }

    private static Stream<Arguments> nonPendingDescendants() {
        return DEPENDENT_TYPES.stream().flatMap(stageType -> Stream.of(
                PIPELINE_STAGE_STATUS_QUEUING, PIPELINE_STAGE_STATUS_RUNNING, PIPELINE_STAGE_STATUS_FINISHED,
                PIPELINE_STAGE_STATUS_FAIL, PIPELINE_STAGE_STATUS_NOT_APPLICABLE, PIPELINE_STAGE_STATUS_ACTION_REQUIRED)
                .map(status -> Arguments.of(stageType, status)));
    }

    private static List<BioPipelineStage> descendants(BioPipelineStage selection) {
        List<BioPipelineStage> stages = new ArrayList<>();
        stages.add(selection);
        for (int stageType : DEPENDENT_TYPES) {
            stages.add(stage(stageType, PIPELINE_STAGE_STATUS_PENDING));
        }
        return stages;
    }

    private static BioPipelineStage findType(List<BioPipelineStage> stages, int stageType) {
        return stages.stream().filter(stage -> stage.getStageType() == stageType).findFirst().orElseThrow();
    }

    private static BioPipelineStage selection() {
        BioPipelineStage selection = stage(PIPELINE_STAGE_REFERENCE_SELECTION, PIPELINE_STAGE_STATUS_FINISHED);
        selection.setOutputInline(NO_MATCH_RESULT);
        return selection;
    }

    private static BioPipelineStage stage(int stageType, int status) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId((long) stageType);
        stage.setPipelineId(7L);
        stage.setStageType(stageType);
        stage.setStageIndex(-1);
        stage.setStatus(status);
        stage.setVersion(INITIAL_VERSION);
        stage.setInputUrl("unused input");
        stage.setParameters("parameters must not be parsed");
        return stage;
    }
}
