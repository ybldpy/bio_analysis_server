package com.xjtlu.bio.analysisPipeline.workflow;

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
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_AMBIGUOUS;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_OUT_PANEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.SeroTypingStageExectuor;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

class StageOrchestratorTaxonomyConfirmationTest {

    private final StageOrchestrator orchestrator = new StageOrchestrator();

    @Test
    void schedulesSupportedSpeciesAfterTaxonomyConfirmation() throws Exception {
        BioPipelineStage taxonomy = taxonomyStage(STATUS_CONFIDENT, taxon(562));
        BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);

        OrchestratePlan plan = downstreamPlan(taxonomy, serotype);

        assertEquals(1, plan.getRunStages().size());
        assertSame(serotype, plan.getRunStages().get(0));
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, serotype.getStatus());
        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand update = plan.getUpdateStageCommands().get(0);
        assertEquals(serotype.getStageId().longValue(), update.getStageId());
        assertEquals(3, update.getCurrentVersion());
        assertEquals(4, update.getUpdateStage().getVersion());

        SeroTypeStageInputUrls input = JsonUtil.toObject(
                serotype.getInputUrl(), SeroTypeStageInputUrls.class);
        assertEquals("contigs.fasta", input.getContigsUrl());
        SeroTypingStageParameters parameters = JsonUtil.toObject(
                serotype.getParameters(), SeroTypingStageParameters.class);
        assertEquals(562, parameters.getTaxonomyContext().getTaxid());
        assertEquals(STATUS_CONFIDENT, parameters.getTaxonomyContext().getStatus());
    }

    @Test
    void skipsSpeciesWithoutSerotypingSupport() throws Exception {
        assertSkipped(downstreamPlan(
                taxonomyStage(STATUS_CONFIDENT, taxon(999)),
                serotypeStage(PIPELINE_STAGE_STATUS_PENDING)));
    }

    @Test
    void skipsMissingBestEvenWhenCandidatesIncludeSupportedSpecies() throws Exception {
        assertSkipped(downstreamPlan(
                taxonomyStage(STATUS_CONFIDENT, null),
                serotypeStage(PIPELINE_STAGE_STATUS_PENDING)));
    }

    @Test
    void skipsMissingTaxonomyResultWithoutNullPointerException() throws Exception {
        BioPipelineStage taxonomy = taxonomyStage(STATUS_CONFIDENT, taxon(562));
        taxonomy.setOutputInline(null);

        assertSkipped(downstreamPlan(taxonomy, serotypeStage(PIPELINE_STAGE_STATUS_PENDING)));
    }

    @Test
    void doesNotRouteUnconfirmedClassificationToSerotyping() throws Exception {
        for (int classification : List.of(STATUS_AMBIGUOUS, STATUS_LOW_CONFIDENCE, STATUS_OUT_PANEL)) {
            assertSkipped(downstreamPlan(
                    taxonomyStage(classification, taxon(562)),
                    serotypeStage(PIPELINE_STAGE_STATUS_PENDING)));
        }
    }

    @Test
    void doesNotScheduleWhileTaxonomyRequiresAction() throws Exception {
        BioPipelineStage taxonomy = taxonomyStage(STATUS_AMBIGUOUS, taxon(562));
        taxonomy.setStatus(PIPELINE_STAGE_STATUS_ACTION_REQUIRED);
        BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);

        OrchestratePlan plan = downstreamPlan(taxonomy, serotype);

        assertTrue(plan.getRunStages().isEmpty());
        assertTrue(plan.getUpdateStageCommands().isEmpty());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, serotype.getStatus());
    }

    @Test
    void doesNotRescheduleSerotypingThatIsNoLongerPending() throws Exception {
        for (int status : List.of(
                PIPELINE_STAGE_STATUS_QUEUING,
                PIPELINE_STAGE_STATUS_RUNNING,
                PIPELINE_STAGE_STATUS_FINISHED,
                PIPELINE_STAGE_STATUS_NOT_APPLICABLE,
                PIPELINE_STAGE_STATUS_FAIL)) {
            BioPipelineStage serotype = serotypeStage(status);

            OrchestratePlan plan = downstreamPlan(taxonomyStage(STATUS_CONFIDENT, taxon(562)), serotype);

            assertTrue(plan.getRunStages().isEmpty());
            assertTrue(plan.getUpdateStageCommands().isEmpty());
            assertEquals(status, serotype.getStatus());
            assertEquals(3, serotype.getVersion());
        }
    }

    @Test
    void missingSerotypeStageReturnsTerminalEmptyPlan() throws Exception {
        BioPipelineStage taxonomy = taxonomyStage(STATUS_CONFIDENT, taxon(562));

        OrchestratePlan plan = orchestrator.makeDownstreamPlan(
                taxonomy, List.of(taxonomy), PIPELINE_REGULAR_BACTERIA);

        assertTrue(plan.isNoNextStage());
        assertTrue(plan.getRunStages().isEmpty());
        assertTrue(plan.getUpdateStageCommands().isEmpty());
    }

    @Test
    void outOfPanelTaxonomyCallbackSkipsPendingSerotype() throws Exception {
        BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
        BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);
        String taxonomyResult = taxonomy.getOutputInline();

        OrchestratePlan plan = downstreamPlan(taxonomy, serotype);

        assertOutOfPanelSkipped(plan, serotype);
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, taxonomy.getStatus());
        assertEquals(4, taxonomy.getVersion());
        assertEquals(taxonomyResult, taxonomy.getOutputInline());
    }

    @Test
    void pollerSkipsOutOfPanelSerotypeBeforeCheckingPrerequisitesOrPreparingInputs() throws Exception {
        BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
        taxonomy.setInputUrl(null);
        BioPipelineStage assembly = assemblyStage(PIPELINE_STAGE_STATUS_RUNNING);
        BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);
        serotype.setParameters("not parsed for a skipped stage");

        OrchestratePlan plan = orchestrator.makePlan(
                List.of(assembly, taxonomy, serotype), serotype.getStageId());

        assertOutOfPanelSkipped(plan, serotype);
        assertEquals("not parsed for a skipped stage", serotype.getParameters());
        assertNull(serotype.getInputUrl());
        assertEquals(PIPELINE_STAGE_STATUS_RUNNING, assembly.getStatus());
        assertEquals(3, assembly.getVersion());
    }

    @Test
    void repeatedOutOfPanelCallbacksAndPollingDoNotRewriteSkippedSerotype() throws Exception {
        for (boolean callbackFirst : List.of(true, false)) {
            BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
            BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);
            List<BioPipelineStage> stages = List.of(taxonomy, serotype);

            OrchestratePlan firstPlan = callbackFirst
                    ? downstreamPlan(taxonomy, serotype)
                    : orchestrator.makePlan(stages, serotype.getStageId());
            assertOutOfPanelSkipped(firstPlan, serotype);
            Date endTime = serotype.getEndTime();
            String outputInline = serotype.getOutputInline();

            assertNoWork(downstreamPlan(taxonomy, serotype));
            assertNoWork(orchestrator.makePlan(stages, serotype.getStageId()));

            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, serotype.getStatus());
            assertEquals(4, serotype.getVersion());
            assertSame(endTime, serotype.getEndTime());
            assertEquals(outputInline, serotype.getOutputInline());
        }
    }

    @Test
    void outOfPanelTaxonomyDoesNotOverwriteNonPendingSerotype() throws Exception {
        for (int status : List.of(
                PIPELINE_STAGE_STATUS_QUEUING,
                PIPELINE_STAGE_STATUS_RUNNING,
                PIPELINE_STAGE_STATUS_FINISHED,
                PIPELINE_STAGE_STATUS_NOT_APPLICABLE,
                PIPELINE_STAGE_STATUS_FAIL,
                PIPELINE_STAGE_STATUS_ACTION_REQUIRED)) {
            BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
            BioPipelineStage serotype = serotypeStage(status);
            Date endTime = new Date(123456789L);
            serotype.setEndTime(endTime);
            serotype.setOutputInline("existing result");
            serotype.setInputUrl("existing input");
            String parameters = serotype.getParameters();

            assertNoWork(downstreamPlan(taxonomy, serotype));
            assertNoWork(orchestrator.makePlan(List.of(taxonomy, serotype), serotype.getStageId()));

            assertEquals(status, serotype.getStatus());
            assertEquals(3, serotype.getVersion());
            assertSame(endTime, serotype.getEndTime());
            assertEquals("existing result", serotype.getOutputInline());
            assertEquals("existing input", serotype.getInputUrl());
            assertEquals(parameters, serotype.getParameters());
        }
    }

    @Test
    void outOfPanelTaxonomyWithoutSerotypeReturnsTerminalEmptyPlan() throws Exception {
        BioPipelineStage taxonomy = outOfPanelTaxonomyStage();

        OrchestratePlan plan = orchestrator.makeDownstreamPlan(
                taxonomy, List.of(taxonomy), PIPELINE_REGULAR_BACTERIA);

        assertTrue(plan.isNoNextStage());
        assertNoWork(plan);
    }

    @Test
    void ambiguousAndLowConfidenceActionRequiredTaxonomyStillBlockSerotype() throws Exception {
        for (int classification : List.of(STATUS_AMBIGUOUS, STATUS_LOW_CONFIDENCE)) {
            BioPipelineStage taxonomy = taxonomyStage(classification, taxon(562));
            taxonomy.setStatus(PIPELINE_STAGE_STATUS_ACTION_REQUIRED);
            BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);

            assertNoWork(downstreamPlan(taxonomy, serotype));
            assertThrows(MissingUpstreamException.class,
                    () -> orchestrator.makePlan(List.of(taxonomy, serotype), serotype.getStageId()));

            assertEquals(PIPELINE_STAGE_STATUS_PENDING, serotype.getStatus());
            assertEquals(3, serotype.getVersion());
            assertNull(serotype.getEndTime());
            assertNull(serotype.getOutputInline());
        }
    }

    @Test
    void notApplicableTaxonomySkipsSerotypeWithoutRequiringOutOfPanelPayload() throws Exception {
        for (String outputInline : Arrays.asList(
                taxonomyStage(STATUS_CONFIDENT, taxon(562)).getOutputInline(),
                taxonomyStage(STATUS_AMBIGUOUS, taxon(562)).getOutputInline(),
                taxonomyStage(STATUS_LOW_CONFIDENCE, taxon(562)).getOutputInline(),
                "", null, "null", "not valid JSON")) {
            for (boolean callback : List.of(true, false)) {
                BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
                taxonomy.setOutputInline(outputInline);
                BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);
                serotype.setParameters("not parsed for a skipped stage");

                OrchestratePlan plan = callback
                        ? downstreamPlan(taxonomy, serotype)
                        : orchestrator.makePlan(List.of(taxonomy, serotype), serotype.getStageId());

                assertRequiredUpstreamSkipped(plan, serotype, taxonomy);
                assertEquals("not parsed for a skipped stage", serotype.getParameters());
                assertEquals(outputInline, taxonomy.getOutputInline());
                assertEquals(4, taxonomy.getVersion());
            }
        }
    }

    @Test
    void outOfPanelResultAloneDoesNotBypassUnfinishedTaxonomy() throws Exception {
        for (int status : List.of(
                PIPELINE_STAGE_STATUS_PENDING,
                PIPELINE_STAGE_STATUS_QUEUING,
                PIPELINE_STAGE_STATUS_RUNNING,
                PIPELINE_STAGE_STATUS_ACTION_REQUIRED,
                PIPELINE_STAGE_STATUS_FAIL)) {
            BioPipelineStage taxonomy = taxonomyStage(STATUS_OUT_PANEL, null);
            taxonomy.setStatus(status);
            BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);

            assertNoWork(downstreamPlan(taxonomy, serotype));
            assertThrows(MissingUpstreamException.class,
                    () -> orchestrator.makePlan(List.of(taxonomy, serotype), serotype.getStageId()));

            assertEquals(PIPELINE_STAGE_STATUS_PENDING, serotype.getStatus());
            assertEquals(3, serotype.getVersion());
            assertNull(serotype.getEndTime());
        }
    }

    @Test
    void requiredNotApplicableAssemblySkipsItsPendingConsumers() throws Exception {
        BioPipelineStage assembly = assemblyStage(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        BioPipelineStage taxonomy = taxonomyStage(STATUS_CONFIDENT, taxon(562));
        BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);

        assertRequiredUpstreamSkipped(orchestrator.makePlan(
                List.of(assembly, taxonomy, serotype), serotype.getStageId()), serotype, assembly);

        for (int stageType : List.of(PIPELINE_STAGE_AMR, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_MLST)) {
            BioPipelineStage consumer = pendingStage(4L, stageType);
            List<BioPipelineStage> stages = List.of(assembly, outOfPanelTaxonomyStage(), consumer);

            assertRequiredUpstreamSkipped(orchestrator.makePlan(
                    stages, consumer.getStageId()), consumer, assembly);
        }
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, assembly.getStatus());
        assertEquals(3, assembly.getVersion());
    }

    @Test
    void unrelatedNotApplicableTaxonomyDoesNotSkipAssemblyConsumersDuringPolling() throws Exception {
        for (int stageType : List.of(PIPELINE_STAGE_AMR, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_MLST)) {
            BioPipelineStage assembly = assemblyStage(PIPELINE_STAGE_STATUS_FINISHED);
            BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
            taxonomy.setOutputInline("not required by this branch");
            BioPipelineStage independent = pendingStage(4L, stageType);

            OrchestratePlan plan = orchestrator.makePlan(
                    List.of(assembly, taxonomy, independent), independent.getStageId());

            assertEquals(List.of(independent), plan.getRunStages());
            assertEquals(1, plan.getUpdateStageCommands().size());
            assertEquals(PIPELINE_STAGE_STATUS_QUEUING, independent.getStatus());
            assertEquals(4, independent.getVersion());
            assertNull(independent.getEndTime());
            assertNull(independent.getOutputInline());
            assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, taxonomy.getStatus());
            assertEquals(4, taxonomy.getVersion());
        }
    }

    @Test
    void outOfPanelSerotypeSkipLeavesIndependentBranchesAvailableForScheduling() throws Exception {
        BioPipelineStage assembly = assemblyStage(PIPELINE_STAGE_STATUS_FINISHED);
        BioPipelineStage taxonomy = outOfPanelTaxonomyStage();
        BioPipelineStage serotype = serotypeStage(PIPELINE_STAGE_STATUS_PENDING);
        BioPipelineStage amr = pendingStage(4L, PIPELINE_STAGE_AMR);
        BioPipelineStage vf = pendingStage(5L, PIPELINE_STAGE_VIRULENCE);
        BioPipelineStage mlst = pendingStage(6L, PIPELINE_STAGE_MLST);
        List<BioPipelineStage> branches = List.of(amr, vf, mlst);
        List<BioPipelineStage> stages = List.of(assembly, taxonomy, serotype, amr, vf, mlst);

        OrchestratePlan skipPlan = orchestrator.makeDownstreamPlan(taxonomy, stages, PIPELINE_REGULAR_BACTERIA);

        assertOutOfPanelSkipped(skipPlan, serotype);
        for (BioPipelineStage branch : branches) {
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, branch.getStatus());
            assertEquals(3, branch.getVersion());
        }

        OrchestratePlan branchPlan = orchestrator.makeDownstreamPlan(assembly, stages, PIPELINE_REGULAR_BACTERIA);

        assertEquals(branches, branchPlan.getRunStages());
        assertEquals(3, branchPlan.getUpdateStageCommands().size());
        for (BioPipelineStage branch : branches) {
            assertEquals(PIPELINE_STAGE_STATUS_QUEUING, branch.getStatus());
            assertEquals(4, branch.getVersion());
            String inputKey = branch.getStageType() == PIPELINE_STAGE_MLST ? "contigUrl" : "contigsUrl";
            assertEquals("contigs.fasta", JsonUtil.toMap(branch.getInputUrl()).get(inputKey));
        }
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, serotype.getStatus());
        assertEquals(4, serotype.getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, taxonomy.getStatus());
        assertEquals(4, taxonomy.getVersion());
    }

    @Test
    void serotypingCapabilityRejectsNullContextAndMissingTaxid() {
        assertFalse(SeroTypingStageExectuor.canDoSeroType(null));
        assertFalse(SeroTypingStageExectuor.canDoSeroType(TaxonomyContext.of(null)));

        TaxonomyResult unresolved = new TaxonomyResult();
        unresolved.setStatus(STATUS_CONFIDENT);
        assertFalse(SeroTypingStageExectuor.canDoSeroType(TaxonomyContext.of(unresolved)));
    }

    @Test
    void serotypingCapabilityRequiresConfidentClassification() {
        TaxonomyResult result = new TaxonomyResult();
        result.setBest(taxon(562));

        for (int status : List.of(STATUS_AMBIGUOUS, STATUS_LOW_CONFIDENCE, STATUS_OUT_PANEL)) {
            result.setStatus(status);
            assertFalse(SeroTypingStageExectuor.canDoSeroType(TaxonomyContext.of(result)));
        }
        result.setStatus(STATUS_CONFIDENT);
        assertTrue(SeroTypingStageExectuor.canDoSeroType(TaxonomyContext.of(result)));
    }

    private OrchestratePlan downstreamPlan(BioPipelineStage taxonomy, BioPipelineStage serotype)
            throws Exception {
        return orchestrator.makeDownstreamPlan(
                taxonomy, List.of(taxonomy, serotype), PIPELINE_REGULAR_BACTERIA);
    }

    private static void assertSkipped(OrchestratePlan plan) {
        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        assertEquals(2L, command.getStageId());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, command.getUpdateStage().getStatus());
    }

    private static void assertOutOfPanelSkipped(OrchestratePlan plan, BioPipelineStage serotype) throws Exception {
        assertSkipped(plan);
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        BioPipelineStage patch = command.getUpdateStage();
        assertEquals(serotype.getStageId().longValue(), command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, patch.getVersion());
        assertEquals(4, serotype.getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, serotype.getStatus());
        assertNotNull(patch.getEndTime());
        assertEquals(patch.getEndTime(), serotype.getEndTime());
        assertEquals(Map.of("skipReason", "Taxonomy is OUT_PANEL; serotyping is not applicable",
                "causeStageId", 1, "causeStatus", PIPELINE_STAGE_STATUS_NOT_APPLICABLE),
                JsonUtil.toMap(patch.getOutputInline()));
        assertEquals(patch.getOutputInline(), serotype.getOutputInline());
        assertNull(patch.getInputUrl());
        assertNull(patch.getParameters());
        assertNull(patch.getStartTime());
        assertNull(serotype.getStartTime());
    }

    private static void assertRequiredUpstreamSkipped(OrchestratePlan plan,
            BioPipelineStage stage, BioPipelineStage upstream) throws Exception {
        assertTrue(plan.getRunStages().isEmpty());
        assertEquals(1, plan.getUpdateStageCommands().size());
        UpdateStageCommand command = plan.getUpdateStageCommands().get(0);
        BioPipelineStage patch = command.getUpdateStage();
        assertEquals(stage.getStageId().longValue(), command.getStageId());
        assertEquals(3, command.getCurrentVersion());
        assertEquals(4, patch.getVersion());
        assertEquals(4, stage.getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, patch.getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, stage.getStatus());
        assertNotNull(patch.getEndTime());
        assertEquals(patch.getEndTime(), stage.getEndTime());
        assertEquals(patch.getOutputInline(), stage.getOutputInline());
        Map<String, Object> reason = JsonUtil.toMap(patch.getOutputInline());
        assertEquals("Required upstream stage is NOT_APPLICABLE", reason.get("skipReason"));
        assertEquals(upstream.getStageId().longValue(), ((Number) reason.get("causeStageId")).longValue());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, reason.get("causeStatus"));
        assertNull(patch.getInputUrl());
        assertNull(patch.getParameters());
        assertNull(patch.getStartTime());
        assertNull(stage.getStartTime());
    }

    private static void assertNoWork(OrchestratePlan plan) {
        assertTrue(plan.getRunStages().isEmpty());
        assertTrue(plan.getUpdateStageCommands().isEmpty());
    }

    private static BioPipelineStage outOfPanelTaxonomyStage() throws Exception {
        BioPipelineStage taxonomy = taxonomyStage(STATUS_OUT_PANEL, null);
        taxonomy.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        return taxonomy;
    }

    private static BioPipelineStage assemblyStage(int status) throws Exception {
        BioPipelineStage assembly = pendingStage(3L, PIPELINE_STAGE_ASSEMBLY);
        assembly.setStatus(status);
        assembly.setOutputUrl(JsonUtil.toJson(new AssemblyResult("contigs.fasta", null)));
        return assembly;
    }

    private static BioPipelineStage pendingStage(long stageId, int stageType) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(stageId);
        stage.setPipelineId(10L);
        stage.setStageType(stageType);
        stage.setStageIndex(-1);
        stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stage.setVersion(3);
        stage.setParameters("{}");
        return stage;
    }

    private static BioPipelineStage taxonomyStage(int classification, TaxonomyResult.Taxon best)
            throws Exception {
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(classification);
        result.setBest(best);
        result.setCandidates(List.of(taxon(562)));

        TaxonomyStageInputUrls inputs = new TaxonomyStageInputUrls();
        inputs.setContigs("contigs.fasta");

        BioPipelineStage taxonomy = new BioPipelineStage();
        taxonomy.setStageId(1L);
        taxonomy.setPipelineId(10L);
        taxonomy.setStageType(PIPELINE_STAGE_TAXONOMY);
        taxonomy.setStageIndex(0);
        taxonomy.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        taxonomy.setVersion(4);
        taxonomy.setOutputInline(JsonUtil.toJson(result));
        taxonomy.setInputUrl(JsonUtil.toJson(inputs));
        return taxonomy;
    }

    private static BioPipelineStage serotypeStage(int status) throws Exception {
        BioPipelineStage serotype = new BioPipelineStage();
        serotype.setStageId(2L);
        serotype.setPipelineId(10L);
        serotype.setStageType(PIPELINE_STAGE_SEROTYPE);
        serotype.setStageIndex(-1);
        serotype.setStatus(status);
        serotype.setVersion(3);
        serotype.setParameters(JsonUtil.toJson(new SeroTypingStageParameters()));
        return serotype;
    }

    private static TaxonomyResult.Taxon taxon(int taxid) {
        TaxonomyResult.Taxon taxon = new TaxonomyResult.Taxon();
        taxon.setTaxid(taxid);
        taxon.setName(taxid == 562 ? "Escherichia coli" : "Unsupported species");
        taxon.setRank("S");
        taxon.setScore(0.96d);
        return taxon;
    }
}
