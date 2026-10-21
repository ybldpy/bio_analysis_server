package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.ibatis.executor.BatchResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.Taxon;
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
import com.xjtlu.bio.requestParameters.ConfirmTaxonomyRequest;
import com.xjtlu.bio.response.TaxonomyConfirmationResponse;
import com.xjtlu.bio.utils.JsonUtil;

/** Uses the real service/orchestrator with in-memory persistence and a mocked queue. */
class PipelineServiceTaxonomyConfirmationTest {

    private static final long TAXONOMY_ID = 123L;
    private static final long SEROTYPE_ID = 124L;

    private final Map<Long, BioPipelineStage> stages = new LinkedHashMap<>();
    private final Set<Long> queuedIds = new HashSet<>();
    private final List<Integer> batchCounts = new ArrayList<>();
    private PipelineService service;
    private BioPipelineStageMapper stageMapper;
    private BioAnalysisPipelineMapper pipelineMapper;
    private PipelineStageTaskDispatcher dispatcher;
    private PlatformTransactionManager transactionManager;
    private TransactionTemplate sharedTransaction;
    private BioAnalysisPipeline pipeline;
    private boolean inTransaction;
    private boolean queueFull;
    private boolean failNextDownstreamUpdate;
    private boolean failPipelineCompletion;
    private boolean advanceTaxonomyVersionBeforeUpdate;
    private Integer concurrentTaxId;
    private int confirmationWrites;
    private int rollbacks;

    @BeforeEach
    void setUp() throws Exception {
        service = new PipelineService();
        stageMapper = mock(BioPipelineStageMapper.class);
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        transactionManager = mock(PlatformTransactionManager.class);
        sharedTransaction = new TransactionTemplate(transactionManager);
        BioAnalysisPipelineMapperExtension pipelineExtension = mock(BioAnalysisPipelineMapperExtension.class);
        BioAnalysisStageMapperExtension stageExtension = mock(BioAnalysisStageMapperExtension.class);
        SqlSessionTemplate batchSession = mock(SqlSessionTemplate.class);

        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stageMapper);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapperExtension", pipelineExtension);
        ReflectionTestUtils.setField(service, "bioAnalysisStageMapperExtension", stageExtension);
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "rcTransactionTemplate", sharedTransaction);
        ReflectionTestUtils.setField(service, "batchSqlSessionTemplate", batchSession);
        ReflectionTestUtils.setField(service, "stageOrchestrator", new StageOrchestrator());

        pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(10L);
        pipeline.setPipelineType(PIPELINE_REGULAR_BACTERIA);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_RUNNING);

        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_AMBIGUOUS);
        result.setEvidenceResource(EVIDENCE_FASTANI);
        result.setCandidates(List.of(taxon(562, "Escherichia coli", 98.71),
                taxon(623, "Shigella boydii", 98.78)));
        result.setMainBinFastaUrl("taxonomy/main_bin.fna");
        result.setContigAssignmentReportUrl("taxonomy/assignments.tsv");
        TaxonomyBinningMetrics metrics = new TaxonomyBinningMetrics();
        metrics.setTotalContigs(42);
        metrics.setTotalAssemblyBases(4_000_000L);
        result.setBinningMetrics(metrics);

        BioPipelineStage taxonomy = stage(TAXONOMY_ID, PIPELINE_STAGE_TAXONOMY,
                PIPELINE_STAGE_STATUS_ACTION_REQUIRED, 4);
        taxonomy.setOutputInline(JsonUtil.toJson(result));
        taxonomy.setOutputUrl("taxonomy/report.json");
        TaxonomyStageInputUrls inputs = new TaxonomyStageInputUrls();
        inputs.setContigs("assembly/contigs.fasta");
        taxonomy.setInputUrl(JsonUtil.toJson(inputs));
        stages.put(TAXONOMY_ID, taxonomy);
        BioPipelineStage serotype = stage(SEROTYPE_ID, PIPELINE_STAGE_SEROTYPE,
                PIPELINE_STAGE_STATUS_PENDING, 3);
        serotype.setParameters(JsonUtil.toJson(new SeroTypingStageParameters()));
        stages.put(SEROTYPE_ID, serotype);
        stages.put(125L, stage(125L, PIPELINE_STAGE_AMR, PIPELINE_STAGE_STATUS_FINISHED, 2));
        stages.put(126L, stage(126L, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_STATUS_FINISHED, 2));
        stages.put(127L, stage(127L, PIPELINE_STAGE_MLST, PIPELINE_STAGE_STATUS_FINISHED, 2));

        when(transactionManager.getTransaction(any(TransactionDefinition.class))).thenAnswer(invocation -> {
            assertFalse(inTransaction, "confirmation and downstream writes must use separate transactions");
            inTransaction = true;
            return new SimpleTransactionStatus();
        });
        doAnswer(invocation -> {
            // TransactionTemplate delegates rollback-only handling to the manager's commit.
            if (invocation.<TransactionStatus>getArgument(0).isRollbackOnly()) {
                rollbacks++;
            }
            inTransaction = false;
            return null;
        })
                .when(transactionManager).commit(any(TransactionStatus.class));
        doAnswer(invocation -> { inTransaction = false; rollbacks++; return null; })
                .when(transactionManager).rollback(any(TransactionStatus.class));

        when(stageMapper.selectByPrimaryKey(anyLong())).thenAnswer(invocation ->
                copy(stages.get(invocation.<Long>getArgument(0)), BioPipelineStage.class));
        when(pipelineMapper.selectByPrimaryKey(anyLong())).thenAnswer(invocation ->
                invocation.<Long>getArgument(0) == 10L ? copy(pipeline, BioAnalysisPipeline.class) : null);
        when(pipelineExtension.selectPipelineTypeByStageId(anyLong())).thenAnswer(invocation ->
                copy(pipeline, BioAnalysisPipeline.class));
        when(stageExtension.selectAllPipelineStagesByStageId(anyLong())).thenAnswer(invocation -> {
            List<BioPipelineStage> snapshots = new ArrayList<>();
            for (BioPipelineStage stored : stages.values()) {
                snapshots.add(copy(stored, BioPipelineStage.class));
            }
            return snapshots;
        });

        when(stageMapper.updateByExampleSelective(any(BioPipelineStage.class),
                any(BioPipelineStageExample.class))).thenAnswer(invocation -> {
            assertTrue(inTransaction, "stage changes must be transactional");
            BioPipelineStage patch = invocation.getArgument(0);
            BioPipelineStageExample condition = invocation.getArgument(1);
            Map<String, Object> criteria = new LinkedHashMap<>();
            condition.getOredCriteria().get(0).getAllCriteria()
                    .forEach(criterion -> criteria.put(criterion.getCondition(), criterion.getValue()));
            BioPipelineStage stored = stages.get(criteria.get("stage_id ="));
            assertEquals(stored.getVersion(), criteria.get("version ="));
            if (stored.getStageId() == TAXONOMY_ID) {
                confirmationWrites++;
                // Type is validated on read; the write CAS uses stage ID, status and version.
                assertEquals(PIPELINE_STAGE_TAXONOMY, stored.getStageType());
                assertEquals(PIPELINE_STAGE_STATUS_ACTION_REQUIRED, criteria.get("status ="));
                if (advanceTaxonomyVersionBeforeUpdate) {
                    stored.setVersion(stored.getVersion() + 1);
                    return 0; // The version changed after the service read its snapshot.
                }
                if (concurrentTaxId != null) {
                    TaxonomyResult winner = JsonUtil.toObject(patch.getOutputInline(), TaxonomyResult.class);
                    winner.setBest(winner.getCandidates().stream()
                            .filter(candidate -> candidate.getTaxid() == concurrentTaxId).findFirst().orElseThrow());
                    winner.getManualConfirmation().setSpeciesTaxId(concurrentTaxId);
                    winner.getManualConfirmation().setConfirmedBy("concurrent reviewer");
                    patch.setOutputInline(JsonUtil.toJson(winner));
                    applyPatch(stored, patch);
                    return 0; // A different transaction won the compare-and-set.
                }
                applyPatch(stored, patch);
                return 1;
            }
            if (failNextDownstreamUpdate) {
                failNextDownstreamUpdate = false;
                batchCounts.add(0);
                return 0;
            }
            applyPatch(stored, patch);
            batchCounts.add(1);
            return 1;
        });
        when(batchSession.getMapper(BioPipelineStageMapper.class)).thenReturn(stageMapper);
        when(batchSession.flushStatements()).thenAnswer(invocation -> {
            BatchResult resultCounts = mock(BatchResult.class);
            when(resultCounts.getUpdateCounts()).thenReturn(batchCounts.stream().mapToInt(Integer::intValue).toArray());
            batchCounts.clear();
            return List.of(resultCounts);
        });
        when(pipelineMapper.updateByExampleSelective(any(BioAnalysisPipeline.class),
                any(BioAnalysisPipelineExample.class))).thenAnswer(invocation -> {
            if (failPipelineCompletion) {
                throw new IllegalStateException("simulated completion persistence failure");
            }
            BioAnalysisPipeline patch = invocation.getArgument(0);
            pipeline.setStatus(patch.getStatus());
            return 1;
        });
        when(dispatcher.addTask(any(BioPipelineStage.class))).thenAnswer(invocation -> {
            assertFalse(inTransaction, "dispatch must happen after commit");
            if (queueFull) {
                return false;
            }
            queuedIds.add(invocation.<BioPipelineStage>getArgument(0).getStageId());
            return true;
        });
    }

    @Test
    void confirmsStoredCandidateAndResumesOnlySerotypeAfterCommit() throws Exception {
        TaxonomyResult original = taxonomyResult();
        Result<TaxonomyConfirmationResponse> response = confirm(562, "reviewed against independent evidence");

        assertEquals(Result.SUCCESS, response.getStatus());
        assertEquals(5, response.getData().getVersion());
        assertTrue(response.getData().isDownstreamScheduled());
        assertFalse(response.getData().isAlreadyConfirmed());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stages.get(TAXONOMY_ID).getStatus());
        TaxonomyResult confirmed = taxonomyResult();
        assertEquals(562, confirmed.getBest().getTaxid());
        assertEquals("S", confirmed.getBest().getRank());
        assertEquals(98.71, confirmed.getBest().getScore());
        assertEquals(STATUS_CONFIDENT, confirmed.getStatus());
        assertEquals(EVIDENCE_USER_CONFIRMED, confirmed.getEvidenceResource());
        assertEquals(STATUS_AMBIGUOUS, confirmed.getManualConfirmation().getOriginalStatus());
        assertEquals(EVIDENCE_FASTANI, confirmed.getManualConfirmation().getOriginalEvidenceResource());
        assertEquals("reviewer", confirmed.getManualConfirmation().getConfirmedBy());
        assertEquals("reviewed against independent evidence", confirmed.getManualConfirmation().getReason());
        assertEquals(stages.get(TAXONOMY_ID).getEndTime(), confirmed.getManualConfirmation().getConfirmedAt());
        assertEquals(JsonUtil.toJson(original.getCandidates()), JsonUtil.toJson(confirmed.getCandidates()));
        assertEquals(JsonUtil.toJson(original.getBinningMetrics()), JsonUtil.toJson(confirmed.getBinningMetrics()));
        assertEquals(original.getMainBinFastaUrl(), confirmed.getMainBinFastaUrl());
        assertEquals(original.getContigAssignmentReportUrl(), confirmed.getContigAssignmentReportUrl());
        assertEquals("taxonomy/report.json", stages.get(TAXONOMY_ID).getOutputUrl());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, stages.get(SEROTYPE_ID).getStatus());
        SeroTypingStageParameters parameters = JsonUtil.toObject(
                stages.get(SEROTYPE_ID).getParameters(), SeroTypingStageParameters.class);
        assertEquals(562, parameters.getTaxonomyContext().getTaxid());
        assertEquals(Set.of(SEROTYPE_ID), queuedIds);
        for (long id : List.of(125L, 126L, 127L)) {
            assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stages.get(id).getStatus());
            assertEquals(2, stages.get(id).getVersion());
        }
        verify(transactionManager).getTransaction(argThat(definition ->
                definition.getPropagationBehavior() == TransactionDefinition.PROPAGATION_REQUIRES_NEW
                        && definition.getIsolationLevel() == TransactionDefinition.ISOLATION_READ_COMMITTED));
        assertEquals(TransactionDefinition.PROPAGATION_REQUIRED, sharedTransaction.getPropagationBehavior());
        assertEquals(TransactionDefinition.ISOLATION_DEFAULT, sharedTransaction.getIsolationLevel());
    }

    @ParameterizedTest
    @ValueSource(ints = {STATUS_LOW_CONFIDENCE, STATUS_OUT_PANEL})
    void confirmsOtherReviewableClassificationsWithoutLosingOriginalStatus(int classification) throws Exception {
        TaxonomyResult original = taxonomyResult();
        original.setStatus(classification);
        stages.get(TAXONOMY_ID).setOutputInline(JsonUtil.toJson(original));

        assertEquals(Result.SUCCESS, confirm(562, null).getStatus());
        assertEquals(classification, taxonomyResult().getManualConfirmation().getOriginalStatus());
    }

    @Test
    void unsupportedSpeciesSkipsSerotypeAndClosesCompletedPipeline() throws Exception {
        Result<TaxonomyConfirmationResponse> response = confirm(623, null);

        assertEquals(Result.SUCCESS, response.getStatus());
        assertTrue(response.getData().isDownstreamScheduled());
        assertEquals(623, taxonomyResult().getBest().getTaxid());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, stages.get(SEROTYPE_ID).getStatus());
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void skippedSerotypeDoesNotCompletePipelineWhileParallelBranchIsRunning() {
        stages.get(125L).setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        Result<TaxonomyConfirmationResponse> response = confirm(623, null);

        assertEquals(Result.SUCCESS, response.getStatus());
        assertTrue(response.getData().isDownstreamScheduled());
        assertEquals(PIPELINE_STAGE_STATUS_NOT_APPLICABLE, stages.get(SEROTYPE_ID).getStatus());
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());
        verifyNoInteractions(dispatcher);

        stages.get(125L).setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        service.pipelineStageDone(125L, true);
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
    }

    @Test
    void missingOptionalSerotypeStageStillRunsCompletionCheck() {
        stages.remove(SEROTYPE_ID);

        assertEquals(Result.SUCCESS, confirm(562, null).getStatus());
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void confirmsLegacyResultWithoutInventingOriginalEvidence() throws Exception {
        TaxonomyResult legacy = taxonomyResult();
        legacy.setEvidenceResource(null);
        stages.get(TAXONOMY_ID).setOutputInline(JsonUtil.toJson(legacy));

        assertEquals(Result.SUCCESS, confirm(562, null).getStatus());
        assertEquals(EVIDENCE_USER_CONFIRMED, taxonomyResult().getEvidenceResource());
        assertNull(taxonomyResult().getManualConfirmation().getOriginalEvidenceResource());
    }

    @Test
    void alreadyAutomaticallyClassifiedResultCannotBeReconfirmedAsManual() throws Exception {
        TaxonomyResult automatic = taxonomyResult();
        automatic.setStatus(STATUS_CONFIDENT);
        automatic.setBest(automatic.getCandidates().get(0));
        stages.get(TAXONOMY_ID).setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        stages.get(TAXONOMY_ID).setOutputInline(JsonUtil.toJson(automatic));

        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertEquals(0, confirmationWrites);
        assertNull(taxonomyResult().getManualConfirmation());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void rejectsSelectionOutsideStoredCandidates() {
        assertEquals(Result.BUSINESS_FAIL, confirm(999, null).getStatus());
        assertNoConfirmationOrDispatch();
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 4, 27})
    void usesCurrentBackendVersionWithoutRequiringRequestVersion(int currentVersion) {
        stages.get(TAXONOMY_ID).setVersion(currentVersion);

        Result<TaxonomyConfirmationResponse> response = confirm(562, null);
        assertEquals(Result.SUCCESS, response.getStatus());
        assertEquals(currentVersion + 1, response.getData().getVersion());
        assertEquals(currentVersion + 1, stages.get(TAXONOMY_ID).getVersion());
    }

    @Test
    void rejectsMissingBackendVersion() {
        stages.get(TAXONOMY_ID).setVersion(null);

        assertEquals(Result.INTERNAL_FAIL, confirm(562, null).getStatus());
        assertNoConfirmationOrDispatch();
    }

    @Test
    void rejectsBackendVersionOverflow() {
        stages.get(TAXONOMY_ID).setVersion(Integer.MAX_VALUE);

        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertNoConfirmationOrDispatch();
        assertEquals(Integer.MAX_VALUE, stages.get(TAXONOMY_ID).getVersion());
    }

    @Test
    void backendVersionChangeBetweenReadAndWriteDoesNotOverwriteLatestResult() {
        advanceTaxonomyVersionBeforeUpdate = true;
        String originalResult = stages.get(TAXONOMY_ID).getOutputInline();

        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertEquals(5, stages.get(TAXONOMY_ID).getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_ACTION_REQUIRED, stages.get(TAXONOMY_ID).getStatus());
        assertEquals(originalResult, stages.get(TAXONOMY_ID).getOutputInline());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, stages.get(SEROTYPE_ID).getStatus());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void rejectsStageNotWaitingForConfirmation() {
        stages.get(TAXONOMY_ID).setStatus(PIPELINE_STAGE_STATUS_RUNNING);

        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertEquals(0, confirmationWrites);
        assertEquals(PIPELINE_STAGE_STATUS_RUNNING, stages.get(TAXONOMY_ID).getStatus());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void rejectsStageThatIsNotTaxonomy() {
        stages.get(TAXONOMY_ID).setStageType(PIPELINE_STAGE_AMR);
        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertNoConfirmationOrDispatch();
    }

    @Test
    void rejectsMissingStage() {
        stages.remove(TAXONOMY_ID);
        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertEquals(0, confirmationWrites);
        verifyNoInteractions(dispatcher);
    }

    @Test
    void rejectsPipelineThatIsNotRunning() {
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_PENDING);
        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertNoConfirmationOrDispatch();
    }

    @Test
    void rejectsMissingResultOrCandidatesWithoutChoosingArbitrarily() throws Exception {
        stages.get(TAXONOMY_ID).setOutputInline(null);
        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        TaxonomyResult noCandidates = new TaxonomyResult();
        noCandidates.setStatus(STATUS_AMBIGUOUS);
        stages.get(TAXONOMY_ID).setOutputInline(JsonUtil.toJson(noCandidates));
        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertNoConfirmationOrDispatch();
    }

    @Test
    void malformedResultRollsBackWithoutDispatch() {
        stages.get(TAXONOMY_ID).setOutputInline("{broken json");
        assertEquals(Result.INTERNAL_FAIL, confirm(562, null).getStatus());
        assertEquals(1, rollbacks);
        assertNoConfirmationOrDispatch();
    }

    @Test
    void repeatedSameConfirmationPreservesAuditAndDoesNotDuplicateTask() throws Exception {
        assertEquals(Result.SUCCESS, confirm(562, "first review").getStatus());
        String original = stages.get(TAXONOMY_ID).getOutputInline();
        Result<TaxonomyConfirmationResponse> repeated = service.confirmTaxonomy(
                request(562, "duplicate attempt"), "another user");

        assertEquals(Result.SUCCESS, repeated.getStatus());
        assertTrue(repeated.getData().isAlreadyConfirmed());
        assertEquals(5, repeated.getData().getVersion());
        assertEquals(1, confirmationWrites);
        assertEquals(original, stages.get(TAXONOMY_ID).getOutputInline());
        assertEquals(4, stages.get(SEROTYPE_ID).getVersion());
        assertEquals(Set.of(SEROTYPE_ID), queuedIds);
    }

    @Test
    void cannotChangeConfirmedSpecies() {
        assertEquals(Result.SUCCESS, confirm(562, null).getStatus());
        assertEquals(Result.BUSINESS_FAIL, confirm(623, null).getStatus());
        assertEquals(1, confirmationWrites);
        assertEquals(5, stages.get(TAXONOMY_ID).getVersion());
    }

    @Test
    void repeatedConfirmationWorksAfterPipelineCompletion() {
        assertEquals(Result.SUCCESS, confirm(623, null).getStatus());
        Result<TaxonomyConfirmationResponse> repeated = confirm(623, null);
        assertEquals(Result.SUCCESS, repeated.getStatus());
        assertTrue(repeated.getData().isAlreadyConfirmed());
        assertEquals(1, confirmationWrites);
        verifyNoInteractions(dispatcher);
    }

    @Test
    void retriesEnqueueWithoutRewritingCommittedConfirmation() {
        queueFull = true;
        Result<TaxonomyConfirmationResponse> first = confirm(562, null);
        assertEquals(Result.INTERNAL_FAIL, first.getStatus());
        assertNotNull(first.getData());
        assertFalse(first.getData().isDownstreamScheduled());
        assertEquals(5, first.getData().getVersion());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stages.get(TAXONOMY_ID).getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, stages.get(SEROTYPE_ID).getStatus());
        assertTrue(queuedIds.isEmpty());

        queueFull = false;
        Result<TaxonomyConfirmationResponse> retry = confirm(562, null);
        assertEquals(Result.SUCCESS, retry.getStatus());
        assertTrue(retry.getData().isAlreadyConfirmed());
        assertTrue(retry.getData().isDownstreamScheduled());
        assertEquals(1, confirmationWrites);
        assertEquals(4, stages.get(SEROTYPE_ID).getVersion());
        assertEquals(Set.of(SEROTYPE_ID), queuedIds);
    }

    @Test
    void retriesDownstreamCompareAndSetFailureWithoutLosingConfirmation() {
        failNextDownstreamUpdate = true;
        Result<TaxonomyConfirmationResponse> first = confirm(562, null);
        assertEquals(Result.INTERNAL_FAIL, first.getStatus());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, stages.get(SEROTYPE_ID).getStatus());
        assertTrue(queuedIds.isEmpty());
        assertEquals(1, rollbacks);

        Result<TaxonomyConfirmationResponse> retry = confirm(562, null);
        assertEquals(Result.SUCCESS, retry.getStatus());
        assertTrue(retry.getData().isAlreadyConfirmed());
        assertEquals(1, confirmationWrites);
        assertEquals(Set.of(SEROTYPE_ID), queuedIds);
    }

    @Test
    void completionPersistenceFailureIsReportedAndCanBeRetried() {
        failPipelineCompletion = true;
        Result<TaxonomyConfirmationResponse> first = confirm(623, null);
        assertEquals(Result.INTERNAL_FAIL, first.getStatus());
        assertNotNull(first.getData());
        assertFalse(first.getData().isDownstreamScheduled());
        assertEquals(PipelineService.PIPELINE_STATUS_RUNNING, pipeline.getStatus());

        failPipelineCompletion = false;
        assertEquals(Result.SUCCESS, confirm(623, null).getStatus());
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, pipeline.getStatus());
        assertEquals(1, confirmationWrites);
        verifyNoInteractions(dispatcher);
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_STAGE_STATUS_RUNNING, PIPELINE_STAGE_STATUS_FINISHED,
            PIPELINE_STAGE_STATUS_NOT_APPLICABLE, PIPELINE_STAGE_STATUS_FAIL})
    void repeatedConfirmationDoesNotRestartPreviouslyScheduledBranch(int status) {
        assertEquals(Result.SUCCESS, confirm(562, null).getStatus());
        stages.get(SEROTYPE_ID).setStatus(status);
        clearInvocations(dispatcher);

        assertEquals(Result.SUCCESS, confirm(562, null).getStatus());
        assertEquals(status, stages.get(SEROTYPE_ID).getStatus());
        assertEquals(4, stages.get(SEROTYPE_ID).getVersion());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void sameConcurrentChoiceIsIdempotentAndPreservesWinningReview() throws Exception {
        concurrentTaxId = 562;
        Result<TaxonomyConfirmationResponse> response = confirm(562, null);
        assertEquals(Result.SUCCESS, response.getStatus());
        assertTrue(response.getData().isAlreadyConfirmed());
        assertEquals("concurrent reviewer", taxonomyResult().getManualConfirmation().getConfirmedBy());
        assertEquals(Set.of(SEROTYPE_ID), queuedIds);
    }

    @Test
    void differentConcurrentChoiceReturnsConflictWithoutSchedulingLosingChoice() throws Exception {
        concurrentTaxId = 623;
        assertEquals(Result.BUSINESS_FAIL, confirm(562, null).getStatus());
        assertEquals(623, taxonomyResult().getBest().getTaxid());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, stages.get(SEROTYPE_ID).getStatus());
        verifyNoInteractions(dispatcher);
    }

    @Test
    void directServiceCallAlsoValidatesRequestBeforeStartingTransaction() {
        assertEquals(Result.PARAMETER_NOT_VALID, service.confirmTaxonomy(null, null).getStatus());
        assertEquals(Result.PARAMETER_NOT_VALID,
                service.confirmTaxonomy(new ConfirmTaxonomyRequest(TAXONOMY_ID, null, null), null).getStatus());
        assertEquals(Result.PARAMETER_NOT_VALID, confirm(0, null).getStatus());
        assertEquals(Result.PARAMETER_NOT_VALID,
                service.confirmTaxonomy(new ConfirmTaxonomyRequest(null, 562, null), null).getStatus());
        assertEquals(Result.PARAMETER_NOT_VALID, confirm(562, "x".repeat(2001)).getStatus());
        verifyNoInteractions(transactionManager, stageMapper, dispatcher);
    }

    private Result<TaxonomyConfirmationResponse> confirm(int taxId, String reason) {
        return service.confirmTaxonomy(request(taxId, reason), "reviewer");
    }

    private static ConfirmTaxonomyRequest request(int taxId, String reason) {
        return new ConfirmTaxonomyRequest(TAXONOMY_ID, taxId, reason);
    }

    private TaxonomyResult taxonomyResult() throws Exception {
        return JsonUtil.toObject(stages.get(TAXONOMY_ID).getOutputInline(), TaxonomyResult.class);
    }

    private void assertNoConfirmationOrDispatch() {
        assertEquals(0, confirmationWrites);
        assertEquals(PIPELINE_STAGE_STATUS_ACTION_REQUIRED, stages.get(TAXONOMY_ID).getStatus());
        verifyNoInteractions(dispatcher);
    }

    private static BioPipelineStage stage(long stageId, int type, int status, int version) {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(stageId);
        stage.setPipelineId(10L);
        stage.setStageType(type);
        stage.setStageIndex(-1);
        stage.setStatus(status);
        stage.setVersion(version);
        return stage;
    }

    private static Taxon taxon(int taxId, String name, double score) {
        Taxon taxon = new Taxon();
        taxon.setTaxid(taxId);
        taxon.setName(name);
        taxon.setScore(score);
        // Deliberately no rank: old results can lack this field.
        return taxon;
    }

    private static <T> T copy(T value, Class<T> type) throws Exception {
        return value == null ? null : JsonUtil.toObject(JsonUtil.toJson(value), type);
    }

    private static void applyPatch(BioPipelineStage stored, BioPipelineStage patch) {
        if (patch.getVersion() != null) stored.setVersion(patch.getVersion());
        if (patch.getStatus() != null) stored.setStatus(patch.getStatus());
        if (patch.getOutputInline() != null) stored.setOutputInline(patch.getOutputInline());
        if (patch.getEndTime() != null) stored.setEndTime(patch.getEndTime());
        if (patch.getInputUrl() != null) stored.setInputUrl(patch.getInputUrl());
        if (patch.getParameters() != null) stored.setParameters(patch.getParameters());
    }
}
