package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.entity.BioReferenceSequenceExample;
import com.xjtlu.bio.entity.BioReferenceSequenceExample.Criterion;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.requestParameters.AnalysisPipelineParameters;
import com.xjtlu.bio.utils.JsonUtil;

/** Checks metadata lookup and parameter snapshots without Spring, a database, or analysis tools. */
class PipelineServiceReferenceTaxonomyTest {

    private static final int INFLUENZA_A = 11320;
    private static final int INFLUENZA_B = 11520;
    private static final int COVID_TAX_ID = 2697049;
    private static final int UNCONFIGURED_TAX_ID = Integer.MAX_VALUE;
    private static final Set<Integer> INFLUENZA_A_MEMBERS = Set.of(
            641809, 93838, 130760, 488241, 335341, 211044, 1332244);

    private PipelineService service;
    private BioReferenceSequenceMapper referenceMapper;
    private BioReferenceTaxonomyMemberMapper memberMapper;

    @BeforeEach
    void setUp() {
        service = new PipelineService();
        referenceMapper = mock(BioReferenceSequenceMapper.class);
        memberMapper = mock(BioReferenceTaxonomyMemberMapper.class);
        ReflectionTestUtils.setField(service, "bioReferenceSequenceMapper", referenceMapper);
        ReflectionTestUtils.setField(service, "referenceTaxonomyResolver",
                new ReferenceTaxonomyResolver(memberMapper));
        when(memberMapper.selectMemberTaxIds(any())).thenReturn(List.of());
        when(memberMapper.selectMemberTaxIds(INFLUENZA_A))
                .thenReturn(List.of(641809, 93838, 130760, 488241, 335341, 211044, 1332244));
        when(memberMapper.selectMemberTaxIds(INFLUENZA_B)).thenReturn(List.of(518987));
        when(referenceMapper.selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class)))
                .thenReturn(List.of());
    }

    @Test
    void configuredTargetQueriesOnlyItsExplicitMembers() {
        findCandidates(PIPELINE_VIRUS, INFLUENZA_A);

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id in", criterion.getCondition());
        List<?> members = (List<?>) criterion.getValue();
        assertEquals(7, members.size());
        assertEquals(INFLUENZA_A_MEMBERS, Set.copyOf(members));
        assertFalse(members.contains(INFLUENZA_A),
                "The target is not an extra member of the database mapping");
        verify(memberMapper).selectMemberTaxIds(INFLUENZA_A);
        verifyNoMoreInteractions(memberMapper);
    }

    @Test
    void singleMemberMappingStillQueriesTheMemberInsteadOfTheTarget() {
        findCandidates(PIPELINE_VIRUS, INFLUENZA_B);

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id in", criterion.getCondition());
        assertEquals(List.of(518987), criterion.getValue());
    }

    @Test
    void unconfiguredTargetKeepsTheExistingExactLookup() {
        findCandidates(PIPELINE_VIRUS, UNCONFIGURED_TAX_ID);

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id =", criterion.getCondition());
        assertEquals(UNCONFIGURED_TAX_ID, criterion.getValue());
    }

    @Test
    void explicitSelfMemberStillUsesExactLookup() {
        when(memberMapper.selectMemberTaxIds(INFLUENZA_A)).thenReturn(List.of(INFLUENZA_A));

        findCandidates(PIPELINE_VIRUS, INFLUENZA_A);

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id =", criterion.getCondition());
        assertEquals(INFLUENZA_A, criterion.getValue());
        verify(memberMapper).selectMemberTaxIds(INFLUENZA_A);
    }

    @Test
    void memberTableUpdatesApplyToTheNextQueryWithoutRecreatingTheService() {
        when(memberMapper.selectMemberTaxIds(INFLUENZA_A))
                .thenReturn(List.of(641809), List.of(130760, 488241), List.of());

        findCandidates(PIPELINE_VIRUS, INFLUENZA_A);
        findCandidates(PIPELINE_VIRUS, INFLUENZA_A);
        findCandidates(PIPELINE_VIRUS, INFLUENZA_A);

        ArgumentCaptor<BioReferenceSequenceExample> captor =
                ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(referenceMapper, times(3)).selectByExampleWithBLOBs(captor.capture());
        List<BioReferenceSequenceExample> queries = captor.getAllValues();
        Criterion first = taxIdCriterion(queries.get(0));
        assertEquals("tax_id in", first.getCondition());
        assertEquals(List.of(641809), first.getValue());
        Criterion updated = taxIdCriterion(queries.get(1));
        assertEquals("tax_id in", updated.getCondition());
        assertEquals(List.of(130760, 488241), updated.getValue());
        Criterion removed = taxIdCriterion(queries.get(2));
        assertEquals("tax_id =", removed.getCondition());
        assertEquals(INFLUENZA_A, removed.getValue());
        verify(memberMapper, times(3)).selectMemberTaxIds(INFLUENZA_A);
        verifyNoMoreInteractions(memberMapper, referenceMapper);
    }

    @Test
    void memberTableFailurePropagatesWithoutQueryingReferences() {
        DataAccessResourceFailureException failure =
                new DataAccessResourceFailureException("Fixture member table unavailable");
        when(memberMapper.selectMemberTaxIds(INFLUENZA_A)).thenThrow(failure);

        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> findCandidates(PIPELINE_VIRUS, INFLUENZA_A)));

        verify(memberMapper).selectMemberTaxIds(INFLUENZA_A);
        verifyNoMoreInteractions(memberMapper);
        verifyNoInteractions(referenceMapper);
    }

    @Test
    void covidUsesItsFixedTargetInsteadOfTheRequestTarget() {
        findCandidates(PIPELINE_VIRUS_COVID, INFLUENZA_A);

        assertOnlyQueriedTaxId(COVID_TAX_ID);
        verify(memberMapper).selectMemberTaxIds(COVID_TAX_ID);
        verifyNoMoreInteractions(memberMapper);
    }

    @Test
    void covidResolvesItsFixedTargetWhenTheRequestOmitsTaxId() {
        findCandidates(PIPELINE_VIRUS_COVID, null);

        assertOnlyQueriedTaxId(COVID_TAX_ID);
        verify(memberMapper).selectMemberTaxIds(COVID_TAX_ID);
        verifyNoMoreInteractions(memberMapper);
    }

    @Test
    void covidExpandsMembersOnlyForItsFixedTarget() {
        when(memberMapper.selectMemberTaxIds(COVID_TAX_ID)).thenReturn(List.of(700001, 700002));

        findCandidates(PIPELINE_VIRUS_COVID, INFLUENZA_A);

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id in", criterion.getCondition());
        assertEquals(List.of(700001, 700002), criterion.getValue());
        verify(memberMapper).selectMemberTaxIds(COVID_TAX_ID);
        verifyNoMoreInteractions(memberMapper);
    }

    @Test
    void bacteriaRetainsExactLookupWithoutUsingTheVirusResolver() {
        findCandidates(PIPELINE_REGULAR_BACTERIA, INFLUENZA_A);

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id =", criterion.getCondition());
        assertEquals(INFLUENZA_A, criterion.getValue());
        verifyNoInteractions(memberMapper);
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_VIRUS, PIPELINE_REGULAR_BACTERIA})
    void absentTargetDoesNotIssueAnUnrestrictedQuery(int pipelineType) {
        assertTrue(findCandidates(pipelineType, null).isEmpty());

        verifyNoInteractions(memberMapper, referenceMapper);
    }

    @Test
    void emptyMappedResultDoesNotFallBackToAnUnconfiguredQuery() {
        assertTrue(findCandidates(PIPELINE_VIRUS, INFLUENZA_A).isEmpty());

        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id in", criterion.getCondition());
        verifyNoMoreInteractions(referenceMapper);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void exactAndMappedQueriesExcludeDisabledAndOutOfScopeRecords(boolean mappedTarget) {
        int targetTaxId = mappedTarget ? INFLUENZA_A : UNCONFIGURED_TAX_ID;
        int memberTaxId = mappedTarget ? 641809 : targetTaxId;
        BioReferenceSequence enabled = reference(901L, memberTaxId, "ENABLED", "Fixture enabled");
        BioReferenceSequence disabled = reference(902L, memberTaxId, "DISABLED", "Fixture disabled");
        disabled.setEnabled(false);
        BioReferenceSequence unspecified = reference(903L, memberTaxId, "UNSPECIFIED", "Fixture unspecified");
        unspecified.setEnabled(null);
        BioReferenceSequence unrelated = reference(904L, 7, "OTHER_TARGET", "Fixture other target");
        simulateCandidateQuery(List.of(enabled, disabled, unspecified, unrelated));

        assertEquals(List.of(enabled), findCandidates(PIPELINE_VIRUS, targetTaxId));
    }

    @Test
    void changingEnabledStateAffectsNextLookupWithoutChangingExistingSnapshot() throws Exception {
        BioReferenceSequence first = reference(901L, UNCONFIGURED_TAX_ID, "FIRST", "Fixture first");
        BioReferenceSequence second = reference(902L, UNCONFIGURED_TAX_ID, "SECOND", "Fixture second");
        second.setEnabled(false);
        simulateCandidateQuery(List.of(first, second));
        List<BioReferenceSequence> originalCandidates = findCandidates(PIPELINE_VIRUS, UNCONFIGURED_TAX_ID);
        BioPipelineStage selection = buildSelection(originalCandidates, SEQUENCE_LEVEL_ASSEMBLY, UNCONFIGURED_TAX_ID);
        String savedParameters = selection.getParameters();

        first.setEnabled(false);
        second.setEnabled(true);

        assertEquals(List.of(second), findCandidates(PIPELINE_VIRUS, UNCONFIGURED_TAX_ID));
        assertEquals(savedParameters, selection.getParameters());
        assertEquals(List.of("FIRST"), candidateSnapshot(selection).stream().map(ReferenceSequence::getAccession).toList());
        assertFalse(JsonUtil.toMap(JsonUtil.toJson(candidateSnapshot(selection).get(0))).containsKey("enabled"),
                "A historical reference snapshot must not become a live availability check");
        verify(referenceMapper, times(2)).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
    }

    @Test
    void restartingExistingStageUsesSavedReferencesWithoutLoadingAvailabilityAgain() throws Exception {
        BioReferenceSequence first = reference(901L, UNCONFIGURED_TAX_ID, "SAVED", "Fixture saved");
        BioPipelineStage selection = buildSelection(List.of(first), SEQUENCE_LEVEL_ASSEMBLY, UNCONFIGURED_TAX_ID);
        selection.setStageId(410L);
        selection.setVersion(5);
        selection.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        String savedParameters = selection.getParameters();
        first.setEnabled(false);

        BioAnalysisStageMapperExtension stagesMapper = mock(BioAnalysisStageMapperExtension.class);
        when(stagesMapper.selectAllPipelineStagesByStageId(410L)).thenReturn(List.of(selection));
        PipelineStageTaskDispatcher dispatcher = mock(PipelineStageTaskDispatcher.class);
        ReflectionTestUtils.setField(service, "bioAnalysisStageMapperExtension", stagesMapper);
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "stageOrchestrator", new StageOrchestrator());
        service = spy(service);
        doReturn(0).when(service).batchUpdateStages(anyList());

        Result<Boolean> restarted = service.restartStage(410L);

        assertEquals(Result.SUCCESS, restarted.getStatus());
        assertEquals(Boolean.TRUE, restarted.getData());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, selection.getStatus());
        assertEquals(savedParameters, selection.getParameters());
        assertEquals("SAVED", candidateSnapshot(selection).get(0).getAccession());
        verify(dispatcher).addTask(selection);
        verifyNoInteractions(referenceMapper, memberMapper);
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_VIRUS, PIPELINE_VIRUS_COVID})
    void onlyDisabledCandidatesPreventStartWithExplicitMessageBeforeStageWrites(int pipelineType) throws Exception {
        int candidateTaxId = pipelineType == PIPELINE_VIRUS_COVID ? COVID_TAX_ID : UNCONFIGURED_TAX_ID;
        BioReferenceSequence disabled = reference(901L, candidateTaxId, "DISABLED", "Fixture disabled");
        disabled.setEnabled(false);
        simulateCandidateQuery(List.of(disabled));
        BioPipelineStageMapper stageMapper = mock(BioPipelineStageMapper.class);
        TransactionTemplate transaction = mock(TransactionTemplate.class);
        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stageMapper);
        ReflectionTestUtils.setField(service, "rcTransactionTemplate", transaction);
        BioAnalysisPipeline pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineId(123L);
        pipeline.setPipelineType(pipelineType);
        pipeline.setPipelineParameters("{\"taxId\":" + UNCONFIGURED_TAX_ID + "}");
        BioPipelineInputFile input = new BioPipelineInputFile();
        input.setFileRole(PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_READ);
        input.setInputKey("sample/0/0");
        input.setFilePath("fixture.fastq");

        Result<?> started = ReflectionTestUtils.invokeMethod(service, "startRegularPipeline", pipeline, List.of(input));

        assertNotNull(started);
        assertEquals(Result.BUSINESS_FAIL, started.getStatus());
        assertTrue(started.getFailMsg().contains("已启用"));
        verifyNoInteractions(stageMapper, transaction);
    }

    @ParameterizedTest
    @ValueSource(ints = {SEQUENCE_LEVEL_READ, SEQUENCE_LEVEL_ASSEMBLY})
    void candidateSnapshotsKeepOriginalTaxIdsAndDoNotRewriteTheTarget(int sequenceLevel)
            throws Exception {
        BioReferenceSequence first = reference(901L, 641809, "TEST_REFERENCE_A", "Fixture A");
        BioReferenceSequence second = reference(902L, 93838, "TEST_REFERENCE_B", "Fixture B");
        when(referenceMapper.selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class)))
                .thenReturn(List.of(first, second));
        List<BioReferenceSequence> candidates = findCandidates(PIPELINE_VIRUS, INFLUENZA_A);

        AnalysisPipelineParameters parameters = new AnalysisPipelineParameters();
        parameters.setTaxId(INFLUENZA_A);
        PipelineSampleInput input = new PipelineSampleInput(
                sequenceLevel == SEQUENCE_LEVEL_READ ? "fixture.fastq" : "fixture.fasta", null);
        input.setSequenceLevel(sequenceLevel);

        List<BioPipelineStage> stages = ReflectionTestUtils.invokeMethod(service,
                "buildRegularPipelineStages", 123L, PIPELINE_VIRUS, candidates, parameters, input);
        assertNotNull(stages);
        BioPipelineStage selection = stages.stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION)
                .findFirst().orElseThrow();

        assertEquals(INFLUENZA_A, parameters.getTaxId());
        assertEquals(641809, first.getTaxId());
        assertEquals(93838, second.getTaxId());
        List<ReferenceSequence> snapshot = candidateSnapshot(selection);
        assertEquals(List.of(641809, 93838),
                snapshot.stream().map(ReferenceSequence::getTaxId).toList());
        assertEquals(List.of("TEST_REFERENCE_A", "TEST_REFERENCE_B"),
                snapshot.stream().map(ReferenceSequence::getAccession).toList());
        assertEquals(first.getRawMetadata(), snapshot.get(0).getRawMetadata());
        assertEquals(first.getPath(), snapshot.get(0).getPath());

        first.setTaxId(12345);
        first.setEnabled(false);
        first.setOrganismName("Changed after building the stage");
        first.setRawMetadata("{\"fixture\":\"changed\"}");
        List<ReferenceSequence> preserved = candidateSnapshot(selection);
        assertEquals(641809, preserved.get(0).getTaxId());
        assertEquals("Fixture A", preserved.get(0).getOrganismName());
        assertEquals("{\"fixture\":\"metadata-only\"}", preserved.get(0).getRawMetadata());
        assertEquals(INFLUENZA_A, parameters.getTaxId());
    }

    private List<BioReferenceSequence> findCandidates(int pipelineType, Integer taxId) {
        List<BioReferenceSequence> candidates = ReflectionTestUtils.invokeMethod(service,
                "findCandidateReferences", pipelineType, taxId);
        assertNotNull(candidates);
        return candidates;
    }

    private Criterion queriedTaxIdCriterion() {
        ArgumentCaptor<BioReferenceSequenceExample> captor =
                ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(referenceMapper).selectByExampleWithBLOBs(captor.capture());
        return taxIdCriterion(captor.getValue());
    }

    private static Criterion taxIdCriterion(BioReferenceSequenceExample query) {
        assertEquals(1, query.getOredCriteria().size(),
                "Member expansion must not add an unrestricted alternative");
        List<Criterion> criteria = query.getOredCriteria().get(0).getAllCriteria();
        assertEquals(2, criteria.size(), "The same AND group must contain both taxId and enabled predicates");
        Criterion enabled = criteria.stream().filter(criterion -> "enabled =".equals(criterion.getCondition()))
                .findFirst().orElseThrow();
        assertEquals(Boolean.TRUE, enabled.getValue());
        return criteria.stream().filter(criterion -> criterion.getCondition().startsWith("tax_id "))
                .findFirst().orElseThrow();
    }

    private void simulateCandidateQuery(List<BioReferenceSequence> records) {
        when(referenceMapper.selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class)))
                .thenAnswer(invocation -> {
                    BioReferenceSequenceExample query = invocation.getArgument(0);
                    Criterion taxId = taxIdCriterion(query);
                    Criterion enabled = query.getOredCriteria().get(0).getAllCriteria().stream()
                            .filter(criterion -> "enabled =".equals(criterion.getCondition())).findFirst().orElseThrow();
                    return records.stream()
                            .filter(record -> Objects.equals(record.getEnabled(), enabled.getValue()))
                            .filter(record -> "tax_id in".equals(taxId.getCondition())
                                    ? ((List<?>) taxId.getValue()).contains(record.getTaxId())
                                    : Objects.equals(record.getTaxId(), taxId.getValue()))
                            .toList();
                });
    }

    private BioPipelineStage buildSelection(List<BioReferenceSequence> candidates, int sequenceLevel, int targetTaxId) {
        AnalysisPipelineParameters parameters = new AnalysisPipelineParameters();
        parameters.setTaxId(targetTaxId);
        PipelineSampleInput input = new PipelineSampleInput(
                sequenceLevel == SEQUENCE_LEVEL_READ ? "fixture.fastq" : "fixture.fasta", null);
        input.setSequenceLevel(sequenceLevel);
        List<BioPipelineStage> stages = ReflectionTestUtils.invokeMethod(service,
                "buildRegularPipelineStages", 123L, PIPELINE_VIRUS, candidates, parameters, input);
        assertNotNull(stages);
        return stages.stream().filter(stage -> stage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION)
                .findFirst().orElseThrow();
    }

    private void assertOnlyQueriedTaxId(int expectedTaxId) {
        Criterion criterion = queriedTaxIdCriterion();
        assertEquals("tax_id =", criterion.getCondition());
        assertEquals(expectedTaxId, criterion.getValue());
    }

    private static List<ReferenceSequence> candidateSnapshot(BioPipelineStage stage) throws Exception {
        ReferenceSelectionStageParameters parameters = JsonUtil.toObject(
                stage.getParameters(), ReferenceSelectionStageParameters.class);
        assertNotNull(parameters.getCandidateReferences());
        return parameters.getCandidateReferences();
    }

    private static BioReferenceSequence reference(long id, int taxId, String accession,
            String organismName) {
        BioReferenceSequence reference = new BioReferenceSequence();
        reference.setReferenceId(id);
        reference.setTaxId(taxId);
        reference.setAccession(accession);
        reference.setOrganismName(organismName);
        reference.setPath("metadata-fixture/" + accession + ".fna");
        reference.setRawMetadata("{\"fixture\":\"metadata-only\"}");
        reference.setEnabled(true);
        return reference;
    }
}
