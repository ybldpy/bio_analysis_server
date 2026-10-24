package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioProject;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.entity.BioReferenceSequenceExample;
import com.xjtlu.bio.entity.BioReferenceSequenceExample.Criterion;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;
import com.xjtlu.bio.requestParameters.AnalysisPipelineParameters;
import com.xjtlu.bio.requestParameters.BatchCreateAnalysisPipelineRequest;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.utils.JsonUtil;

/** Synthetic metadata only: no Spring context, database, files, or analysis executors. */
class PipelineServiceEnabledReferencesTest {

    private static final int TARGET = 700001;
    private static final int MEMBER = 700002;
    private static final int OTHER_TARGET = 700003;
    private static final int COVID_TARGET = 2697049;

    private PipelineService service;
    private BioAnalysisPipelineMapper pipelineMapper;
    private BioPipelineStageMapper stageMapper;
    private BioReferenceSequenceMapper referenceMapper;
    private BioReferenceTaxonomyMemberMapper memberMapper;
    private PipelineInputService inputService;
    private PipelineStageTaskDispatcher dispatcher;
    private TransactionTemplate transaction;
    private final Map<Long, BioAnalysisPipeline> storedPipelines = new HashMap<>();
    private final List<BioReferenceSequence> references = new ArrayList<>();

    @BeforeEach
    void setUp() {
        service = spy(new PipelineService());
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        stageMapper = mock(BioPipelineStageMapper.class);
        referenceMapper = mock(BioReferenceSequenceMapper.class);
        memberMapper = mock(BioReferenceTaxonomyMemberMapper.class);
        inputService = mock(PipelineInputService.class);
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        transaction = mock(TransactionTemplate.class);
        ReflectionTestUtils.setField(service, "analysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stageMapper);
        ReflectionTestUtils.setField(service, "bioReferenceSequenceMapper", referenceMapper);
        ReflectionTestUtils.setField(service, "referenceTaxonomyResolver",
                new ReferenceTaxonomyResolver(memberMapper));
        ReflectionTestUtils.setField(service, "pipelineInputService", inputService);
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "stageOrchestrator", new StageOrchestrator());
        ReflectionTestUtils.setField(service, "rcTransactionTemplate", transaction);
        ProjectService projects = mock(ProjectService.class);
        doReturn(List.of(new BioProject())).when(projects).queryProjects(any());
        ReflectionTestUtils.setField(service, "projectService", projects);

        doReturn(List.of()).when(memberMapper).selectMemberTaxIds(any());
        references.add(reference(901L, TARGET));
        references.add(reference(902L, TARGET));
        references.add(reference(903L, TARGET));
        doAnswer(invocation -> selectReferences(invocation.getArgument(0)))
                .when(referenceMapper).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));

        AtomicLong ids = new AtomicLong(100L);
        doAnswer(invocation -> {
            BioAnalysisPipeline pipeline = invocation.getArgument(0);
            pipeline.setPipelineId(ids.incrementAndGet());
            storedPipelines.put(pipeline.getPipelineId(), pipeline);
            return 1;
        }).when(pipelineMapper).insertSelective(any());
        doAnswer(invocation -> storedPipelines.get(invocation.getArgument(0)))
                .when(pipelineMapper).selectByPrimaryKey(anyLong());
        doReturn(1).when(pipelineMapper).updateByExampleSelective(any(), any());
        doAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(mock(TransactionStatus.class));
        }).when(transaction).execute(any());
        doReturn(true).when(inputService).lockDownPipelineUploading(anyLong());
        doReturn(List.of(input(PIPELINE_INPUT_TYPE_SEQUENCE_READ))).when(inputService).queryInputs(any());
        doAnswer(invocation -> {
            BioPipelineStage stage = invocation.getArgument(0);
            stage.setStageId(ids.incrementAndGet());
            return 1;
        }).when(stageMapper).insertSelective(any());
        doReturn(0).when(service).batchUpdateStages(anyList());
    }

    @Test
    void singleCreationPreservesExplicitIdsAcrossRequestStorageAndReload() throws Exception {
        CreateAnalysisPipelineRequest request = request(PIPELINE_VIRUS, "[902,901]");

        Result<Long> created = service.createPipeline(request);

        assertEquals(Result.SUCCESS, created.getStatus());
        AnalysisPipelineParameters restored = restoredParameters(created.getData());
        assertEquals(List.of(902L, 901L), request.getPipelineStageParameters().getEnabledReferenceIds());
        assertEquals(List.of(902L, 901L), restored.getEnabledReferenceIds());
        assertEquals(TARGET, restored.getTaxId());
        verify(referenceMapper).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    @ParameterizedTest
    @ValueSource(strings = {"omitted", "null"})
    void absentOrNullIdsDoNotQueryOnCreateAndInheritCurrentCandidatesAtStart(String field) throws Exception {
        Result<Long> created = service.createPipeline(request(PIPELINE_VIRUS, field));

        assertEquals(Result.SUCCESS, created.getStatus());
        assertNull(restoredParameters(created.getData()).getEnabledReferenceIds());
        verifyNoInteractions(referenceMapper, memberMapper);
        references.get(0).setEnabled(false);

        assertEquals(Result.SUCCESS, service.startPipeline(created.getData()).getStatus());
        assertEquals(List.of(902L, 903L), snapshotIds(selectionStage()));
        BioReferenceSequenceExample query = lastQuery();
        assertEquals(2, query.getOredCriteria().get(0).getAllCriteria().size());
        assertFalse(query.getOredCriteria().get(0).getAllCriteria().stream()
                .anyMatch(criterion -> criterion.getCondition().startsWith("reference_id")));
        verifyNoMoreInteractions(referenceMapper);
    }

    @Test
    void explicitSubsetKeepsRequestOrderAndUsesOneAndGroup() {
        List<BioReferenceSequence> selected = findCandidates(PIPELINE_VIRUS, TARGET, List.of(902L, 901L));

        assertEquals(List.of(902L, 901L), selected.stream().map(BioReferenceSequence::getReferenceId).toList());
        BioReferenceSequenceExample query = lastQuery();
        assertEquals(1, query.getOredCriteria().size());
        assertEquals(3, query.getOredCriteria().get(0).getAllCriteria().size());
        assertEquals(Boolean.TRUE, criterion(query, "enabled =").getValue());
        assertEquals(TARGET, criterion(query, "tax_id =").getValue());
        assertEquals(List.of(902L, 901L), criterion(query, "reference_id in").getValue());
        verifyNoMoreInteractions(referenceMapper);
    }

    @Test
    void mappedTargetAcceptsOnlyItsExplicitMembersWithoutAddingParent() {
        doReturn(List.of(MEMBER)).when(memberMapper).selectMemberTaxIds(TARGET);
        references.get(0).setTaxId(MEMBER);

        assertEquals(List.of(901L), findCandidates(PIPELINE_VIRUS, TARGET, List.of(901L)).stream()
                .map(BioReferenceSequence::getReferenceId).toList());
        assertThrows(IllegalArgumentException.class,
                () -> findCandidates(PIPELINE_VIRUS, TARGET, List.of(901L, 902L)));
        ArgumentCaptor<BioReferenceSequenceExample> queries = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(referenceMapper, times(2)).selectByExampleWithBLOBs(queries.capture());
        for (BioReferenceSequenceExample query : queries.getAllValues()) {
            assertEquals(List.of(MEMBER), criterion(query, "tax_id in").getValue());
        }
        verifyNoMoreInteractions(referenceMapper);
    }

    @Test
    void explicitSelectionWithoutVirusTargetIsRejectedWithoutUnrestrictedLookup() {
        assertThrows(IllegalArgumentException.class,
                () -> findCandidates(PIPELINE_VIRUS, null, List.of(901L)));

        verifyNoInteractions(referenceMapper, memberMapper);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void covidUsesFixedTargetIncludingWhenRequestTargetIsMissing(boolean missingTaxId) {
        references.get(0).setTaxId(COVID_TARGET);

        assertEquals(List.of(901L), findCandidates(PIPELINE_VIRUS_COVID,
                missingTaxId ? null : TARGET, List.of(901L)).stream()
                        .map(BioReferenceSequence::getReferenceId).toList());

        assertEquals(COVID_TARGET, criterion(lastQuery(), "tax_id =").getValue());
        verify(memberMapper).selectMemberTaxIds(COVID_TARGET);
        verifyNoMoreInteractions(memberMapper, referenceMapper);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "[null]", "[0]", "[-1]", "[901,901]"})
    void malformedExplicitListIsRejectedBeforeCreateInsertOrReferenceLookup(String ids) throws Exception {
        CreateAnalysisPipelineRequest request = request(PIPELINE_VIRUS, "[null]".equals(ids) ? "null" : ids);
        if ("[null]".equals(ids)) {
            // JSON rejects null elements earlier; direct callers still need service-level validation.
            request.getPipelineStageParameters().setEnabledReferenceIds(Arrays.asList((Long) null));
        }

        Result<Long> created = service.createPipeline(request);

        assertEquals(Result.PARAMETER_NOT_VALID, created.getStatus());
        assertNotNull(created.getFailMsg());
        verifyNoInteractions(pipelineMapper, referenceMapper, memberMapper, stageMapper, transaction, dispatcher);
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "disabled", "unspecifiedEnabled", "otherTarget"})
    void anyUnavailableIdRejectsEntireExplicitSelectionWithoutFallback(String failure) {
        BioReferenceSequence unavailable = references.get(1);
        switch (failure) {
            case "missing" -> references.remove(unavailable);
            case "disabled" -> unavailable.setEnabled(false);
            case "unspecifiedEnabled" -> unavailable.setEnabled(null);
            case "otherTarget" -> unavailable.setTaxId(OTHER_TARGET);
            default -> throw new AssertionError(failure);
        }

        assertThrows(IllegalArgumentException.class,
                () -> findCandidates(PIPELINE_VIRUS, TARGET, List.of(901L, 902L)));

        verify(referenceMapper).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    @Test
    void unavailableIdPreventsSingleCreateInsert() throws Exception {
        references.get(1).setEnabled(false);

        Result<Long> created = service.createPipeline(request(PIPELINE_VIRUS, "[901,902]"));

        assertEquals(Result.PARAMETER_NOT_VALID, created.getStatus());
        verifyNoInteractions(pipelineMapper, stageMapper, transaction, dispatcher);
        verify(referenceMapper).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    @ParameterizedTest
    @ValueSource(ints = {102, 200, 300, 401})
    void unsupportedTypesRejectExplicitSelectionAtCreateAndHistoricalStart(int pipelineType) throws Exception {
        Result<Long> created = service.createPipeline(request(pipelineType, "[901]"));
        assertEquals(Result.PARAMETER_NOT_VALID, created.getStatus());
        verifyNoInteractions(pipelineMapper, referenceMapper, memberMapper);

        BioAnalysisPipeline historical = new BioAnalysisPipeline();
        historical.setPipelineId(999L);
        historical.setPipelineType(pipelineType);
        historical.setStatus(PipelineService.PIPELINE_STATUS_PENDING);
        historical.setPipelineParameters("{\"taxId\":" + TARGET + ",\"enabledReferenceIds\":[901]}");
        storedPipelines.put(999L, historical);

        Result<?> started = service.startPipeline(999L);

        assertEquals(Result.PARAMETER_NOT_VALID, started.getStatus());
        assertNoStartWrites();
        verifyNoInteractions(referenceMapper, memberMapper);
        verify(pipelineMapper, never()).insertSelective(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"disabled", "memberRemoved"})
    void changedAvailabilityBetweenCreateAndStartRejectsBeforeAnyStageOrStatusWrites(String change)
            throws Exception {
        doReturn(List.of(MEMBER)).when(memberMapper).selectMemberTaxIds(TARGET);
        references.get(0).setTaxId(MEMBER);
        Result<Long> created = service.createPipeline(request(PIPELINE_VIRUS, "[901]"));
        assertEquals(Result.SUCCESS, created.getStatus());
        if ("disabled".equals(change)) {
            references.get(0).setEnabled(false);
        } else {
            doReturn(List.of(OTHER_TARGET)).when(memberMapper).selectMemberTaxIds(TARGET);
        }

        Result<?> started = service.startPipeline(created.getData());

        assertEquals(Result.PARAMETER_NOT_VALID, started.getStatus());
        assertNoStartWrites();
        assertEquals(PipelineService.PIPELINE_STATUS_PENDING, storedPipelines.get(created.getData()).getStatus());
        verify(referenceMapper, times(2)).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    @ParameterizedTest
    @ValueSource(ints = {PIPELINE_INPUT_TYPE_SEQUENCE_READ, PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY})
    void selectedSubsetBecomesIndependentSnapshotAndExtraParamsCannotOverrideIt(int inputType) throws Exception {
        doReturn(List.of(input(inputType))).when(inputService).queryInputs(any());
        CreateAnalysisPipelineRequest request = request(PIPELINE_VIRUS, "[902,901]");
        request.getPipelineStageParameters().setExtraParams(Map.of("stageParams", Map.of("44", Map.of(
                "candidateReferences", List.of(Map.of("referenceId", 903L, "accession", "INJECTED"))))));
        Result<Long> created = service.createPipeline(request);
        assertEquals(Result.SUCCESS, created.getStatus());

        assertEquals(Result.SUCCESS, service.startPipeline(created.getData()).getStatus());

        BioPipelineStage selection = selectionStage();
        String savedParameters = selection.getParameters();
        assertEquals(List.of(902L, 901L), snapshotIds(selection));
        assertEquals(List.of("FIXTURE_902", "FIXTURE_901"), snapshot(selection).stream()
                .map(ReferenceSequence::getAccession).toList());
        assertEquals("fixture/902.fasta", snapshot(selection).get(0).getPath());
        assertFalse(JsonUtil.toMap(JsonUtil.toJson(snapshot(selection).get(0))).containsKey("enabled"));

        references.get(1).setEnabled(false);
        references.get(1).setAccession("CHANGED_AFTER_START");
        references.get(1).setPath("changed-after-start.fasta");
        references.get(1).setTaxId(OTHER_TARGET);
        doReturn(List.of(OTHER_TARGET)).when(memberMapper).selectMemberTaxIds(TARGET);
        storedPipelines.get(created.getData()).setPipelineParameters(
                "{\"taxId\":" + TARGET + ",\"enabledReferenceIds\":[903]}");

        assertEquals(savedParameters, selection.getParameters());
        assertEquals(List.of(902L, 901L), snapshotIds(selection));
        assertEquals("FIXTURE_902", snapshot(selection).get(0).getAccession());
        assertEquals(TARGET, snapshot(selection).get(0).getTaxId());
        assertEquals("fixture/902.fasta", snapshot(selection).get(0).getPath());
        verify(referenceMapper, times(2)).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    @Test
    void batchCreationPreservesIndependentSelectionsAndNullDefaults() throws Exception {
        BatchCreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":100,"pipelines":[
                  {"pipelineName":"explicit","pipelineParameters":{"taxId":700001,"enabledReferenceIds":[902,901]}},
                  {"pipelineName":"null-default","pipelineParameters":{"taxId":700001,"enabledReferenceIds":null}},
                  {"pipelineName":"omitted-default","pipelineParameters":{"taxId":700001}}
                ]}
                """, BatchCreateAnalysisPipelineRequest.class);

        Result<List<Long>> created = service.batchCreateAnalysisPipelines(request);

        assertEquals(Result.SUCCESS, created.getStatus());
        assertEquals(3, created.getData().size());
        assertEquals(List.of(902L, 901L), restoredParameters(created.getData().get(0)).getEnabledReferenceIds());
        assertNull(restoredParameters(created.getData().get(1)).getEnabledReferenceIds());
        assertNull(restoredParameters(created.getData().get(2)).getEnabledReferenceIds());
        verify(pipelineMapper, times(3)).insertSelective(any());
        verify(referenceMapper).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    @Test
    void invalidLaterBatchItemRejectsWholeBatchBeforeAnyInsert() throws Exception {
        BatchCreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":100,"pipelines":[
                  {"pipelineName":"valid-first","pipelineParameters":{"taxId":700001,"enabledReferenceIds":[901]}},
                  {"pipelineName":"invalid-second","pipelineParameters":{"taxId":700001,"enabledReferenceIds":[902,999]}}
                ]}
                """, BatchCreateAnalysisPipelineRequest.class);

        Result<List<Long>> created = service.batchCreateAnalysisPipelines(request);

        assertEquals(Result.PARAMETER_NOT_VALID, created.getStatus());
        assertTrue(storedPipelines.isEmpty());
        verifyNoInteractions(pipelineMapper, stageMapper, transaction, dispatcher);
        verify(referenceMapper, times(2)).selectByExampleWithBLOBs(any(BioReferenceSequenceExample.class));
        verifyNoMoreInteractions(referenceMapper);
    }

    private CreateAnalysisPipelineRequest request(int pipelineType, String ids) throws Exception {
        String field = "omitted".equals(ids) ? "" : ",\"enabledReferenceIds\":" + ids;
        return JsonUtil.toObject("""
                {"projectId":1,"pipelineType":%d,"analysisName":"reference-fixture",
                 "pipelineStageParameters":{"taxId":%d%s}}
                """.formatted(pipelineType, TARGET, field), CreateAnalysisPipelineRequest.class);
    }

    private AnalysisPipelineParameters restoredParameters(long pipelineId) throws Exception {
        return JsonUtil.toObject(storedPipelines.get(pipelineId).getPipelineParameters(), AnalysisPipelineParameters.class);
    }

    private List<BioReferenceSequence> findCandidates(int pipelineType, Integer taxId, List<Long> ids) {
        List<BioReferenceSequence> result = ReflectionTestUtils.invokeMethod(service,
                "findCandidateReferences", pipelineType, taxId, ids);
        assertNotNull(result);
        return result;
    }

    private List<BioReferenceSequence> selectReferences(BioReferenceSequenceExample query) {
        assertEquals(1, query.getOredCriteria().size(), "Scope predicates must share one AND group");
        assertEquals(Boolean.TRUE, criterion(query, "enabled =").getValue());
        assertTrue(query.getOredCriteria().get(0).getAllCriteria().stream()
                .anyMatch(item -> item.getCondition().startsWith("tax_id ")), "Taxonomy scope is mandatory");
        return references.stream().filter(reference -> query.getOredCriteria().get(0).getAllCriteria().stream()
                .allMatch(item -> matches(reference, item))).toList();
    }

    private static boolean matches(BioReferenceSequence reference, Criterion criterion) {
        return switch (criterion.getCondition()) {
            case "enabled =" -> Objects.equals(reference.getEnabled(), criterion.getValue());
            case "tax_id =" -> Objects.equals(reference.getTaxId(), criterion.getValue());
            case "tax_id in" -> ((List<?>) criterion.getValue()).contains(reference.getTaxId());
            case "reference_id in" -> ((List<?>) criterion.getValue()).contains(reference.getReferenceId());
            default -> throw new AssertionError("Unexpected reference predicate: " + criterion.getCondition());
        };
    }

    private BioReferenceSequenceExample lastQuery() {
        ArgumentCaptor<BioReferenceSequenceExample> captor = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(referenceMapper).selectByExampleWithBLOBs(captor.capture());
        return captor.getValue();
    }

    private static Criterion criterion(BioReferenceSequenceExample query, String condition) {
        return query.getOredCriteria().get(0).getAllCriteria().stream()
                .filter(item -> condition.equals(item.getCondition())).findFirst().orElseThrow();
    }

    private void assertNoStartWrites() {
        verifyNoInteractions(stageMapper, transaction, dispatcher);
        verify(pipelineMapper, never()).updateByExampleSelective(any(), any());
    }

    private BioPipelineStage selectionStage() {
        ArgumentCaptor<BioPipelineStage> stages = ArgumentCaptor.forClass(BioPipelineStage.class);
        verify(stageMapper, org.mockito.Mockito.atLeastOnce()).insertSelective(stages.capture());
        return stages.getAllValues().stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION)
                .findFirst().orElseThrow();
    }

    private static List<Long> snapshotIds(BioPipelineStage stage) throws Exception {
        return snapshot(stage).stream().map(ReferenceSequence::getReferenceId).toList();
    }

    private static List<ReferenceSequence> snapshot(BioPipelineStage stage) throws Exception {
        ReferenceSelectionStageParameters parameters = JsonUtil.toObject(
                stage.getParameters(), ReferenceSelectionStageParameters.class);
        assertNotNull(parameters.getCandidateReferences());
        return parameters.getCandidateReferences();
    }

    private static BioPipelineInputFile input(int type) {
        BioPipelineInputFile input = new BioPipelineInputFile();
        input.setInputKey("sample/0/0");
        input.setFileRole(type);
        input.setFilePath(type == PIPELINE_INPUT_TYPE_SEQUENCE_READ ? "fixture.fastq" : "fixture.fasta");
        return input;
    }

    private static BioReferenceSequence reference(long id, int taxId) {
        BioReferenceSequence reference = new BioReferenceSequence();
        reference.setReferenceId(id);
        reference.setTaxId(taxId);
        reference.setEnabled(true);
        reference.setAccession("FIXTURE_" + id);
        reference.setOrganismName("Synthetic metadata fixture");
        reference.setPath("fixture/" + id + ".fasta");
        reference.setRawMetadata("{\"fixture\":true}");
        return reference;
    }
}
