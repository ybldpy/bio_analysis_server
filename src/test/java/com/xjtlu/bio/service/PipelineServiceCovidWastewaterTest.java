package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID_WASTEWATER;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_COVID_WASTEWATER;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY;
import static com.xjtlu.bio.service.PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_READ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CovidWastewaterStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.CovidWastewaterStageParameters;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioProject;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapperExtension;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.requestParameters.BatchCreateAnalysisPipelineRequest;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.utils.JsonUtil;

/** Pure request/routing/state tests: mocked persistence and dispatcher, no analysis tools. */
class PipelineServiceCovidWastewaterTest {

    private PipelineService service;
    private BioAnalysisPipelineMapper pipelineMapper;
    private BioPipelineStageMapper stageMapper;
    private PipelineInputService inputService;
    private PipelineStageTaskDispatcher dispatcher;
    private BioReferenceSequenceMapper referenceMapper;
    private ReferenceTaxonomyResolver taxonomyResolver;
    private final Map<Long, BioAnalysisPipeline> storedPipelines = new HashMap<>();

    @BeforeEach
    void setUp() {
        service = spy(new PipelineService());
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        stageMapper = mock(BioPipelineStageMapper.class);
        inputService = mock(PipelineInputService.class);
        dispatcher = mock(PipelineStageTaskDispatcher.class);
        referenceMapper = mock(BioReferenceSequenceMapper.class);
        taxonomyResolver = mock(ReferenceTaxonomyResolver.class);
        ReflectionTestUtils.setField(service, "analysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", pipelineMapper);
        ReflectionTestUtils.setField(service, "bioPipelineStageMapper", stageMapper);
        ReflectionTestUtils.setField(service, "pipelineInputService", inputService);
        ReflectionTestUtils.setField(service, "pipelineStageTaskDispatcher", dispatcher);
        ReflectionTestUtils.setField(service, "bioReferenceSequenceMapper", referenceMapper);
        ReflectionTestUtils.setField(service, "referenceTaxonomyResolver", taxonomyResolver);
        ReflectionTestUtils.setField(service, "stageOrchestrator", new StageOrchestrator());

        TransactionTemplate transaction = mock(TransactionTemplate.class);
        when(transaction.execute(any())).thenAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(mock(TransactionStatus.class));
        });
        ReflectionTestUtils.setField(service, "rcTransactionTemplate", transaction);
        ProjectService projectService = mock(ProjectService.class);
        when(projectService.queryProjects(any())).thenReturn(List.of(new BioProject()));
        ReflectionTestUtils.setField(service, "projectService", projectService);

        AtomicLong ids = new AtomicLong(100L);
        when(pipelineMapper.insertSelective(any())).thenAnswer(invocation -> {
            BioAnalysisPipeline pipeline = invocation.getArgument(0);
            pipeline.setPipelineId(ids.incrementAndGet());
            storedPipelines.put(pipeline.getPipelineId(), pipeline);
            return 1;
        });
        when(pipelineMapper.selectByPrimaryKey(anyLong()))
                .thenAnswer(invocation -> storedPipelines.get(invocation.getArgument(0)));
        when(pipelineMapper.updateByExampleSelective(any(), any())).thenReturn(1);
        when(inputService.lockDownPipelineUploading(anyLong())).thenReturn(true);
        // Return R2 first to ensure routing follows slots, not upload order.
        when(inputService.queryInputs(any())).thenReturn(List.of(pairedReads().get(1), pairedReads().get(0)));
        when(stageMapper.insertSelective(any())).thenAnswer(invocation -> {
            BioPipelineStage stage = invocation.getArgument(0);
            assertEquals(PIPELINE_STAGE_STATUS_PENDING, stage.getStatus());
            stage.setStageId(ids.incrementAndGet());
            return 1;
        });
        doReturn(0).when(service).batchUpdateStages(anyList());
    }

    @Test
    void singleCreateAndStartWithoutTaxIdRoutesExactlyOneStageAndPreservesPairedInput() throws Exception {
        long pipelineId = create("""
                {"sequencingPlatform":1001,
                 "extraParams":{"stageParams":{"92":{"threads":4,"timeoutSeconds":90,"analysisProfile":"fixture"}}}}
                """);

        Result<?> started = service.startPipeline(pipelineId);

        assertEquals(Result.SUCCESS, started.getStatus());
        BioPipelineStage stage = dispatchedStage();
        assertEquals(pipelineId, stage.getPipelineId());
        assertEquals(PIPELINE_STAGE_COVID_WASTEWATER, stage.getStageType());
        assertEquals(0, stage.getStageIndex());
        assertEquals(PIPELINE_STAGE_STATUS_QUEUING, stage.getStatus());
        CovidWastewaterStageInputUrls input = JsonUtil.toObject(
                stage.getInputUrl(), CovidWastewaterStageInputUrls.class);
        assertEquals("fixture/R1.fastq.gz", input.getR1Url());
        assertEquals("fixture/R2.fastq.gz", input.getR2Url());
        assertEquals(1001, input.getSequencingPlatform());
        CovidWastewaterStageParameters params = parameters(stage);
        assertEquals(4, params.getThreads());
        assertEquals(90L, params.getTimeoutSeconds());
        assertEquals("fixture", params.getAnalysisProfile());
        assertNull(params.getReadMeta());
        assertNull(params.getReferenceGenome());
        verify(stageMapper).insertSelective(any());
        verify(inputService).unlockPipelineUploading(pipelineId);
        verifyNoInteractions(referenceMapper, taxonomyResolver);
    }

    @Test
    void batchCreateAndStartUsesSameRouteAndRetainsPerPipelineParameters() throws Exception {
        BatchCreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":102,"pipelines":[
                 {"pipelineName":"wastewater-a","pipelineParameters":{"taxId":123,
                  "sequencingPlatform":2001,"extraParams":{"stageParams":{"92":{"threads":3}}}}},
                 {"pipelineName":"wastewater-b"}]}
                """, BatchCreateAnalysisPipelineRequest.class);

        Result<List<Long>> created = service.batchCreateAnalysisPipelines(request);

        assertEquals(Result.SUCCESS, created.getStatus());
        assertEquals(2, created.getData().size());
        for (long id : created.getData()) {
            assertEquals(PIPELINE_VIRUS_COVID_WASTEWATER, storedPipelines.get(id).getPipelineType());
            assertEquals(Result.SUCCESS, service.startPipeline(id).getStatus());
        }
        ArgumentCaptor<BioPipelineStage> captor = ArgumentCaptor.forClass(BioPipelineStage.class);
        verify(dispatcher, times(2)).addTask(captor.capture());
        assertEquals(3, parameters(captor.getAllValues().get(0)).getThreads());
        assertEquals(2, parameters(captor.getAllValues().get(1)).getThreads());
        assertEquals(7200L, parameters(captor.getAllValues().get(1)).getTimeoutSeconds());
        for (BioPipelineStage stage : captor.getAllValues()) {
            assertEquals(PIPELINE_STAGE_COVID_WASTEWATER, stage.getStageType());
        }
        verifyNoInteractions(referenceMapper, taxonomyResolver);
    }

    @Test
    void singleEndInputIsAllowed() throws Exception {
        long id = create("{}");
        when(inputService.queryInputs(any())).thenReturn(List.of(pairedReads().get(0)));

        assertEquals(Result.SUCCESS, service.startPipeline(id).getStatus());

        CovidWastewaterStageInputUrls input = JsonUtil.toObject(
                dispatchedStage().getInputUrl(), CovidWastewaterStageInputUrls.class);
        assertNull(input.getR2Url());
        assertNull(input.getSequencingPlatform());
    }

    @ParameterizedTest
    @MethodSource("invalidInputs")
    void rejectsInvalidInputBeforePersistenceOrDispatch(List<BioPipelineInputFile> inputs, String reason)
            throws Exception {
        long id = create("{}");
        when(inputService.queryInputs(any())).thenReturn(inputs);

        Result<?> result = service.startPipeline(id);

        assertEquals(Result.BUSINESS_FAIL, result.getStatus());
        assertTrue(result.getFailMsg().contains(reason), result.getFailMsg());
        verifyNoInteractions(stageMapper, dispatcher, referenceMapper, taxonomyResolver);
        verify(pipelineMapper, never()).updateByExampleSelective(any(), any());
        verify(inputService).unlockPipelineUploading(id);
    }

    static Stream<Arguments> invalidInputs() {
        return Stream.of(
                Arguments.of(List.of(), "R1"),
                Arguments.of(List.of(input("sample/0/1", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "R2.fastq")), "R1"),
                Arguments.of(List.of(input("sample/0/0", PIPELINE_INPUT_TYPE_SEQUENCE_READ, " ")), "R1"),
                Arguments.of(List.of(input("sample/0/0", PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY, "contigs.fa")), "READ"),
                Arguments.of(List.of(pairedReads().get(0),
                        input("sample/0/1", PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY, "contigs.fa")), "READ"),
                Arguments.of(List.of(pairedReads().get(0),
                        input("sample/1/0", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "other.fastq")), "单样本"),
                Arguments.of(List.of(pairedReads().get(0), pairedReads().get(0)), "重复"),
                Arguments.of(List.of(input("broken-key", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "reads.fastq")), "输入键"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"threads\":0}", "{\"threads\":257}", "{\"timeoutSeconds\":0}",
            "{\"timeoutSeconds\":604801}", "{\"analysisProfile\":\"../profile\"}", "\"not-an-object\""})
    void rejectsInvalidStageParametersWithoutStartingPipeline(String stageParameters) throws Exception {
        long id = create("{\"extraParams\":{\"stageParams\":{\"92\":" + stageParameters + "}}}");

        Result<?> result = service.startPipeline(id);

        assertEquals(Result.PARAMETER_NOT_VALID, result.getStatus());
        assertTrue(result.getFailMsg().contains("92"));
        verifyNoInteractions(stageMapper, dispatcher, referenceMapper, taxonomyResolver);
        verify(pipelineMapper, never()).updateByExampleSelective(any(), any());
    }

    @Test
    void candidateLookupNeverConsultsTaxonomyOrReferenceDatabaseForThisType() {
        List<?> candidates = ReflectionTestUtils.invokeMethod(service,
                "findCandidateReferences", PIPELINE_VIRUS_COVID_WASTEWATER, 123);

        assertNotNull(candidates);
        assertTrue(candidates.isEmpty());
        verifyNoInteractions(referenceMapper, taxonomyResolver);
    }

    @Test
    void completedSingleStageCompletesPipelineWithoutDispatchingAnotherStage() throws Exception {
        long pipelineId = create("{}");
        BioAnalysisPipeline pipeline = storedPipelines.get(pipelineId);
        pipeline.setStatus(PipelineService.PIPELINE_STATUS_RUNNING);
        BioPipelineStage finished = new BioPipelineStage();
        finished.setPipelineId(pipelineId);
        finished.setStageId(900L);
        finished.setStageType(PIPELINE_STAGE_COVID_WASTEWATER);
        finished.setStageIndex(0);
        finished.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        finished.setVersion(2);
        BioAnalysisPipelineMapperExtension pipelineExtension = mock(BioAnalysisPipelineMapperExtension.class);
        when(pipelineExtension.selectPipelineTypeByStageId(900L)).thenReturn(pipeline);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapperExtension", pipelineExtension);
        BioAnalysisStageMapperExtension stagesExtension = mock(BioAnalysisStageMapperExtension.class);
        when(stagesExtension.selectAllPipelineStagesByStageId(900L)).thenReturn(List.of(finished));
        ReflectionTestUtils.setField(service, "bioAnalysisStageMapperExtension", stagesExtension);

        service.pipelineStageDone(900L, true);

        ArgumentCaptor<BioAnalysisPipeline> patch = ArgumentCaptor.forClass(BioAnalysisPipeline.class);
        verify(pipelineMapper).updateByExampleSelective(patch.capture(), any(BioAnalysisPipelineExample.class));
        assertEquals(PipelineService.PIPELINE_STATUS_COMPELETE, patch.getValue().getStatus());
        verifyNoInteractions(dispatcher, referenceMapper, taxonomyResolver);
    }

    private long create(String parameters) throws Exception {
        CreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":102,"analysisName":"wastewater-fixture",
                 "pipelineStageParameters":%s}
                """.formatted(parameters), CreateAnalysisPipelineRequest.class);
        Result<Long> result = service.createPipeline(request);
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(PIPELINE_VIRUS_COVID_WASTEWATER,
                storedPipelines.get(result.getData()).getPipelineType());
        return result.getData();
    }

    private BioPipelineStage dispatchedStage() {
        ArgumentCaptor<BioPipelineStage> captor = ArgumentCaptor.forClass(BioPipelineStage.class);
        verify(dispatcher).addTask(captor.capture());
        return captor.getValue();
    }

    private static CovidWastewaterStageParameters parameters(BioPipelineStage stage) throws Exception {
        return JsonUtil.toObject(stage.getParameters(), CovidWastewaterStageParameters.class);
    }

    private static List<BioPipelineInputFile> pairedReads() {
        return List.of(input("sample/0/0", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "fixture/R1.fastq.gz"),
                input("sample/0/1", PIPELINE_INPUT_TYPE_SEQUENCE_READ, "fixture/R2.fastq.gz"));
    }

    private static BioPipelineInputFile input(String key, int role, String path) {
        BioPipelineInputFile input = new BioPipelineInputFile();
        input.setInputKey(key);
        input.setFileRole(role);
        input.setFilePath(path);
        return input;
    }
}
