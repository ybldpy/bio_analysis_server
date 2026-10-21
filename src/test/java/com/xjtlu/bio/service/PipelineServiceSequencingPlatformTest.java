package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ONT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.PACBIO;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.UNKNOWN;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.requestParameters.AnalysisPipelineParameters;
import com.xjtlu.bio.requestParameters.BatchCreateAnalysisPipelineRequest;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.utils.JsonUtil;

/** Exercises the request/storage/builder boundary without a database or analysis tools. */
class PipelineServiceSequencingPlatformTest {

    private static final long PIPELINE_ID = 123L;

    @ParameterizedTest
    @CsvSource({"true, 2001", "false, 2001"})
    void creationPreservesExplicitPlatformAndOverridesLegacyDefault(boolean virus, int platform) throws Exception {
        String parameters = """
                {"sequencingPlatform":%d,
                 "extraParams":{"sequenceInputMeta":{"sequencingPlatform":2002}}}
                """.formatted(platform);

        List<BioPipelineStage> stages = createAndBuild(virus, parameters, readsInput());

        assertEquals(platform, inspectInput(stages).getSequenceInputMeta().getSequencingPlatform());
        for (BioPipelineStage stage : stages) {
            assertEquals(PIPELINE_ID, stage.getPipelineId());
            assertNull(JsonUtil.toObject(stage.getParameters(), BaseStageParams.class).getReadMeta(),
                    "Resolved metadata must still be supplied by ReadInspect");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "\"sequencingPlatform\":null,", "\"sequencingPlatform\":0,"})
    void unspecifiedPlatformRetainsLegacyDeclaration(String field) throws Exception {
        String parameters = "{"
                + field
                + "\"extraParams\":{\"sequenceInputMeta\":{\"sequencingPlatform\":2002}}}";

        List<BioPipelineStage> stages = createAndBuild(false, parameters, readsInput());

        assertEquals(PACBIO, inspectInput(stages).getSequenceInputMeta().getSequencingPlatform());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"sequencingPlatform\":null}", "{\"sequencingPlatform\":0}"})
    void absentDeclarationLeavesAutomaticDetectionAvailable(String parameters) throws Exception {
        List<BioPipelineStage> stages = createAndBuild(false, parameters, readsInput());

        assertNull(inspectInput(stages).getSequenceInputMeta());
    }

    @Test
    void existingSampleDeclarationTakesPrecedence() throws Exception {
        PipelineSampleInput input = readsInput();
        input.setSequencePlatform(PACBIO);

        List<BioPipelineStage> stages = createAndBuild(false,
                "{\"sequencingPlatform\":2001}", input);

        assertEquals(PACBIO, inspectInput(stages).getSequenceInputMeta().getSequencingPlatform());
    }

    @Test
    void unknownSamplePlatformInheritsPipelineDeclaration() throws Exception {
        PipelineSampleInput input = readsInput();
        input.setSequencePlatform(UNKNOWN);

        List<BioPipelineStage> stages = createAndBuild(false,
                "{\"sequencingPlatform\":2001}", input);

        assertEquals(ONT, inspectInput(stages).getSequenceInputMeta().getSequencingPlatform());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void assemblyInputDoesNotCreateReadInspection(boolean virus) throws Exception {
        PipelineSampleInput input = new PipelineSampleInput("sample.contigs.fasta", null);
        input.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);

        List<BioPipelineStage> stages = createAndBuild(virus,
                "{\"sequencingPlatform\":2001}", input);

        assertFalse(stages.stream().anyMatch(stage -> stage.getStageType() == PIPELINE_STAGE_READ_INSPECT));
        for (BioPipelineStage stage : stages) {
            assertEquals(SEQUENCE_LEVEL_ASSEMBLY,
                    JsonUtil.toObject(stage.getParameters(), BaseStageParams.class)
                            .getReadMeta().getSequenceLevel());
        }
    }

    @Test
    void batchParameterMapPreservesSameFieldWhenReloaded() throws Exception {
        BatchCreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":200,"pipelines":[
                  {"pipelineName":"platform-test","pipelineParameters":{"sequencingPlatform":2001}}
                ]}
                """, BatchCreateAnalysisPipelineRequest.class);
        // Batch creation persists this map directly rather than using PipelineStageParameters.
        String storedJson = JsonUtil.toJson(request.getPipelines().get(0).getPipelineParameters());
        AnalysisPipelineParameters restored = JsonUtil.toObject(storedJson, AnalysisPipelineParameters.class);

        List<BioPipelineStage> stages = build(new PipelineService(), false, restored, readsInput());

        assertEquals(ONT, restored.getSequencingPlatform());
        assertEquals(ONT, inspectInput(stages).getSequenceInputMeta().getSequencingPlatform());
    }

    private static List<BioPipelineStage> createAndBuild(boolean virus, String parameters,
            PipelineSampleInput input) throws Exception {
        CreateAnalysisPipelineRequest request = JsonUtil.toObject("""
                {"projectId":1,"pipelineType":%d,"analysisName":"platform-test",
                 "pipelineStageParameters":%s}
                """.formatted(pipelineType(virus), parameters), CreateAnalysisPipelineRequest.class);
        PipelineService service = new PipelineService();
        BioAnalysisPipelineMapper mapper = mock(BioAnalysisPipelineMapper.class);
        ReflectionTestUtils.setField(service, "bioAnalysisPipelineMapper", mapper);
        when(mapper.insertSelective(any(BioAnalysisPipeline.class))).thenAnswer(invocation -> {
            BioAnalysisPipeline pipeline = invocation.getArgument(0);
            pipeline.setPipelineId(PIPELINE_ID);
            return 1;
        });

        Result<Long> result = service.createPipeline(request);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(PIPELINE_ID, result.getData());
        ArgumentCaptor<BioAnalysisPipeline> captured = ArgumentCaptor.forClass(BioAnalysisPipeline.class);
        verify(mapper).insertSelective(captured.capture());
        AnalysisPipelineParameters restored = JsonUtil.toObject(
                captured.getValue().getPipelineParameters(), AnalysisPipelineParameters.class);
        assertEquals(request.getPipelineStageParameters().getSequencingPlatform(),
                restored.getSequencingPlatform());
        return build(service, virus, restored, input);
    }

    private static List<BioPipelineStage> build(PipelineService service, boolean virus,
            AnalysisPipelineParameters parameters, PipelineSampleInput input) {
        List<BioPipelineStage> stages = ReflectionTestUtils.invokeMethod(service,
                "buildRegularPipelineStages", PIPELINE_ID, pipelineType(virus), List.of(), parameters, input);
        assertNotNull(stages);
        assertFalse(stages.isEmpty());
        return stages;
    }

    private static ReadInspectStageInputUrls inspectInput(List<BioPipelineStage> stages) throws Exception {
        BioPipelineStage inspect = stages.stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_READ_INSPECT)
                .findFirst().orElseThrow();
        return JsonUtil.toObject(inspect.getInputUrl(), ReadInspectStageInputUrls.class);
    }

    private static PipelineSampleInput readsInput() {
        PipelineSampleInput input = new PipelineSampleInput("sample_R1.fastq.gz", "sample_R2.fastq.gz");
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);
        return input;
    }

    private static int pipelineType(boolean virus) {
        return virus ? PIPELINE_VIRUS : PIPELINE_REGULAR_BACTERIA;
    }
}
