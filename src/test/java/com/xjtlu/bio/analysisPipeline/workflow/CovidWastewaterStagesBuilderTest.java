package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ILLUMINA;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ONT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.PACBIO;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.EXTRA_PARAM_STAGE_PARAMS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CovidWastewaterStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.CovidWastewaterStageParameters;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

/** Pure stage-construction contracts; no Spring, database or external analysis. */
class CovidWastewaterStagesBuilderTest {

    @Test
    void createsOnlyThePendingWastewaterEntryStage() throws Exception {
        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildCovidWastewaterStages(
                readsInput(), new PipelineConfigurations());

        assertEquals(1, stages.size());
        BioPipelineStage stage = stages.get(0);
        assertEquals(92, stage.getStageType());
        assertEquals("新冠污水分析", stage.getStageName());
        assertEquals(0, stage.getStageIndex());
        assertEquals(PIPELINE_STAGE_STATUS_PENDING, stage.getStatus());
        assertDefaultParameters(parameters(stage));
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {ILLUMINA, ONT, PACBIO})
    void passesReadUrlsAndDeclaredPlatformWithoutInventingReadMetadata(Integer platform) throws Exception {
        PipelineSampleInput input = readsInput();
        input.setSequencePlatform(platform);

        BioPipelineStage stage = singleStage(input, new PipelineConfigurations());

        CovidWastewaterStageInputUrls urls = inputs(stage);
        assertEquals(input.getR1(), urls.getR1Url());
        assertEquals(input.getR2(), urls.getR2Url());
        assertEquals(platform, urls.getSequencingPlatform());
        assertNull(parameters(stage).getReadMeta());
    }

    @Test
    void permitsSingleReadInputWithoutAnR2Url() throws Exception {
        PipelineSampleInput input = readsInput();
        input.setR2(null);

        CovidWastewaterStageInputUrls urls = inputs(singleStage(input, new PipelineConfigurations()));

        assertEquals(input.getR1(), urls.getR1Url());
        assertNull(urls.getR2Url());
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 1, 2, 99})
    void rejectsAnySequenceLevelOtherThanReads(int sequenceLevel) {
        PipelineSampleInput input = readsInput();
        input.setSequenceLevel(sequenceLevel);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> singleStage(input, new PipelineConfigurations()));

        assertTrue(error.getMessage().contains("READ"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t\n"})
    void rejectsMissingR1EvenWhenR2IsPresent(String r1) {
        PipelineSampleInput input = readsInput();
        input.setR1(r1);

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> singleStage(input, new PipelineConfigurations()));

        assertTrue(error.getMessage().contains("R1"));
    }

    @Test
    void nullConfigurationsRetainDefaultParameters() throws Exception {
        assertDefaultParameters(parameters(singleStage(readsInput(), null)));
    }

    @Test
    void emptyStageOverridesRetainDefaultParameters() throws Exception {
        assertDefaultParameters(parameters(singleStage(readsInput(), configurations(Map.of()))));
    }

    @Test
    void appliesWastewaterStageOverridesByStringStageType() throws Exception {
        PipelineConfigurations configurations = configurations(Map.of(
                "threads", 6,
                "timeoutSeconds", 3600L,
                "analysisProfile", "test-profile"));

        CovidWastewaterStageParameters parameters = parameters(singleStage(readsInput(), configurations));

        assertEquals(6, parameters.getThreads());
        assertEquals(3600L, parameters.getTimeoutSeconds());
        assertEquals("test-profile", parameters.getAnalysisProfile());
        assertNull(parameters.getReadMeta());
    }

    @Test
    void partialOverridesPreserveDefaultsForOmittedParameters() throws Exception {
        CovidWastewaterStageParameters parameters = parameters(singleStage(readsInput(),
                configurations(Map.of("analysisProfile", "custom-profile"))));

        assertEquals(2, parameters.getThreads());
        assertEquals(7200L, parameters.getTimeoutSeconds());
        assertEquals("custom-profile", parameters.getAnalysisProfile());
        assertNull(parameters.getReadMeta());
    }

    @Test
    void overridesForOtherStagesDoNotChangeWastewaterDefaults() throws Exception {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_STAGE_PARAMS,
                Map.of("91", Map.of("threads", 9, "timeoutSeconds", 900L))));

        assertDefaultParameters(parameters(singleStage(readsInput(), configurations)));
    }

    @Test
    void ignoresRequestReadMetadataWhilePreservingSupportedOverrides() throws Exception {
        PipelineConfigurations configurations = configurations(Map.of(
                "threads", 4,
                "readMeta", Map.of(
                        "sequenceLevel", SEQUENCE_LEVEL_READ,
                        "sequencingPlatform", ILLUMINA,
                        "qualityEncoding", 33)));

        CovidWastewaterStageParameters parameters = parameters(singleStage(readsInput(), configurations));

        assertEquals(4, parameters.getThreads());
        assertNull(parameters.getReadMeta(), "The standalone entry must not manufacture resolved read metadata");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, 257})
    void rejectsThreadsOutsideSupportedRange(int threads) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> singleStage(readsInput(), configurations(Map.of("threads", threads))));

        assertTrue(error.getMessage().contains("threads"));
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, -1L, 604801L})
    void rejectsTimeoutOutsideSupportedRange(long timeoutSeconds) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> singleStage(readsInput(), configurations(Map.of("timeoutSeconds", timeoutSeconds))));

        assertTrue(error.getMessage().contains("timeoutSeconds"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"../profile", "profile name", "/server/profile", "--option"})
    void rejectsInvalidProfileIdentifier(String profile) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> singleStage(readsInput(), configurations(Map.of("analysisProfile", profile))));

        assertTrue(error.getMessage().contains("analysisProfile"));
    }

    @Test
    void acceptsInclusiveUpperParameterBounds() throws Exception {
        CovidWastewaterStageParameters parameters = parameters(singleStage(readsInput(), configurations(Map.of(
                "threads", 256,
                "timeoutSeconds", 604800L,
                "analysisProfile", "profile-1.0_default"))));

        assertEquals(256, parameters.getThreads());
        assertEquals(604800L, parameters.getTimeoutSeconds());
        assertEquals("profile-1.0_default", parameters.getAnalysisProfile());
    }

    private static PipelineSampleInput readsInput() {
        PipelineSampleInput input = new PipelineSampleInput("uploads/sample_R1.fastq.gz", "uploads/sample_R2.fastq.gz");
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);
        return input;
    }

    private static PipelineConfigurations configurations(Map<String, Object> stageParameters) {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_STAGE_PARAMS, Map.of("92", stageParameters)));
        return configurations;
    }

    private static BioPipelineStage singleStage(PipelineSampleInput input, PipelineConfigurations configurations)
            throws Exception {
        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildCovidWastewaterStages(input, configurations);
        assertEquals(1, stages.size());
        return stages.get(0);
    }

    private static CovidWastewaterStageInputUrls inputs(BioPipelineStage stage) throws Exception {
        CovidWastewaterStageInputUrls inputs = JsonUtil.toObject(stage.getInputUrl(), CovidWastewaterStageInputUrls.class);
        assertNotNull(inputs);
        return inputs;
    }

    private static CovidWastewaterStageParameters parameters(BioPipelineStage stage) throws Exception {
        CovidWastewaterStageParameters parameters = JsonUtil.toObject(stage.getParameters(),
                CovidWastewaterStageParameters.class);
        assertNotNull(parameters);
        return parameters;
    }

    private static void assertDefaultParameters(CovidWastewaterStageParameters parameters) {
        assertEquals(2, parameters.getThreads());
        assertEquals(7200L, parameters.getTimeoutSeconds());
        assertNull(parameters.getAnalysisProfile());
        assertNull(parameters.getReadMeta());
    }
}
