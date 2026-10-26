package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;
import static com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.EXTRA_PARAM_EVIDENCE_QUALITY;
import static com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.EXTRA_PARAM_STAGE_PARAMS;
import static com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.EXTRA_PARAM_SEQUENCE_INPUT_META;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.requestParameters.AnalysisPipelineParameters;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest.PipelineStageParameters;
import com.xjtlu.bio.utils.JsonUtil;

class AnalysisPipelineStagesBuilderTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void allStagesHaveNamesForReadAndAssemblyInputs(boolean virus) throws Exception {
        PipelineSampleInput assemblyInput = new PipelineSampleInput("sample.contigs.fasta", null);
        assemblyInput.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);

        for (PipelineSampleInput input : List.of(readsInput(), assemblyInput)) {
            List<BioPipelineStage> stages = buildStages(virus, input, emptyConfigurations());

            assertFalse(stages.isEmpty());
            for (BioPipelineStage stage : stages) {
                String context = "virus=" + virus + ", sequenceLevel=" + input.getSequenceLevel()
                        + ", stageType=" + stage.getStageType();
                assertNotNull(stage.getStageName(), context);
                assertFalse(stage.getStageName().isBlank(), context);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void qcWritesSeparateDefaultFilterSnapshotsForBothPipelineTypes(boolean virus) throws Exception {
        BioPipelineStage qc = qcStage(buildStages(virus, readsInput(), emptyConfigurations()));

        Map<String, Object> snapshot = JsonUtil.toMap(qc.getParameters());
        assertTrue(snapshot.containsKey("shortReadFilters"));
        assertTrue(snapshot.containsKey("longReadFilters"));
        QcParameters parameters = JsonUtil.toObject(qc.getParameters(), QcParameters.class);
        assertDefaultQcFilters(parameters);
        assertNull(parameters.getReadMeta(), "Resolved metadata must still come from ReadInspect");
        assertEquals(virus ? BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS
                : BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, parameters.getAnalysisTargetType());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void qcIgnoresRequestFilterOverridesForBothPipelineTypes(boolean virus) throws Exception {
        PipelineConfigurations configurations = emptyConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_STAGE_PARAMS,
                Map.of(Integer.toString(PIPELINE_STAGE_QC), Map.of(
                        "shortReadFilters", Map.of("minimumLength", 123, "qualifiedQualityPhred", 25),
                        "longReadFilters", Map.of("minimumLength", 456, "nPercentLimit", 80)))));

        BioPipelineStage qc = qcStage(buildStages(virus, readsInput(), configurations));

        assertDefaultQcFilters(JsonUtil.toObject(qc.getParameters(), QcParameters.class));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void qcDoesNotInterpretUnexposedRequestParameters(boolean virus) throws Exception {
        PipelineConfigurations configurations = emptyConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_STAGE_PARAMS,
                Map.of(Integer.toString(PIPELINE_STAGE_QC), "not-an-exposed-configuration")));

        BioPipelineStage qc = qcStage(buildStages(virus, readsInput(), configurations));

        assertDefaultQcFilters(JsonUtil.toObject(qc.getParameters(), QcParameters.class));
    }

    private static BioPipelineStage qcStage(List<BioPipelineStage> stages) {
        return stages.stream().filter(stage -> stage.getStageType() == PIPELINE_STAGE_QC)
                .findFirst().orElseThrow();
    }

    private static void assertDefaultQcFilters(QcParameters parameters) {
        var shortRead = parameters.getShortReadFilters();
        assertNotNull(shortRead);
        assertEquals(15, shortRead.getQualifiedQualityPhred());
        assertEquals(40.0d, shortRead.getUnqualifiedPercentLimit(), 0.0d);
        assertEquals(0.0d, shortRead.getMeanQuality(), 0.0d);
        assertEquals(15, shortRead.getMinimumLength());
        assertEquals(0, shortRead.getMaximumLength());
        assertEquals(5, shortRead.getNBaseLimit());

        var longRead = parameters.getLongReadFilters();
        assertNotNull(longRead);
        assertEquals(15, longRead.getQualifiedQualityPhred());
        assertEquals(40.0d, longRead.getUnqualifiedPercentLimit(), 0.0d);
        assertEquals(0.0d, longRead.getMeanQuality(), 0.0d);
        assertEquals(20, longRead.getMinimumLength());
        assertEquals(0, longRead.getMaximumLength());
        assertEquals(1_000_000, longRead.getNBaseLimit());
        assertEquals(10.0d, longRead.getNPercentLimit(), 0.0d);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void declaredPlatformBelongsToReadInspectInputNotStageParameters(boolean virus) throws Exception {
        PipelineConfigurations configurations = emptyConfigurations();
        Map<String, Object> staleReadMeta = Map.of(
                "readLenType", Constants.SequenceInput.READ_LEN_TYPE_SHORT,
                "qualityEncoding", Constants.SequenceInput.QUALITY_ENCODING_33,
                "sequencingPlatform", Constants.SequencingPlatform.ILLUMINA);
        configurations.setExtraParams(Map.of(
                EXTRA_PARAM_SEQUENCE_INPUT_META,
                Map.of("sequencingPlatform", Constants.SequencingPlatform.ONT),
                EXTRA_PARAM_STAGE_PARAMS, Map.of(
                        Integer.toString(PIPELINE_STAGE_READ_INSPECT), Map.of("readMeta", staleReadMeta),
                        Integer.toString(PIPELINE_STAGE_ASSEMBLY), Map.of("readMeta", staleReadMeta),
                        Integer.toString(PIPELINE_STAGE_MAPPING), Map.of("readMeta", staleReadMeta),
                        Integer.toString(PIPELINE_STAGE_QC), Map.of("readMeta", staleReadMeta))));
        PipelineSampleInput input = readsInput();

        List<BioPipelineStage> stages = buildStages(virus, input, configurations);

        ReadInspectStageInputUrls inspectInput = readInspectInput(stages);
        assertEquals(input.getR1(), inspectInput.getRead1Url());
        assertEquals(input.getR2(), inspectInput.getRead2Url());
        assertNotNull(inspectInput.getSequenceInputMeta());
        assertEquals(Constants.SequencingPlatform.ONT,
                inspectInput.getSequenceInputMeta().getSequencingPlatform());
        for (BioPipelineStage stage : stages) {
            BaseStageParams parameters = JsonUtil.toObject(stage.getParameters(), BaseStageParams.class);
            assertNull(parameters.getReadMeta(), "Stage " + stage.getStageType() + " must await ReadInspect");
            assertEquals(virus ? BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS
                    : BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, parameters.getAnalysisTargetType());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void absentDeclarationDoesNotInventPlatformOrResolvedMetadata(boolean virus) throws Exception {
        PipelineSampleInput input = readsInput();
        assertNull(input.getSequencePlatform());

        List<BioPipelineStage> stages = buildStages(virus, input, emptyConfigurations());

        assertNull(readInspectInput(stages).getSequenceInputMeta());
        for (BioPipelineStage stage : stages) {
            assertNull(JsonUtil.toObject(stage.getParameters(), BaseStageParams.class).getReadMeta());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void unknownSamplePlatformRetainsPipelineDefault(boolean virus) throws Exception {
        PipelineConfigurations configurations = emptyConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_SEQUENCE_INPUT_META,
                Map.of("sequencingPlatform", Constants.SequencingPlatform.ONT)));
        PipelineSampleInput input = readsInput();
        input.setSequencePlatform(Constants.SequencingPlatform.UNKNOWN);

        ReadInspectStageInputUrls inspectInput = readInspectInput(buildStages(virus, input, configurations));

        assertEquals(Constants.SequencingPlatform.ONT,
                inspectInput.getSequenceInputMeta().getSequencingPlatform());
        assertEquals(Constants.SequencingPlatform.ONT,
                ((Map<?, ?>) configurations.getExtraParams().get(EXTRA_PARAM_SEQUENCE_INPUT_META))
                        .get("sequencingPlatform"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void explicitSamplePlatformWorksWithoutExtraConfiguration(boolean virus) throws Exception {
        PipelineSampleInput input = readsInput();
        input.setSequencePlatform(Constants.SequencingPlatform.PACBIO);

        ReadInspectStageInputUrls inspectInput = readInspectInput(
                buildStages(virus, input, emptyConfigurations()));

        assertEquals(Constants.SequencingPlatform.PACBIO,
                inspectInput.getSequenceInputMeta().getSequencingPlatform());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void emptyDeclarationLeavesPlatformUnspecified(boolean virus) throws Exception {
        PipelineConfigurations configurations = emptyConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_SEQUENCE_INPUT_META, Map.of()));

        ReadInspectStageInputUrls inspectInput = readInspectInput(
                buildStages(virus, readsInput(), configurations));

        assertNotNull(inspectInput.getSequenceInputMeta());
        assertNull(inspectInput.getSequenceInputMeta().getSequencingPlatform());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rejectsNonObjectSequenceInputMeta(boolean virus) {
        PipelineConfigurations configurations = emptyConfigurations();
        configurations.setExtraParams(Map.of(EXTRA_PARAM_SEQUENCE_INPUT_META, "ONT"));

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> buildStages(virus, readsInput(), configurations));

        assertEquals("extraParams.sequenceInputMeta must be an object", error.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void assemblyInputRetainsSequenceLevelWithoutReadInspect(boolean virus) throws Exception {
        PipelineSampleInput input = new PipelineSampleInput("sample.contigs.fasta", null);
        input.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);

        List<BioPipelineStage> stages = buildStages(virus, input, emptyConfigurations());

        assertFalse(stages.stream().anyMatch(stage -> stage.getStageType() == PIPELINE_STAGE_READ_INSPECT));
        for (BioPipelineStage stage : stages) {
            BaseStageParams parameters = JsonUtil.toObject(stage.getParameters(), BaseStageParams.class);
            assertNotNull(parameters);
            assertNotNull(parameters.getReadMeta());
            assertEquals(SEQUENCE_LEVEL_ASSEMBLY, parameters.getReadMeta().getSequenceLevel());
            assertEquals(virus ? BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS
                    : BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, parameters.getAnalysisTargetType());
        }
    }

    @Test
    void inputMetadataSurvivesPersistedRequestParameters() throws Exception {
        PipelineStageParameters request = new PipelineStageParameters();
        request.setExtraParams(Map.of(EXTRA_PARAM_SEQUENCE_INPUT_META,
                Map.of("sequencingPlatform", Constants.SequencingPlatform.ONT)));
        AnalysisPipelineParameters restored = JsonUtil.toObject(JsonUtil.toJson(request),
                AnalysisPipelineParameters.class);
        PipelineConfigurations configurations = emptyConfigurations();
        configurations.setExtraParams(restored.getExtraParams());

        ReadInspectStageInputUrls inspectInput = readInspectInput(
                buildStages(false, readsInput(), configurations));

        assertEquals(Constants.SequencingPlatform.ONT,
                inspectInput.getSequenceInputMeta().getSequencingPlatform());
    }

    private static PipelineConfigurations emptyConfigurations() {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of());
        return configurations;
    }

    private static PipelineSampleInput readsInput() {
        PipelineSampleInput input = new PipelineSampleInput("reads_1.fastq.gz", "reads_2.fastq.gz");
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);
        return input;
    }

    private static List<BioPipelineStage> buildStages(boolean virus, PipelineSampleInput input,
            PipelineConfigurations configurations) throws Exception {
        return virus ? AnalysisPipelineStagesBuilder.buildVirusStages(input, configurations)
                : AnalysisPipelineStagesBuilder.buildRegularBacteriaPipeline(input, configurations);
    }

    private static ReadInspectStageInputUrls readInspectInput(List<BioPipelineStage> stages) throws Exception {
        BioPipelineStage stage = stages.stream()
                .filter(candidate -> candidate.getStageType() == PIPELINE_STAGE_READ_INSPECT)
                .findFirst().orElseThrow();
        return JsonUtil.toObject(stage.getInputUrl(), ReadInspectStageInputUrls.class);
    }

    @Test
    void writesConfiguredVariantFilterIntoVirusStage() throws Exception {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of());
        configurations.setExtraParams(variantStageParams(Map.of(
                "threads", 6,
                "minimumMappingQuality", 31,
                "minimumBaseQuality", 27,
                "minimumVariantQuality", 42.5d,
                "minimumVariantDepth", 18,
                "minimumAlternateDepth", 9,
                "minimumAlternateAlleleFraction", 0.82d)));

        VarientCallParameters actual = buildVariantParameters(configurations);

        assertEquals(6, actual.getThreads());
        assertEquals(31, actual.getMinimumMappingQuality());
        assertEquals(27, actual.getMinimumBaseQuality());
        assertEquals(42.5d, actual.getMinimumVariantQuality());
        assertEquals(18, actual.getMinimumVariantDepth());
        assertEquals(9, actual.getMinimumAlternateDepth());
        assertEquals(0.82d, actual.getMinimumAlternateAlleleFraction());

        MappingParameters mapping = buildMappingParameters(configurations);
        assertEquals(31, mapping.getMinimumMappingQuality());
        assertEquals(27, mapping.getMinimumBaseQuality());
    }

    @Test
    void sharedEvidenceQualityOverridesLegacyValuesForBothStages()
            throws Exception {
        Map<String, Object> extraParams = new HashMap<>(variantStageParams(Map.of(
                "minimumMappingQuality", 31,
                "minimumBaseQuality", 27)));
        extraParams.put(EXTRA_PARAM_EVIDENCE_QUALITY, Map.of(
                "minimumMappingQuality", 35));

        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of());
        configurations.setExtraParams(extraParams);

        MappingParameters mapping = buildMappingParameters(configurations);
        VarientCallParameters variant = buildVariantParameters(configurations);

        assertEquals(35, mapping.getMinimumMappingQuality());
        assertEquals(27, mapping.getMinimumBaseQuality());
        assertEquals(35, variant.getMinimumMappingQuality());
        assertEquals(27, variant.getMinimumBaseQuality());
    }

    @Test
    void nullVariantConfigurationFallsBackToStageDefaults() throws Exception {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of());
        configurations.setExtraParams(null);

        VarientCallParameters actual = buildVariantParameters(configurations);

        assertEquals(VarientCallParameters.DEFAULT_THREADS, actual.getThreads());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_MAPPING_QUALITY,
                actual.getMinimumMappingQuality());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_BASE_QUALITY,
                actual.getMinimumBaseQuality());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_VARIANT_QUALITY,
                actual.getMinimumVariantQuality());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_VARIANT_DEPTH,
                actual.getMinimumVariantDepth());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_ALTERNATE_DEPTH,
                actual.getMinimumAlternateDepth());
        assertEquals(
                VarientCallParameters.DEFAULT_MINIMUM_ALTERNATE_ALLELE_FRACTION,
                actual.getMinimumAlternateAlleleFraction());

        MappingParameters mapping = buildMappingParameters(configurations);
        assertEquals(MappingParameters.DEFAULT_MINIMUM_MAPPING_QUALITY,
                mapping.getMinimumMappingQuality());
        assertEquals(MappingParameters.DEFAULT_MINIMUM_BASE_QUALITY,
                mapping.getMinimumBaseQuality());
    }

    @Test
    void variantConfigurationSurvivesPersistedPipelineParameters()
            throws Exception {
        PipelineStageParameters requestParameters = new PipelineStageParameters();
        requestParameters.setTaxId(11_320);
        requestParameters.setExtraParams(variantStageParams(Map.of(
                "minimumVariantQuality", 35.0d,
                "minimumVariantDepth", 22)));

        AnalysisPipelineParameters restored = JsonUtil.toObject(
                JsonUtil.toJson(requestParameters),
                AnalysisPipelineParameters.class);

        assertNotNull(restored.getExtraParams());

        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of());
        configurations.setExtraParams(restored.getExtraParams());
        VarientCallParameters actual = buildVariantParameters(configurations);

        assertEquals(35.0d, actual.getMinimumVariantQuality());
        assertEquals(22, actual.getMinimumVariantDepth());
        assertEquals(VarientCallParameters.DEFAULT_MINIMUM_ALTERNATE_DEPTH,
                actual.getMinimumAlternateDepth());
    }

    @Test
    void readsLegacyExtraParametersPropertyName() throws Exception {
        AnalysisPipelineParameters restored = JsonUtil.toObject(
                "{\"extraParameters\":{\"stageParams\":{}}}",
                AnalysisPipelineParameters.class);

        assertNotNull(restored.getExtraParams());
        assertNotNull(restored.getExtraParams().get(EXTRA_PARAM_STAGE_PARAMS));
    }

    @Test
    void writesConfiguredSelectionParametersForReads() throws Exception {
        ReferenceSequence candidate = new ReferenceSequence();
        candidate.setAccession("CANDIDATE_1");
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of(candidate));
        configurations.setExtraParams(Map.of(EXTRA_PARAM_STAGE_PARAMS,
                Map.of(Integer.toString(PIPELINE_STAGE_REFERENCE_SELECTION), Map.of(
                        "threads", 6,
                        "readFallbackEnabled", false,
                        "minimumReadReferenceCoverage", 0.75d,
                        "minimumReadSequenceIdentity", 92.0d,
                        "minimumReadMappingQuality", 25))));
        PipelineSampleInput input = new PipelineSampleInput("sample.input", null);
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);

        BioPipelineStage selection = AnalysisPipelineStagesBuilder.buildVirusStages(input, configurations)
                .stream().filter(stage -> stage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION)
                .findFirst().orElseThrow();
        ReferenceSelectionStageParameters parameters = JsonUtil.toObject(
                selection.getParameters(), ReferenceSelectionStageParameters.class);

        assertEquals(Constants.StageType.PIPELINE_STAGE_NAME_REFERENCE_SELECTION, selection.getStageName());
        assertEquals(6, parameters.getThreads());
        assertFalse(parameters.isReadFallbackEnabled());
        assertEquals(0.75d, parameters.getMinimumReadReferenceCoverage());
        assertEquals(92.0d, parameters.getMinimumReadSequenceIdentity());
        assertEquals(25, parameters.getMinimumReadMappingQuality());
        assertEquals(List.of("CANDIDATE_1"), parameters.getCandidateReferences().stream()
                .map(ReferenceSequence::getAccession).toList());
    }

    @Test
    void absentSelectionConfigurationUsesDefaultsForReads() throws Exception {
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of());
        PipelineSampleInput input = new PipelineSampleInput("sample.input", null);
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);

        BioPipelineStage selection = AnalysisPipelineStagesBuilder.buildVirusStages(input, configurations)
                .stream().filter(stage -> stage.getStageType() == PIPELINE_STAGE_REFERENCE_SELECTION)
                .findFirst().orElseThrow();
        ReferenceSelectionStageParameters parameters = JsonUtil.toObject(
                selection.getParameters(), ReferenceSelectionStageParameters.class);

        assertTrue(parameters.isReadFallbackEnabled());
        assertEquals(0.5d, parameters.getMinimumReadReferenceCoverage());
        assertEquals(80.0d, parameters.getMinimumReadSequenceIdentity());
        assertEquals(20, parameters.getMinimumReadMappingQuality());
    }

    @ParameterizedTest
    @ValueSource(strings = {"sample.contigs.fasta", "sample.contigs.fasta.gz"})
    void assemblyInputContainsOnlyComparisonWithAllCandidateSnapshots(String fastaUrl) throws Exception {
        ReferenceSequence first = new ReferenceSequence();
        first.setAccession("CANDIDATE_1");
        first.setPath("references/candidate-1.fasta");
        first.setRawMetadata("first candidate snapshot");
        ReferenceSequence second = new ReferenceSequence();
        second.setAccession("CANDIDATE_2");
        second.setPath("references/candidate-2.fasta");
        second.setAnnotationFile("references/candidate-2.gff");
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of(first, second));
        PipelineSampleInput input = new PipelineSampleInput(fastaUrl, null);
        input.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);

        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(input, configurations);

        assertEquals(List.of(PIPELINE_STAGE_REFERENCE_COMPARISON),
                stages.stream().map(BioPipelineStage::getStageType).toList());
        BioPipelineStage comparison = stages.get(0);
        assertEquals(Constants.StageType.PIPELINE_STAGE_NAME_REFERENCE_COMPARISON, comparison.getStageName());
        assertEquals(0, comparison.getStageIndex());
        assertEquals(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING, comparison.getStatus());
        ReferenceComparisonStageInputUrls comparisonInput = JsonUtil.toObject(
                comparison.getInputUrl(), ReferenceComparisonStageInputUrls.class);
        assertEquals(fastaUrl, comparisonInput.getFastaUrl());
        ReferenceComparisonStageParameters parameters = JsonUtil.toObject(
                comparison.getParameters(), ReferenceComparisonStageParameters.class);
        assertEquals(JsonUtil.toJson(List.of(first, second)), JsonUtil.toJson(parameters.getCandidateReferences()));
        assertNull(parameters.getReferenceGenome(), "FASTA comparison must not preselect a reference");
        assertEquals(BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS, parameters.getAnalysisTargetType());
        assertEquals(SEQUENCE_LEVEL_ASSEMBLY, parameters.getReadMeta().getSequenceLevel());
    }

    @Test
    void assemblyComparisonPreservesStage43OverridesAndInitializesAuthoritativeMetadata() throws Exception {
        ReferenceSequence candidate = new ReferenceSequence();
        candidate.setAccession("CONFIGURED_CANDIDATE");
        PipelineConfigurations configurations = new PipelineConfigurations();
        configurations.setCandidateReferenceSequences(List.of(candidate));
        configurations.setExtraParams(Map.of(EXTRA_PARAM_STAGE_PARAMS,
                Map.of(Integer.toString(PIPELINE_STAGE_REFERENCE_COMPARISON), Map.of(
                        "taxonomyContext", Map.of("taxid", 1234, "speciesName", "Configured context"),
                        "analysisTargetType", BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA,
                        "readMeta", Map.of("sequenceLevel", SEQUENCE_LEVEL_READ),
                        "candidateReferences", List.of(Map.of("accession", "IGNORED_OVERRIDE"))),
                        Integer.toString(PIPELINE_STAGE_REFERENCE_SELECTION), "unused selection settings")));
        PipelineSampleInput input = new PipelineSampleInput("sample.contigs.fasta", null);
        input.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);

        BioPipelineStage comparison = AnalysisPipelineStagesBuilder.buildVirusStages(input, configurations).get(0);
        ReferenceComparisonStageParameters parameters = JsonUtil.toObject(
                comparison.getParameters(), ReferenceComparisonStageParameters.class);

        assertEquals(1234, parameters.getTaxonomyContext().getTaxid());
        assertEquals("Configured context", parameters.getTaxonomyContext().getSpeciesName());
        assertEquals(List.of("CONFIGURED_CANDIDATE"), parameters.getCandidateReferences().stream()
                .map(ReferenceSequence::getAccession).toList());
        assertEquals(BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS, parameters.getAnalysisTargetType());
        assertEquals(SEQUENCE_LEVEL_ASSEMBLY, parameters.getReadMeta().getSequenceLevel());
    }

    @Test
    void readsStillUseSelectionAndMappingWithoutComparison() throws Exception {
        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(
                readsInput(), emptyConfigurations());

        assertEquals(List.of(PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_QC, PIPELINE_STAGE_ASSEMBLY,
                PIPELINE_STAGE_REFERENCE_SELECTION, PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_VARIANT_CALL,
                Constants.StageType.PIPELINE_STAGE_CONSENSUS, Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION),
                stages.stream().map(BioPipelineStage::getStageType).toList());
        assertEquals(0, stages.get(0).getStageIndex());
        assertTrue(stages.stream().skip(1).allMatch(stage -> stage.getStageIndex() == -1));
    }

    private static VarientCallParameters buildVariantParameters(
            PipelineConfigurations configurations) throws Exception {
        return buildStageParameters(
                configurations,
                PIPELINE_STAGE_VARIANT_CALL,
                VarientCallParameters.class);
    }

    private static MappingParameters buildMappingParameters(
            PipelineConfigurations configurations) throws Exception {
        return buildStageParameters(
                configurations,
                PIPELINE_STAGE_MAPPING,
                MappingParameters.class);
    }

    private static <T> T buildStageParameters(
            PipelineConfigurations configurations,
            int stageType,
            Class<T> parameterType) throws Exception {
        PipelineSampleInput input = new PipelineSampleInput("r1.fastq.gz",
                "r2.fastq.gz");
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);

        BioPipelineStage stage = AnalysisPipelineStagesBuilder
                .buildVirusStages(input, configurations)
                .stream()
                .filter(candidate -> candidate.getStageType() == stageType)
                .findFirst()
                .orElseThrow();

        return JsonUtil.toObject(stage.getParameters(), parameterType);
    }

    private static Map<String, Object> variantStageParams(
            Map<String, Object> variantParameters) {
        return Map.of(
                EXTRA_PARAM_STAGE_PARAMS,
                Map.of(Integer.toString(PIPELINE_STAGE_VARIANT_CALL),
                        variantParameters));
    }
}
