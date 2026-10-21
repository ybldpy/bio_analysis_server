package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CovidWastewaterStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls.SequenceInputMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.StageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AMRParamters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AssemblyStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.CovidWastewaterStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.EvidenceQualityStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SNPAnnotationStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VFParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

public class AnalysisPipelineStagesBuilder {

    public static final String EXTRA_PARAM_STAGE_PARAMS = "stageParams";
    public static final String EXTRA_PARAM_EVIDENCE_QUALITY = "evidenceQuality";
    public static final String EXTRA_PARAM_SEQUENCE_INPUT_META = "sequenceInputMeta";

    private static BioPipelineStage createStage(int stageType)
            throws JsonProcessingException {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageType(stageType);
        stage.setStageName(STAGE_NAME_MAP.get(stageType));
        stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        return stage;
    }

    private static final Map<Integer, Supplier<? extends BaseStageParams>> STAGE_PARAMS_FACTORY_MAP = Map.ofEntries(
            Map.entry(PIPELINE_STAGE_QC, QcParameters::new),
            Map.entry(PIPELINE_STAGE_ASSEMBLY, AssemblyStageParameters::new),
            Map.entry(PIPELINE_STAGE_MAPPING, MappingParameters::new),
            Map.entry(PIPELINE_STAGE_REFERENCE_SELECTION, ReferenceSelectionStageParameters::new),
            Map.entry(PIPELINE_STAGE_REFERENCE_COMPARISON, ReferenceComparisonStageParameters::new),
            Map.entry(PIPELINE_STAGE_VARIANT_CALL, VarientCallParameters::new),
            Map.entry(PIPELINE_STAGE_CONSENSUS, ConsensusStageParameters::new),
            Map.entry(PIPELINE_STAGE_COVID_WASTEWATER, CovidWastewaterStageParameters::new),
            Map.entry(PIPELINE_STAGE_SNP_ANNOTATION, SNPAnnotationStageParameters::new),
            Map.entry(PIPELINE_STAGE_AMR, AMRParamters::new),
            Map.entry(PIPELINE_STAGE_VIRULENCE, VFParameters::new),
            Map.entry(PIPELINE_STAGE_SEROTYPE, SeroTypingStageParameters::new));

    private static BaseStageParams stageParams(int stageType) {
        return STAGE_PARAMS_FACTORY_MAP
                .getOrDefault(stageType, BaseStageParams::new)
                .get();
    }

    private static BaseStageParams stageParams(
            int stageType,
            PipelineConfigurations pipelineConfigurations)
            throws JsonProcessingException {
        BaseStageParams defaultParameters = stageParams(stageType);
        // QC filters are an internal parameter snapshot, not a request-level override yet.
        if (stageType == PIPELINE_STAGE_QC) {
            return defaultParameters;
        }
        if (pipelineConfigurations == null
                || pipelineConfigurations.getExtraParams() == null) {
            return defaultParameters;
        }

        Object configuredStageParams = pipelineConfigurations.getExtraParams()
                .get(EXTRA_PARAM_STAGE_PARAMS);
        if (configuredStageParams == null) {
            return defaultParameters;
        }
        if (!(configuredStageParams instanceof Map<?, ?> stageParamsByType)) {
            throw new IllegalArgumentException(
                    "extraParams.stageParams must be an object");
        }

        Object configuredParameters = stageParamsByType.get(
                Integer.toString(stageType));
        if (configuredParameters == null) {
            return defaultParameters;
        }
        if (!(configuredParameters instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(
                    "extraParams.stageParams." + stageType
                            + " must be an object");
        }

        return JsonUtil.toObject(
                JsonUtil.toJson(configuredParameters),
                defaultParameters.getClass());
    }

    private static void synchronizeEvidenceQuality(
            PipelineConfigurations pipelineConfigurations,
            MappingParameters mappingParameters,
            VarientCallParameters variantCallParameters)
            throws JsonProcessingException {
        EvidenceQualityOverrides overrides = evidenceQualityOverrides(
                pipelineConfigurations);

        int minimumMappingQuality = overrides.getMinimumMappingQuality() == null
                ? variantCallParameters.getMinimumMappingQuality()
                : overrides.getMinimumMappingQuality();
        int minimumBaseQuality = overrides.getMinimumBaseQuality() == null
                ? variantCallParameters.getMinimumBaseQuality()
                : overrides.getMinimumBaseQuality();

        validateEvidenceQuality(
                minimumMappingQuality,
                minimumBaseQuality);
        applyEvidenceQuality(
                mappingParameters,
                minimumMappingQuality,
                minimumBaseQuality);
        applyEvidenceQuality(
                variantCallParameters,
                minimumMappingQuality,
                minimumBaseQuality);
    }

    private static EvidenceQualityOverrides evidenceQualityOverrides(
            PipelineConfigurations pipelineConfigurations)
            throws JsonProcessingException {
        if (pipelineConfigurations == null
                || pipelineConfigurations.getExtraParams() == null) {
            return new EvidenceQualityOverrides();
        }

        Object configuredQuality = pipelineConfigurations.getExtraParams()
                .get(EXTRA_PARAM_EVIDENCE_QUALITY);
        if (configuredQuality == null) {
            return new EvidenceQualityOverrides();
        }
        if (!(configuredQuality instanceof Map<?, ?>)) {
            throw new IllegalArgumentException(
                    "extraParams.evidenceQuality must be an object");
        }

        return JsonUtil.toObject(
                JsonUtil.toJson(configuredQuality),
                EvidenceQualityOverrides.class);
    }

    private static void validateEvidenceQuality(
            int minimumMappingQuality,
            int minimumBaseQuality) {
        if (minimumMappingQuality < 0) {
            throw new IllegalArgumentException(
                    "minimumMappingQuality must be at least 0");
        }
        if (minimumBaseQuality < 0) {
            throw new IllegalArgumentException(
                    "minimumBaseQuality must be at least 0");
        }
    }

    private static void applyEvidenceQuality(
            EvidenceQualityStageParameters parameters,
            int minimumMappingQuality,
            int minimumBaseQuality) {
        parameters.setMinimumMappingQuality(minimumMappingQuality);
        parameters.setMinimumBaseQuality(minimumBaseQuality);
    }

    private static final class EvidenceQualityOverrides {
        private Integer minimumMappingQuality;
        private Integer minimumBaseQuality;

        public Integer getMinimumMappingQuality() {
            return minimumMappingQuality;
        }

        public void setMinimumMappingQuality(Integer minimumMappingQuality) {
            this.minimumMappingQuality = minimumMappingQuality;
        }

        public Integer getMinimumBaseQuality() {
            return minimumBaseQuality;
        }

        public void setMinimumBaseQuality(Integer minimumBaseQuality) {
            this.minimumBaseQuality = minimumBaseQuality;
        }
    }

    public static class PipelineConfigurations {

        private List<ReferenceSequence> candidateReferenceSequences;
        private Map<String, Object> extraParams;

        public List<ReferenceSequence> getCandidateReferenceSequences() {
            return candidateReferenceSequences;
        }

        public void setCandidateReferenceSequences(List<ReferenceSequence> candidateReferenceSequences) {
            this.candidateReferenceSequences = candidateReferenceSequences;
        }

        public Map<String, Object> getExtraParams() {
            return extraParams;
        }

        public void setExtraParams(Map<String, Object> extraParams) {
            this.extraParams = extraParams;
        }

    }

    public static class PipelineSampleInput {

        private String r1;
        private String r2;
        private Integer sequencePlatform;
        private int sequenceLevel;

        public Integer getSequencePlatform() {
            return sequencePlatform;
        }

        public void setSequencePlatform(Integer sequencePlatform) {
            this.sequencePlatform = sequencePlatform;
        }

        public PipelineSampleInput() {
        }

        public PipelineSampleInput(String r1, String r2) {
            this.r1 = r1;
            this.r2 = r2;
        }

        public String getR1() {
            return r1;
        }

        public String getR2() {
            return r2;
        }

        public void setR1(String r1) {
            this.r1 = r1;
        }

        public void setR2(String r2) {
            this.r2 = r2;
        }

        public int getSequenceLevel() {
            return sequenceLevel;
        }

        public void setSequenceLevel(int sequenceLevel) {
            this.sequenceLevel = sequenceLevel;
        }

        // public int getReadType() {
        // return readType;
        // }

        // public void setReadType(int readType) {
        // this.readType = readType;
        // }
    }

    public static List<BioPipelineStage> buildBacteriaStages() {
        // todo
        return null;
    }

    public static void initializeParameters(BaseStageParams baseStageParams, int sequenceLevel,
            int analysisTargetType) {

        baseStageParams.setAnalysisTargetType(analysisTargetType);
        if (sequenceLevel == Constants.SequenceInput.SEQUENCE_LEVEL_READ) {
            // ReadInspect is the source of resolved metadata for every reads stage.
            baseStageParams.setReadMeta(null);
            return;
        }
        SequenceMeta sequenceMeta = new SequenceMeta();
        sequenceMeta.setSequenceLevel(sequenceLevel);
        baseStageParams.setReadMeta(sequenceMeta);
    }

    private static void initializeStageParameters(BioPipelineStage stage,
            PipelineSampleInput pipelineInput, PipelineConfigurations pipelineConfigurations,
            int analysisTargetType) throws JsonProcessingException {
        BaseStageParams parameters = StringUtils.isBlank(stage.getParameters())
                ? stageParams(stage.getStageType(), pipelineConfigurations)
                : JsonUtil.toObject(stage.getParameters(), stageParams(stage.getStageType()).getClass());
        initializeParameters(parameters, pipelineInput.getSequenceLevel(), analysisTargetType);
        stage.setParameters(JsonUtil.toJson(parameters));
    }

    private static SequenceInputMeta sequenceInputMeta(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {
        SequenceInputMeta inputMeta = null;
        if (pipelineConfigurations != null && pipelineConfigurations.getExtraParams() != null) {
            Object configuredMeta = pipelineConfigurations.getExtraParams()
                    .get(EXTRA_PARAM_SEQUENCE_INPUT_META);
            if (configuredMeta != null) {
                if (!(configuredMeta instanceof Map<?, ?>)) {
                    throw new IllegalArgumentException("extraParams.sequenceInputMeta must be an object");
                }
                inputMeta = JsonUtil.toObject(JsonUtil.toJson(configuredMeta), SequenceInputMeta.class);
            }
        }
        // A specified sample platform takes precedence; null and UNKNOWN both inherit the default.
        if (pipelineInput.getSequencePlatform() != null
                && pipelineInput.getSequencePlatform() != Constants.SequencingPlatform.UNKNOWN) {
            if (inputMeta == null) {
                inputMeta = new SequenceInputMeta();
            }
            inputMeta.setSequencingPlatform(pipelineInput.getSequencePlatform());
        }
        return inputMeta;
    }

    private static void buildReadInspectAndQcStages(List<BioPipelineStage> stages, PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        BioPipelineStage readInspectStage = createStage(PIPELINE_STAGE_READ_INSPECT);
        ReadInspectStageInputUrls readInspectStageInputUrls = new ReadInspectStageInputUrls(pipelineInput.getR1(),
                pipelineInput.getR2());
        readInspectStageInputUrls.setSequenceInputMeta(sequenceInputMeta(pipelineInput, pipelineConfigurations));
        String serializedInputUrls = JsonUtil.toJson(readInspectStageInputUrls);
        readInspectStage.setInputUrl(serializedInputUrls);
        stages.add(readInspectStage);
        BioPipelineStage qc = createStage(PIPELINE_STAGE_QC);
        stages.add(qc);

    }

    public static List<BioPipelineStage> buildRegularBacteriaPipeline(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>();

        Set<Integer> entryStages = new HashSet<>();
        if (pipelineInput.sequenceLevel == Constants.SequenceInput.SEQUENCE_LEVEL_READ) {
            buildReadInspectAndQcStages(stages, pipelineInput, pipelineConfigurations);
            entryStages.add(PIPELINE_STAGE_READ_INSPECT);
            if (Constants.SequenceInput.isFasta(pipelineInput.getR1())) {
                stages.removeIf(s -> s.getStageType() == PIPELINE_STAGE_QC);
            }
            BioPipelineStage assembly = createStage(PIPELINE_STAGE_ASSEMBLY);
            stages.add(assembly);
        } else {
            entryStages.addAll(List.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_AMR, PIPELINE_STAGE_VIRULENCE,
                    PIPELINE_STAGE_MLST));
        }

        BioPipelineStage taxonomy = createStage(PIPELINE_STAGE_TAXONOMY);
        stages.add(taxonomy);

        BioPipelineStage amr = createStage(PIPELINE_STAGE_AMR);
        stages.add(amr);

        BioPipelineStage vf = createStage(PIPELINE_STAGE_VIRULENCE);
        stages.add(vf);

        BioPipelineStage mlst = createStage(PIPELINE_STAGE_MLST);
        stages.add(mlst);

        BioPipelineStage serotype = createStage(PIPELINE_STAGE_SEROTYPE);
        stages.add(serotype);

        if (pipelineInput.sequenceLevel == Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY) {
            TaxonomyStageInputUrls taxonomyStageInputUrls = new TaxonomyStageInputUrls();
            taxonomyStageInputUrls.setContigs(pipelineInput.getR1());
            taxonomyStageInputUrls.setR1(pipelineInput.getR1());
            taxonomy.setInputUrl(JsonUtil.toJson(taxonomyStageInputUrls));

            AMRInputUrls amrInputUrls = new AMRInputUrls();
            amrInputUrls.setContigsUrl(pipelineInput.getR1());
            amr.setInputUrl(JsonUtil.toJson(amrInputUrls));

            MLSTStageInputUrls mlstStageInputUrls = new MLSTStageInputUrls();
            mlstStageInputUrls.setContigUrl(pipelineInput.getR1());
            mlst.setInputUrl(JsonUtil.toJson(mlstStageInputUrls));

            VFStageInputUrls vfStageInputUrls = new VFStageInputUrls();
            vfStageInputUrls.setContigsUrl(pipelineInput.getR1());
            vf.setInputUrl(JsonUtil.toJson(vfStageInputUrls));

        }

        for (BioPipelineStage stage : stages) {
            if (!entryStages.contains(stage.getStageType())) {
                stage.setStageIndex(-1);
            } else {
                stage.setStageIndex(0);
            }
            initializeStageParameters(
                    stage, pipelineInput, pipelineConfigurations,
                    BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA);
        }

        return stages;

    }

    public static BioPipelineStage buildSNPAnalysisMergeStage() {
        BioPipelineStage pipelineStage = new BioPipelineStage();
        pipelineStage.setStageType(PIPELINE_STAGE_SNP_MERGE_RESULT);
        return pipelineStage;
    }

    public static List<BioPipelineStage> buildSNPAnalysisStages(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        return Collections.emptyList();

    }

    /**
     * Independent single-stage entry for declared SARS-CoV-2 amplicon FASTQ inputs.
     * This boundary validates the input contract, not the laboratory preparation type.
     */
    public static List<BioPipelineStage> buildCovidWastewaterStages(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {
        if (pipelineInput == null
                || pipelineInput.getSequenceLevel() != Constants.SequenceInput.SEQUENCE_LEVEL_READ) {
            throw new IllegalArgumentException("新冠污水分析仅支持 READ 输入，请上传新冠扩增子 FASTQ");
        }
        if (StringUtils.isBlank(pipelineInput.getR1())) {
            throw new IllegalArgumentException("新冠污水分析缺少 R1，请上传新冠扩增子 FASTQ");
        }

        CovidWastewaterStageParameters parameters = (CovidWastewaterStageParameters) stageParams(
                PIPELINE_STAGE_COVID_WASTEWATER, pipelineConfigurations);
        parameters.validate();
        initializeParameters(parameters, pipelineInput.getSequenceLevel(),
                BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS);

        CovidWastewaterStageInputUrls input = new CovidWastewaterStageInputUrls();
        input.setR1Url(pipelineInput.getR1());
        input.setR2Url(pipelineInput.getR2());
        // Preserve the user's declaration; no ReadInspect-derived metadata is fabricated.
        input.setSequencingPlatform(pipelineInput.getSequencePlatform());

        BioPipelineStage stage = createStage(PIPELINE_STAGE_COVID_WASTEWATER);
        stage.setStageIndex(0);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(parameters));
        return List.of(stage);
    }

    public static List<BioPipelineStage> buildVirusStages(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);


        boolean isInputRead = pipelineInput.getSequenceLevel() == Constants.SequenceInput.SEQUENCE_LEVEL_READ;
        BioPipelineStage startStage = null;

        // qc: 如果sample为read 就做
        // 如果不是就直接reference comparsion
        if (!isInputRead) {
            BioPipelineStage referenceSelectionStage = createStage(PIPELINE_STAGE_REFERENCE_SELECTION);

            BioPipelineStage referenceComparisonStage = createStage(PIPELINE_STAGE_REFERENCE_COMPARISON);

            ReferenceSelectionStageInputUrls selectionInputUrls = new ReferenceSelectionStageInputUrls();
            selectionInputUrls.setContigsUrl(pipelineInput.getR1());

            ReferenceComparisonStageInputUrls comparisonInputUrls = new ReferenceComparisonStageInputUrls();
            comparisonInputUrls.setFastaUrl(pipelineInput.getR1());

            ReferenceSelectionStageParameters selectionParameters = (ReferenceSelectionStageParameters) stageParams(
                    PIPELINE_STAGE_REFERENCE_SELECTION, pipelineConfigurations);

            List<ReferenceSequence> candidates = pipelineConfigurations.getCandidateReferenceSequences();

            selectionParameters.setCandidateReferences(candidates);

            referenceSelectionStage.setParameters(
                    JsonUtil.toJson(selectionParameters));

            referenceSelectionStage.setInputUrl(
                    JsonUtil.toJson(selectionInputUrls));

            referenceComparisonStage.setInputUrl(
                    JsonUtil.toJson(comparisonInputUrls));

            referenceSelectionStage.setStageIndex(0);
            referenceComparisonStage.setStageIndex(-1);

            stages.add(referenceSelectionStage);
            stages.add(referenceComparisonStage);
            for (BioPipelineStage stage : stages) {
                initializeStageParameters(stage, pipelineInput, pipelineConfigurations,
                        BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS);
            }

            return stages;
        }

        buildReadInspectAndQcStages(stages, pipelineInput, pipelineConfigurations);
        startStage = stages.stream()
                .filter(s -> s.getStageType() == PIPELINE_STAGE_READ_INSPECT)
                .findAny()
                .orElse(null);

        BioPipelineStage assembly = createStage(PIPELINE_STAGE_ASSEMBLY);
        stages.add(assembly);

        BioPipelineStage referenceSelection = createStage(PIPELINE_STAGE_REFERENCE_SELECTION);
        ReferenceSelectionStageParameters referenceSelectionParameters = (ReferenceSelectionStageParameters) stageParams(
                PIPELINE_STAGE_REFERENCE_SELECTION, pipelineConfigurations);
        referenceSelectionParameters.setCandidateReferences(
                pipelineConfigurations.getCandidateReferenceSequences());
        referenceSelection.setParameters(JsonUtil.toJson(referenceSelectionParameters));
        stages.add(referenceSelection);

        BioPipelineStage mapping = createStage(PIPELINE_STAGE_MAPPING);
        MappingParameters mappingParameters = (MappingParameters) stageParams(
                PIPELINE_STAGE_MAPPING, pipelineConfigurations);
        stages.add(mapping);

        BioPipelineStage varientCall = createStage(PIPELINE_STAGE_VARIANT_CALL);
        VarientCallParameters variantCallParameters =
                (VarientCallParameters) stageParams(
                        PIPELINE_STAGE_VARIANT_CALL,
                        pipelineConfigurations);
        synchronizeEvidenceQuality(
                pipelineConfigurations,
                mappingParameters,
                variantCallParameters);
        mapping.setParameters(JsonUtil.toJson(mappingParameters));
        varientCall.setParameters(JsonUtil.toJson(variantCallParameters));
        stages.add(varientCall);

        BioPipelineStage consensus = createStage(PIPELINE_STAGE_CONSENSUS);
        stages.add(consensus);

        BioPipelineStage snp = createStage(PIPELINE_STAGE_SNP_ANNOTATION);
        stages.add(snp);

        for (BioPipelineStage stage : stages) {
            if (stage == startStage) {
                stage.setStageIndex(0);
            } else {
                stage.setStageIndex(-1);
            }
            initializeStageParameters(stage, pipelineInput, pipelineConfigurations,
                    BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS);

        }
        return stages;
    }

    public static List<BioPipelineStage> buildMetagenomeAnalysisPipeline(PipelineSampleInput pipelineSampleInput,
            int specificType) {

        BioPipelineStage bioPipelineStage = new BioPipelineStage();
        bioPipelineStage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        bioPipelineStage.setStageType(specificType);
        bioPipelineStage.setStageName(STAGE_NAME_MAP.get(bioPipelineStage.getStageType()));
        if (specificType == Constants.StageType.PIPELINE_STAGE_METAGENOMICS_SHORTGUN) {
        } else {

        }
        return null;
    }
}
