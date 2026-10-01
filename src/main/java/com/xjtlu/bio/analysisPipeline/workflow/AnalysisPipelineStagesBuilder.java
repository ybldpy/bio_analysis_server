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
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.StageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AMRParamters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AssemblyStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
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
            Map.entry(PIPELINE_STAGE_SNP_ANNOTATION, SNPAnnotationStageParameters::new),
            Map.entry(PIPELINE_STAGE_AMR, AMRParamters::new),
            Map.entry(PIPELINE_STAGE_VIRULENCE, VFParameters::new),
            Map.entry(PIPELINE_STAGE_SEROTYPE, SeroTypingStageParameters::new));

    private static BaseStageParams stageParams(int stageType) {
        return STAGE_PARAMS_FACTORY_MAP
                .getOrDefault(stageType, BaseStageParams::new)
                .get();
    }

    public static class PipelineConfigurations {

        private List<ReferenceSequence> candidateReferenceSequences;

        public List<ReferenceSequence> getCandidateReferenceSequences() {
            return candidateReferenceSequences;
        }

        public void setCandidateReferenceSequences(List<ReferenceSequence> candidateReferenceSequences) {
            this.candidateReferenceSequences = candidateReferenceSequences;
        }

    }

    public static class PipelineSampleInput {

        private String r1;
        private String r2;
        private int sequencePlatform;
        private int sequenceLevel;

        public int getSequencePlatform() {
            return sequencePlatform;
        }

        public void setSequencePlatform(int sequencePlatform) {
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
        SequenceMeta sequenceMeta = new SequenceMeta();
        sequenceMeta.setSequenceLevel(sequenceLevel);
        sequenceMeta.setQualityEncoding(Constants.SequenceInput.QUALITY_ENCODING_33);
        sequenceMeta.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_SHORT);

        baseStageParams.setReadMeta(sequenceMeta);
    }

    private static void buildReadInspectAndQcStages(List<BioPipelineStage> stages, PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        BioPipelineStage readInspectStage = createStage(PIPELINE_STAGE_READ_INSPECT);
        ReadInspectStageInputUrls readInspectStageInputUrls = new ReadInspectStageInputUrls(pipelineInput.getR1(),
                pipelineInput.getR2());
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
            BaseStageParams parameters = stageParams(stage.getStageType());
            initializeParameters(
                    parameters,
                    pipelineInput.getSequenceLevel(),
                    BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA);
            stage.setParameters(JsonUtil.toJson(parameters));
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

        // List<BioPipelineStage> stages = new ArrayList<>();
        // String refseqObject = pipelineConfigurations.getRefseqObjName();

        // RefSeqConfig refSeqConfig = new RefSeqConfig();
        // refSeqConfig.setInnerRefSeq(false);
        // refSeqConfig.setRefseqObjectName(refseqObject);

        // BioPipelineStage firstStage = null;

        // if (false) {
        //     BioPipelineStage qc = new BioPipelineStage();
        //     qc.setStageType(PIPELINE_STAGE_QC);
        //     QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
        //     qcStageInputUrls.setRead1(pipelineInput.getR1());
        //     qcStageInputUrls.setRead2(pipelineInput.getR2());
        //     qc.setInputUrl(JsonUtil.toJson(qcStageInputUrls));
        //     QcParameters qcParameters = new QcParameters();
        //     qcParameters.setRefSeqConfig(refSeqConfig);
        //     qc.setParameters(JsonUtil.toJson(qcParameters));
        //     qc.setStageName(STAGE_NAME_MAP.get(PIPELINE_STAGE_QC));
        //     firstStage = qc;
        // } else {
        //     BioPipelineStage mapping = new BioPipelineStage();
        //     mapping.setStageType(PIPELINE_STAGE_MAPPING);
        //     mapping.setStageName(STAGE_NAME_MAP.get(PIPELINE_STAGE_MAPPING));

        //     MappingInputUrls mappingInputUrls = new MappingInputUrls();
        //     mappingInputUrls.setR1Url(pipelineInput.getR1());
        //     mappingInputUrls.setR2Url(pipelineInput.getR2());

        //     MappingParameters mappingParameters = new MappingParameters();
        //     mappingParameters.setRefSeqConfig(refSeqConfig);

        //     mapping.setInputUrl(JsonUtil.toJson(mappingInputUrls));
        //     mapping.setParameters(JsonUtil.toJson(mappingParameters));

        //     firstStage = mapping;

        // }

        // firstStage.setStageIndex(0);
        // firstStage.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        // stages.add(firstStage);

        // BioPipelineStage varientCall = new BioPipelineStage();
        // varientCall.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        // varientCall.setStageName(STAGE_NAME_MAP.get(PIPELINE_STAGE_VARIANT_CALL));
        // VarientCallParameters varientCallParameters = new VarientCallParameters();
        // varientCallParameters.setRefSeqConfig(refSeqConfig);

        // varientCall.setParameters(JsonUtil.toJson(varientCallParameters));
        // varientCall.setStageIndex(-1);
        // varientCall.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        // stages.add(varientCall);

        return Collections.emptyList();

    }

    public static List<BioPipelineStage> buildVirusStages(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);

        // BaseStageParams baseStageParams = new BaseStageParams();
        // initializeParameters(baseStageParams, pipelineInput.sequenceLevel,
        // BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS, false
        // ,pipelineConfigurations.getRefseqObjName(),
        // pipelineConfigurations.getGff3ObjName());

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

            ReferenceSelectionStageParameters selectionParameters = new ReferenceSelectionStageParameters();

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
        ReferenceSelectionStageParameters referenceSelectionParameters = new ReferenceSelectionStageParameters();
        referenceSelectionParameters.setCandidateReferences(
                pipelineConfigurations.getCandidateReferenceSequences());
        referenceSelection.setParameters(JsonUtil.toJson(referenceSelectionParameters));
        stages.add(referenceSelection);

        BioPipelineStage mapping = createStage(PIPELINE_STAGE_MAPPING);
        stages.add(mapping);

        BioPipelineStage varientCall = createStage(PIPELINE_STAGE_VARIANT_CALL);
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
            if (StringUtils.isBlank(stage.getParameters())) {
                stage.setParameters(JsonUtil.toJson(stageParams(stage.getStageType())));
            }

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
