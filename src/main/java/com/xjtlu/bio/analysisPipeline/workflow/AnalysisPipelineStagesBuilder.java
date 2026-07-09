package com.xjtlu.bio.analysisPipeline.workflow;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tomcat.util.bcel.classfile.Constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializable.Base;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

public class AnalysisPipelineStagesBuilder {

    public static class PipelineConfigurations {
        private long refId;
        private List<String> refseqAccessions;

        private boolean requireSNPAnnotation;
        private boolean requireCoverageDepth;

        private String refseqObjName;

        public PipelineConfigurations() {
            this.refId = -1;

        }

        public long getRefId() {
            return refId;
        }

        public boolean isRequireSNPAnnotation() {
            return requireSNPAnnotation;
        }

        public void setRequireSNPAnnotation(boolean requireSNPAnnotation) {
            this.requireSNPAnnotation = requireSNPAnnotation;
        }

        public boolean isRequireCoverageDepth() {
            return requireCoverageDepth;
        }

        public void setRequireCoverageDepth(boolean requireCoverageDepth) {
            this.requireCoverageDepth = requireCoverageDepth;
        }

        public void setRefId(long refId) {
            this.refId = refId;
        }

        public List<String> getRefseqAccessions() {
            return refseqAccessions;
        }

        public void setRefseqAccessions(List<String> refseqAccessions) {
            this.refseqAccessions = refseqAccessions;
        }

        public String getRefseqObjName() {
            return refseqObjName;
        }

        public void setRefseqObjName(String refseqObjName) {
            this.refseqObjName = refseqObjName;
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

    public static void initializeParameters(BaseStageParams baseStageParams, int sequenceLevel, int analysisTargetType, boolean isInnerRefseq, String refseqObjectName){

        baseStageParams.setAnalysisTargetType(analysisTargetType);
        SequenceMeta sequenceMeta = new SequenceMeta();
        sequenceMeta.setSequenceLevel(sequenceLevel);
        sequenceMeta.setQualityEncoding(Constants.SequenceInput.QUALITY_ENCODING_33);
        sequenceMeta.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        
        baseStageParams.setReadMeta(sequenceMeta);
        RefSeqConfig refSeqConfig = new RefSeqConfig();
        refSeqConfig.setInnerRefSeq(isInnerRefseq);
        refSeqConfig.setRefseqObjectName(refseqObjectName);
        baseStageParams.setRefSeqConfig(refSeqConfig);
        
    }

    private static void buildReadInspectAndQcStages(List<BioPipelineStage> stages, PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        BioPipelineStage readInspectStage = new BioPipelineStage();
        readInspectStage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        ReadInspectStageInputUrls readInspectStageInputUrls = new ReadInspectStageInputUrls(pipelineInput.getR1(),
                pipelineInput.getR2());
        String serializedInputUrls = JsonUtil.toJson(readInspectStageInputUrls);

        readInspectStage.setInputUrl(serializedInputUrls);
        stages.add(readInspectStage);

        BioPipelineStage qc = new BioPipelineStage();
        qc.setStageType(PIPELINE_STAGE_QC);

        stages.add(qc);

    }

    public static List<BioPipelineStage> buildRegularBacteriaPipeline(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>();
        
        Set<Integer> entryStages = new HashSet<>();
        if(pipelineInput.sequenceLevel == Constants.SequenceInput.SEQUENCE_LEVEL_READ){
            buildReadInspectAndQcStages(stages, pipelineInput, pipelineConfigurations);
            entryStages.add(PIPELINE_STAGE_READ_INSPECT);
            if(Constants.SequenceInput.isFasta(pipelineInput.getR1())){
                stages.removeIf(s->s.getStageType() == PIPELINE_STAGE_QC);
            }
            BioPipelineStage assembly = new BioPipelineStage();
            assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
            stages.add(assembly);
        }else{
            entryStages.addAll(List.of(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_AMR, PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_MLST));
        }
        

        BioPipelineStage taxonomy = new BioPipelineStage();
        taxonomy.setStageType(PIPELINE_STAGE_TAXONOMY);
        stages.add(taxonomy);
        

        BioPipelineStage amr = new BioPipelineStage();
        amr.setStageType(PIPELINE_STAGE_AMR);
        stages.add(amr);
        
        BioPipelineStage vf = new BioPipelineStage();
        vf.setStageType(PIPELINE_STAGE_VIRULENCE);
        stages.add(vf);

        BioPipelineStage mlst = new BioPipelineStage();
        mlst.setStageType(PIPELINE_STAGE_MLST);
        stages.add(mlst);

        BioPipelineStage serotype = new BioPipelineStage();
        serotype.setStageType(PIPELINE_STAGE_SEROTYPE);
        stages.add(serotype);        

        BaseStageParams baseStageParams = new BaseStageParams();

        initializeParameters(baseStageParams, pipelineInput.getSequenceLevel(), BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, false, null);
        
        String serializedPamras = JsonUtil.toJson(baseStageParams);

        if(pipelineInput.sequenceLevel == Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY){
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
            if(!entryStages.contains(stage.getStageType())){
                stage.setStageIndex(-1);
            }else{
                stage.setStageIndex(0);
            }
            stage.setParameters(serializedPamras);
            stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            stage.setStageName(STAGE_NAME_MAP.get(stage.getStageType()));
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

        List<BioPipelineStage> stages = new ArrayList<>();
        String refseqObject = pipelineConfigurations.getRefseqObjName();

        RefSeqConfig refSeqConfig = new RefSeqConfig();
        refSeqConfig.setInnerRefSeq(false);
        refSeqConfig.setRefseqObjectName(refseqObject);

        BioPipelineStage firstStage = null;

        if (false) {
            BioPipelineStage qc = new BioPipelineStage();
            qc.setStageType(PIPELINE_STAGE_QC);
            QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
            qcStageInputUrls.setRead1(pipelineInput.getR1());
            qcStageInputUrls.setRead2(pipelineInput.getR2());
            qc.setInputUrl(JsonUtil.toJson(qcStageInputUrls));
            QcParameters qcParameters = new QcParameters();
            qcParameters.setRefSeqConfig(refSeqConfig);
            qc.setParameters(JsonUtil.toJson(qcParameters));
            qc.setStageName(STAGE_NAME_MAP.get(PIPELINE_STAGE_QC));
            firstStage = qc;
        } else {
            BioPipelineStage mapping = new BioPipelineStage();
            mapping.setStageType(PIPELINE_STAGE_MAPPING);
            mapping.setStageName(STAGE_NAME_MAP.get(PIPELINE_STAGE_MAPPING));

            MappingInputUrls mappingInputUrls = new MappingInputUrls();
            mappingInputUrls.setR1Url(pipelineInput.getR1());
            mappingInputUrls.setR2Url(pipelineInput.getR2());

            MappingParameters mappingParameters = new MappingParameters();
            mappingParameters.setRefSeqConfig(refSeqConfig);

            mapping.setInputUrl(JsonUtil.toJson(mappingInputUrls));
            mapping.setParameters(JsonUtil.toJson(mappingParameters));

            firstStage = mapping;

        }

        firstStage.setStageIndex(0);
        firstStage.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        stages.add(firstStage);

        BioPipelineStage varientCall = new BioPipelineStage();
        varientCall.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        varientCall.setStageName(STAGE_NAME_MAP.get(PIPELINE_STAGE_VARIANT_CALL));
        VarientCallParameters varientCallParameters = new VarientCallParameters();
        varientCallParameters.setRefSeqConfig(refSeqConfig);

        varientCall.setParameters(JsonUtil.toJson(varientCallParameters));
        varientCall.setStageIndex(-1);
        varientCall.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        stages.add(varientCall);

        return stages;

    }

    public static List<BioPipelineStage> buildVirusStages(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);

        RefSeqConfig refSeqConfig = new RefSeqConfig();
        refSeqConfig.setRefseqObjectName(pipelineConfigurations.getRefseqObjName());
        BaseStageParams baseStageParams = new BaseStageParams();
        initializeParameters(baseStageParams, pipelineInput.sequenceLevel, BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS, false ,pipelineConfigurations.getRefseqObjName());

        BioPipelineStage startStage = null;

        if (pipelineInput.sequenceLevel == Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY) {

            BioPipelineStage referenceComparison = new BioPipelineStage();
            referenceComparison.setStageIndex(0);

            ReferenceComparisonStageParameters referenceComparisonStageParameters = new ReferenceComparisonStageParameters();
            referenceComparisonStageParameters.setRefSeqConfig(refSeqConfig);

            ReferenceComparisonStageInputUrls referenceComparisonStageInputUrls = new ReferenceComparisonStageInputUrls();
            referenceComparisonStageInputUrls.setFastaUrl(pipelineInput.getR1());

            String serializedInput = JsonUtil.toJson(referenceComparisonStageInputUrls);
            String serializedParameters = JsonUtil.toJson(referenceComparisonStageParameters);

            referenceComparison.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            referenceComparison.setStageType(Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON);
            referenceComparison
                    .setStageName(STAGE_NAME_MAP.get(Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON));
            referenceComparison.setInputUrl(serializedInput);
            referenceComparison.setParameters(serializedParameters);

            stages.add(referenceComparison);

            return stages;
        }

        buildReadInspectAndQcStages(stages, pipelineInput, pipelineConfigurations);

        startStage = stages.stream().filter(s -> s.getStageType() == PIPELINE_STAGE_READ_INSPECT).findAny().orElse(null);

        BioPipelineStage mapping = new BioPipelineStage();
        mapping.setStageType(PIPELINE_STAGE_MAPPING);
        stages.add(mapping);

        BioPipelineStage varientCall = new BioPipelineStage();
        varientCall.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        stages.add(varientCall);

        BioPipelineStage consensus = new BioPipelineStage();
        consensus.setStageType(PIPELINE_STAGE_CONSENSUS);
        stages.add(consensus);

        if (pipelineConfigurations.isRequireSNPAnnotation()) {
            BioPipelineStage snp = new BioPipelineStage();
            snp.setStageType(PIPELINE_STAGE_SNP_ANNOTATION);
            stages.add(snp);
        }

        if (pipelineConfigurations.isRequireCoverageDepth()) {
            BioPipelineStage depth = new BioPipelineStage();
            depth.setStageType(PIPELINE_STAGE_DEPTH_COVERAGE);
            stages.add(depth);
        }

        String serializedParams = JsonUtil.toJson(baseStageParams);
        for (BioPipelineStage stage : stages) {
            if (stage == startStage) {
                stage.setStageIndex(0);
            } else {
                stage.setStageIndex(-1);
            }
            stage.setStageName(STAGE_NAME_MAP.get(stage.getStageType()));
            stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            stage.setParameters(serializedParams);
        }

        return stages;

    }
}
