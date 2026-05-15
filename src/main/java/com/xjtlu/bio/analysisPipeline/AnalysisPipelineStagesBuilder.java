package com.xjtlu.bio.analysisPipeline;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

public class AnalysisPipelineStagesBuilder {

    public static class PipelineConfigurations {
        private long refId;
        private List<String> refseqAccessions;

        private boolean requireSNPAnnotation;
        private boolean requireCoverageDepth;

        private int sequencingPlatform;

        private String refseqObjName;

        public PipelineConfigurations() {
            this.refId = -1;
            this.sequencingPlatform = Constants.SequencingPlatform.UNKNOWN;
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

        public int getSequencingPlatform() {
            return sequencingPlatform;
        }

        public void setSequencingPlatform(int sequencingPlatform) {
            this.sequencingPlatform = sequencingPlatform;
        }

    }

    public static class PipelineSampleInput {

        private String r1;
        private String r2;

        private int readType;

        public PipelineSampleInput() {
        }

        public static int READ_TYPE_FASTA = 0;
        public static int READ_TYPE_FASTQ = 1;

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

        public int getReadType() {
            return readType;
        }

        public void setReadType(int readType) {
            this.readType = readType;
        }
    }

    public static List<BioPipelineStage> buildBacteriaStages() {
        // todo
        return null;
    }

    private static void buildReadInspectAndQcStages(List<BioPipelineStage> stages, PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        BioPipelineStage readInspectStage = new BioPipelineStage();
        readInspectStage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        readInspectStage.setStageIndex(0);
        ReadInspectStageInputUrls readInspectStageInputUrls = new ReadInspectStageInputUrls(pipelineInput.getR1(),
                pipelineInput.getR2());
        String serializedInputUrls = JsonUtil.toJson(readInspectStageInputUrls);

        readInspectStage.setInputUrl(serializedInputUrls);
        stages.add(readInspectStage);

        BioPipelineStage qc = new BioPipelineStage();
        qc.setStageType(PIPELINE_STAGE_QC);
        qc.setStageIndex(-1);

        stages.add(qc);

    }

    public static List<BioPipelineStage> buildRegularBacteriaPipeline(PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>();

        int readType = pipelineInput.getReadType();

        BioPipelineStage entry = null;

        if (readType == PipelineSampleInput.READ_TYPE_FASTQ) {
            buildReadInspectAndQcStages(stages, pipelineInput, pipelineConfigurations);

            entry = stages.stream().filter(s -> s.getStageIndex() == 0).findAny().orElse(null);

            BioPipelineStage assembly = new BioPipelineStage();
            assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
            stages.add(assembly);
        }

        BioPipelineStage taxonomy = new BioPipelineStage();
        taxonomy.setStageType(PIPELINE_STAGE_TAXONOMY);
        stages.add(taxonomy);

        if (readType == PipelineSampleInput.READ_TYPE_FASTA) {
            TaxonomyStageInputUrls taxonomyStageInputUrls = new TaxonomyStageInputUrls();
            taxonomyStageInputUrls.setR1(pipelineInput.getR1());
            taxonomyStageInputUrls.setR2(pipelineInput.getR2());
            taxonomy.setInputUrl(JsonUtil.toJson(taxonomyStageInputUrls));
            taxonomy.setStageIndex(0);
            entry = taxonomy;
        }

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
        String serializedPamras = JsonUtil.toJson(baseStageParams);

        for (BioPipelineStage stage : stages) {
            if (stage != entry) {
                stage.setStageIndex(-1);
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

        if (pipelineInput.getReadType() == PipelineSampleInput.READ_TYPE_FASTQ) {
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
        BaseStageParams baseStageParams = new BaseStageParams(refSeqConfig, null);

        BioPipelineStage startStage = null;

        buildReadInspectAndQcStages(stages, pipelineInput, pipelineConfigurations);

        startStage = stages.stream().filter(s -> s.getStageIndex() == 0).findAny().orElse(null);

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
