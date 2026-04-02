package com.xjtlu.bio.analysisPipeline;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

public class AnalysisPipelineStagesBuilder {

    public static class PipelineConfigurations {
        private long refId;
        private List<String> refseqAccessions;

        private boolean requireSNPAnnotation;
        private boolean requireCoverageDepth;
        // 1. firstly taxonomy idenfiticatiton
        // 2. let user to upload reference.
        // usually used for bacteria part
        private String customReferenceSequenceObjectName;

        private int readsType;

        public static final int READ_TYPE_FASTQ = 0;
        public static final int READ_TYPE_FASTA = 1;

        public PipelineConfigurations() {

        }

        public String getCustomReferenceSequenceObjectName() {
            return customReferenceSequenceObjectName;
        }

        public void setCustomReferenceSequenceObjectName(String customReferenceSequenceObjectName) {
            this.customReferenceSequenceObjectName = customReferenceSequenceObjectName;
        }

        public int getReadsType() {
            return readsType;
        }

        public void setReadsType(int readsType) {
            this.readsType = readsType;
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

    }

    public static class PipelineSampleInput {

        private String r1;
        private String r2;

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
    }

    public static List<BioPipelineStage> buildBacteriaStages() {
        // todo
        return null;
    }

    public static List<BioPipelineStage> buildRegularBacteriaPipeline(long pid, PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>();

        int readType = pipelineConfigurations.getReadsType();

        if (readType == PipelineConfigurations.READ_TYPE_FASTQ) {

            BioPipelineStage qc = new BioPipelineStage();
            QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
            qcStageInputUrls.setRead1(pipelineInput.getR1());
            qcStageInputUrls.setRead2(pipelineInput.getR2());
            qc.setStageType(PIPELINE_STAGE_QC);
            qc.setInputUrl(JsonUtil.toJson(qcStageInputUrls));
            stages.add(qc);
        }



        BioPipelineStage taxonomy = new BioPipelineStage();
        taxonomy.setStageType(PIPELINE_STAGE_TAXONOMY);
        stages.add(taxonomy);

        if(readType == PipelineConfigurations.READ_TYPE_FASTA){
            TaxonomyStageInputUrls taxonomyStageInputUrls = new TaxonomyStageInputUrls();
            taxonomyStageInputUrls.setR1(pipelineInput.getR1());
            taxonomyStageInputUrls.setR2(pipelineInput.getR2());
            taxonomy.setInputUrl(JsonUtil.toJson(taxonomyStageInputUrls));
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
        stages.forEach(s -> {
            if(readType == PipelineConfigurations.READ_TYPE_FASTQ && s.getStageType() == PIPELINE_STAGE_QC){
                    s.setStageIndex(0);
            }
            else if(readType == PipelineConfigurations.READ_TYPE_FASTA && s.getStageType() == PIPELINE_STAGE_TAXONOMY){
                s.setStageIndex(0);
            }else {
                s.setStageIndex(-1);
            }
            s.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            // TODO: now all use base params here. To be improved later
            s.setParameters(serializedPamras);
            s.setStageName(STAGE_NAME_MAP.get(s.getStageType()));
            s.setPipelineId(pid);
        });

        return stages;

    }

    public static List<BioPipelineStage> buildVirusStages(long pid, PipelineSampleInput pipelineInput,
            PipelineConfigurations pipelineConfigurations) throws JsonProcessingException {

        ArrayList<BioPipelineStage> stages = new ArrayList<>(16);
        String qcInputRead1 = pipelineInput.getR1();
        String qcInputRead2 = pipelineInput.getR2();

        long refseqId = pipelineConfigurations.getRefId();
        RefSeqConfig refSeqConfig = new RefSeqConfig();
        refSeqConfig.setAccessions(pipelineConfigurations.getRefseqAccessions());
        refSeqConfig.setRefseqId(refseqId);
        refSeqConfig.setInnerRefSeq(refseqId >= 0);
        BaseStageParams baseStageParams = new BaseStageParams(refSeqConfig, null);

        BioPipelineStage qc = new BioPipelineStage();
        QcStageInputUrls qcStageInputUrls = new QcStageInputUrls();
        qcStageInputUrls.setRead1(qcInputRead1);
        qcStageInputUrls.setRead2(qcInputRead2);
        qc.setStageType(PIPELINE_STAGE_QC);
        qc.setInputUrl(JsonUtil.toJson(qcStageInputUrls));

        stages.add(qc);

        if (refSeqConfig.getRefseqId() == -1) {
            BioPipelineStage assembly = new BioPipelineStage();
            assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
            stages.add(assembly);
        }

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
            if (stage.getStageType() == PIPELINE_STAGE_QC) {
                stage.setStageIndex(0);
            } else {
                stage.setStageIndex(-1);
            }
            stage.setPipelineId(pid);
            stage.setStageName(STAGE_NAME_MAP.get(stage.getStageType()));
            stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
            stage.setParameters(serializedParams);
        }

        return stages;

    }
}
