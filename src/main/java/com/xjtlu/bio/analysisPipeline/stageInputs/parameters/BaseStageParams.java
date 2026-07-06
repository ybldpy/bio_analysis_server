package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;

public class BaseStageParams {

    private SequenceMeta readMeta;

    private RefSeqConfig refSeqConfig;
    private TaxonomyContext taxonomyContext;

    private int analysisTargetType;

    public static final int ANALYSIS_TARGET_TYPE_VIRUS = 10;
    public static final int ANALYSIS_TARGET_TYPE_BACTERIA = 20;



    

    public SequenceMeta getReadMeta() {
        return readMeta;
    }

    public int getAnalysisTargetType() {
        return analysisTargetType;
    }

    public void setAnalysisTargetType(int analysisTargetType) {
        this.analysisTargetType = analysisTargetType;
    }

    public BaseStageParams(int analysisTargetType, RefSeqConfig refSeqConfig, TaxonomyContext taxonomyContext, SequenceMeta readMeta) {
        this.refSeqConfig = refSeqConfig;
        this.taxonomyContext = taxonomyContext;
        this.readMeta = readMeta;
        this.analysisTargetType = analysisTargetType;
    }

    public BaseStageParams(int pipelineType, RefSeqConfig refSeqConfig, TaxonomyContext taxonomyContext){
        this(pipelineType, refSeqConfig, taxonomyContext, null);
    }


    public BaseStageParams(){
    }
    public RefSeqConfig getRefSeqConfig() {
        return refSeqConfig;
    }
    public SequenceMeta getSequenceMeta() {
        return readMeta;
    }




    public void setReadMeta(SequenceMeta readMeta) {
        this.readMeta = readMeta;
    }




    public void setRefSeqConfig(RefSeqConfig refSeqConfig) {
        this.refSeqConfig = refSeqConfig;
    }
    public TaxonomyContext getTaxonomyContext() {
        return taxonomyContext;
    }
    public void setTaxonomyContext(TaxonomyContext taxonomyContext) {
        this.taxonomyContext = taxonomyContext;
    }

    

}
