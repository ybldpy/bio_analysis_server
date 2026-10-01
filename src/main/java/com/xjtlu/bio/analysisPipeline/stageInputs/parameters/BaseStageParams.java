package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;

public class BaseStageParams {

    private SequenceMeta readMeta;
    private TaxonomyContext taxonomyContext;
    private ReferenceGenome referenceGenome;


    private int analysisTargetType;

    public static final int ANALYSIS_TARGET_TYPE_VIRUS = 10;
    public static final int ANALYSIS_TARGET_TYPE_BACTERIA = 20;


    

    public ReferenceGenome getReferenceGenome() {
        return referenceGenome;
    }

    public void setReferenceGenome(ReferenceGenome referenceGenome) {
        this.referenceGenome = referenceGenome;
    }

    public SequenceMeta getReadMeta() {
        return readMeta;
    }

    public int getAnalysisTargetType() {
        return analysisTargetType;
    }

    public void setAnalysisTargetType(int analysisTargetType) {
        this.analysisTargetType = analysisTargetType;
    }

    public BaseStageParams(int analysisTargetType, TaxonomyContext taxonomyContext, SequenceMeta readMeta) {
        this.taxonomyContext = taxonomyContext;
        this.readMeta = readMeta;
        this.analysisTargetType = analysisTargetType;
    }

    public BaseStageParams(int pipelineType, TaxonomyContext taxonomyContext) {
        this(pipelineType, taxonomyContext, null);
    }


    public BaseStageParams() {

    }
    public SequenceMeta getSequenceMeta() {
        return readMeta;
    }




    public void setReadMeta(SequenceMeta readMeta) {
        this.readMeta = readMeta;
    }

    public TaxonomyContext getTaxonomyContext() {
        return taxonomyContext;
    }
    public void setTaxonomyContext(TaxonomyContext taxonomyContext) {
        this.taxonomyContext = taxonomyContext;
    }

    

}
