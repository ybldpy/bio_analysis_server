package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

public class BaseStageParams {

    private RefSeqConfig refSeqConfig;
    private TaxonomyContext taxonomyContext;
    public BaseStageParams(RefSeqConfig refSeqConfig, TaxonomyContext taxonomyContext) {
        this.refSeqConfig = refSeqConfig;
        this.taxonomyContext = taxonomyContext;
    }

    public BaseStageParams(){
    }
    public RefSeqConfig getRefSeqConfig() {
        return refSeqConfig;
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
