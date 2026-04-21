package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import com.xjtlu.bio.analysisPipeline.context.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.meta.ReadMeta;

public class BaseStageParams {






    private ReadMeta readMeta;

    private RefSeqConfig refSeqConfig;
    private TaxonomyContext taxonomyContext;




    public BaseStageParams(RefSeqConfig refSeqConfig, TaxonomyContext taxonomyContext, ReadMeta readMeta) {
        this.refSeqConfig = refSeqConfig;
        this.taxonomyContext = taxonomyContext;
        this.readMeta = readMeta;
    }

    public BaseStageParams(RefSeqConfig refSeqConfig, TaxonomyContext taxonomyContext){
        this(refSeqConfig, taxonomyContext, null);
    }


    public BaseStageParams(){
    }
    public RefSeqConfig getRefSeqConfig() {
        return refSeqConfig;
    }
    public ReadMeta getReadMeta() {
        return readMeta;
    }




    public void setReadMeta(ReadMeta readMeta) {
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
