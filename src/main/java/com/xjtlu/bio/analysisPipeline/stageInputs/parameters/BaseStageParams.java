package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;

public class BaseStageParams {

    private SequenceMeta readMeta;

    private RefSeqConfig refSeqConfig;
    private TaxonomyContext taxonomyContext;




    public BaseStageParams(RefSeqConfig refSeqConfig, TaxonomyContext taxonomyContext, SequenceMeta readMeta) {
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
