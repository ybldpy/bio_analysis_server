package com.xjtlu.bio.requestParameters;

import java.util.Map;

public class AnalysisPipelineParameters {


    private Long referenceId;

    public AnalysisPipelineParameters() {
    }

    private Map<String,Object> extraParameters;

    public Long getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(Long referenceId) {
        this.referenceId = referenceId;
    }

    public Map<String, Object> getExtraParameters() {
        return extraParameters;
    }

    public void setExtraParameters(Map<String, Object> extraParameters) {
        this.extraParameters = extraParameters;
    }

    public AnalysisPipelineParameters(Long referenceId, Map<String, Object> extraParameters) {
        this.referenceId = referenceId;
        this.extraParameters = extraParameters;
    }

    

    

    



}
