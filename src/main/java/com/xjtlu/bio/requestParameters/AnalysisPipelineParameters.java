package com.xjtlu.bio.requestParameters;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAlias;

public class AnalysisPipelineParameters {

    private Long referenceId;
    private Integer taxId;
    // 用户的平台声明；null、未传和 UNKNOWN（0）均表示未指定平台，允许自动检测。
    private Integer sequencingPlatform;

    public Integer getSequencingPlatform() {
        return sequencingPlatform;
    }

    public void setSequencingPlatform(Integer sequencingPlatform) {
        this.sequencingPlatform = sequencingPlatform;
    }

    public AnalysisPipelineParameters() {
    }

    @JsonAlias("extraParameters")
    private Map<String, Object> extraParams;

    public Long getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(Long referenceId) {
        this.referenceId = referenceId;
    }

    public Map<String, Object> getExtraParams() {
        return extraParams;
    }

    public void setExtraParams(Map<String, Object> extraParams) {
        this.extraParams = extraParams;
    }

    public AnalysisPipelineParameters(Long referenceId, Map<String, Object> extraParams) {
        this.referenceId = referenceId;
        this.extraParams = extraParams;
    }

    public Integer getTaxId() {
        return taxId;
    }

    public void setTaxId(Integer taxId) {
        this.taxId = taxId;
    }

}
