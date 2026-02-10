package com.xjtlu.bio.requestParameters;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateSampleRequest {

    public static class PipelineStageParameters {
        private Long refseq;

        Map<String, Object> extraParams;

        public Map<String, Object> getExtraParams() {
            return extraParams;
        }

        public Long getRefseq() {
            return refseq;
        }

        public void setRefseq(Long refseq) {
            this.refseq = refseq;
        }

        public void setExtraParams(Map<String, Object> extraParams) {
            this.extraParams = extraParams;
        }

    }



    @NotBlank(message = "sampleName不能为空")
    private String sampleName;

    // 用 Boolean 才能判断“没传 vs 传了false”
    @NotNull(message = "isPair不能为空")
    @JsonProperty("isPair")
    private Boolean isPair;

    @NotNull(message = "projectId不能为空")
    private Integer projectId;

    @NotNull(message = "sampleType不能为空")
    private Integer sampleType;

    @NotBlank(message = "read1OriginName不能为空")
    private String read1OriginName;

    // pair=true 时建议必填（后续用自定义校验实现）
    private String read2OriginName;

    
    private PipelineStageParameters pipelineStageParameters;

    public String getSampleName() {
        return sampleName;
    }

    public void setSampleName(String sampleName) {
        this.sampleName = sampleName;
    }

    public boolean isPair() {
        return isPair;
    }

    public void setPair(boolean isPair) {
        this.isPair = isPair;
    }

    public Integer getProjectId() {
        return projectId;
    }

    public void setProjectId(Integer projectId) {
        this.projectId = projectId;
    }

    public Integer getSampleType() {
        return sampleType;
    }

    public void setSampleType(Integer sampleType) {
        this.sampleType = sampleType;
    }

    public String getRead1OriginName() {
        return read1OriginName;
    }

    public void setRead1OriginName(String read1OriginName) {
        this.read1OriginName = read1OriginName;
    }

    public String getRead2OriginName() {
        return read2OriginName;
    }

    public void setRead2OriginName(String read2OriginName) {
        this.read2OriginName = read2OriginName;
    }

    public PipelineStageParameters getPipelineStageParameters() {
        return pipelineStageParameters;
    }

    public void setPipelineStageParameters(PipelineStageParameters pipelineStageParameters) {
        this.pipelineStageParameters = pipelineStageParameters;
    }

}
