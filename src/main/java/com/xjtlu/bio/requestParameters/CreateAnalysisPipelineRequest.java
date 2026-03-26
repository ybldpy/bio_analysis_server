package com.xjtlu.bio.requestParameters;


import jakarta.validation.constraints.*;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateAnalysisPipelineRequest {

    public static class PipelineStageParameters {
        private Integer taxId;

        Map<String, Object> extraParams;

        public Map<String, Object> getExtraParams() {
            return extraParams;
        }

        

        public PipelineStageParameters(Integer taxId, Map<String, Object> extraParams) {
            this.taxId = taxId;
            this.extraParams = extraParams;
        }



        public void setExtraParams(Map<String, Object> extraParams) {
            this.extraParams = extraParams;
        }



        public Integer getTaxId() {
            return taxId;
        }



        public void setTaxId(Integer taxId) {
            this.taxId = taxId;
        }



        

    }




    // 用 Boolean 才能判断“没传 vs 传了false”
    @NotNull(message = "isPair不能为空")
    @JsonProperty("isPair")
    private Boolean isPair;

    @NotNull(message = "projectId不能为空")
    private Long projectId;

    @NotNull(message = "sampleType不能为空")
    private Integer pipelineType;

    @NotBlank(message = "read1OriginName不能为空")
    private String read1OriginName;

    // pair=true 时建议必填（后续用自定义校验实现）
    private String read2OriginName;

    @NotNull(message = "分析名称不能为空")
    private String analysisName;


    
    private PipelineStageParameters pipelineStageParameters;

    

    public void setPipelineType(Integer pipelineType) {
        this.pipelineType = pipelineType;
    }

    public String getAnalysisName() {
        return analysisName;
    }

    public void setAnalysisName(String analysisName) {
        this.analysisName = analysisName;
    }

    public boolean isPair() {
        return isPair;
    }

    public void setPair(boolean isPair) {
        this.isPair = isPair;
    }

    public Long getProjectId() {
        return projectId;
    }

    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    public void setPipelineType(int pipelineType){
        this.pipelineType = pipelineType;
    }

    public int getPipelineType(){
        return this.pipelineType;
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
