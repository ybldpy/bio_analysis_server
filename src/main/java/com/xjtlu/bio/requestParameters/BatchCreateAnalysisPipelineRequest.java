package com.xjtlu.bio.requestParameters;

import java.util.List;
import java.util.Map;

import jakarta.validation.constraints.NotNull;

public class BatchCreateAnalysisPipelineRequest {


    public static class AnalysisPipeline {
        @NotNull
        private String pipelineName;
        private Map<String,Object> pipelineParameters;
        public AnalysisPipeline() {
        }
        public AnalysisPipeline(@NotNull String pipelineName, Map<String, Object> pipelineParameters) {
            this.pipelineName = pipelineName;
            this.pipelineParameters = pipelineParameters;
        }
        public String getPipelineName() {
            return pipelineName;
        }
        public void setPipelineName(String pipelineName) {
            this.pipelineName = pipelineName;
        }
        public Map<String, Object> getPipelineParameters() {
            return pipelineParameters;
        }
        public void setPipelineParameters(Map<String, Object> pipelineParameters) {
            this.pipelineParameters = pipelineParameters;
        }
    }
    @NotNull
    private Integer pipelineType;
    @NotNull
    private Long projectId;
    @NotNull
    private List<AnalysisPipeline> pipelines;
    public BatchCreateAnalysisPipelineRequest(@NotNull Integer pipelineType, @NotNull Long projectId,
            @NotNull List<AnalysisPipeline> pipelines) {
        this.pipelineType = pipelineType;
        this.projectId = projectId;
        this.pipelines = pipelines;
    }
    public BatchCreateAnalysisPipelineRequest() {
    }
    public Integer getPipelineType() {
        return pipelineType;
    }
    public void setPipelineType(Integer pipelineType) {
        this.pipelineType = pipelineType;
    }
    public Long getProjectId() {
        return projectId;
    }
    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }
    public List<AnalysisPipeline> getPipelines() {
        return pipelines;
    }
    public void setPipelines(List<AnalysisPipeline> pipelines) {
        this.pipelines = pipelines;
    }

    


}
