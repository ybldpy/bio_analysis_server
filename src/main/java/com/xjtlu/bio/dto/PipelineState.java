package com.xjtlu.bio.dto;

import java.util.List;

import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineStage;

public class PipelineState {
    private long pipelineId;
    private String pipelineName;
    private int pipelineType;
    private String projectName;    
    private List<BioPipelineInputFile> pipelineInputs;
    private List<BioPipelineStage> pipelineStages;
    public long getPipelineId() {
        return pipelineId;
    }
    public void setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
    }
    public String getPipelineName() {
        return pipelineName;
    }
    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }
    public int getPipelineType() {
        return pipelineType;
    }
    public void setPipelineType(int pipelineType) {
        this.pipelineType = pipelineType;
    }
    public String getProjectName() {
        return projectName;
    }
    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
    public List<BioPipelineInputFile> getPipelineInputs() {
        return pipelineInputs;
    }
    public void setPipelineInputs(List<BioPipelineInputFile> pipelineInputs) {
        this.pipelineInputs = pipelineInputs;
    }
    public List<BioPipelineStage> getPipelineStages() {
        return pipelineStages;
    }
    public void setPipelineStages(List<BioPipelineStage> pipelineStages) {
        this.pipelineStages = pipelineStages;
    }
    public PipelineState() {
    }
    public PipelineState(long pipelineId, String pipelineName, int pipelineType, String projectName,
            List<BioPipelineInputFile> pipelineInputs, List<BioPipelineStage> pipelineStages) {
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
        this.pipelineType = pipelineType;
        this.projectName = projectName;
        this.pipelineInputs = pipelineInputs;
        this.pipelineStages = pipelineStages;
    }


    
}
