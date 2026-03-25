package com.xjtlu.bio.analysisPipeline.stageResult;

public class CoverageDepthStageResult implements StageResult{

    private String depthTable;
    private String summary;
    public String getDepthTable() {
        return depthTable;
    }
    public CoverageDepthStageResult() {
    }
    public CoverageDepthStageResult(String depthTable, String summary) {
        this.depthTable = depthTable;
        this.summary = summary;
    }
    public void setDepthTable(String depthTable) {
        this.depthTable = depthTable;
    }
    public String getSummary() {
        return summary;
    }
    public void setSummary(String summary) {
        this.summary = summary;
    }

}
