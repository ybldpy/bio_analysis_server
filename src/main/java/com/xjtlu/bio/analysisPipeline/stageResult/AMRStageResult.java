package com.xjtlu.bio.analysisPipeline.stageResult;

public class AMRStageResult implements StageResult{

    private String amrResult;

    public AMRStageResult(String amrResult) {
        this.amrResult = amrResult;
    }

    public AMRStageResult() {
    }

    public String getAmrResult() {
        return amrResult;
    }

    public void setAmrResult(String amrResult) {
        this.amrResult = amrResult;
    }

    

}
