package com.xjtlu.bio.analysisPipeline.stageResult;

public class VFResult implements StageResult{


    private String amrTsv;

    public String getAmrTsv() {
        return amrTsv;
    }

    public void setAmrTsv(String amrTsv) {
        this.amrTsv = amrTsv;
    }

    public VFResult() {
    }

    public VFResult(String amrTsv) {
        this.amrTsv = amrTsv;
    }

    

}
