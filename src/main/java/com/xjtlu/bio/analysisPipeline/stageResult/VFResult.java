package com.xjtlu.bio.analysisPipeline.stageResult;

import com.fasterxml.jackson.annotation.JsonAlias;

public class VFResult implements StageResult {

    // Read historical result snapshots without emitting the old AMR property.
    @JsonAlias("amrTsv")
    private String vfTsv;

    public String getVfTsv() {
        return vfTsv;
    }

    public void setVfTsv(String vfTsv) {
        this.vfTsv = vfTsv;
    }

    public VFResult() {
    }

    public VFResult(String vfTsv) {
        this.vfTsv = vfTsv;
    }
}
