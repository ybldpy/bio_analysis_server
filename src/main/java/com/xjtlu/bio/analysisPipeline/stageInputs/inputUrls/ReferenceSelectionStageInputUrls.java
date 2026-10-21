package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ReferenceSelectionStageInputUrls implements StageInputUrls {
    private String contigsUrl;
    // Optional QC reads. Assembly-level entry stages only supply contigsUrl.
    private String r1Url;
    private String r2Url;

    public ReferenceSelectionStageInputUrls() {
    }

    public ReferenceSelectionStageInputUrls(String contigsUrl) {
        this.contigsUrl = contigsUrl;
    }

    public String getContigsUrl() {
        return contigsUrl;
    }

    public void setContigsUrl(String contigsUrl) {
        this.contigsUrl = contigsUrl;
    }

    public String getR1Url() {
        return r1Url;
    }

    public void setR1Url(String r1Url) {
        this.r1Url = r1Url;
    }

    public String getR2Url() {
        return r2Url;
    }

    public void setR2Url(String r2Url) {
        this.r2Url = r2Url;
    }
}
