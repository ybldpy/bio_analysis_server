package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ReferenceSelectionStageInputUrls implements StageInputUrls {
    private String contigsUrl;

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
}
