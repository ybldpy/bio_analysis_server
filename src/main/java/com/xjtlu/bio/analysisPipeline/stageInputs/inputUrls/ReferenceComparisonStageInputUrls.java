package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ReferenceComparisonStageInputUrls implements StageInputUrls{


    private String fastaUrl;

    public ReferenceComparisonStageInputUrls() {
    }

    public ReferenceComparisonStageInputUrls(String fastaUrl) {
        this.fastaUrl = fastaUrl;
    }

    public String getFastaUrl() {
        return fastaUrl;
    }

    public void setFastaUrl(String fastaUrl) {
        this.fastaUrl = fastaUrl;
    }
    


}
