package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;


public class MLSTStageInputUrls implements StageInputUrls{


    private String contigUrl;

    public MLSTStageInputUrls(String contigUrl) {
        this.contigUrl = contigUrl;
    }

    public String getContigUrl() {
        return contigUrl;
    }

    public void setContigUrl(String contigUrl) {
        this.contigUrl = contigUrl;
    }

    




}
