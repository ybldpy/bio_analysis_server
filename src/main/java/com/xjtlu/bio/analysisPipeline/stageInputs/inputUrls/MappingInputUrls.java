package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class MappingInputUrls implements StageInputUrls{

    private String r1Url;
    private String r2Url;
    public String getR1Url() {
        return r1Url;
    }
    public MappingInputUrls() {
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
