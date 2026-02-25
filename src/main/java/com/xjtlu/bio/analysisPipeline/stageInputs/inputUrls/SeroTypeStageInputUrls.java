package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class SeroTypeStageInputUrls implements StageInputUrls{

    private String contigsUrl;

    private String r1Url;
    private String r2Url;

    public SeroTypeStageInputUrls() {
    }

    public SeroTypeStageInputUrls(String contigsUrl) {
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

    public SeroTypeStageInputUrls(String r1,String r2){
        this.r1Url = r1;
        this.r2Url = r2;
    }

    

    public String getContigsUrl() {
        return contigsUrl;
    }

    public void setContigsUrl(String contigsUrl) {
        this.contigsUrl = contigsUrl;
    }
    

}
