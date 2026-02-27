package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class VarientCallInputUrls implements StageInputUrls{

    private String bamUrl;
    private String bamIndexUrl;
    public String getBamUrl() {
        return bamUrl;
    }
    public void setBamUrl(String bamUrl) {
        this.bamUrl = bamUrl;
    }
    public VarientCallInputUrls() {
    }
    public String getBamIndexUrl() {
        return bamIndexUrl;
    }
    public void setBamIndexUrl(String bamIndexUrl) {
        this.bamIndexUrl = bamIndexUrl;
    }
    

}
