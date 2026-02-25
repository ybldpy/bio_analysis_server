package com.xjtlu.bio.analysisPipeline.stageResult;

public class MappingResult implements StageResult{

    private String bamUrl;
    private String bamIndexUrl;
    public String getBamUrl() {
        return bamUrl;
    }
    public MappingResult(String bamUrl, String bamIndexUrl) {
        this.bamUrl = bamUrl;
        this.bamIndexUrl = bamIndexUrl;
    }
    public MappingResult() {
    }
    public void setBamUrl(String bamUrl) {
        this.bamUrl = bamUrl;
    }
    public String getBamIndexUrl() {
        return bamIndexUrl;
    }
    public void setBamIndexUrl(String bamIndexUrl) {
        this.bamIndexUrl = bamIndexUrl;
    }

}
