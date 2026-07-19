package com.xjtlu.bio.analysisPipeline.stageResult;

public class MappingResult implements StageResult{

    private String bamUrl;
    private String bamIndexUrl;
    private String coverageDepthUrl;




    public String getBamUrl() {
        return bamUrl;
    }
    public MappingResult(String bamUrl, String bamIndexUrl, String coverageDepthUrl) {
        this.bamUrl = bamUrl;
        this.bamIndexUrl = bamIndexUrl;
        this.coverageDepthUrl = coverageDepthUrl;
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
    public String getCoverageDepthUrl() {
        return coverageDepthUrl;
    }
    public void setCoverageDepthUrl(String coverageDepthUrl) {
        this.coverageDepthUrl = coverageDepthUrl;
    }

}
