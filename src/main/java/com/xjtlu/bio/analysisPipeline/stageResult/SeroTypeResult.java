package com.xjtlu.bio.analysisPipeline.stageResult;

public class SeroTypeResult implements StageResult{

    public SeroTypeResult() {
    }

    public String getSerotypeResultUrl() {
        return serotypeResultUrl;
    }

    public void setSerotypeResultUrl(String serotypeResultUrl) {
        this.serotypeResultUrl = serotypeResultUrl;
    }

    public SeroTypeResult(String serotypeResultUrl) {
        this.serotypeResultUrl = serotypeResultUrl;
    }

    private String serotypeResultUrl;
    

}
