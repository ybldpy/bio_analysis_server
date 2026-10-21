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
    private String kaptiveKResultUrl;
    private String kaptiveOResultUrl;

    public String getKaptiveKResultUrl() {
        return kaptiveKResultUrl;
    }

    public void setKaptiveKResultUrl(String kaptiveKResultUrl) {
        this.kaptiveKResultUrl = kaptiveKResultUrl;
    }

    public String getKaptiveOResultUrl() {
        return kaptiveOResultUrl;
    }

    public void setKaptiveOResultUrl(String kaptiveOResultUrl) {
        this.kaptiveOResultUrl = kaptiveOResultUrl;
    }
    

}
