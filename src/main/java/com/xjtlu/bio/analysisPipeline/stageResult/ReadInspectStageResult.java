package com.xjtlu.bio.analysisPipeline.stageResult;

public class ReadInspectStageResult implements StageResult{

    private int qualityEncoding;
    private int readLenType;

    private String r1Url;
    private String r2Url;

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

    public ReadInspectStageResult() {
    }

    public ReadInspectStageResult(int qualityEncoding, int readLenType) {
        this.qualityEncoding = qualityEncoding;
        this.readLenType = readLenType;
    }
    public int getQualityEncoding() {
        return qualityEncoding;
    }
    public void setQualityEncoding(int qualityEncoding) {
        this.qualityEncoding = qualityEncoding;
    }
    public int getReadLenType() {
        return readLenType;
    }
    public void setReadLenType(int readLenType) {
        this.readLenType = readLenType;
    }

}
