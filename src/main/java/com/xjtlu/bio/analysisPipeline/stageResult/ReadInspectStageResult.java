package com.xjtlu.bio.analysisPipeline.stageResult;

public class ReadInspectStageResult {

    private int qualityEncoding;
    private int readLenType;

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
