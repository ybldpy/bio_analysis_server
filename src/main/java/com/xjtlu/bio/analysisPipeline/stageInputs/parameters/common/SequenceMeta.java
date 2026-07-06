package com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common;

public class SequenceMeta {

    

    public SequenceMeta() {
    }

    public int getSequencingPlatform() {
        return sequencingPlatform;
    }

    public void setSequencingPlatform(int sequencingPlatform) {
        this.sequencingPlatform = sequencingPlatform;
    }

    private int qualityEncoding;
    private int readLenType;
    private int sequencingPlatform;
    private int sequenceLevel;

    

    
    
    public int getQualityEncoding() {
        return qualityEncoding;
    }

    public int getSequenceLevel() {
        return sequenceLevel;
    }

    public void setSequenceLevel(int sequenceLevel) {
        this.sequenceLevel = sequenceLevel;
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
