package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class ReadInspectStageOutput implements StageOutput{


    private int qualityEncoding;
    private int readLenType;

    





    public ReadInspectStageOutput(int qualityEncoding, int readLenType) {
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







    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getParentPath'");
    }



}
