package com.xjtlu.bio.analysisPipeline.stageResult;

public class ReadLenStageResult implements StageResult{

    private boolean isLongRead;

    public boolean isLongRead() {
        return isLongRead;
    }

    public ReadLenStageResult() {
    }

    public void setLongRead(boolean isLongRead) {
        this.isLongRead = isLongRead;
    }

    public ReadLenStageResult(boolean isLongRead) {
        this.isLongRead = isLongRead;
    }
    

}
