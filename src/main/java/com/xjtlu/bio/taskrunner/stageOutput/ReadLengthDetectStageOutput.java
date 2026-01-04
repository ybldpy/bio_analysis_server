package com.xjtlu.bio.taskrunner.stageOutput;

public class ReadLengthDetectStageOutput implements StageOutput{

    private boolean isLongRead;

    public boolean isLongRead() {
        return isLongRead;
    }

    public void setLongRead(boolean isLongRead) {
        this.isLongRead = isLongRead;
    }

    public ReadLengthDetectStageOutput(boolean isLongRead) {
        this.isLongRead = isLongRead;
    }

    

}
