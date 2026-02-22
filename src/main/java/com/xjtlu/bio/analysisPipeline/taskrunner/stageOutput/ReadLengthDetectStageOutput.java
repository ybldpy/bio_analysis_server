package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

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



    
    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return null;
    }



    

}
