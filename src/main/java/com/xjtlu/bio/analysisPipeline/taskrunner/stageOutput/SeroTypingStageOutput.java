package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class SeroTypingStageOutput implements StageOutput{


    private Path resultPath;

    

    public SeroTypingStageOutput(Path resultPath) {
        this.resultPath = resultPath;
    }



    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getParentPath'");
    }



    public Path getResultPath() {
        return resultPath;
    }



    public void setResultPath(Path resultPath) {
        this.resultPath = resultPath;
    }



}
