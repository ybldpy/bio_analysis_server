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
        return resultPath.getParent();
    }



    public Path getResultPath() {
        return resultPath;
    }



    public void setResultPath(Path resultPath) {
        this.resultPath = resultPath;
    }



}
