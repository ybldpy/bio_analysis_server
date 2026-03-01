package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class AmrStageOutput implements StageOutput{

    private Path AmrResultPath;

    public AmrStageOutput(Path amrResultPath) {
        AmrResultPath = amrResultPath;
    }

    public Path getAmrResultPath() {
        return AmrResultPath;
    }

    public void setAmrResultPath(Path amrResultPath) {
        AmrResultPath = amrResultPath;
    }


    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return AmrResultPath.getParent();
    }
}
