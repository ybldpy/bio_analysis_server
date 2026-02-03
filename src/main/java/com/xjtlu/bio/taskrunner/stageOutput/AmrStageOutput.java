package com.xjtlu.bio.taskrunner.stageOutput;

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
}
