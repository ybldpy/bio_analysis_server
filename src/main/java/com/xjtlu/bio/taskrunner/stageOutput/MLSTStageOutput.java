package com.xjtlu.bio.taskrunner.stageOutput;

import java.nio.file.Path;

public class MLSTStageOutput implements StageOutput{


    private final Path mlstPath;

    public MLSTStageOutput(Path mlstPath) {
        this.mlstPath = mlstPath;
    }

    public Path getMlstPath() {
        return mlstPath;
    }
}
