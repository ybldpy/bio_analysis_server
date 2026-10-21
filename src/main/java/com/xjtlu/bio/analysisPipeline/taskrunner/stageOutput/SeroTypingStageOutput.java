package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class SeroTypingStageOutput implements StageOutput{


    private Path resultPath;
    private Path kaptiveKResultPath;
    private Path kaptiveOResultPath;

    

    public SeroTypingStageOutput(Path resultPath) {
        this(resultPath, null, null);
    }

    public SeroTypingStageOutput(Path resultPath, Path kaptiveKResultPath, Path kaptiveOResultPath) {
        this.resultPath = resultPath;
        this.kaptiveKResultPath = kaptiveKResultPath;
        this.kaptiveOResultPath = kaptiveOResultPath;
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

    public Path getKaptiveKResultPath() {
        return kaptiveKResultPath;
    }

    public void setKaptiveKResultPath(Path kaptiveKResultPath) {
        this.kaptiveKResultPath = kaptiveKResultPath;
    }

    public Path getKaptiveOResultPath() {
        return kaptiveOResultPath;
    }

    public void setKaptiveOResultPath(Path kaptiveOResultPath) {
        this.kaptiveOResultPath = kaptiveOResultPath;
    }



}
