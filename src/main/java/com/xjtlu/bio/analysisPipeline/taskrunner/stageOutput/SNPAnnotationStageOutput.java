package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class SNPAnnotationStageOutput implements StageOutput{


    private Path annotatedFilePath;

    public SNPAnnotationStageOutput(Path outputFilePath){
        this.annotatedFilePath = outputFilePath;
    }


    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return annotatedFilePath.getParent();
    }


    public Path getAnnotatedFilePath() {
        return annotatedFilePath;
    }


    public void setAnnotatedFilePath(Path annotatedFilePath) {
        this.annotatedFilePath = annotatedFilePath;
    }

}
