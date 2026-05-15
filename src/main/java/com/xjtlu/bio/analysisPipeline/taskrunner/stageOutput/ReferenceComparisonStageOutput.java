package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class ReferenceComparisonStageOutput implements StageOutput {

    private Path alignmentPafPath;
    private Path differenceTsvPath;

    public ReferenceComparisonStageOutput(Path alignmentPafPath, Path differenceTsvPath) {
        this.alignmentPafPath = alignmentPafPath;
        this.differenceTsvPath = differenceTsvPath;
    }

    public Path getAlignmentPafPath() {
        return alignmentPafPath;
    }

    public ReferenceComparisonStageOutput() {
    }

    public void setAlignmentPafPath(Path alignmentPafPath) {
        this.alignmentPafPath = alignmentPafPath;
    }

    public Path getDifferenceTsvPath() {
        return differenceTsvPath;
    }

    public void setDifferenceTsvPath(Path differenceTsvPath) {
        this.differenceTsvPath = differenceTsvPath;
    }

    @Override
    public Path getParentPath() {
        return differenceTsvPath.getParent();
    }

}
