package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class CoverageDepthStageOutput implements StageOutput{


    private Path detphTable;
    private Path summary;
    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return detphTable.getParent();
    }
    public CoverageDepthStageOutput(Path detphTable, Path summary) {
        this.detphTable = detphTable;
        this.summary = summary;
    }
    public CoverageDepthStageOutput() {
    }
    public Path getDetphTable() {
        return detphTable;
    }
    public void setDetphTable(Path detphTable) {
        this.detphTable = detphTable;
    }
    public Path getSummary() {
        return summary;
    }
    public void setSummary(Path summary) {
        this.summary = summary;
    }

    



}
