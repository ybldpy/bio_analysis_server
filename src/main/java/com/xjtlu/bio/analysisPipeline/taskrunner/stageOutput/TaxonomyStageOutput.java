package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class TaxonomyStageOutput implements StageOutput{


    private final Path report;
    private final Path output;

    public TaxonomyStageOutput(Path output, Path report){
        this.report = report;
        this.output = output;
    }

    public Path getReport() {
        return report;
    }
    public Path getOutput() {
        return output;
    }
    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return output.getParent();
    }


}
