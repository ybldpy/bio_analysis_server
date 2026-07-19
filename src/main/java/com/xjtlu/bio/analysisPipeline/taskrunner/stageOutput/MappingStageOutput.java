package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class MappingStageOutput implements StageOutput{


    public static final String BAM = "aln.sorted.bam";
    public static final String BAM_INDEX = "aln.sorted.bam.bai";

    public static final String COVERAGE_DEPTH = "depth.tsv";

    private Path bamPath;
    private Path bamIndexPath;
    private Path coverageDepthPath;


    public Path getCoverageDepthPath() {
        return coverageDepthPath;
    }
    public void setCoverageDepthPath(Path coverageDepthPath) {
        this.coverageDepthPath = coverageDepthPath;
    }
    public Path getBamPath() {
        return bamPath;
    }
    public void setBamPath(Path bamPath) {
        this.bamPath = bamPath;
    }
    public MappingStageOutput(Path bamPath, Path bamIndexPath, Path coverageDepthPath) {
        this.bamPath = bamPath;
        this.bamIndexPath = bamIndexPath;
        this.coverageDepthPath = coverageDepthPath;
    }
    public Path getBamIndexPath() {
        return bamIndexPath;
    }
    public void setBamIndexPath(Path bamIndexPath) {
        this.bamIndexPath = bamIndexPath;
    }

    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return bamIndexPath.toAbsolutePath().getParent();
    }
    

}
