package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class MappingStageOutput implements StageOutput{


    public static final String BAM = "aln.sorted.bam";
    public static final String BAM_INDEX = "aln.sorted.bam.bai";

    private String bamPath;
    private String bamIndexPath;
    public String getBamPath() {
        return bamPath;
    }
    public void setBamPath(String bamPath) {
        this.bamPath = bamPath;
    }
    public MappingStageOutput(String bamPath, String bamIndexPath) {
        this.bamPath = bamPath;
        this.bamIndexPath = bamIndexPath;
    }
    public String getBamIndexPath() {
        return bamIndexPath;
    }
    public void setBamIndexPath(String bamIndexPath) {
        this.bamIndexPath = bamIndexPath;
    }

    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return Path.of(bamPath).getParent();
    }
    

}
