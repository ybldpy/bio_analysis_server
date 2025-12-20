package com.xjtlu.bio.taskrunner.stageOutput;

public class AssemblyStageOutput implements StageOutput{

    private String contigPath;
    private String scaffoldPath;
    public String getContigPath() {
        return contigPath;
    }
    public AssemblyStageOutput(String contigPath, String scaffoldPath) {
        this.contigPath = contigPath;
        this.scaffoldPath = scaffoldPath;
    }
    public String getScaffoldPath() {
        return scaffoldPath;
    }

    

}
