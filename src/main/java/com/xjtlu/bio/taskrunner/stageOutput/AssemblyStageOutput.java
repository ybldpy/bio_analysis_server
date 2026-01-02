package com.xjtlu.bio.taskrunner.stageOutput;

public class AssemblyStageOutput implements StageOutput{



    public static final String CONTIG = "contigs.fasta";
    public static final String SCAFFOLD = "scaffolds.fasta";

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
    public void setContigPath(String contigPath) {
        this.contigPath = contigPath;
    }
    public void setScaffoldPath(String scaffoldPath) {
        this.scaffoldPath = scaffoldPath;
    }

    

}
