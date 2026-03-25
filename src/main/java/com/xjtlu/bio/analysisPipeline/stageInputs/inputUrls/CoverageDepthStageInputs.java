package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class CoverageDepthStageInputs implements StageInputUrls{

    private String bam;
  
    private String bamIndex;
    public String getBam() {
        return bam;
    }
    public CoverageDepthStageInputs(String bam, String bamIndex) {
        this.bam = bam;
        this.bamIndex = bamIndex;
    }
    public CoverageDepthStageInputs() {
    }
    public void setBam(String bam) {
        this.bam = bam;
    }
    
    public String getBamIndex() {
        return bamIndex;
    }
    public void setBamIndex(String bamIndex) {
        this.bamIndex = bamIndex;
    }

}
