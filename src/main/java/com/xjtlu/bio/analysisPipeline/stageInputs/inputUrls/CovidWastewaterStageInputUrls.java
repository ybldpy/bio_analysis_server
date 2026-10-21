package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

/** File object keys; the library type is a declared input contract, not inferred from FASTQ. */
public class CovidWastewaterStageInputUrls implements StageInputUrls {
    private String r1Url;
    private String r2Url;
    private Integer sequencingPlatform;

    public String getR1Url() { return r1Url; }
    public void setR1Url(String r1Url) { this.r1Url = r1Url; }
    public String getR2Url() { return r2Url; }
    public void setR2Url(String r2Url) { this.r2Url = r2Url; }
    public Integer getSequencingPlatform() { return sequencingPlatform; }
    public void setSequencingPlatform(Integer sequencingPlatform) { this.sequencingPlatform = sequencingPlatform; }
}
