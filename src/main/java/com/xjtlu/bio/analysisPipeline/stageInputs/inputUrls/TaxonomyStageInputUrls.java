package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class TaxonomyStageInputUrls implements StageInputUrls{


    //provide reads and contigs format if required. 
    private String r1;
    private String r2;
    private String contigs;


    public TaxonomyStageInputUrls() {
    }
    public String getR1() {
        return r1;
    }
    public void setR1(String r1) {
        this.r1 = r1;
    }
    public String getR2() {
        return r2;
    }
    public void setR2(String r2) {
        this.r2 = r2;
    }
    public String getContigs() {
        return contigs;
    }
    public void setContigs(String contigs) {
        this.contigs = contigs;
    }
    

}
