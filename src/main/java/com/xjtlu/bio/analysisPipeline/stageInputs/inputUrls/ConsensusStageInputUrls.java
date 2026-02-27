package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ConsensusStageInputUrls implements StageInputUrls{

    private String vcfGz;
    private String vcfTbi;

    

    public String getVcfGz() {
        return vcfGz;
    }



    public void setVcfGz(String vcfGz) {
        this.vcfGz = vcfGz;
    }



    public String getVcfTbi() {
        return vcfTbi;
    }



    public void setVcfTbi(String vcfTbi) {
        this.vcfTbi = vcfTbi;
    }



    public ConsensusStageInputUrls() {
    }
    

}
