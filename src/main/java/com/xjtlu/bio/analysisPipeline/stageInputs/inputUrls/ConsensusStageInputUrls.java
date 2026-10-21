package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ConsensusStageInputUrls implements StageInputUrls {

    private String vcfGz;
    private String vcfTbi;
    private String coverageDepthUrl;

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

    public String getCoverageDepthUrl() {
        return coverageDepthUrl;
    }

    public void setCoverageDepthUrl(String coverageDepthUrl) {
        this.coverageDepthUrl = coverageDepthUrl;
    }


    public ConsensusStageInputUrls() {
    }
    

}
