package com.xjtlu.bio.analysisPipeline.stageResult;

public class VarientCallStageResult implements StageResult{

    private String vcfGzUrl;
    private String vcfTbiUrl;


    public VarientCallStageResult(){

    }

    


    public VarientCallStageResult(String vcfGzUrl, String vcfTbiUrl) {
        this.vcfGzUrl = vcfGzUrl;
        this.vcfTbiUrl = vcfTbiUrl;
    }




    public String getVcfGzUrl() {
        return vcfGzUrl;
    }
    public void setVcfGzUrl(String vcfGzUrl) {
        this.vcfGzUrl = vcfGzUrl;
    }
    public String getVcfTbiUrl() {
        return vcfTbiUrl;
    }
    public void setVcfTbiUrl(String vcfTbiUrl) {
        this.vcfTbiUrl = vcfTbiUrl;
    }



    

}
