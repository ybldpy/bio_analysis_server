package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class SNPAnnotationInputs implements StageInputUrls{

    private String vcfUrl;

    public SNPAnnotationInputs(String vcfUrl) {
        this.vcfUrl = vcfUrl;
    }

    public String getVcfUrl() {
        return vcfUrl;
    }

    public SNPAnnotationInputs() {
    }

    public void setVcfUrl(String vcfUrl) {
        this.vcfUrl = vcfUrl;
    }

    

}
