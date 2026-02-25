package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class VFStageInputUrls implements StageInputUrls{

    private String contigsUrl;

    public VFStageInputUrls(){
    }

    public VFStageInputUrls(String contigsUrl) {
        this.contigsUrl = contigsUrl;
    }

    public String getContigsUrl() {
        return contigsUrl;
    }

    public void setContigsUrl(String contigsUrl) {
        this.contigsUrl = contigsUrl;
    }

    
    

}
