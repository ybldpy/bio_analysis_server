package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class AssemblyInputUrls implements StageInputUrls{

    private String read1Url;
    private String read2Url;
    public String getRead1Url() {
        return read1Url;
    }
    public void setRead1Url(String read1Url) {
        this.read1Url = read1Url;
    }
    public String getRead2Url() {
        return read2Url;
    }
    public AssemblyInputUrls() {
    }
    public void setRead2Url(String read2Url) {
        this.read2Url = read2Url;
    }

    

}
