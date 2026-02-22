package com.xjtlu.bio.analysisPipeline.stageResult;

public class AssemblyResult {

    private String contigsUrl;
    private String scaffoldUrl;
    public AssemblyResult(String contigsUrl, String scaffoldUrl) {
        this.contigsUrl = contigsUrl;
        this.scaffoldUrl = scaffoldUrl;
    }
    public String getContigsUrl() {
        return contigsUrl;
    }
    public void setContigsUrl(String contigsUrl) {
        this.contigsUrl = contigsUrl;
    }
    public String getScaffoldUrl() {
        return scaffoldUrl;
    }
    public void setScaffoldUrl(String scaffoldUrl) {
        this.scaffoldUrl = scaffoldUrl;
    }
    

}
