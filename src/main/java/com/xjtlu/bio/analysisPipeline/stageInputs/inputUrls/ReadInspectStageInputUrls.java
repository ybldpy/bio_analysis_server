package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ReadInspectStageInputUrls implements StageInputUrls{

    private String read1Url;
    private String read2Url;



    public ReadInspectStageInputUrls(String read1Url,String read2Url) {
        this.read1Url = read1Url;
        this.read2Url = read2Url;
    }

    public ReadInspectStageInputUrls() {
    }

    public String getRead1Url() {
        return read1Url;
    }


    public void setRead1Url(String read1Url) {
        this.read1Url = read1Url;
    }

    public String getRead2Url() {
        return read2Url;
    }

    public void setRead2Url(String read2Url) {
        this.read2Url = read2Url;
    }

    

    


}
