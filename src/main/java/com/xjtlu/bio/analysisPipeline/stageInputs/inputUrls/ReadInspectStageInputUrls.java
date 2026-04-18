package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ReadInspectStageInputUrls implements StageInputUrls{

    private String readUrl;

    public ReadInspectStageInputUrls(String readUrl) {
        this.readUrl = readUrl;
    }

    public ReadInspectStageInputUrls() {
    }

    public String getReadUrl() {
        return readUrl;
    }

    public void setReadUrl(String readUrl) {
        this.readUrl = readUrl;
    }

    


}
