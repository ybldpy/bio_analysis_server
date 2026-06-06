package com.xjtlu.bio.analysisPipeline.stageResult;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;

public class ReadInspectStageResult implements StageResult{

    private SequenceMeta readMeta;

    private String r1Url;
    private String r2Url;

    public SequenceMeta getReadMeta() {
        return readMeta;
    }

    public void setReadMeta(SequenceMeta readMeta) {
        this.readMeta = readMeta;
    }

    public String getR1Url() {
        return r1Url;
    }

    public void setR1Url(String r1Url) {
        this.r1Url = r1Url;
    }

    public String getR2Url() {
        return r2Url;
    }

    public void setR2Url(String r2Url) {
        this.r2Url = r2Url;
    }

    public ReadInspectStageResult() {
    }

    

}
