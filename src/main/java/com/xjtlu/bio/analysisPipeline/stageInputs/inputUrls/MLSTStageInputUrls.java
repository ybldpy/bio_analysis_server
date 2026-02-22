package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_MLST_INPUT;

import com.xjtlu.bio.service.PipelineService;

public class MLSTStageInputUrls {


    private String contigUrl;

    public MLSTStageInputUrls(String contigUrl) {
        this.contigUrl = contigUrl;
    }

    public String getContigUrl() {
        return contigUrl;
    }

    public void setContigUrl(String contigUrl) {
        this.contigUrl = contigUrl;
    }

    




}
