package com.xjtlu.bio.analysisPipeline.stageDoneHandler;


import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;



@Component
public class FailStageDoneHandler extends AbstractStageDoneHandler implements StageDoneHandler{

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return -1;
    }

    @Override
    protected Pair buildUploadConfigAndOutputUrlMap(StageRunResult stageRunResult) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'buildUploadConfigAndOutputUrlMap'");
    }

    @Override
    public void handleStageDone(StageRunResult stageRunResult) {

        StageContext stageContext = stageRunResult.getStageContext();
        handleFail(stageContext, stageRunResult.getStageOutput().getParentPath().toString());
        return;


    }



}
