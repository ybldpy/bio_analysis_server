package com.xjtlu.bio.analysisPipeline.stageDoneHandler;


import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
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
    protected void doHandleStageDone(StageRunResult stageRunResult) {

        StageContext stageContext = stageRunResult.getStageContext();
        handleFail(stageContext, stageRunResult.getWorkDir() == null
                ? null : stageRunResult.getWorkDir().toString(), stageRunResult.getFailReason());
        return;


    }



}
