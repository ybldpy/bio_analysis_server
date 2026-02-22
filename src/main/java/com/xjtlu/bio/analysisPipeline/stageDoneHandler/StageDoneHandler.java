package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;

public interface StageDoneHandler<T extends StageOutput> {

    int getType();
    boolean handleStageDone(StageRunResult<T> stageRunResult);
    

}
