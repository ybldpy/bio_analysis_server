package com.xjtlu.bio.stageDoneHandler;

import com.xjtlu.bio.taskrunner.StageRunResult;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;

public interface StageDoneHandler<T extends StageOutput> {

    int getType();
    boolean handleStageDone(StageRunResult<T> stageRunResult);
    

}
