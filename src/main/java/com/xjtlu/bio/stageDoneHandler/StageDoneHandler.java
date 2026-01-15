package com.xjtlu.bio.stageDoneHandler;

import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;

public interface StageDoneHandler<T extends StageOutput> {

    int getType();
    void handleStageDone(StageRunResult<T> stageRunResult);

}
