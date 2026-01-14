package com.xjtlu.bio.stageDoneHandler;

import com.xjtlu.bio.common.StageRunResult;

public interface StageDoneHandler {

    int getType();
    void handleStageDone(StageRunResult stageRunResult);

}
