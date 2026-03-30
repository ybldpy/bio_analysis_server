package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import org.springframework.scheduling.annotation.Async;

import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;

public interface StageDoneHandler<T extends StageOutput> {

    

    int getType();
    void handleStageDone(StageRunResult<T> stageRunResult);
    

}
