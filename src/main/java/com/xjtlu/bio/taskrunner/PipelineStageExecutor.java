package com.xjtlu.bio.taskrunner;

import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;

public interface PipelineStageExecutor<T extends StageOutput> {

    public int id();
    public StageRunResult execute(BioPipelineStage bioPipelineStage);
}
