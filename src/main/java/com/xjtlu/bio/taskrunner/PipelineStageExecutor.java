package com.xjtlu.bio.taskrunner;

import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;

public interface PipelineStageExecutor {

    public int id();
    public StageRunResult execute(BioPipelineStage bioPipelineStage);
}
