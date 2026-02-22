package com.xjtlu.bio.analysisPipeline.taskrunner;

import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

public interface PipelineStageExecutor<T extends StageOutput> {

    public int id();
    public StageRunResult execute(BioPipelineStage bioPipelineStage);
}
