package com.xjtlu.bio.taskrunner;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;

import jakarta.annotation.Resource;

@Component
public class PipelineStageRunner {

    @Resource
    private PipelineService pipelineService;

    
    //是否启动成功
    @Async
    public boolean runStage(BioPipelineStage bioPipelineStage){
        return false;
    }

    


}
