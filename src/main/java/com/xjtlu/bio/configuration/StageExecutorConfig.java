package com.xjtlu.bio.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.AbstractPipelineStageExector;
import com.xjtlu.bio.taskrunner.PipelineStageExecutor;

@Configuration
public class StageExecutorConfig {

    private final PipelineService pipelineService;


    StageExecutorConfig(PipelineService pipelineService) {
        this.pipelineService = pipelineService;
    }


    @Bean(name = "stageExecutorMap")
    public Map<Integer, PipelineStageExecutor> makeStageExecutorMap(List<PipelineStageExecutor> executors){
        
        Map<Integer, PipelineStageExecutor> map = new HashMap<>();


        for(PipelineStageExecutor pipelineStageExecutor:executors){
            map.put(pipelineStageExecutor.id(), pipelineStageExecutor);
        }

        return map;
        
    }


}
