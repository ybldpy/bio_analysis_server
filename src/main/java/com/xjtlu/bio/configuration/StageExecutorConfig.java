package com.xjtlu.bio.configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.xjtlu.bio.analysisPipeline.taskrunner.AbstractPipelineStageExector;
import com.xjtlu.bio.analysisPipeline.taskrunner.PipelineStageExecutor;
import com.xjtlu.bio.service.PipelineService;

@Configuration
public class StageExecutorConfig {

    @Bean(name = "stageExecutorMap")
    public Map<Integer, PipelineStageExecutor> makeStageExecutorMap(List<PipelineStageExecutor> executors){
        
        Map<Integer, PipelineStageExecutor> map = new HashMap<>();
        for(PipelineStageExecutor pipelineStageExecutor:executors){
            map.put(pipelineStageExecutor.id(), pipelineStageExecutor);
        }
        return map;
        
    }


}
