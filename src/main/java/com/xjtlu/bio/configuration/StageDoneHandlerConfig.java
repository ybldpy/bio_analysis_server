package com.xjtlu.bio.configuration;


import com.xjtlu.bio.stageDoneHandler.StageDoneHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class StageDoneHandlerConfig {


    @Bean(name="stageDoneHandlerMap")
    public Map<Integer, StageDoneHandler> createStageHandlerMap(List<StageDoneHandler> stageDoneHandlerList){

        HashMap<Integer, StageDoneHandler> map = new HashMap<>();
        for(StageDoneHandler stageDoneHandler: stageDoneHandlerList){
            map.put(stageDoneHandler.getType(), stageDoneHandler);
        }
        return map;
    }
}
