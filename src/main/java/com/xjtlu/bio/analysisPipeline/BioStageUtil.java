package com.xjtlu.bio.analysisPipeline;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.*;
import com.xjtlu.bio.entity.BioPipelineStage;

@Component
public class BioStageUtil {


    @Value("${analysis-pipeline.stage.baseWorkDir}")
    private String stageWorkDirPath;
    @Value("${analysis-pipeline.stage.baseInputDir}")
    private String stageInputDirPath;


    public Path stageExecutorWorkDir(StageContext stageContext){
        return Paths.get(stageWorkDirPath, String.valueOf(stageContext.getRunStageId()));
    }

    public Path stageExecutorInputDir(StageContext bioPipelineStage){
        return Paths.get(stageInputDirPath, String.valueOf(bioPipelineStage.getRunStageId()));
    }

    public String createStoreObjectName(StageContext stageContext, String name){
        return String.format(
            "stageOutput/%d/%s",
            stageContext.getRunStageId(),
            name
        );
    }


}
