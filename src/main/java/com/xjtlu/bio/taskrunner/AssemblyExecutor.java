package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;



@Component
public class AssemblyExecutor extends AbstractPipelineStageExector{

    


    

    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrl = bioPipelineStage.getInputUrl();
        Map<String,String> inputUrlMap = null;
        try {
            inputUrlMap = this.objectMapper.readValue(inputUrl, Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return parseError(bioPipelineStage);
        }

        String r1 = inputUrlMap.get(PipelineService.PIPELINE_STAGE_QC_INPUT_R1);
        String r2 = inputUrlMap.get(PipelineService.PIPELINE_STAGE_QC_INPUT_R2);



        Path tempInputDir = Paths.get(String.format("%s/%d", this.stageInputTmpBasePath, bioPipelineStage.getStageId()));
        Path r1Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r1.substring(r1.lastIndexOf("/")+1), ""));
        Path r2Path = null;
        if(r2!=null){
            r2Path =tempInputDir.resolve(appendSuffixBeforeExtensions(r2.substring(r2.lastIndexOf("/")+1), ""));
        }

        File[] readFiles = moveSampleReadFilesToTmpPath(inputUrl, r1Path, inputUrl, r2Path);
        if(readFiles[0] == null || (r2Path!=null && readFiles[1]==null)){
            if(readFiles[0]!=null){readFiles[0].delete();}
            if(readFiles[1]!=null){readFiles[1].delete();}
            return this.runFail(bioPipelineStage, "读取样本文件出错");
        }

        Path workDir = Paths.get();


        
    }




    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_ASSEMBLY;
    }



}
