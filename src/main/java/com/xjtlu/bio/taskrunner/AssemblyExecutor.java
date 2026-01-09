package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.taskrunner.stageOutput.AssemblyStageOutput;

@Component
public class AssemblyExecutor extends AbstractPipelineStageExector {


    @Value("${analysisPipeline.tools.spades}")
    protected List<String> spadesTool;

    

    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrl = bioPipelineStage.getInputUrl();
        Map<String, String> inputUrlMap = null;
        try {
            inputUrlMap = this.objectMapper.readValue(inputUrl, Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return parseError(bioPipelineStage);
        }

        String r1 = inputUrlMap.get(PipelineService.PIPELINE_STAGE_QC_INPUT_R1);
        String r2 = inputUrlMap.get(PipelineService.PIPELINE_STAGE_QC_INPUT_R2);

        Path tempInputDir = stageInputPath(bioPipelineStage);
        Path workDir = workDirPath(bioPipelineStage);

        
        try{
            Files.createDirectories(tempInputDir);
            Files.createDirectories(workDir);
        }catch(IOException e){
            this.deleteTmpFiles(List.of(tempInputDir.toFile(), workDir.toFile()));
            return this.runFail(bioPipelineStage, "创建临时目录失败",e);
        }


        // Path r1Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r1.substring(r1.lastIndexOf("/") + 1), ""));
        Path r1Path = tempInputDir.resolve(r1.substring(r1.lastIndexOf("/")+1));

        Path r2Path = null;
        if (r2 != null) {
            // r2Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r2.substring(r2.lastIndexOf("/") + 1), ""));
            r2Path = tempInputDir.resolve(r2.substring(r2.lastIndexOf("/")+1));
        }

        Map<String,GetObjectResult> getR1AndR2Results = this.loadInput(r2!=null?Map.of(r1, r1Path, r2, r2Path):Map.of(r1,r1Path));

        String failedLoadRead = findFailedLoadingObject(getR1AndR2Results);
        if(failedLoadRead!=null){
            this.deleteTmpFiles(List.of(tempInputDir.toFile()));
            return this.runFail(bioPipelineStage, "加载"+failedLoadRead+"失败", getR1AndR2Results.get(failedLoadRead).e());
        }

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.spadesTool);
        cmd.add("-t");
        cmd.add(String.valueOf(3));
        if (r2Path != null) { // 双端
            cmd.add("-1");
            cmd.add(r1Path.toString());
            cmd.add("-2");
            cmd.add(r2Path.toString());
        } else { // 单端
            cmd.add("-s");
            cmd.add(r1Path.toString());
        }
        cmd.add("-o");
        cmd.add(workDir.toString());

        ExecuteResult executeResult = execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "运行spades tool失败", executeResult.ex, tempInputDir, workDir);
        }


        AssemblyStageOutput assemblyStageOutput = bioStageUtil.assemblyOutput(bioPipelineStage, workDir);

        Path contigs = Path.of(assemblyStageOutput.getContigPath());
        Path scaffolds = Path.of(assemblyStageOutput.getScaffoldPath());
        List<StageOutputValidationResult> errOutputValidationResults = validateOutputFiles(contigs);
        
        if(!errOutputValidationResults.isEmpty()){
            this.deleteTmpFiles(List.of(tempInputDir.toFile(), workDir.toFile()));
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errOutputValidationResults));
        }
        



        boolean hasScaffold = true;
        try{
            if(!this.requireNonEmpty(scaffolds)){
                hasScaffold = false;
            }
        }catch(IOException e){
            //if error happen here, just ingore. The callback will know it and handle
            hasScaffold = false;
        }

        if(!hasScaffold){
            assemblyStageOutput.setScaffoldPath(null);
        }
        return StageRunResult.OK(assemblyStageOutput, bioPipelineStage);
        
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_ASSEMBLY;
    }

}
