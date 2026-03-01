package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;

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
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.utils.JsonUtil;

@Component
public class AssemblyExecutor extends AbstractPipelineStageExector<AssemblyStageOutput> {


    @Value("analysis-pipeline.stage.contigsFileName")
    private String contigsFileName;

    @Value("analysis-pipeline.stage.scaffoldFileName")
    private String scaffoldsFileName;

    

    @Override
    public StageRunResult<AssemblyStageOutput> _execute(StageExecutionInput stageExecutionInput) throws JsonMappingException, JsonProcessingException {
        // TODO Auto-generated method stub

        BioPipelineStage bioPipelineStage = stageExecutionInput.bioPipelineStage;
        String inputUrl = bioPipelineStage.getInputUrl();
        AssemblyInputUrls assemblyInputUrls = JsonUtil.toObject(inputUrl, AssemblyInputUrls.class);

        String r1 = assemblyInputUrls.getRead1Url();
        String r2 = assemblyInputUrls.getRead2Url();

        Path tempInputDir = stageExecutionInput.inputDir;
        Path workDir = stageExecutionInput.workDir;

        // Path r1Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r1.substring(r1.lastIndexOf("/") + 1), ""));
        Path r1Path = tempInputDir.resolve(r1.substring(r1.lastIndexOf("/")+1));

        Path r2Path = null;
        if (r2 != null) {
            // r2Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r2.substring(r2.lastIndexOf("/") + 1), ""));
            r2Path = tempInputDir.resolve(r2.substring(r2.lastIndexOf("/")+1));
        }

        boolean res = this.loadInput(r2!=null?Map.of(r1, r1Path, r2, r2Path):Map.of(r1,r1Path));

        if(!res){
            return this.runFail(bioPipelineStage, "load fail");
        }

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getSpades());
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

        ExecuteResult executeResult = _execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "运行spades tool失败", executeResult.ex, tempInputDir, workDir);
        }


        

        Path contigs = workDir.resolve(contigsFileName);
        Path scaffolds = workDir.resolve(scaffoldsFileName);
        AssemblyStageOutput assemblyStageOutput = new AssemblyStageOutput(contigs.toString(), scaffolds.toString());

        List<StageOutputValidationResult> errOutputValidationResults = validateOutputFiles(contigs);
        
        if(!errOutputValidationResults.isEmpty()){
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
        return PIPELINE_STAGE_ASSEMBLY;
    }

}
