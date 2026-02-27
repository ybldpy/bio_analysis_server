package com.xjtlu.bio.analysisPipeline.taskrunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AMRInputUrls;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AmrStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.utils.JsonUtil;
import org.springframework.stereotype.Component;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_AMR;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * bacteria part
 */

@Component
public class AmrStageExecutor extends AbstractPipelineStageExector<AmrStageOutput> implements PipelineStageExecutor<AmrStageOutput>{





    @Override
    protected StageRunResult<AmrStageOutput> _execute(StageExecutionInput stageExecutionInput) throws JsonMappingException, JsonProcessingException {
        BioPipelineStage stage = stageExecutionInput.bioPipelineStage;
        AMRInputUrls amrInputUrls = JsonUtil.toObject(stage.getInputUrl(), AMRInputUrls.class);


        String inputUrl = amrInputUrls.getContigsUrl();

        Path inputSamplePath = stageExecutionInput.inputDir.resolve(inputUrl.substring(inputUrl.lastIndexOf("/")+1));
        StorageService.GetObjectResult getObjectResult = this.storageService.getObject(inputUrl, inputSamplePath.toString());
        if (!getObjectResult.success()){
            Exception e=getObjectResult.e();
            logger.error("{} load input failed", stage, e);
            return runFail(stage, "load input failed");
        }

        Path resultPath = stageExecutionInput.workDir.resolve("amrResult.tsv");

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getAmrfinder());
        runCmd.add("-n");
        runCmd.add(inputSamplePath.toString());
        runCmd.add("-o");
        runCmd.add(resultPath.toString());

        ExecuteResult executeResult = this._execute(runCmd, stageExecutionInput.workDir);
        if (!executeResult.success()){
            logger.error("{} amr run failed. run code = {}. ", stage, executeResult.runCode, executeResult.ex);
            return this.runFail(stage, "amr run failed");
        }
        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(resultPath);
        if(!stageOutputValidationResults.isEmpty()){
            logger.error("{} amr run failed. {} Amr result not generated", stage, stageOutputValidationResults.get(0).path, stageOutputValidationResults.get(0).ioException);
            return this.runFail(stage, "Amr result not generated");
        }
        return StageRunResult.OK(new AmrStageOutput(resultPath), stage);
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_AMR;
    }
}
