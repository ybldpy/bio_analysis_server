package com.xjtlu.bio.taskrunner;

import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.MLSTStageOutput;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class MLSTStageExecutor extends AbstractPipelineStageExector<MLSTStageOutput> implements PipelineStageExecutor<MLSTStageOutput>{
    @Override
    protected StageRunResult<MLSTStageOutput> _execute(StageExecutionInput stageExecutionInput) {
        BioPipelineStage stage = stageExecutionInput.bioPipelineStage;

        Map<String,String> inputUrlMap = this.loadInputUrlMap(stage);
        if(inputUrlMap == null || inputUrlMap.isEmpty()){
            return this.runFail(stage, "No input");
        }

        String contigUrl = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MLST_INPUT);
        Path contigPath = stageExecutionInput.inputDir.resolve("in.contig");
        Path resultPath = stageExecutionInput.workDir.resolve("mlstResult.tsv");
        boolean loadSuccess = this.loadInput(Map.of(contigUrl,contigPath));
        if(!loadSuccess){
            return this.runFail(stage, "load input failed");
        }

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getMlst());
        runCmd.add(contigPath.toString());

        ExecuteResult executeResult = this._execute(runCmd, stageExecutionInput.workDir, resultPath, null);

        if(!executeResult.success()){
            logger.error("{} run failed. code = {}", stage, executeResult.runCode, executeResult.ex);
            return this.runFail(stage, "run failed");
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(resultPath);
        if(!stageOutputValidationResults.isEmpty()){
            logger.error("{} no output", stage, stageOutputValidationResults.get(0).ioException);
            return this.runFail(stage, "no output");
        }
        return StageRunResult.OK(new MLSTStageOutput(resultPath), stage);
    }

    @Override
    public int id() {
        return PipelineService.PIPELINE_STAGE_MLST;
    }
}
