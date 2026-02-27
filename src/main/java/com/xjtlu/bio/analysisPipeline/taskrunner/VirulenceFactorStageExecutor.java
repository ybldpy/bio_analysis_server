package com.xjtlu.bio.analysisPipeline.taskrunner;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VirulenceFactorStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

import org.springframework.stereotype.Component;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VIRULENCE;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class VirulenceFactorStageExecutor extends AbstractPipelineStageExector<VirulenceFactorStageOutput> implements PipelineStageExecutor<VirulenceFactorStageOutput>{


    @Override
    protected StageRunResult<VirulenceFactorStageOutput> _execute(StageExecutionInput stageExecutionInput) {
        BioPipelineStage stage = stageExecutionInput.bioPipelineStage;

        // Map<String,String> inputUrlMap = this.loadInputUrlMap(stage);
        // if (inputUrlMap == null){
        //     return runFail(stage, "load input failed");
        // }
        VFStageInputUrls vfStageInputUrls = JsonUtil.toObject(stage.getInputUrl(), VFStageInputUrls.class);



        String inputContigsUrl = vfStageInputUrls.getContigsUrl();
        Path inputContigPath = stageExecutionInput.inputDir.resolve("in.contig");

        boolean success = this.loadInput(Map.of(inputContigsUrl, inputContigPath));
        if(!success){
            return this.runFail(stage, "load input fail");
        }

        Path resultPath = stageExecutionInput.workDir.resolve("vf.tsv");
        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getVirulenceFactor());
        runCmd.add("--db");
        runCmd.add("");
        runCmd.add("--minid");
        runCmd.add("90");
        runCmd.add("--mincov");
        runCmd.add("60");
        runCmd.add(inputContigPath.toString());

        ExecuteResult executeResult = this._execute(runCmd, stageExecutionInput.workDir, resultPath, null);
        if(!executeResult.success()){
            logger.error("{} run failed. code = {}", stage, executeResult.runCode, executeResult.ex);
            return this.runFail(stage, "run failed");
        }

        List<StageOutputValidationResult> validationResults = validateOutputFiles(resultPath);
        if(!validationResults.isEmpty()){
            logger.error("{} no output generated", stage);
            return this.runFail(stage, "no output generated");
        }

        return StageRunResult.OK(new VirulenceFactorStageOutput(resultPath), stage);
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_VIRULENCE;
    }
}
