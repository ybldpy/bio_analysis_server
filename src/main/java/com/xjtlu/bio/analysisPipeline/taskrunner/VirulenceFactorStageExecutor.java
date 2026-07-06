package com.xjtlu.bio.analysisPipeline.taskrunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VFStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VirulenceFactorStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

import org.springframework.stereotype.Component;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VIRULENCE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class VirulenceFactorStageExecutor
        extends AbstractPipelineStageExector<VirulenceFactorStageOutput, VFStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<VirulenceFactorStageOutput> {

    @Override
    protected Class<VFStageInputUrls> stageInputType() {
        return VFStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    @Override
    protected StageRunResult<VirulenceFactorStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {
        StageContext stage = stageExecutionInput.stageContext;

        // Map<String,String> inputUrlMap = this.loadInputUrlMap(stage);
        // if (inputUrlMap == null){
        // return runFail(stage, "load input failed");
        // }
        VFStageInputUrls vfStageInputUrls = stageExecutionInput.input;

        String inputContigsUrl = vfStageInputUrls.getContigsUrl();
        Path inputContigPath = stageExecutionInput.inputDir
                .resolve(inputContigsUrl.substring(inputContigsUrl.lastIndexOf("/") + 1));

        this.loadInput(Map.of(inputContigsUrl, inputContigPath));

        try {
            inputContigPath = uncompressIfCompressedFormat(inputContigPath);
        } catch (IOException e) {
            String failReason = String.format(
                    "Failed to uncompress contig file. source=%s, reason=%s",
                    inputContigPath.toAbsolutePath(),
                    e.getMessage());
            this.logger.error(failReason, e);
            return this.runFail(stage, failReason, stageExecutionInput.workDir);
        }

        Path resultPath = stageExecutionInput.workDir.resolve("vf.tsv");
        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getVirulenceFactor());
        runCmd.add("--db");
        runCmd.add("vfdb");
        runCmd.add("--minid");
        runCmd.add("90");
        runCmd.add("--mincov");
        runCmd.add("60");
        runCmd.add(inputContigPath.toString());

        ExecuteResult executeResult = this._execute(runCmd, stageExecutionInput.workDir, resultPath, null);
        if (!executeResult.success()) {
            logger.error("{} run failed. code = {}", stage, executeResult.runCode, executeResult.ex);
            return this.runFail(stage, "run failed", stageExecutionInput.workDir);
        }

        List<StageOutputValidationResult> validationResults = validateOutputFiles(resultPath);
        if (!validationResults.isEmpty()) {
            logger.error("{} no output generated", stage);
            return this.runFail(stage, "no output generated", stageExecutionInput.workDir);
        }

        return OK(new VirulenceFactorStageOutput(resultPath), stageExecutionInput);
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_VIRULENCE;
    }
}
