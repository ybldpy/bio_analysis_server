package com.xjtlu.bio.analysisPipeline.taskrunner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MLSTStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MLSTStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MLST;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class MLSTStageExecutor
        extends AbstractPipelineStageExector<MLSTStageOutput, MLSTStageInputUrls, BaseStageParams>
        implements PipelineStageExecutor<MLSTStageOutput> {

    @Override
    protected Class<MLSTStageInputUrls> stageInputType() {
        return MLSTStageInputUrls.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        return BaseStageParams.class;
    }

    @Value("${analysis-pipeline.stage.mlst.tsvFileName}")
    private String mlstFileName;

    @Override
    protected StageRunResult<MLSTStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {
        StageContext stage = stageExecutionInput.stageContext;

        MLSTStageInputUrls mlstStageInputUrls = stageExecutionInput.input;
        String contigUrl = mlstStageInputUrls.getContigUrl();
        Path contigPath = stageExecutionInput.inputDir.resolve(contigUrl.substring(contigUrl.lastIndexOf("/") + 1));
        Path resultPath = stageExecutionInput.workDir.resolve("mlstResult.tsv");
        this.loadInput(Map.of(contigUrl, contigPath));

        try {
            contigPath = uncompressIfCompressedFormat(contigPath);
        } catch (IOException e) {
            String failReason = String.format(
                    "Failed to uncompress contig file. source=%s, reason=%s",
                    contigPath.toAbsolutePath(),
                    e.getMessage());
            this.logger.error(failReason, e);
            return this.runFail(stage, failReason, stageExecutionInput.workDir);
        }

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getMlst());
        runCmd.add(contigPath.toString());

        ExecuteResult executeResult = this._execute(runCmd, stageExecutionInput.workDir, resultPath, null);

        if (!executeResult.success()) {
            logger.error("{} run failed. code = {}", stage, executeResult.runCode, executeResult.ex);
            return this.runFail(stage, "run failed", stageExecutionInput.workDir);
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(resultPath);
        if (!stageOutputValidationResults.isEmpty()) {
            logger.error("{} no output", stage, stageOutputValidationResults.get(0).ioException);
            return this.runFail(stage, "no output", stageExecutionInput.workDir);
        }
        return OK(new MLSTStageOutput(resultPath), stageExecutionInput);
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_MLST;
    }
}
