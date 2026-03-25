package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_DEPTH_COVERAGE;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CoverageDepthStageInputs;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.CoverageDepthStageOutput;

import java.io.File;
import java.nio.file.Path;

public class CoverageDepthStageExecutor extends AbstractPipelineStageExector<CoverageDepthStageOutput, CoverageDepthStageInputs, BaseStageParams> implements PipelineStageExecutor<CoverageDepthStageOutput>{

    @Override
    protected Class<CoverageDepthStageInputs> stageInputType() {
        // TODO Auto-generated method stub
        return CoverageDepthStageInputs.class;
    }

    @Override
    protected Class<BaseStageParams> stageParameterType() {
        // TODO Auto-generated method stub
        return BaseStageParams.class;
    }

    @Override
    protected StageRunResult<CoverageDepthStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {
        // TODO Auto-generated method stub

        CoverageDepthStageInputs coverageDepthStageInputs = stageExecutionInput.input;

        Path bam = stageExecutionInput.inputDir.resolve("in.bam");
        Path bamIndex = stageExecutionInput.inputDir.resolve("in.bam.bai");

        Map<String,Path> loadInputMap = Map.of(
        coverageDepthStageInputs.getBam(), bam, 
        coverageDepthStageInputs.getBamIndex(), bamIndex);

        loadInput(loadInputMap);
        List<String> cmd = this.analysisPipelineToolsConfig.getSamtools();
        cmd.add("depth");
        cmd.add("-a");
        cmd.add(bam.toString());


        Path depthTable = stageExecutionInput.workDir.resolve("depth.tsv");
        Path summary = stageExecutionInput.workDir.resolve("summary.json");
        boolean executeRes = this._execute(cmd, null, stageExecutionInput, depthTable, summary);
        if(!executeRes){
            return this.runFail(stageExecutionInput.stageContext, ERROR_EXECUTE_FAIL);
        }
        return StageRunResult.OK(new CoverageDepthStageOutput(depthTable, summary), stageExecutionInput.stageContext);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_DEPTH_COVERAGE;
    }




}
