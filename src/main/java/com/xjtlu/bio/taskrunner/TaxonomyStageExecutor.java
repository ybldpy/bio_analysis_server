package com.xjtlu.bio.taskrunner;

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_TAXONOMY;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_TAXONOMY_INPUT;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.entity.BioPipelineStage;

import com.xjtlu.bio.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

public class TaxonomyStageExecutor extends AbstractPipelineStageExector<TaxonomyStageOutput>
        implements PipelineStageExecutor<TaxonomyStageOutput> {

    @Override
    protected StageRunResult<TaxonomyStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException {

        BioPipelineStage bioPipelineStage = stageExecutionInput.bioPipelineStage;
        Path inputDirPath = stageExecutionInput.inputDir;
        Path workDirPath = stageExecutionInput.workDir;

        Map<String, String> inputMap = JsonUtil.toMap(bioPipelineStage.getInputUrl(), String.class);

        String contigUrl = inputMap.get(PIPELINE_STAGE_TAXONOMY_INPUT);

        Path contigPath = inputDirPath.resolve("sample.contig");

        boolean loadResult = this.loadInput(Map.of(contigUrl, contigPath));

        if(!loadResult){
            return this.runFail(bioPipelineStage, "加载input失败");
        }


        Path reportPath = workDirPath.resolve("taxonomny.report");
        Path outputPath = workDirPath.resolve("taxonomy.output");

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getKraken2());
        runCmd.add(contigPath.toString());
        runCmd.add("--report");
        runCmd.add(reportPath.toString());
        runCmd.add("--output");
        runCmd.add(outputPath.toString());


        logger.info("stage = {} start to run", bioPipelineStage);
        ExecuteResult executeResult = this._execute(runCmd, workDirPath);

        if(executeResult.runCode!=0 || executeResult.ex!=null){
            logger.error("stage = {} run failed. Code = {}", bioPipelineStage, executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "execution failed");
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(outputPath);
        if(!stageOutputValidationResults.isEmpty()){
            logger.error("stage = {}. Validate file = {} failed", bioPipelineStage, stageOutputValidationResults.get(0).path.toString(), stageOutputValidationResults.get(0).ioException);
            return this.runFail(bioPipelineStage, "validate output file failed");
        }

        return StageRunResult.OK(new TaxonomyStageOutput(outputPath, reportPath), bioPipelineStage);

    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_TAXONOMY;
    }

}
