package com.xjtlu.bio.analysisPipeline.taskrunner;


import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
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
        TaxonomyStageInputUrls taxonomyStageInputUrls = JsonUtil.toObject(bioPipelineStage.getInputUrl(), TaxonomyStageInputUrls.class);

        String r1Url = taxonomyStageInputUrls.getR1();
        String r2Url = taxonomyStageInputUrls.getR2();



        Path r1Path = inputDirPath.resolve("r1.fastq");
        Path r2Path = StringUtils.isBlank(r2Url)?null:inputDirPath.resolve("r2.fastq");

        boolean loadResult = r2Path == null?this.loadInput(Map.of(r1Url, r1Path)):this.loadInput(Map.of(r1Url, r1Path, r2Url, r2Path));


        if(!loadResult){
            return this.runFail(bioPipelineStage, "加载input失败");
        }


        Path reportPath = workDirPath.resolve("taxonomny.report");
        Path outputPath = workDirPath.resolve("taxonomy.output");

        List<String> runCmd = new ArrayList<>();
        runCmd.addAll(this.analysisPipelineToolsConfig.getKraken2());
        runCmd.add(r1Path.toString());
        if(r2Path!=null){
            runCmd.add(r2Path.toString());
            runCmd.add("--paired");
        }
        runCmd.add("--report");
        runCmd.add(reportPath.toString());
        runCmd.add("--output");
        runCmd.add(outputPath.toString());


        logger.info("stage = {} start to run", bioPipelineStage);
        ExecuteResult executeResult = _execute(runCmd, workDirPath);

        if(executeResult.runCode!=0 || executeResult.ex!=null){
            logger.error("stage = {} run failed. Code = {}", bioPipelineStage, executeResult.runCode, executeResult.ex);
            return this.runFail(bioPipelineStage, "execution failed");
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(reportPath);
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
