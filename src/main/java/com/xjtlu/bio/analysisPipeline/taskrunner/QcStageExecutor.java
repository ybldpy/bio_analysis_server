package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.QCStageOutput;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.utils.JsonUtil;

@Component
public class QcStageExecutor extends AbstractPipelineStageExector<QCStageOutput> implements PipelineStageExecutor<QCStageOutput> {



    private static final Logger logger = LoggerFactory.getLogger(QcStageExecutor.class);

    @Override
    public StageRunResult<QCStageOutput> _execute(StageExecutionInput stageExecutionInput) throws JsonMappingException, JsonProcessingException {
        // TODO Auto-generated method stub

        BioPipelineStage bioPipelineStage = stageExecutionInput.bioPipelineStage;
        String inputUrlsJson = bioPipelineStage.getInputUrl();
        QcStageInputUrls qcStageInputUrls = JsonUtil.toObject(inputUrlsJson, QcStageInputUrls.class);

        

        String inputUrl1 = qcStageInputUrls.getRead1();
        String input1FileName = inputUrl1.substring(inputUrl1.lastIndexOf("/") + 1);
        String inputUrl2 =  StringUtils.isBlank(qcStageInputUrls.getRead2()) ? null : qcStageInputUrls.getRead2();
        String input2FileName = inputUrl2 == null ? null : inputUrl2.substring(inputUrl2.lastIndexOf("/") + 1);

        Path outputDir = stageExecutionInput.workDir;
        Path inputDir = stageExecutionInput.inputDir;

        String params = bioPipelineStage.getParameters();
        QcParameters qcParams = JsonUtil.toObject(bioPipelineStage.getParameters(), QcParameters.class);

        

        QCStageOutput qcStageOutput = this.bioStageUtil.qcStageOutput(outputDir, inputUrl2 != null);

        Path trimmedR1Path = Path.of(qcStageOutput.getR1Path());
        Path trimmedR2Path = inputUrl2 == null ? null
                : Path.of(qcStageOutput.getR2Path());

        if(inputUrl2 == null){
            qcStageOutput.setR2Path(null);
        }
        Path outputQcJson = Path.of(qcStageOutput.getJsonPath());
        Path outputQcHtml = Path.of(qcStageOutput.getHtmlPath());


        Path r1Path = inputDir.resolve(input1FileName);
        Path r2Path = inputUrl2 == null? null: inputDir.resolve(input2FileName);


        GetObjectResult objectResult = storageService.getObject(inputUrl1, r1Path.toString());
        if (!objectResult.success()) {
            if(objectResult.e()==null){
                logger.error("{} 加载input url {} 失败", bioPipelineStage, inputUrl1);
            }else {
                logger.error("{} 加载input url {} 失败", bioPipelineStage, inputUrl1, objectResult.e());
            }
            return StageRunResult.fail("加载read1失败", bioPipelineStage,objectResult.e());
        }

        File inputFile1 = objectResult.objectFile();
        File inputFile2 = null;
        if (StringUtils.isNotBlank(inputUrl2)) {
            GetObjectResult r2ObjectGetResult = storageService.getObject(inputUrl2, r2Path.toString());
            if (!r2ObjectGetResult.success()) {
                if(objectResult.e()==null){
                    logger.error("{} 加载input url {} 失败", bioPipelineStage, inputUrl1);
                }else {
                    logger.error("{} 加载input url {} 失败", bioPipelineStage, inputUrl1, objectResult.e());
                }
                return StageRunResult.fail("加载read2失败", bioPipelineStage, r2ObjectGetResult.e());
            }
            inputFile2 = r2ObjectGetResult.objectFile();
        }

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getFastp());
        if (inputUrl2 != null) {
            // 双端
            cmd.addAll(List.of(
                    "-i", inputFile1.getAbsolutePath(),
                    "-I", inputFile2.getAbsolutePath(),
                    "-o", trimmedR1Path.toString(),
                    "-O", trimmedR2Path.toString()));
        } else {
            // 单端
            cmd.addAll(List.of(
                    "-i", inputFile1.getAbsolutePath(),
                    "-o", trimmedR1Path.toString()));
        }
        cmd.addAll(List.of(
                "--json", outputQcJson.toString(),
                "--html", outputQcHtml.toString(),
                "--thread", String.valueOf(Math.max(2, Runtime.getRuntime().availableProcessors() / 4))));

        int runResult = 0;
        Exception runException = null;
        try {
            logger.info("{} qc process start", bioPipelineStage);
            runResult = runSubProcess(cmd, outputDir);
        } catch (IOException | InterruptedException e) {
            runResult = -1;
            runException = e;
        }

        if(runResult!=0){

            if(runException!=null) {
                logger.error("{} qc failed. exit code = {}", bioPipelineStage, runResult, runException);
            }else {
                logger.error("{} qc failed. exit code = {}", bioPipelineStage, runResult);
            }

            return this.runFail(bioPipelineStage, "运行qc tool失败", runException, inputDir, outputDir);
        }

        List<StageOutputValidationResult> errStageOutputValidationResults = null;
        if(inputUrl2 == null){
            errStageOutputValidationResults = validateOutputFiles(trimmedR1Path, outputQcJson, outputQcHtml);
        }else {
            errStageOutputValidationResults = validateOutputFiles(trimmedR1Path, trimmedR2Path, outputQcJson, outputQcHtml);
        }

        if(!errStageOutputValidationResults.isEmpty()){
            String errorMsg = createStageOutputValidationErrorMessge(errStageOutputValidationResults);
            logger.error("{} qc no output. {}", bioPipelineStage, errorMsg);
            return this.runFail(bioPipelineStage, errorMsg);
        }

        return StageRunResult.OK(
                qcStageOutput,
                bioPipelineStage);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_QC;
    }

}
