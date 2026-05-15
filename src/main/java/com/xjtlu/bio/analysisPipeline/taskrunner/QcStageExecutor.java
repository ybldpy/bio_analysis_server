package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.ReadMeta;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.QCStageOutput;


import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.service.StorageService.GetObjectResult;

@Component
public class QcStageExecutor extends AbstractPipelineStageExector<QCStageOutput, QcStageInputUrls, QcParameters> implements PipelineStageExecutor<QCStageOutput> {


    @Override
    protected Class<QcStageInputUrls> stageInputType() {
        return QcStageInputUrls.class;
    }

    @Override
    protected Class<QcParameters> stageParameterType() {
        return QcParameters.class;
    }

    private static final Logger logger = LoggerFactory.getLogger(QcStageExecutor.class);
    private static final int TOOL_CODE_FASTQ = 0;
    private static final int TOOL_CODE_FASTQ_LONG = 1;


    private List<String> buildQcRunCmd(int toolCode, Path read1InputPath, Path read2InputPath, Path outputRead1Path, Path outputRead2Path, Path jsonPath, Path htmlPath){

        List<String> cmd = new ArrayList<>();

        if(toolCode == TOOL_CODE_FASTQ){
            cmd.addAll(analysisPipelineToolsConfig.getFastp());
            cmd.add("-i");
            cmd.add(read1InputPath.toString());
            cmd.add("-o");
            cmd.add(outputRead1Path.toString());
            cmd.add("--json");
            cmd.add(jsonPath.toString());
            cmd.add("--html");
            cmd.add(htmlPath.toString());

            if(read2InputPath != null){
                cmd.add("-I");
                cmd.add(read2InputPath.toString());
                cmd.add("-O");
                cmd.add(outputRead2Path.toString());
            }

        }else {
            cmd.addAll(analysisPipelineToolsConfig.getFastplong());
            cmd.addAll(
                List.of(
                    "-i",
                    read1InputPath.toString(),
                    "-o",
                    outputRead1Path.toString(),
                    "-j",
                    jsonPath.toString(),
                    "-h",
                    htmlPath.toString()
                )
            );

        }

        return cmd;
    }
 
    @Override
    public StageRunResult<QCStageOutput> _execute(StageExecutionInput stageExecutionInput) throws JsonMappingException, JsonProcessingException, LoadFailException {
        // TODO Auto-generated method stub

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        QcStageInputUrls qcStageInputUrls = stageExecutionInput.input;

        

        

        Path outputDir = stageExecutionInput.workDir;
        Path inputDir = stageExecutionInput.inputDir;

        
        QcParameters qcParams = stageExecutionInput.stageParameters;


        

        String inputUrl1 = qcStageInputUrls.getRead1();
        String input1FileName = inputUrl1.substring(inputUrl1.lastIndexOf("/") + 1);
        String inputUrl2 =  StringUtils.isBlank(qcStageInputUrls.getRead2()) ? null : qcStageInputUrls.getRead2();

        boolean hasR2 = inputUrl2 != null && qcParams.getReadMeta().getReadLenType()!=ReadMeta.READ_LEN_TYPE_LONG;

        String input2FileName = !hasR2? null : inputUrl2.substring(inputUrl2.lastIndexOf("/") + 1);
        boolean isGz = input1FileName.endsWith(".gz");
        QCStageOutput qcStageOutput = new QCStageOutput(outputDir.resolve(input1FileName).toString(), 
        !hasR2? null:outputDir.resolve(input2FileName).toString(), 
        outputDir.resolve("cleaned.html").toString(),
        outputDir.resolve("cleaned.json").toString(), isGz);

        Path trimmedR1Path = Path.of(qcStageOutput.getR1Path());
        Path trimmedR2Path = !hasR2 ? null
                : Path.of(qcStageOutput.getR2Path());

        if(!hasR2){
            qcStageOutput.setR2Path(null);
        }
        Path outputQcJson = Path.of(qcStageOutput.getJsonPath());
        Path outputQcHtml = Path.of(qcStageOutput.getHtmlPath());


        Path r1Path = inputDir.resolve(input1FileName);
        Path r2Path = !hasR2? null: inputDir.resolve(input2FileName);

        HashMap<String,Path> loadMap = new HashMap<>();
        loadMap.put(inputUrl1, r1Path);
        if(hasR2){
            loadMap.put(inputUrl2, r2Path);
        }

        loadInput(loadMap);

        List<String> cmd = buildQcRunCmd(qcParams.getReadMeta().getReadLenType() == ReadMeta.READ_LEN_TYPE_SHORT?TOOL_CODE_FASTQ:TOOL_CODE_FASTQ_LONG, r1Path, r2Path, trimmedR1Path, trimmedR2Path, outputQcJson, outputQcHtml);

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
