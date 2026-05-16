package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafParser;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafParser.PafParseResult;

@Component
public class ReferenceComparsionStageExecutor extends
        AbstractPipelineStageExector<ReferenceComparisonStageOutput, ReferenceComparisonStageInputUrls, ReferenceComparisonStageParameters> {

    @Override
    protected Class<ReferenceComparisonStageInputUrls> stageInputType() {
        return ReferenceComparisonStageInputUrls.class;
    }

    @Override
    protected Class<ReferenceComparisonStageParameters> stageParameterType() {
        return ReferenceComparisonStageParameters.class;
    }

    @Override
    protected StageRunResult<ReferenceComparisonStageOutput> _execute(
            StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException, NotGetRefSeqException {

        ReferenceComparisonStageParameters referenceComparisonStageParameters = stageExecutionInput.stageParameters;
        Path inputDir = stageExecutionInput.inputDir;
        Path workDir = stageExecutionInput.workDir;
        HashMap<String, Path> loadMap = new HashMap<>();

        RefSeqConfig refSeqConfig = referenceComparisonStageParameters.getRefSeqConfig();
        Path refseqLocalPath = inputDir.resolve(
                refSeqConfig.getRefseqObjectName().substring(refSeqConfig.getRefseqObjectName().lastIndexOf("/") + 1));
        loadMap.put(refSeqConfig.getRefseqObjectName(), refseqLocalPath);

        ReferenceComparisonStageInputUrls inputUrls = stageExecutionInput.input;
        Path inputLocalPath = inputDir
                .resolve(inputUrls.getFastaUrl().substring(inputUrls.getFastaUrl().lastIndexOf("/") + 1));
        loadMap.put(inputUrls.getFastaUrl(), inputLocalPath);

        this.loadInput(loadMap);

        Path alignmentPafPath = workDir.resolve("alignment.paf");
        Path differenceTsvPath = workDir.resolve("reference_difference.tsv");

        // 1. minimap2 reference vs input fasta
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getMinimap2());
        cmd.add("-x");
        cmd.add("asm5");
        cmd.add("--cs=long");
        cmd.add("--secondary=no");
        cmd.add(refseqLocalPath.toString());
        cmd.add(inputLocalPath.toString());
        cmd.add("-o");
        cmd.add(alignmentPafPath.toString());

        ExecuteResult executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            String cmdText = String.join(" ", cmd);

            if (executeResult.ex != null) {
                String msg = String.format(
                        "Reference comparison execute failed. exitCode=%d, workDir=%s, cmd=%s",
                        executeResult.runCode,
                        workDir,
                        cmdText);

                return runFail(stageExecutionInput.stageContext, msg, stageExecutionInput.workDir);
            }

            String msg = String.format(
                    "Reference comparison command returned non-zero exit code. exitCode=%d, workDir=%s, cmd=%s",
                    executeResult.runCode,
                    workDir,
                    cmdText);
            return runFail(stageExecutionInput.stageContext, msg, stageExecutionInput.workDir);
        }

        List<StageOutputValidationResult> stageOutputValidationResults = validateOutputFiles(alignmentPafPath);
        if (!stageOutputValidationResults.isEmpty()) {
            // no validate output file here.
            logger.error(
                    "Reference comparison output validation failed. alignmentPafPath={}, validationResults={}",
                    alignmentPafPath,
                    stageOutputValidationResults);

            return runFail(stageExecutionInput.stageContext, ERROR_EXECUTE_FAIL, stageExecutionInput.workDir);
        }

        PafParseResult pafParseResult = PafParser.parseToDifferenceTsv(alignmentPafPath, differenceTsvPath);

        if (!pafParseResult.isSuccess()) {

            logger.error(
                    "Failed to parse PAF to difference TSV. alignmentPafPath={}, differenceTsvPath={}",
                    alignmentPafPath,
                    differenceTsvPath,
                    pafParseResult.getException());

            return runFail(stageExecutionInput.stageContext, ERROR_EXECUTE_FAIL, stageExecutionInput.workDir);
        }

        return OK(new ReferenceComparisonStageOutput(alignmentPafPath, differenceTsvPath),
                stageExecutionInput);

    }

    @Override
    public int id() {
        return Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
    }

}
