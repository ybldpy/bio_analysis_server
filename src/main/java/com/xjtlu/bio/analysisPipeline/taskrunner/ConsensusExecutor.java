package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_CONSENSUS;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder.FaiBuildException;

import jakarta.annotation.Resource;

@Component
public class ConsensusExecutor
        extends AbstractPipelineStageExector<ConsensusStageOutput, ConsensusStageInputUrls, ConsensusStageParameters>
        implements PipelineStageExecutor<ConsensusStageOutput> {

    @Value("${analysis-pipeline.stage.consensus.fastaFileName}")
    private String consensusFastaFileName;

    @Value("${analysis-pipeline.stage.varient.vcfFileName}")
    private String vcfFileName;

    @Value("${analysis-pipeline.stage.varient.vcfIndexFileName}")
    private String vcfTbiFileName;

    @Resource
    private FaiBuilder faiBuilder;

    @Override
    protected Class<ConsensusStageInputUrls> stageInputType() {
        return ConsensusStageInputUrls.class;
    }

    @Override
    protected Class<ConsensusStageParameters> stageParameterType() {
        return ConsensusStageParameters.class;
    }

    @Override
    public StageRunResult<ConsensusStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {
        // TODO Auto-generated method stub

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        ConsensusStageInputUrls consensusStageInputUrls = stageExecutionInput.input;
        ConsensusStageParameters consensusStageParameters = stageExecutionInput.stageParameters;

        RefSeqConfig refSeqConfig = consensusStageParameters.getRefSeqConfig();

        if (refSeqConfig == null) {
            return this.runFail(bioPipelineStage, "未找到参考基因文件", stageExecutionInput.workDir);
        }

        Path inputTmpDir = stageExecutionInput.inputDir;
        Path resultDir = stageExecutionInput.workDir;

        String vcfGzUrl = consensusStageInputUrls.getVcfGz();
        String vcfTbiUrl = consensusStageInputUrls.getVcfTbi();

        Path vcfGzTmpPath = inputTmpDir.resolve(vcfFileName);
        Path vcfTbiTmpPath = inputTmpDir.resolve(vcfTbiFileName);

        String referenceObjName = refSeqConfig.getRefseqObjectName();
        String referenceFileName = referenceObjName.substring(referenceObjName.lastIndexOf("/") + 1);
        Path refseqLocalPath = inputTmpDir.resolve(referenceFileName);

        loadInput(Map.of(vcfGzUrl, vcfGzTmpPath, vcfTbiUrl, vcfTbiTmpPath, referenceObjName, refseqLocalPath));

        try {
            faiBuilder.build(refseqLocalPath);
        } catch (IOException | InterruptedException | FaiBuildException e) {

            logger.error("Failed to build fai for reference: {}", refseqLocalPath, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            String msg = String.format(
                    "Failed to build fai for reference %s: %s",
                    refseqLocalPath,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return this.runFail(bioPipelineStage, msg, stageExecutionInput.workDir);
        }

        

        String consensus = "consensus";
        Path consensusPath = stageExecutionInput.workDir.resolve(consensusFastaFileName);

        ConsensusStageOutput consensusStageOutput = new ConsensusStageOutput(
                stageExecutionInput.workDir.resolve(consensusFastaFileName).toString());

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.addAll(List.of(
                consensus,
                "-f",
                refseqLocalPath.toString(),
                "-H",
                String.valueOf(1),
                "-o",
                consensusPath.toString(),
                vcfGzTmpPath.toString()));

        boolean runFail = false;
        Exception runFailException = null;
        try {
            int code = this.runSubProcess(cmd, resultDir);
            if (code != 0) {
                runFail = true;
            }
        } catch (Exception e) {
            runFail = true;
            runFailException = e;
        }

        if (runFail) {
            return this.runFail(bioPipelineStage, "运行consensus tool失败", runFailException, resultDir);
        }

        List<StageOutputValidationResult> errOutputValidationResults = validateOutputFiles(consensusPath);
        if (!errOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errOutputValidationResults), stageExecutionInput.workDir);
        }

        return OK(consensusStageOutput, stageExecutionInput);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_CONSENSUS;
    }

}
