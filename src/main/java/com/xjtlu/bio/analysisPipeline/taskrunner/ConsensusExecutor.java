package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_CONSENSUS;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.ConsensusMaskBuilder;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder.FaiBuildException;
import com.xjtlu.bio.utils.JsonUtil;

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

    @Value("${analysis-pipeline.stage.consensus.maskBedFileName}")
    private String maskBedFileName;

    @Value("${analysis-pipeline.stage.consensus.metricsFileName:${analysis-pipeline.stage.consensus.qcFileName:consensus.metrics.json}}")
    private String metricsFileName;

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
        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        ConsensusStageInputUrls consensusStageInputUrls = stageExecutionInput.input;
        ConsensusStageParameters consensusStageParameters = stageExecutionInput.stageParameters;

        if (consensusStageInputUrls == null || consensusStageParameters == null) {
            return this.runFail(
                    bioPipelineStage,
                    "Consensus input and parameters must not be null",
                    stageExecutionInput.workDir);
        }

        ReferenceGenome referenceGenome = consensusStageParameters.getReferenceGenome();

        Path inputTmpDir = stageExecutionInput.inputDir;
        Path resultDir = stageExecutionInput.workDir;

        String vcfGzUrl = consensusStageInputUrls.getVcfGz();
        String vcfTbiUrl = consensusStageInputUrls.getVcfTbi();
        String coverageDepthUrl = consensusStageInputUrls.getCoverageDepthUrl();
        if (StringUtils.isAnyBlank(vcfGzUrl, vcfTbiUrl, coverageDepthUrl)) {
            return this.runFail(
                    bioPipelineStage,
                    "VCF, VCF index and coverage depth inputs are required for consensus",
                    stageExecutionInput.workDir);
        }

        int minimumDepth = consensusStageParameters.getMinimumConsensusDepth();
        if (minimumDepth < 1) {
            return this.runFail(
                    bioPipelineStage,
                    "minimumConsensusDepth must be at least 1",
                    stageExecutionInput.workDir);
        }

        Path vcfGzTmpPath = inputTmpDir.resolve(vcfFileName);
        Path vcfTbiTmpPath = inputTmpDir.resolve(vcfTbiFileName);
        Path coverageDepthTmpPath = inputTmpDir.resolve("coverage_depth.tsv");

        Path refseqLocalPath = materializeReferenceGenome(referenceGenome, inputTmpDir);

        loadInput(Map.of(
                vcfGzUrl, vcfGzTmpPath,
                vcfTbiUrl, vcfTbiTmpPath,
                coverageDepthUrl, coverageDepthTmpPath));

        Path referenceFaiPath;
        try {
            referenceFaiPath = faiBuilder.build(refseqLocalPath);
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
        Path maskBedPath = stageExecutionInput.workDir.resolve(maskBedFileName);
        Path metricsPath = stageExecutionInput.workDir.resolve(metricsFileName);

        ConsensusResultMetrics metrics;
        try {
            metrics = ConsensusMaskBuilder.build(
                    coverageDepthTmpPath,
                    referenceFaiPath,
                    maskBedPath,
                    minimumDepth);
            Files.writeString(
                    metricsPath,
                    JsonUtil.toJson(metrics),
                    StandardCharsets.UTF_8);
        } catch (IOException | IllegalArgumentException e) {
            logger.error(
                    "Failed to create consensus depth mask. depth={}, fai={}",
                    coverageDepthTmpPath,
                    referenceFaiPath,
                    e);
            return this.runFail(
                    bioPipelineStage,
                    "Failed to create consensus depth mask: " + e.getMessage(),
                    e,
                    resultDir);
        }

        ConsensusStageOutput consensusStageOutput = new ConsensusStageOutput(
                consensusPath.toString(),
                maskBedPath.toString(),
                metricsPath.toString(),
                metrics);

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.addAll(List.of(
                consensus,
                "-f",
                refseqLocalPath.toString(),
                "-H",
                "1"));
        if (consensusStageParameters.isApplyOnlyPassingVariants()) {
            cmd.addAll(List.of(
                    "--include",
                    "FILTER=\"PASS\" || FILTER=\".\""));
        }
        if (metrics.getMaskedBases() > 0L) {
            cmd.addAll(List.of(
                    "--mask",
                    maskBedPath.toString(),
                    "--mask-with",
                    "N"));
        }
        cmd.addAll(List.of(
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

        List<StageOutputValidationResult> errOutputValidationResults = validateOutputFiles(
                consensusPath,
                metricsPath);
        if (!errOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errOutputValidationResults), stageExecutionInput.workDir);
        }
        if (!Files.isRegularFile(maskBedPath)) {
            return this.runFail(
                    bioPipelineStage,
                    "Consensus mask BED was not generated: " + maskBedPath,
                    stageExecutionInput.workDir);
        }

        return OK(consensusStageOutput, stageExecutionInput);
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_CONSENSUS;
    }

}
