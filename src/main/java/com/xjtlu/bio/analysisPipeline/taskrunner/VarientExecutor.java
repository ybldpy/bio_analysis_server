package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder.FaiBuildException;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.VariantFilterExpressionBuilder;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.VariantVcfMetricsParser;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

@Component
public class VarientExecutor
        extends AbstractPipelineStageExector<VariantStageOutput, VarientCallInputUrls, VarientCallParameters>
        implements PipelineStageExecutor<VariantStageOutput> {


    @Resource
    private FaiBuilder faiBuilder;
    
    @Override
    protected Class<VarientCallInputUrls> stageInputType() {
        return VarientCallInputUrls.class;
    }

    @Override
    protected Class<VarientCallParameters> stageParameterType() {
        return VarientCallParameters.class;
    }

    @Override
    public StageRunResult<VariantStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws JsonMappingException, JsonProcessingException, LoadFailException {
        // TODO Auto-generated method stub

        StageContext bioPipelineStage = stageExecutionInput.stageContext;
        VarientCallInputUrls varientCallInputUrls = stageExecutionInput.input;
        VarientCallParameters varientCallParameters = stageExecutionInput.stageParameters;

        final String variantFilterExpression;
        try {
            variantFilterExpression =
                    VariantFilterExpressionBuilder.buildExclusionExpression(
                            varientCallParameters);
        } catch (IllegalArgumentException | NullPointerException e) {
            return this.runFail(
                    bioPipelineStage,
                    "Invalid variant call parameters: " + e.getMessage(),
                    e,
                    stageExecutionInput.workDir);
        }

        ReferenceGenome referenceGenome = varientCallParameters.getReferenceGenome();

        String bamUrl = varientCallInputUrls.getBamUrl();
        String bamIndexUrl = varientCallInputUrls.getBamIndexUrl();

        Path inputTempDir = stageExecutionInput.inputDir;
        // 结果目录
        Path workDir = stageExecutionInput.workDir;



        Path bam = inputTempDir.resolve("aln.bam");
        Path bai = inputTempDir.resolve("aln.bam.bai");

        // 先用 samtools 生成参考索引

        Path refseqLocalPath = materializeReferenceGenome(referenceGenome, inputTempDir);
        loadInput(Map.of(bamUrl, bam, bamIndexUrl, bai));


        try {
            faiBuilder.build(refseqLocalPath);
        } catch (IOException | InterruptedException | FaiBuildException e) {
            logger.error("Failed to build fai for reference: {}", refseqLocalPath, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            String msg = String.format(
                    "Failed to build reference index for reference %s: %s",
                    refseqLocalPath,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
            return this.runFail(bioPipelineStage, msg, stageExecutionInput.workDir);
        }

        

        // 工具路径与参数

        int threads = varientCallParameters.getThreads();

        // 中间与最终产物
        Path bcfRaw = workDir.resolve("raw.bcf");
        Path rawVcfGz = workDir.resolve(VariantStageOutput.RAW_VCF_GZ);
        Path rawVcfTbi = workDir.resolve(VariantStageOutput.RAW_VCF_TBI);
        Path normalizedVcfGz = workDir.resolve("variants.normalized.vcf.gz");
        Path vcfGz = workDir.resolve(VariantStageOutput.VCF_GZ);
        Path vcfTbi = workDir.resolve(VariantStageOutput.VCF_TBI);
        Path metricsJson = stageExecutionInput.workDir.resolve(VariantStageOutput.METRICS_JSON);

        // ---------- 1) mpileup: BAM -> BCF ----------
        // -Ou 输出未压缩 BCF 到 stdout（这里我们直接 -o 写文件，避免管道）
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("mpileup");
        cmd.add("-f");
        cmd.add(refseqLocalPath.toString());
        cmd.add("-q");
        cmd.add(String.valueOf(varientCallParameters.getMinimumMappingQuality()));
        cmd.add("-Q");
        cmd.add(String.valueOf(varientCallParameters.getMinimumBaseQuality()));
        cmd.add("-a");
        cmd.add("DP,AD"); // 输出深度/等位深度
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add("-O");
        cmd.add("u"); // uncompressed BCF in memory format
        cmd.add("-o");
        cmd.add(bcfRaw.toString()); // 直接落盘
        cmd.add(bam.toString());

        ExecuteResult executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            return this.runFail(bioPipelineStage, "生成bcf.gz失败", executeResult.ex, workDir);
        }

        List<StageOutputValidationResult> errorOutputValidationResults = validateOutputFiles(bcfRaw);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null, workDir);
        }

        // ---------- 2) call: BCF -> raw VCF.GZ ----------
        cmd.clear();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("call");
        cmd.add("-m"); // multiallelic caller
        cmd.add("--ploidy");
        cmd.add("1"); // 病毒倍性=1
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add("-Oz"); // 压缩 VCF
        cmd.add("-o");
        cmd.add(rawVcfGz.toString());
        cmd.add(bcfRaw.toString());

        executeResult = _execute(cmd, workDir);

        if (!executeResult.success()) {
            return this.runFail(bioPipelineStage, "生成VCF.gz失败", executeResult.ex, workDir);
        }

        errorOutputValidationResults = validateOutputFiles(rawVcfGz);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null, workDir);
        }

        // ---------- 3) index raw VCF ----------
        cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("index");
        cmd.add("-t"); // 生成 TBI
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add(rawVcfGz.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            return this.runFail(bioPipelineStage, "Failed to index raw VCF", executeResult.ex, workDir);
        }

        rawVcfTbi = workDir.resolve(rawVcfGz.getFileName() + ".tbi");
        errorOutputValidationResults = validateOutputFiles(rawVcfTbi);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null, workDir);
        }

        // ---------- 4) normalize calls against the selected reference ----------
        cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("norm");
        cmd.add("-f");
        cmd.add(refseqLocalPath.toString());
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add("-Oz");
        cmd.add("-o");
        cmd.add(normalizedVcfGz.toString());
        cmd.add(rawVcfGz.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            return this.runFail(
                    bioPipelineStage,
                    "Failed to normalize raw VCF",
                    executeResult.ex,
                    workDir);
        }

        errorOutputValidationResults = validateOutputFiles(normalizedVcfGz);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(
                    bioPipelineStage,
                    createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null,
                    workDir);
        }

        // ---------- 5) soft-filter normalized calls ----------
        // Passing records are reset to PASS; rejected records remain in the
        // VCF and are tagged LowConfidence for audit.
        cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("filter");
        cmd.add("-m");
        cmd.add("x");
        cmd.add("-s");
        cmd.add(VariantFilterExpressionBuilder.LOW_CONFIDENCE_FILTER_NAME);
        cmd.add("-e");
        cmd.add(variantFilterExpression);
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add("-Oz");
        cmd.add("-o");
        cmd.add(vcfGz.toString());
        cmd.add(normalizedVcfGz.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            return this.runFail(
                    bioPipelineStage,
                    "Failed to filter normalized VCF",
                    executeResult.ex,
                    workDir);
        }

        errorOutputValidationResults = validateOutputFiles(vcfGz);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(
                    bioPipelineStage,
                    createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null,
                    workDir);
        }

        // ---------- 6) index filtered VCF ----------
        cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("index");
        cmd.add("-t");
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add(vcfGz.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            return this.runFail(
                    bioPipelineStage,
                    "Failed to index filtered VCF",
                    executeResult.ex,
                    workDir);
        }

        vcfTbi = workDir.resolve(vcfGz.getFileName() + ".tbi");
        errorOutputValidationResults = validateOutputFiles(vcfTbi);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(
                    bioPipelineStage,
                    createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null,
                    workDir);
        }

        VariantCallResultMetrics metrics;
        try {
            metrics = VariantVcfMetricsParser.parse(vcfGz);
            Files.writeString(
                    metricsJson,
                    JsonUtil.toJson(metrics),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.error("Failed to calculate variant metrics from {}", vcfGz, e);
            return this.runFail(
                    bioPipelineStage,
                    "Failed to calculate variant metrics: " + e.getMessage(),
                    e,
                    workDir);
        }

        errorOutputValidationResults = validateOutputFiles(metricsJson);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(
                    bioPipelineStage,
                    createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null,
                    workDir);
        }

        return OK(
                new VariantStageOutput(
                        rawVcfGz.toString(),
                        rawVcfTbi.toString(),
                        vcfGz.toString(),
                        vcfTbi.toString(),
                        metricsJson.toString(),
                        metrics),
                stageExecutionInput);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_VARIANT_CALL;
    }

}
