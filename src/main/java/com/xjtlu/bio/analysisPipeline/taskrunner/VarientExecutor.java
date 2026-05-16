package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder.FaiBuildException;

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

        RefSeqConfig refSeqConfig = varientCallParameters.getRefSeqConfig();
        if (refSeqConfig == null) {
            logger.error("stage id = {}, params = {}, unable to load refseq config", bioPipelineStage);
            return runFail(bioPipelineStage,"未能加载参考基因文件", stageExecutionInput.workDir);
        }



        String bamUrl = varientCallInputUrls.getBamUrl();
        String bamIndexUrl = varientCallInputUrls.getBamIndexUrl();

        Path inputTempDir = stageExecutionInput.inputDir;
        // 结果目录
        Path workDir = stageExecutionInput.workDir;



        Path bam = inputTempDir.resolve("aln.bam");
        Path bai = inputTempDir.resolve("aln.bam.bai");

        // 先用 samtools 生成参考索引

        String refseqUrl = varientCallParameters.getRefSeqConfig().getRefseqObjectName();


        Path refseqLocalPath = inputTempDir.resolve(refseqUrl.substring(refseqUrl.lastIndexOf("/")+1));
        loadInput(Map.of(bamUrl, bam, bamIndexUrl, bai, refseqUrl, refseqLocalPath));


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

        int threads = 2;

        // 中间与最终产物
        Path bcfRaw = workDir.resolve("raw.bcf");

        Path vcfGz = stageExecutionInput.workDir.resolve(VariantStageOutput.VCF_GZ);
        Path vcfTbi = stageExecutionInput.workDir.resolve(VariantStageOutput.VCF_TBI);

        // ---------- 1) mpileup: BAM -> BCF ----------
        // -Ou 输出未压缩 BCF 到 stdout（这里我们直接 -o 写文件，避免管道）
        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("mpileup");
        cmd.add("-f");
        cmd.add(refseqLocalPath.toString());
        cmd.add("-q");
        cmd.add("20"); // 最小比对质量
        cmd.add("-Q");
        cmd.add("20"); // 最小碱基质量
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

        // ---------- 2) call: BCF -> VCF.GZ ----------
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
        cmd.add(vcfGz.toString());
        cmd.add(bcfRaw.toString());

        executeResult = _execute(cmd, workDir);

        if (!executeResult.success()) {
            return this.runFail(bioPipelineStage, "生成VCF.gz失败", executeResult.ex, workDir);
        }

        errorOutputValidationResults = validateOutputFiles(vcfGz);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null, workDir);
        }

        // ---------- 3) index: VCF.GZ -> TBI ----------
        cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.add("index");
        cmd.add("-t"); // 生成 TBI
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add(vcfGz.toString());

        executeResult = _execute(cmd, workDir);
        if (!executeResult.success()) {
            return this.runFail(bioPipelineStage, "生成TBI失败", executeResult.ex, workDir);
        }

        vcfTbi = workDir.resolve(vcfGz.getFileName() + ".tbi");
        errorOutputValidationResults = validateOutputFiles(vcfTbi);
        if (!errorOutputValidationResults.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults),
                    null, workDir);
        }

        return OK(new VariantStageOutput(vcfGz.toString(), vcfTbi.toString()), stageExecutionInput);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_VARIANT_CALL;
    }

}
