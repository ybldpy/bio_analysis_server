package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;


@Component
public class VarientExecutor extends AbstractPipelineStageExector {


    @Value("${analysisPipeline.tools.bcftools}")
    private String bcftools;
    @Value("${analysisPipeline.tools.samtools}")
    private String samtools;


    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrls = bioPipelineStage.getInputUrl();
        Map<String, String> inputUrlMap = null;
        Map<String,Object> params = null;
        try {
            inputUrlMap = this.objectMapper.readValue(inputUrls, Map.class);
            params = this.objectMapper.readValue(bioPipelineStage.getParameters(), Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.parseError(bioPipelineStage);
        }

        RefSeqConfig refSeqConfig = this.getRefSeqConfigFromParams(params);
        if(refSeqConfig == null){
            return StageRunResult.fail("未能加载参考基因文件",bioPipelineStage, null);
        }

        File refseq = null;
        if(refSeqConfig.getRefseqId()>=0){
            refseq = this.refSeqService.getRefseq(refSeqConfig.getRefseqId());
        }else {
            refseq = this.refSeqService.getRefseq(refSeqConfig.getRefseqObjectName());
        }

        String bamPath = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY);
        String bamIndexPath = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY);

        Path inputTempDir = this.stageInputPath(bioPipelineStage);


        // 结果目录
        Path workDir = this.workDirPath(bioPipelineStage);
        try {
            Files.createDirectories(workDir);
        } catch (IOException e) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return StageRunResult.fail("创建目录失败", bioPipelineStage, e);
        }

        try {
            Files.createDirectories(inputTempDir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.runException(bioPipelineStage, e);
        }

        Path refSeqFileLink = null;
        try {
            refSeqFileLink = Files.createSymbolicLink(inputTempDir.resolve(refseq.getName()), refseq.toPath());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Path bam = inputTempDir.resolve("aln.bam");
        Path bai = inputTempDir.resolve("aln.bam.bai");

        // 先用 samtools 生成参考索引
        File refSeqIndexFile = refSeqConfig.getRefseqId()>=0?this.refSeqService.getRefSeqIndex(refSeqConfig.getRefseqId()):this.refSeqService.getRefSeqIndex(refSeqConfig.getRefseqObjectName());

        if (refSeqIndexFile == null || !refSeqIndexFile.exists() || refSeqIndexFile.length() < 1) {
            try {
                Files.delete(inputTempDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return this.runFail(bioPipelineStage, "生成参考索引失败");
        }

        Path refSeqIndexFileLinkPath = null;

        try {
            refSeqIndexFileLinkPath = Files.createSymbolicLink(inputTempDir.resolve(refseq.getName()+".fai"), refSeqIndexFile.toPath());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            return this.runFail(bioPipelineStage, "加载参考基因组索引失败", e, inputTempDir, workDir);
        }

        GetObjectResult bamFileGetResult = this.storageService.getObject(bamPath, bam.toString());
        if (!bamFileGetResult.success()) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return StageRunResult.fail("bam文件加载失败", bioPipelineStage, bamFileGetResult.e());
        }

        GetObjectResult baiFileGetResult = this.storageService.getObject(bamIndexPath, bai.toString());
        if (!baiFileGetResult.success()) {
            return this.runFail(bioPipelineStage, "加载bai文件失败", baiFileGetResult.e(), inputTempDir, workDir);
        }

        // 工具路径与参数
        String bcftools = this.bcftools;
        String samtools = this.samtools;
        int threads = 2;

        // 中间与最终产物
        Path bcfRaw = workDir.resolve("raw.bcf");
        VariantStageOutput variantStageOutput = bioStageUtil.varientOutput(bioPipelineStage, workDir);
        Path vcfGz = Path.of(variantStageOutput.getVcfGz());
        Path vcfTbi = Path.of(variantStageOutput.getVcfTbi());

        // ---------- 1) mpileup: BAM -> BCF ----------
        // -Ou 输出未压缩 BCF 到 stdout（这里我们直接 -o 写文件，避免管道）
        List<String> cmd = new ArrayList<>();
        cmd.add(bcftools);
        cmd.add("mpileup");
        cmd.add("-f");
        cmd.add(refSeqFileLink.toString());
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

        ExecuteResult executeResult = execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "生成bcf.gz失败", executeResult.ex, inputTempDir, workDir);
        }

        List<StageOutputValidationResult> errorOutputValidationResults = validateOutputFiles(bcfRaw);
        if(!errorOutputValidationResults.isEmpty()){
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults), null, inputTempDir, workDir);
        }

        // ---------- 2) call: BCF -> VCF.GZ ----------
        cmd.clear();
        cmd.add(this.bcftools);
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

        executeResult = execute(cmd, workDir);

        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "生成VCF.gz失败", executeResult.ex, inputTempDir, workDir);
        }

        errorOutputValidationResults = validateOutputFiles(vcfGz);
        if(!errorOutputValidationResults.isEmpty()){
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults), null, inputTempDir, workDir);
        }


        // ---------- 3) index: VCF.GZ -> TBI ----------
        cmd = new ArrayList<>();
        cmd.add(bcftools);
        cmd.add("index");
        cmd.add("-t"); // 生成 TBI
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add(vcfTbi.toString());

        executeResult = execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "生成TBI失败", executeResult.ex, inputTempDir, workDir);
        }
        errorOutputValidationResults = validateOutputFiles(vcfTbi);
        if(!errorOutputValidationResults.isEmpty()){
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutputValidationResults), null, inputTempDir, workDir);
        }
        return ok(bioPipelineStage,variantStageOutput, inputTempDir);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_VARIANT_CALL;
    }

}
