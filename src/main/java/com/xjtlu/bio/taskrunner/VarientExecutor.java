package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;


@Component
public class VarientExecutor extends AbstractPipelineStageExector {


    private String bcftools;
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


        

        String bamPath = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY);
        String bamIndexPath = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY);
        String refSeqAccession = inputUrlMap.get(PipelineService.PIPELINE_REFSEQ_ACCESSION_KEY);

        File refSeq = refSeqService.getRefSeqByAccession(refSeqAccession);

        if (refSeq == null || !refSeq.exists()) {
            return this.runFail(bioPipelineStage, "未找到参考基因组文件");
        }

        Path inputTempDir = Paths
                .get(String.format("%s/%d", this.stageInputTmpBasePath, bioPipelineStage.getStageId()));

        try {
            Files.createDirectories(inputTempDir, null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.runException(bioPipelineStage, e);
        }

        Path bam = inputTempDir.resolve("aln.bam");
        Path bai = inputTempDir.resolve("aln.bam.bai");

        Path refSeqIndex = inputTempDir.resolve("refFa.fai");

        // 先用 samtools 生成参考索引
        File refSeqIndexFile = this.refSeqService.getRefSeqIndex(refSeqAccession);
        if (refSeqIndexFile == null || !refSeqIndexFile.exists()) {
            refSeqIndexFile = this.refSeqService.buildRefSeqIndex(refSeqAccession);
        }

        if (refSeqIndexFile == null || !refSeqIndexFile.exists() || refSeqIndexFile.length() < 1) {
            try {
                Files.delete(inputTempDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return this.runFail(bioPipelineStage, "生成参考索引失败");
        }

        GetObjectResult bamFileGetResult = this.storageService.getObject(bamPath, bam.toString());
        if (bamFileGetResult.e() != null) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return this.runException(bioPipelineStage, bamFileGetResult.e());
        }

        GetObjectResult baiFileGetResult = this.storageService.getObject(bamIndexPath, bai.toString());
        if (baiFileGetResult.e() != null) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return this.runException(bioPipelineStage, baiFileGetResult.e());
        }

        // 结果目录
        Path workDir = Paths
                .get(String.format("%s/%d", this.stageResultTmpBasePath, bioPipelineStage.getStageId()));
        try {
            Files.createDirectories(workDir);
        } catch (IOException e) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return this.runException(bioPipelineStage, e);
        }

        // 工具路径与参数
        String bcftools = this.bcftools;
        String samtools = this.samtools;
        int threads = 2;

        // 中间与最终产物
        Path bcfRaw = workDir.resolve("raw.bcf");
        Path vcfGz = workDir.resolve("variants.vcf.gz");
        Path vcfTbi = workDir.resolve("variants.vcf.gz.tbi");
        Path depthTsv = workDir.resolve("depth.tsv");

        // ---------- 1) mpileup: BAM -> BCF ----------
        // -Ou 输出未压缩 BCF 到 stdout（这里我们直接 -o 写文件，避免管道）
        List<String> cmd = new ArrayList<>();
        cmd.add(bcftools);
        cmd.add("mpileup");
        cmd.add("-f");
        cmd.add(refSeq.getAbsolutePath());
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

        try {
            int c1 = this.runSubProcess(cmd, workDir);
            if (c1 != 0||!requireNonEmpty(depthTsv)) {
                this.deleteTmpFiles(List.of(inputTempDir.toFile(), workDir.toFile()));
                return this.runFail(bioPipelineStage, "bcftools mpileup 运行失败，exitCode=" + c1);
            }
        } catch (Exception e) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return this.runException(bioPipelineStage, e);
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

        try {
            int c2 = this.runSubProcess(cmd, workDir);
            if (c2 != 0 || !requireNonEmpty(vcfGz)) {
                this.deleteTmpFiles(List.of(inputTempDir.toFile()));
                return this.runFail(bioPipelineStage, "bcftools call 运行失败，exitCode=" + c2);
            }
        } catch (Exception e) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile(),workDir.toFile()));
            return this.runException(bioPipelineStage, e);
        }

        // ---------- 3) index: VCF.GZ -> TBI ----------
        cmd = new ArrayList<>();
        cmd.add(bcftools);
        cmd.add("index");
        cmd.add("-t"); // 生成 TBI
        cmd.add("--threads");
        cmd.add(String.valueOf(threads));
        cmd.add(vcfGz.toString());

        try {
            int c3 = this.runSubProcess(cmd, workDir);
            if (c3 != 0||!this.requireNonEmpty(vcfTbi)) {
                this.deleteTmpFiles(List.of(inputTempDir.toFile(), workDir.toFile()));
                return this.runFail(bioPipelineStage, "bcftools index 运行失败，exitCode=" + c3);
            }
            
        } catch (Exception e) {
            this.deleteTmpFiles(List.of(inputTempDir.toFile()));
            return this.runException(bioPipelineStage, e);
        }


        // 清理：本地输入缓存与中间 bcf（可选保留以便复现）
        // this.deleteTmpFiles(List.of(inputTempDir.toFile()));
        // try { Files.deleteIfExists(bcfRaw); } catch (IOException ignore) {}

        // ---------- 输出组装 ----------
        Map<String, String> out = new HashMap<>();
        out.put(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ, vcfGz.toString());
        out.put(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI, vcfTbi.toString());
        this.deleteTmpFiles(List.of(inputTempDir.toFile(), bcfRaw.toFile()));

        
        return StageRunResult.OK(new VariantStageOutput(vcfGz.toAbsolutePath().toString(), vcfTbi.toAbsolutePath().toString()), bioPipelineStage);


    }

    private File createRefSeqIndex(Path refSeqIndexPath, File refSeq) {
        List<String> cmd = new ArrayList<>();
        return null;
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_VARIANT_CALL;
    }

}
