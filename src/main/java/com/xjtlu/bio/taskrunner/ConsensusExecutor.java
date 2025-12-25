package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.ConsensusStageOutput;

public class ConsensusExecutor extends AbstractPipelineStageExector implements PipelineStageExecutor{



    private String bcftools;
    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrls = bioPipelineStage.getInputUrl();
        Map<String,String> inputUrlMap = null;
        Map<String,Object> paramsMap = null;
        
        try {
            inputUrlMap = this.objectMapper.readValue(inputUrls,Map.class);
            paramsMap = this.objectMapper.readValue(bioPipelineStage.getParameters(), Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.parseError(bioPipelineStage);
        }


        RefSeqConfig refSeqConfig = this.getRefSeqConfigFromParams(paramsMap);


        if (refSeqConfig == null) {
            return this.runFail(bioPipelineStage, "未找到参考基因文件");
        }

        File refseqFile = refSeqConfig.isInnerRefSeq()?this.refSeqService.getRefseq(refSeqConfig.getRefseqId()):this.refSeqService.getRefseq(refSeqConfig.getRefseqObjectName());


        if(refseqFile == null){
            
        }
        File refSeqIndexFile = refSeqConfig.isInnerRefSeq()?this.refSeqService.getRefSeqIndex(refSeqConfig.getRefseqId()):this.refSeqService.getRefSeqIndex(refSeqConfig.getRefseqObjectName());
        if (refSeqIndexFile==null || !refSeqIndexFile.exists()) {
            return this.runFail(bioPipelineStage, "未找到参考基因文件");
        }

        if (refSeqIndexFile == null || !refSeqIndexFile.exists()) {
            return this.runFail(bioPipelineStage, "生成参考基因文件索引时错误");
        }

        Path inputTmpDir = Paths.get(String.format("%s/%d", this.stageInputTmpBasePath, bioPipelineStage.getStageId()));
        Path resultDir = Paths.get(String.format("%s/%d", this.stageResultTmpBasePath, bioPipelineStage.getStageId()));
        try {
            Files.createDirectories(inputTmpDir);
            Files.createDirectories(resultDir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.runException(bioPipelineStage, e);
        }

        String vcfGzUrl = inputUrlMap.get(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ);
        String vcfTbiUrl = inputUrlMap.get(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI);

        Path vcfGzTmpPath = inputTmpDir.resolve("vcf.gz");
        Path vcfTbiTmpPath = inputTmpDir.resolve("vcf.tbi");

        
        
        String consensus = "consensus";
        Path consensusPath = resultDir.resolve("consensus.fa");
        List<String> cmd = List.of(
            this.bcftools,
            consensus,
            "-f",
            refSeqFile.getAbsolutePath(),
            "-H",
            String.valueOf(1), 
            "-o",
            consensusPath.toString(),
            vcfGzTmpPath.toString()
        );


        try {
            int code = this.runSubProcess(cmd, resultDir);
            if (code != 0 || !requireNonEmpty(consensusPath)) {
                this.deleteTmpFiles(List.of(inputTmpDir.toFile()));
                return this.runFail(bioPipelineStage, "bcftools consensus 运行失败，exitCode=" + code);
            }else {
                this.deleteTmpFiles(List.of(inputTmpDir.toFile()));
            }
        } catch (Exception e) {
            this.deleteTmpFiles(List.of(inputTmpDir.toFile(), resultDir.toFile()));
            return this.runException(bioPipelineStage, e);
        }

        // 8) 组织输出
        Map<String,String> out = new HashMap<>();
        out.put(PipelineService.PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA, consensusPath.toAbsolutePath().toString());
        


        return StageRunResult.OK(new ConsensusStageOutput(consensusPath.toAbsolutePath().toString()), bioPipelineStage);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_CONSENSUS;
    }

}
