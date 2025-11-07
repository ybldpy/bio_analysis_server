package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;

public class VarientExecutor extends AbstractPipelineStageExector{




    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrls = bioPipelineStage.getInputUrl();
        Map<String,String> inputUrlMap = null;
        try {
            inputUrlMap = this.objectMapper.readValue(inputUrls, Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.parseError(bioPipelineStage);
        }

        String bamPath = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY);
        String bamIndexPath = inputUrlMap.get(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY);
        String refSeqAccession = inputUrlMap.get(PipelineService.PIPELINE_REFSEQ_ACCESSION_KEY);

        File refSeq = refSeqService.getRefSeqByAccession(refSeqAccession);
        
        
        if(refSeq == null || !refSeq.exists()){
            return this.runFail(bioPipelineStage, "未找到参考基因组文件");
        }

        Path inputTempDir = Paths.get(String.format("%s/%d", this.stageInputTmpBasePath, bioPipelineStage.getStageId()));
        
        
        try {
            Files.createDirectories(inputTempDir,null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return this.runException(bioPipelineStage, e);
        }

        Path bam = inputTempDir.resolve("aln.bam");
        Path bai = inputTempDir.resolve("aln.bam.bai");
        
        Path refSeqIndex = inputTempDir.resolve("ref.fa");
        
        //先用 samtools 生成参考索引
    }


    private File createRefSeqIndex(Path refSeqIndexPath, File refSeq){
        List<String> cmd = new ArrayList<>();
        
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_VARIANT_CALL;
    }




}
