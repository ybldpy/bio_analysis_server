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
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.ConsensusStageOutput;


@Component
public class ConsensusExecutor extends AbstractPipelineStageExector<ConsensusStageOutput> implements PipelineStageExecutor<ConsensusStageOutput>{





    @Override
    public StageRunResult<ConsensusStageOutput> _execute(StageExecutionInput stageExecutionInput) {
        // TODO Auto-generated method stub

        BioPipelineStage bioPipelineStage = stageExecutionInput.bioPipelineStage;
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
            return this.runFail(bioPipelineStage, "未找到参考基因文件");
        }
        File refSeqIndexFile = refSeqConfig.isInnerRefSeq()?this.refSeqService.getRefSeqIndex(refSeqConfig.getRefseqId()):this.refSeqService.getRefSeqIndex(refSeqConfig.getRefseqObjectName());
        if (refSeqIndexFile==null || !refSeqIndexFile.exists()) {
            return this.runFail(bioPipelineStage, "未找到参考基因索引文件");
        }


        Path inputTmpDir = stageExecutionInput.inputDir;
        Path resultDir = stageExecutionInput.workDir;

        String vcfGzUrl = inputUrlMap.get(PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ);
        String vcfTbiUrl = inputUrlMap.get(PipelineService.PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI);

        Path vcfGzTmpPath = inputTmpDir.resolve("vcf.gz");
        Path vcfTbiTmpPath = inputTmpDir.resolve("vcf.gz.tbi");

        
        Map<String, GetObjectResult> getResults = loadInput(Map.of(vcfGzUrl, vcfGzTmpPath, vcfTbiUrl, vcfTbiTmpPath));
        String objectFailLoad = findFailedLoadingObject(getResults);
        if(objectFailLoad!=null){
            return this.runFail(bioPipelineStage, "加载文件"+objectFailLoad+"失败", getResults.get(objectFailLoad).e());
        }

        
        ConsensusStageOutput consensusStageOutput = bioStageUtil.consensusOutput(bioPipelineStage, resultDir);
        String consensus = "consensus";
        Path consensusPath = Path.of(consensusStageOutput.getConsensusFa());

        List<String> cmd = new ArrayList<>();
        cmd.addAll(this.analysisPipelineToolsConfig.getBcftools());
        cmd.addAll(List.of(
                consensus,
            "-f",
            refseqFile.getAbsolutePath(),
            "-H",
            String.valueOf(1),
            "-o",
            consensusPath.toString(),
            vcfGzTmpPath.toString()
        )
        );





        boolean runFail = false;
        Exception runFailException = null;
        try {
            int code = this.runSubProcess(cmd, resultDir);
            if(code!=0){runFail = true;}
        } catch (Exception e) {
            runFail = true;
            runFailException = e;
        }

        if (runFail) {
            return this.runFail(bioPipelineStage, "运行consensus tool失败", runFailException, inputTmpDir, resultDir);
        }

        List<StageOutputValidationResult> errOutputValidationResults = validateOutputFiles(consensusPath);
        if(!errOutputValidationResults.isEmpty()){
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errOutputValidationResults));
        }
        
        return StageRunResult.OK(consensusStageOutput, bioPipelineStage);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_CONSENSUS;
    }

}
