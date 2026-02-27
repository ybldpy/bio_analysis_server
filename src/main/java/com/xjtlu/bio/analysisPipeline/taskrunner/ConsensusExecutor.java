package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_CONSENSUS;

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
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ConsensusStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ConsensusStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.utils.JsonUtil;


@Component
public class ConsensusExecutor extends AbstractPipelineStageExector<ConsensusStageOutput> implements PipelineStageExecutor<ConsensusStageOutput>{





    @Override
    public StageRunResult<ConsensusStageOutput> _execute(StageExecutionInput stageExecutionInput) {
        // TODO Auto-generated method stub

        BioPipelineStage bioPipelineStage = stageExecutionInput.bioPipelineStage;
        String inputUrls = bioPipelineStage.getInputUrl();
        ConsensusStageInputUrls consensusStageInputUrls = JsonUtil.toObject(inputUrls, ConsensusStageInputUrls.class);
        ConsensusStageParameters consensusStageParameters = JsonUtil.toObject(bioPipelineStage.getParameters(), ConsensusStageParameters.class);

        RefSeqConfig refSeqConfig = consensusStageParameters.getRefSeqConfig();


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

        String vcfGzUrl = consensusStageInputUrls.getVcfGz();
        String vcfTbiUrl = consensusStageInputUrls.getVcfTbi();

        Path vcfGzTmpPath = inputTmpDir.resolve("vcf.gz");
        Path vcfTbiTmpPath = inputTmpDir.resolve("vcf.gz.tbi");

        
       boolean loadRes = loadInput(Map.of(vcfGzUrl, vcfGzTmpPath, vcfTbiUrl, vcfTbiTmpPath));
        if(!loadRes){
            return this.runFail(bioPipelineStage, "load failed");
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
        return PIPELINE_STAGE_CONSENSUS;
    }

}
