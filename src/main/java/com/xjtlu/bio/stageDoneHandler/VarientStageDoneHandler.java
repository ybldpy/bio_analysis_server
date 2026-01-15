package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static com.xjtlu.bio.service.PipelineService.*;

@Component
public class VarientStageDoneHandler extends AbstractStageDoneHandler<VariantStageOutput> implements StageDoneHandler<VariantStageOutput>{


    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_VARIANT_CALL;
    }

    @Override
    public void handleStageDone(StageRunResult<VariantStageOutput> stageRunResult) {
        VariantStageOutput variantStageOutput = (VariantStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String vcfGzObjctName = String.format(
                stageOutputFormat,
                bioPipelineStage.getStageId(),
                bioPipelineStage.getStageName(),
                variantStageOutput.getVcfGz().substring(variantStageOutput.getVcfGz().lastIndexOf("/") + 1));

        String vcfTbiObjectName = String.format(
                stageOutputFormat,
                bioPipelineStage.getStageId(),
                bioPipelineStage.getStageName(),
                variantStageOutput.getVcfTbi().substring(variantStageOutput.getVcfTbi().lastIndexOf("/") + 1));

        boolean uploadSuccess = this.batchUploadObjectsFromLocal(Map.of(
                variantStageOutput.getVcfGz(),
                vcfGzObjctName,
                variantStageOutput.getVcfTbi(),
                vcfTbiObjectName
        ));

        Path resultDirPath = Path.of(variantStageOutput.getVcfGz()).getParent();

        if (!uploadSuccess) {
            this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
            return;
        }
        this.deleteStageResultDir(resultDirPath.toString());

        String outputUrl = String.format(
                "{\"%s\": \"%s\", \"%s\":\"%s\"}",
                PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ,
                vcfGzObjctName,
                PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI,
                vcfTbiObjectName);

        int updateRes = pipelineService.markStageFinish(bioPipelineStage, outputUrl);

        if (updateRes != 1) {
            return;
        }

        BioPipelineStageExample consensusStageExample = new BioPipelineStageExample();
        consensusStageExample.createCriteria()
                .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageTypeEqualTo(PIPELINE_STAGE_CONSENSUS);

        List<BioPipelineStage> consensusStageList = pipelineService.getStagesFromExample(consensusStageExample);

        if (consensusStageList == null || consensusStageList.isEmpty()) {
            return;
        }

        BioPipelineStage consensusStage = consensusStageList.get(0);
        Map<String,String> inputMap = Map.of(PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ, vcfGzObjctName, PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI, vcfTbiObjectName);
        String serializedInputMap = null;
        try {
            serializedInputMap = JsonUtil.toJson(inputMap);
        } catch (JsonProcessingException e) {
            logger.error("{} happens exception when serialzing inputMap", bioPipelineStage, e);
        }

        BioPipelineStage updateConsensusStage = new BioPipelineStage();

        int curVersion = consensusStage.getVersion();
        consensusStage.setInputUrl(serializedInputMap);
        consensusStage.setParameters(bioPipelineStage.getParameters());
        consensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);


        updateConsensusStage.setInputUrl(serializedInputMap);
        updateConsensusStage.setParameters(bioPipelineStage.getParameters());
        updateConsensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

        updateConsensusStage.setVersion(curVersion+1);
        consensusStage.setVersion(curVersion+1);

        int res = this.updateStageFromVersion(updateConsensusStage, consensusStage.getStageId(),
                curVersion);
        if (res == 1) {
            pipelineService.addStageTask(consensusStage);
        }
    }
}
