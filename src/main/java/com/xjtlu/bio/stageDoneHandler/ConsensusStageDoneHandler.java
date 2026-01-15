package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.xjtlu.bio.service.PipelineService.*;

@Component
public class ConsensusStageDoneHandler extends AbstractStageDoneHandler<ConsensusStageOutput> implements StageDoneHandler<ConsensusStageOutput>{


    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_CONSENSUS;
    }

    @Override
    public void handleStageDone(StageRunResult<ConsensusStageOutput> stageRunResult) {

        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        ConsensusStageOutput consensusStageOutput = stageRunResult.getStageOutput();

        String consesusOutputObjName = createStoreObjectName(bioPipelineStage, consensusStageOutput.getConsensusFa());

        Path outputParentDir = Path.of(consensusStageOutput.getConsensusFa()).getParent();

        HashMap<String,String> outputMap = new HashMap<>();
        outputMap.put(PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA, consesusOutputObjName);
        String serializedOutputMap = null;

        try {
            serializedOutputMap = JsonUtil.toJson(outputMap);
        }catch (JsonProcessingException e) {
            try {
                Files.delete(outputParentDir);
            } catch (IOException ex) {
                logger.error("delete dir exception", e);
            }
            logger.error("{} parsing {} exception", bioPipelineStage, outputMap, e);
        }

        boolean uploadRes = this.batchUploadObjectsFromLocal(Map.of(consensusStageOutput.getConsensusFa(), consesusOutputObjName));
        if (!uploadRes) {
            try {
                Files.delete(outputParentDir);
            } catch (IOException e) {
                logger.error("delete file exception", e);
            }

            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            updateStage.setVersion(bioPipelineStage.getVersion()+1);
            this.updateStageFromVersion(updateStage, bioPipelineStage.getPipelineId(), bioPipelineStage.getVersion());
            return;
        }

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setEndTime(new Date());
        updateStage.setOutputUrl(serializedOutputMap);
        updateStage.setVersion(bioPipelineStage.getVersion()+1);
        this.updateStageFromVersion(updateStage, bioPipelineStage.getPipelineId(), bioPipelineStage.getVersion());

        try {
            Files.delete(outputParentDir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error("delete file exception", e);
        }

    }
}
