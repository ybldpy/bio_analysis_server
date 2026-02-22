package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ConsensusStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.JsonUtil;

import org.apache.commons.lang3.tuple.Pair;
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


    private static final String CONSENSUS_FA_NAME = "consensus.fa";
    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_CONSENSUS;
    }


    @Override
    protected Pair<Map<String, String>, Map<String, Object>> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ConsensusStageOutput> stageRunResult) {
        String consensusUrl = this.createStoreObjectName(stageRunResult.getStage(), CONSENSUS_FA_NAME);
        return Pair.of(
            Map.of(stageRunResult.getStageOutput().getConsensusFa(), consensusUrl),
            Map.of(PipelineService.PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA, consensusUrl)
        );
    }


    // @Override
    // protected Map<String, String> createOutputUrlMap(StageRunResult<ConsensusStageOutput> stageOutput) {
    //     // TODO Auto-generated method stub
    //     return Map.of(PipelineService.PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA, this.createStoreObjectName(stageOutput.getStage(), CONSENSUS_FA_NAME));
    // }

    // @Override
    // protected boolean batchUploadObjectsFromLocal(StageRunResult<ConsensusStageOutput> stageRunResult) {
    //     // TODO Auto-generated method stub
    //     return this.batchUploadObjectsFromLocal(Map.of(stageRunResult.getStageOutput().getConsensusFa(), this.createStoreObjectName(stageRunResult.getStage(), CONSENSUS_FA_NAME)));
    // }

    // @Override
    // public void handleStageDone(StageRunResult<ConsensusStageOutput> stageRunResult) {

    //     BioPipelineStage bioPipelineStage = stageRunResult.getStage();
    //     ConsensusStageOutput consensusStageOutput = stageRunResult.getStageOutput();

    //     String consesusOutputObjName = createStoreObjectName(bioPipelineStage, CONSENSUS_FA_NAME);

    //     Path outputParentDir = Path.of(consensusStageOutput.getConsensusFa()).getParent();

    //     HashMap<String,String> outputMap = new HashMap<>();
    //     outputMap.put(PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA, consesusOutputObjName);
    //     String serializedOutputMap = null;

    //     try {
    //         serializedOutputMap = JsonUtil.toJson(outputMap);
    //     }catch (JsonProcessingException e) {
    //         try {
    //             Files.delete(outputParentDir);
    //         } catch (IOException ex) {
    //             logger.error("delete dir exception", e);
    //         }
    //         logger.error("{} parsing {} exception", bioPipelineStage, outputMap, e);
    //     }

    //     boolean uploadRes = this.batchUploadObjectsFromLocal(Map.of(consensusStageOutput.getConsensusFa(), consesusOutputObjName));
    //     this.deleteStageResultDir(outputParentDir.toString());
    //     if (!uploadRes) {
    //         this.handleFail(bioPipelineStage, outputParentDir.toString());
    //         return;
    //     }

    //     BioPipelineStage updateStage = new BioPipelineStage();
    //     updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
    //     updateStage.setEndTime(new Date());
    //     updateStage.setOutputUrl(serializedOutputMap);
    //     updateStage.setVersion(bioPipelineStage.getVersion()+1);
    //     this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion());
        

    // }
}
