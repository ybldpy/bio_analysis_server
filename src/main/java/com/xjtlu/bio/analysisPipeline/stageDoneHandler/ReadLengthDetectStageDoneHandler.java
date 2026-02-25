package com.xjtlu.bio.analysisPipeline.stageDoneHandler;


import com.xjtlu.bio.analysisPipeline.stageResult.ReadLenStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadLengthDetectStageOutput;
import com.xjtlu.bio.service.PipelineService;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.util.*;


@Component
public class ReadLengthDetectStageDoneHandler extends AbstractStageDoneHandler<ReadLengthDetectStageOutput> implements StageDoneHandler<ReadLengthDetectStageOutput>{


    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT;
    }

    @Override
    protected Pair<Map<String, String>, ReadLenStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ReadLengthDetectStageOutput> stageRunResult) {
        

        return Pair.of(
            null,
            new ReadLenStageResult(false);
        );

    }



    

    // @Override
    // public boolean handleStageDone(StageRunResult<ReadLengthDetectStageOutput> stageRunResult) {
    //     boolean longRead = stageRunResult.getStageOutput().isLongRead();
    //     BioPipelineStage bioPipelineStage = stageRunResult.getStage();

    //     BioPipelineStage updateStage = new BioPipelineStage();
    //     updateStage.setEndTime(new Date());
    //     updateStage.setOutputUrl(String.valueOf(longRead));
    //     updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);

    //     int curVersion = bioPipelineStage.getVersion();
    //     updateStage.setVersion(curVersion+1);
    //     int updateRes = this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(),
    //             curVersion);

    //     if (updateRes != 1) {
    //         return false;
    //     }
    //     BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
    //     bioPipelineStageExample.createCriteria()
    //             .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
    //             .andStageIndexGreaterThan(bioPipelineStage.getStageIndex());

    //     List<BioPipelineStage> nextStages = pipelineService.getStagesFromExample(bioPipelineStageExample);
    //     if (nextStages == null || nextStages.isEmpty()) {
    //         return false;
    //     }

    //     List<BioPipelineStage> updateNextStages = new ArrayList<>();
    //     BioPipelineStage nextRunStage = null;

    //     for (BioPipelineStage nextStage : nextStages) {
    //         if(nextStage.getStatus()!=PIPELINE_STAGE_STATUS_PENDING){
    //             return;
    //         }
    //         Map<String, Object> params = null;
    //         try {
    //             params = JsonUtil.toMap(nextStage.getParameters());
    //         } catch (JsonMappingException e) {
    //             // TODO Auto-generated catch block
    //             e.printStackTrace();
    //             return;
    //         } catch (JsonProcessingException e) {
    //             // TODO Auto-generated catch block
    //             e.printStackTrace();
    //             return;
    //         }

    //         params = params == null ? new HashMap<>() : params;
    //         params.put(PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY, longRead);
    //         String serializedParams = null;
    //         try {
    //             serializedParams = JsonUtil.toJson(params);
    //         } catch (JsonProcessingException e) {
    //             // TODO Auto-generated catch block
    //             logger.error("{} parsing exception exception", bioPipelineStage, e);
    //             return;
    //         }

    //         BioPipelineStage nextUpdateStage = new BioPipelineStage();

    //         nextUpdateStage.setParameters(serializedParams);
    //         nextUpdateStage.setVersion(nextStage.getVersion());

    //         if(bioPipelineStage.getStageIndex()+1 == nextStage.getStageIndex()){
    //             nextRunStage = nextStage;
    //             nextUpdateStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
    //             nextRunStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
    //             nextRunStage.setParameters(serializedParams);
    //             nextRunStage.setVersion(nextStage.getVersion()+1);
    //         }

    //         nextUpdateStage.setStageId(nextStage.getStageId());
    //         updateNextStages.add(nextUpdateStage);
    //     }

    //     updateRes = pipelineService.batchUpdateStages(updateNextStages);

    //     if(updateRes == 1){
    //         pipelineService.addStageTask(nextRunStage);
    //     }

    // }
}
