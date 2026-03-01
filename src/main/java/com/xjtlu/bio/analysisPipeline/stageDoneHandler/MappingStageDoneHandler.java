package com.xjtlu.bio.analysisPipeline.stageDoneHandler;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.JsonUtil;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;

@Component
public class MappingStageDoneHandler extends AbstractStageDoneHandler<MappingStageOutput> implements StageDoneHandler<MappingStageOutput>{

    private static final String BAM_NAME = "mapping.bam";
    private static final String BAM_INDEX_NAME = "mapping.bam.bai";

    @Override
    public int getType() {
        return PIPELINE_STAGE_MAPPING;
    }


    // @Override
    // protected Map<String, String> createOutputUrlMap(StageRunResult<MappingStageOutput> stageOutput) {
    //     // TODO Auto-generated method stub
    //     return Map.of(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY, this.createStoreObjectName(stageOutput.getStage(), BAM_NAME), PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY, this.createStoreObjectName(stageOutput.getStage(), BAM_INDEX_NAME));
    // }

    // @Override
    // protected boolean batchUploadObjectsFromLocal(StageRunResult<MappingStageOutput> stageRunResult) {
    //     // TODO Auto-generated method stub
    //     return batchUploadObjectsFromLocal(Map.of(stageRunResult.getStageOutput().getBamPath(), this.createStoreObjectName(stageRunResult.getStage(), BAM_NAME), stageRunResult.getStageOutput().getBamIndexPath(), this.createStoreObjectName(stageRunResult.getStage(), BAM_INDEX_NAME)));
    // }


    @Override
    protected Pair<Map<String, String>, MappingResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<MappingStageOutput> stageRunResult) {

        String bamUrl = this.createStoreObjectName(stageRunResult.getStage(),BAM_NAME);
        String bamIndexUrl = this.createStoreObjectName(stageRunResult.getStage(), BAM_INDEX_NAME);

        return Pair.of(
            Map.of(stageRunResult.getStageOutput().getBamPath(), bamUrl, stageRunResult.getStageOutput().getBamIndexPath(), bamIndexUrl),
            new MappingResult(bamUrl, bamIndexUrl)
        );
    }




    // @Override
    // public void handleStageDone(StageRunResult<MappingStageOutput> stageRunResult) {
    //     MappingStageOutput mappingStageOutput = (MappingStageOutput) stageRunResult.getStageOutput();
    //     BioPipelineStage bioPipelineStage = stageRunResult.getStage();

    //     String bamObjectName = createStoreObjectName(bioPipelineStage,
    //             substractFileNameFromPath(mappingStageOutput.getBamPath()));

    //     String bamIndexObjectName = createStoreObjectName(bioPipelineStage,
    //             substractFileNameFromPath(mappingStageOutput.getBamIndexPath()));

    //     HashMap<String, String> outputStoreMap = new HashMap<>();
    //     outputStoreMap.put(mappingStageOutput.getBamPath().toString(), bamObjectName);
    //     outputStoreMap.put(mappingStageOutput.getBamIndexPath().toString(), bamIndexObjectName);

    //     boolean storeSuccss  = this.batchUploadObjectsFromLocal(outputStoreMap);
    //     Path outputDirPath = Path.of(mappingStageOutput.getBamPath()).getParent();
    //     if (!storeSuccss) {
    //         this.handleUnsuccessUpload(bioPipelineStage, outputDirPath.toString());
    //         return;
    //     }

    //     this.deleteStageResultDir(outputDirPath.toString());

    //     String serializedOutput = null;
    //     HashMap<String,String> outputUrlMap = new HashMap<>();
    //     outputUrlMap.put(PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY, bamObjectName);
    //     outputUrlMap.put(PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY, bamIndexObjectName);
    //     try {
    //         serializedOutput = JsonUtil.toJson(outputUrlMap);
    //     } catch (JsonProcessingException e) {
            
    //         logger.error("stage id = {}. parsing {} to json error", bioPipelineStage.getStageId(), outputDirPath);
    //     }

    //     int updateRes = updateStageFinish(bioPipelineStage, serializedOutput);
    //     if(updateRes!=1){
    //         return;            
    //     }



    //     BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
    //     nextStagesExample.createCriteria()
    //             .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
    //             .andStageIndexGreaterThanOrEqualTo(bioPipelineStage.getStageIndex() + 1);

    //     List<BioPipelineStage> nextStages = pipelineService.getStagesFromExample(nextStagesExample);

    //     if (nextStages == null || nextStages.isEmpty()) {
    //         return;
    //     }

    //     // 这里主要是用来看是否后续阶段。
    //     BioPipelineStage varientStage = nextStages.stream()
    //             .filter(stage -> stage.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

    //     if (varientStage != null) {
    //         BioPipelineStage updateVarientStage = new BioPipelineStage();
    //         HashMap<String, Object> inputMap = new HashMap<>();
    //         inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamObjectName);
    //         inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_INDEX_KEY, bamIndexObjectName);
    //         String serializedInputUrl = null;
    //         try {
    //             serializedInputUrl = JsonUtil.toJson(inputMap);
    //         } catch (JsonProcessingException e) {
    //             // TODO Auto-generated catch block
    //             logger.error("{} parsing json exception", varientStage, e);
    //         }

    //         int curVersion = varientStage.getVersion();

    //         updateVarientStage.setInputUrl(serializedInputUrl);
    //         varientStage.setInputUrl(serializedInputUrl);

    //         updateVarientStage.setParameters(bioPipelineStage.getParameters());
    //         varientStage.setParameters(updateVarientStage.getParameters());
    //         updateVarientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
    //         varientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

    //         updateVarientStage.setVersion(varientStage.getVersion()+1);
    //         varientStage.setVersion(varientStage.getVersion()+1);

    //         updateRes = this.updateStageFromVersion(updateVarientStage, varientStage.getStageId(),
    //                 curVersion);
    //         if (updateRes != 1) {
    //             return;
    //         }
    //         pipelineService.addStageTask(varientStage);
    //         return;
    //     }
    //     // todo: bacterial part. do it later
    // }
}
