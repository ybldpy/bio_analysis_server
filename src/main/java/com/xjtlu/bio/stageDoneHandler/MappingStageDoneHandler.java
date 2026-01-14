package com.xjtlu.bio.stageDoneHandler;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.MappingStageOutput;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

import static com.xjtlu.bio.service.PipelineService.*;

@Component
public class MappingStageDoneHandler extends AbstractStageDoneHandler implements StageDoneHandler{


    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_MAPPING;
    }

    @Override
    public void handleStageDone(StageRunResult stageRunResult) {
        MappingStageOutput mappingStageOutput = (MappingStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String bamObjectName = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(mappingStageOutput.getBamPath()));

        String bamIndexObjectName = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(mappingStageOutput.getBamIndexPath()));

        HashMap<String, String> outputStoreMap = new HashMap<>();
        outputStoreMap.put(mappingStageOutput.getBamPath(), bamObjectName);
        outputStoreMap.put(mappingStageOutput.getBamIndexPath(), bamIndexObjectName);

        boolean storeSuccss  = this.batchUploadObjectsFromLocal(outputStoreMap);
        Path outputDirPath = Path.of(mappingStageOutput.getBamPath()).getParent();
        if (!storeSuccss) {

            this.handleUnsuccessUpload(bioPipelineStage, outputDirPath.toString());
            return;
        }

        BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
        nextStagesExample.createCriteria()
                .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageIndexGreaterThanOrEqualTo(bioPipelineStage.getStageIndex() + 1);

        List<BioPipelineStage> nextStages = pipelineService.getStagesFromExample(nextStagesExample);

        if (nextStages == null || nextStages.isEmpty()) {
            return;
        }

        // 这里主要是用来看是否后续阶段。
        BioPipelineStage varientStage = nextStages.stream()
                .filter(stage -> stage.getStageType() == PIPELINE_STAGE_VARIANT_CALL).findFirst().orElse(null);

        if (varientStage != null) {
            BioPipelineStage updateVarientStage = new BioPipelineStage();
            HashMap<String, Object> inputMap = new HashMap<>();
            inputMap.put(PIPELINE_STAGE_VARIENT_CALL_INPUT_BAM_KEY, bamIndexObjectName);
            String serializedInputUrl = null;
            try {
                serializedInputUrl = this.jsonMapper.writeValueAsString(inputMap);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            int curVersion = varientStage.getVersion();

            updateVarientStage.setInputUrl(serializedInputUrl);
            varientStage.setInputUrl(serializedInputUrl);

            updateVarientStage.setParameters(bioPipelineStage.getParameters());
            varientStage.setParameters(updateVarientStage.getParameters());
            updateVarientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            varientStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

            updateVarientStage.setVersion(varientStage.getVersion()+1);
            varientStage.setVersion(varientStage.getVersion()+1);

            int updateRes = this.updateStageFromVersion(updateVarientStage, varientStage.getStageId(),
                    curVersion);
            if (updateRes != 1) {
                return;
            }
            pipelineService.addStageTask(varientStage);
            return;
        }
        // todo: bacterial part. do it later
    }
}
