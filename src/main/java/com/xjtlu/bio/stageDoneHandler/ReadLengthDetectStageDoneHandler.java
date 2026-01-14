package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.ReadLengthDetectStageOutput;
import io.micrometer.common.util.StringUtils;
import org.apache.ibatis.executor.BatchResult;
import org.springframework.stereotype.Component;

import java.util.*;

import static com.xjtlu.bio.service.PipelineService.*;

@Component
public class ReadLengthDetectStageDoneHandler extends AbstractStageDoneHandler implements StageDoneHandler{


    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_READ_LENGTH_DETECT;
    }

    @Override
    public void handleStageDone(StageRunResult stageRunResult) {
        boolean longRead = ((ReadLengthDetectStageOutput) stageRunResult.getStageOutput()).isLongRead();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setEndTime(new Date());
        updateStage.setOutputUrl(String.valueOf(longRead));
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);

        int curVersion = bioPipelineStage.getVersion();
        updateStage.setVersion(curVersion+1);
        int updateRes = this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(),
                curVersion);

        if (updateRes != 1) {
            return;
        }
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria()
                .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageIndexGreaterThan(bioPipelineStage.getStageIndex());

        List<BioPipelineStage> nextStages = pipelineService.getStagesFromExample(bioPipelineStageExample);
        if (nextStages == null || nextStages.isEmpty()) {
            return;
        }

        List<BioPipelineStage> updateNextStages = new ArrayList<>();
        BioPipelineStage nextRunStage = null;

        for (BioPipelineStage nextStage : nextStages) {
            if(nextStage.getStatus()!=PIPELINE_STAGE_STATUS_PENDING){
                return;
            }
            Map<String, Object> params = null;
            try {
                params = StringUtils.isNotBlank(nextStage.getParameters())
                        ? jsonMapper.readValue(nextStage.getParameters(), Map.class)
                        : null;
            } catch (JsonMappingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return;
            }

            params = params == null ? new HashMap<>() : params;
            params.put(PIPELINE_STAGE_PARAMETERS_LONG_READ_KEY, longRead);
            String serializedParams = null;
            try {
                serializedParams = jsonMapper.writeValueAsString(params);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                logger.error("{} parsing exception exception", bioPipelineStage, e);
                return;
            }

            BioPipelineStage nextUpdateStage = new BioPipelineStage();

            nextUpdateStage.setParameters(serializedParams);
            nextUpdateStage.setVersion(nextStage.getVersion());

            if(bioPipelineStage.getStageIndex()+1 == nextStage.getStageIndex()){
                nextRunStage = nextStage;
                nextUpdateStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                nextRunStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                nextRunStage.setParameters(serializedParams);
                nextRunStage.setVersion(nextStage.getVersion()+1);
            }

            nextUpdateStage.setStageId(nextStage.getStageId());
            updateNextStages.add(nextUpdateStage);
        }

        updateRes = pipelineService.batchUpdateStages(updateNextStages);

        if(updateRes == 1){
            pipelineService.addStageTask(nextRunStage);
        }

    }
}
