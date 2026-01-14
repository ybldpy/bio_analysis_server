package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.AssemblyStageOutput;
import org.apache.ibatis.executor.BatchResult;
import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.*;

import static com.xjtlu.bio.service.PipelineService.*;

@Component
public class AssemblyStageDoneHandler extends AbstractStageDoneHandler implements StageDoneHandler{


    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_ASSEMBLY;
    }

    @Override
    public void handleStageDone(StageRunResult stageRunResult) {

        AssemblyStageOutput assemblyStageOutput = (AssemblyStageOutput) stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();

        String contigOutputKey = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(assemblyStageOutput.getContigPath()));

        String scaffoldOuputKey = createStoreObjectName(bioPipelineStage,
                substractFileNameFromPath(assemblyStageOutput.getScaffoldPath()));

        HashMap<String, String> outputMap = new HashMap<>();

        boolean hasScaffold = assemblyStageOutput.getScaffoldPath() != null;

        Path resultDirPath = Path.of(assemblyStageOutput.getContigPath()).getParent();

        outputMap.put(assemblyStageOutput.getContigPath(), contigOutputKey);
        if (hasScaffold) {
            outputMap.put(assemblyStageOutput.getScaffoldPath(), scaffoldOuputKey);
        }
        boolean success = this.batchUploadObjectsFromLocal(outputMap);

        if (!success) {
            this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
            return;
        }
        HashMap<String, String> outputPathMap = new HashMap<>();
        outputPathMap.put(PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY, contigOutputKey);
        outputPathMap.put(PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY, hasScaffold ? scaffoldOuputKey : null);
        String serializedOutputPath = null;
        try {
            serializedOutputPath = jsonMapper.writeValueAsString(outputPathMap);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error("{} serializing exception", outputPathMap,e);
        }

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(serializedOutputPath);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setEndTime(new Date());
        updateStage.setVersion(bioPipelineStage.getVersion()+1);

        int updateRes = this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(),
                bioPipelineStage.getVersion());
        if (updateRes != 1) {
            return;
        }

        BioPipelineStageExample nextStagesExample = new BioPipelineStageExample();
        nextStagesExample.createCriteria().andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                .andStageIndexGreaterThan(bioPipelineStage.getStageIndex());

        List<BioPipelineStage> nextStages = pipelineService.getStagesFromExample(nextStagesExample);
        if (nextStages == null || nextStages.isEmpty()) {
            return;
        }


        List<BioPipelineStage> updateStages = new ArrayList<>(nextStages.size());
        BioPipelineStage nextStage = null;
        for(BioPipelineStage stage: nextStages){
            if(stage.getStatus()!=PIPELINE_STAGE_STATUS_PENDING){
                return;
            }

            updateStage = new BioPipelineStage();
            updateStage.setStageId(stage.getStageId());
            updateStage.setVersion(stage.getVersion());
            boolean isNextStage = stage.getStageIndex() == bioPipelineStage.getStageIndex()+1;
            if(isNextStage){
                nextStage = stage;
                updateStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                nextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
                //这里nextStage是直接作为副本使用的，所以直接设置成新版本
                nextStage.setVersion(stage.getVersion()+1);

                try {
                    Map<String,String> inputMap = bioStageUtil.createInputMapForNextStage(bioPipelineStage, stage);
                    String serializedInput = this.jsonMapper.writeValueAsString(inputMap);
                    updateStage.setInputUrl(serializedInput);
                    nextStage.setInputUrl(serializedInput);
                } catch (JsonProcessingException e) {
                    logger.error("json parsing exception", e);
                    return;
                }
            }

            try {
                Map<String,Object> params = this.jsonMapper.readValue(stage.getParameters(), Map.class);
                params.remove(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG);
                RefSeqConfig refSeqConfig = new RefSeqConfig(contigOutputKey);
                params.put(PIPELINE_STAGE_PARAMETER_REFSEQ_CONFIG, refSeqConfig);
                String serializedParams = this.jsonMapper.writeValueAsString(params);
                updateStage.setParameters(serializedParams);
                if(isNextStage){
                    nextStage.setParameters(serializedParams);
                }
            } catch (JsonProcessingException e) {
                logger.error("json parsing exception", e);
                return;
            }

            updateStages.add(updateStage);
        }

        updateRes = pipelineService.batchUpdateStages(updateStages);

        if (updateRes==1){
            pipelineService.addStageTask(nextStage);
            return;
        }

        // TODO: bacteria part. do it later




    }
}
