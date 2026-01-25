package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.QCStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

import org.springframework.stereotype.Component;

import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.xjtlu.bio.service.PipelineService.*;


@Component
public class QcStageDoneHandler extends AbstractStageDoneHandler<QCStageOutput> implements StageDoneHandler<QCStageOutput>{





    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_QC;
    }

    @Override
    public void handleStageDone(StageRunResult<QCStageOutput> stageRunResult) {

        QCStageOutput qcStageOutput = stageRunResult.getStageOutput();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        String qcR1Path = qcStageOutput.getR1Path();
        String qcR2Path = qcStageOutput.getR2Path();
        boolean hasR2 = qcR2Path != null;

        String qcJsonPath = qcStageOutput.getJsonPath();
        String qcHTMLPath = qcStageOutput.getHtmlPath();

        String r1OutputPath = createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcR1Path));
        String r2OutputPath = hasR2 ? createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcR2Path))
                : null;
        String jsonOutputPath = createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcJsonPath));
        String htmlOutputPath = createStoreObjectName(bioPipelineStage, substractFileNameFromPath(qcHTMLPath));

        Path resultDirPath = Path.of(qcR1Path).getParent();
        Map<String, String> params = new HashMap<>();
        params.put(qcR1Path, r1OutputPath);
        if (hasR2) {
            params.put(qcR2Path, r2OutputPath);
        }
        params.put(qcJsonPath, jsonOutputPath);
        params.put(qcHTMLPath, htmlOutputPath);


        logger.info("{} done. uploading {}", bioPipelineStage, params);
        boolean uploadSuccess = this.batchUploadObjectsFromLocal(params);
        if (!uploadSuccess) {
            this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
            logger.error("{} -> {} result upload failed.", bioPipelineStage, params);
            return;
        }


        Map<String, String> outputPathMap = new HashMap<>();
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R1, r1OutputPath);
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R2, r2OutputPath);
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_JSON, jsonOutputPath);
        outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_HTML, htmlOutputPath);
        try {
            String outputPathMapJson = JsonUtil.toJson(outputPathMap);
            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
            updateStage.setOutputUrl(outputPathMapJson);
            updateStage.setEndTime(new Date());
            updateStage.setVersion(bioPipelineStage.getVersion()+1);
            int curVersion = bioPipelineStage.getVersion();
            bioPipelineStage.setVersion(bioPipelineStage.getVersion()+1);


            int updateRes = this.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(),
                    curVersion);
            if (updateRes != 1) {
                return;
            }

            BioPipelineStageExample nextStageExample = new BioPipelineStageExample();
            nextStageExample.createCriteria().andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
                    .andStageIndexEqualTo(bioPipelineStage.getStageIndex() + 1);

            List<BioPipelineStage> nextStages = pipelineService.getStagesFromExample(nextStageExample);
            if (nextStages == null || nextStages.isEmpty()) {
                return;
            }

            BioPipelineStage nextStage = nextStages.get(0);
            BioPipelineStage updateNextStage = new BioPipelineStage();
            updateNextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            nextStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
            HashMap<String, String> inputMap = new HashMap<>();
            inputMap.put(PIPELINE_STAGE_INPUT_READ1_KEY, r1OutputPath);
            inputMap.put(PIPELINE_STAGE_INPUT_READ2_KEY, r2OutputPath);

            String nextStageInput = JsonUtil.toJson(inputMap);
            updateNextStage.setInputUrl(nextStageInput);
            nextStage.setInputUrl(nextStageInput);
            updateNextStage.setVersion(bioPipelineStage.getVersion()+1);
            int nextStageCurrentVersion = nextStage.getVersion();
            nextStage.setVersion(nextStage.getVersion()+1);

            updateRes = this.updateStageFromVersion(updateNextStage, nextStage.getStageId(),
                    nextStageCurrentVersion);
            if (updateRes != 1) {
                return;
            }

            pipelineService.addStageTask(nextStage);

        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error("{} parsing Json exception", bioPipelineStage, e);
        }

        this.deleteStageResultDir(resultDirPath.toString());
    }
}
