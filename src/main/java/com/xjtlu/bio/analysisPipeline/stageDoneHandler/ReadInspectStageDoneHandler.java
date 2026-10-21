package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;


@Component
public class ReadInspectStageDoneHandler extends AbstractStageDoneHandler<ReadInspectStageOutput>
        implements StageDoneHandler<ReadInspectStageOutput> {

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
    }

    @Override
    protected void doHandleStageDone(StageRunResult<ReadInspectStageOutput> stageRunResult)
            throws JsonProcessingException {

        ReadInspectStageOutput readInspectStageOutput = stageRunResult.getStageOutput();

        Long runStageId = stageRunResult.getStageContext().getRunStageId();

        logger.info("ReadInspectStageDoneHandler start, stageId={}", runStageId);

        String r1Url = null;
        String r2Url = null;
        Path r1Path = readInspectStageOutput.getR1Path();
        Path r2Path = readInspectStageOutput.getR2Path();

        // Each slot may independently retain its original object or publish a
        // materialized file. A missing R2 path can also mean single-end input.
        if (r1Path == null || r2Path == null) {
            BioPipelineStage runStage = this.pipelineService.queryStageById(runStageId);
            if (runStage == null) {
                throw new IllegalStateException("Cannot find run stage, stageId=" + runStageId);
            }

            ReadInspectStageInputUrls readInspectStageInputUrls = JsonUtil.toObject(
                    runStage.getInputUrl(), ReadInspectStageInputUrls.class);

            r1Url = readInspectStageInputUrls.getRead1Url();
            r2Url = readInspectStageInputUrls.getRead2Url();
        }

        Map<String, String> uploadMap = new LinkedHashMap<>();
        if (r1Path != null) {
            r1Url = createStoreObjectName(stageRunResult.getStageContext(), r1Path.getFileName().toString());
            uploadMap.put(r1Path.toString(), r1Url);
        }
        if (r2Path != null) {
            r2Url = createStoreObjectName(stageRunResult.getStageContext(), r2Path.getFileName().toString());
            uploadMap.put(r2Path.toString(), r2Url);
        }

        if (!uploadMap.isEmpty()) {
            boolean uploadRes = this.batchUploadObjectsFromLocal(uploadMap);

            if (!uploadRes) {
                handleFail(stageRunResult.getStageContext(), stageRunResult.getWorkDir() == null
                        ? null : stageRunResult.getWorkDir().toString(), "Failed to upload ReadInspect outputs");
                return;
            }
        }

        ReadInspectStageResult readInspectStageResult = new ReadInspectStageResult();
        readInspectStageResult.setReadMeta(readInspectStageOutput.getReadMeta());

        readInspectStageResult.setR1Url(r1Url);
        readInspectStageResult.setR2Url(r2Url);

        String serializedResult = JsonUtil.toJson(readInspectStageResult);
        int updateRes = updateStageFinish(stageRunResult.getStageContext(), serializedResult);
        if (updateRes > 0) {
            cleanupAfterCompletion(stageRunResult.getWorkDir() == null
                    ? null : stageRunResult.getWorkDir().toString());
        }
        notifyStageDone(stageRunResult.getStageContext(), updateRes > 0);

    }

    @Override
    protected Pair<Map<String, String>, ReadInspectStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ReadInspectStageOutput> stageRunResult) {

        // unsupported in this subclass
        return null;
    }

}
