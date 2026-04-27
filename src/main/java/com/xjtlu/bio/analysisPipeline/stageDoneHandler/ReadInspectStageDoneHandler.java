package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tomcat.util.bcel.classfile.Constant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

public class ReadInspectStageDoneHandler extends AbstractStageDoneHandler<ReadInspectStageOutput>
        implements StageDoneHandler<ReadInspectStageOutput> {

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_READ_INSPECT;
    }

    @Override
    public void handleStageDone(StageRunResult<ReadInspectStageOutput> stageRunResult) {

        ReadInspectStageOutput readInspectStageOutput = stageRunResult.getStageOutput();

        Long runStageId = stageRunResult.getStageContext().getRunStageId();

        logger.info("ReadInspectStageDoneHandler start, stageId={}", runStageId);

        String r1Url = null;
        String r2Url = null;

        if (readInspectStageOutput.getR1Path() == null) {
            BioPipelineStage runStage = null;
            try {
                runStage = this.pipelineService
                        .queryStageById(stageRunResult.getStageContext().getRunStageId());
            } catch (Exception e) {
                logger.error("Failed to query run stage in ReadInspectStageDoneHandler, stageId={}",
                        runStageId, e);
                handleFail(stageRunResult.getStageContext(), readInspectStageOutput.getParentPath().toString());
            }

            // unable to find run stage. Something must happen; discard update
            if (runStage == null) {
                logger.debug("Cannot find run stage, stageId={}", runStageId);
                handleFail(stageRunResult.getStageContext(), readInspectStageOutput.getParentPath().toString());
            }

            ReadInspectStageInputUrls readInspectStageInputUrls;
            try {
                readInspectStageInputUrls = JsonUtil.toObject(runStage.getInputUrl(),
                        ReadInspectStageInputUrls.class);
            } catch (JsonProcessingException e) {
                logger.error(
                        "Failed to parse ReadInspectStageInputUrls, stageId={}, inputUrl={}",
                        runStageId,
                        runStage.getInputUrl(),
                        e);

                handleFail(
                        stageRunResult.getStageContext(),
                        "failed to parse ReadInspectStageInputUrls, stageId=" + runStageId);
                return;
            }

            r1Url = readInspectStageInputUrls.getRead1Url();
            r2Url = readInspectStageInputUrls.getRead2Url();

        } else {

            String r1ObjectName = createStoreObjectName(stageRunResult.getStageContext(),
                    readInspectStageOutput.getR1Path().getFileName().toString());
            String r2ObjectName = createStoreObjectName(stageRunResult.getStageContext(),
                    readInspectStageOutput.getR2Path().getFileName().toString());

            Map<String, String> uploadMap = Map.of(readInspectStageOutput.getR1Path().toString(), r1ObjectName,
                    readInspectStageOutput.getR2Path().toString(), r2ObjectName);

            boolean uploadRes = this.batchUploadObjectsFromLocal(uploadMap);

            if (!uploadRes) {
                handleFail(stageRunResult.getStageContext(), readInspectStageOutput.getParentPath().toString());
                return;
            }
            r1Url = r1ObjectName;
            r2Url = r2ObjectName;
        }

        ReadInspectStageResult readInspectStageResult = new ReadInspectStageResult();
        readInspectStageResult.setQualityEncoding(readInspectStageOutput.getQualityEncoding());
        readInspectStageResult.setReadLenType(readInspectStageOutput.getReadLenType());

        readInspectStageResult.setR1Url(r1Url);
        readInspectStageResult.setR2Url(r2Url);

        String serializedResult = null;
        try {
            serializedResult = JsonUtil.toJson(readInspectStageResult);
        } catch (JsonProcessingException e) {
            logger.error(
                    "Failed to serialize ReadInspectStageResult, stageId={}, result={}",
                    runStageId,
                    readInspectStageResult,
                    e);

            handleFail(
                    stageRunResult.getStageContext(),
                    "failed to serialize ReadInspectStageResult, stageId=" + runStageId);
            return;
        }

        try {
            updateStageFinish(stageRunResult.getStageContext(), serializedResult);
        } catch (Exception e) {

            logger.error(
                    "Failed to update ReadInspect stage finish status, stageId={}, serializedResult={}",
                    runStageId,
                    serializedResult,
                    e);
        }

        this.deleteStageResultDir(readInspectStageOutput.getParentPath().toString());

    }

    @Override
    protected Pair<Map<String, String>, ReadInspectStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ReadInspectStageOutput> stageRunResult) {

        // unsupported in this subclass
        return null;
    }

}
