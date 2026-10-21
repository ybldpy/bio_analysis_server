package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.exceptions.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.dao.DataAccessException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;

public abstract class AbstractStageDoneHandler<T extends StageOutput> implements StageDoneHandler<T> {

    protected static final JsonMapper jsonMapper = new JsonMapper();

    @Resource
    protected StorageService storageService;

    @Resource
    @Lazy
    protected PipelineService pipelineService;

    @Resource
    protected BioStageUtil bioStageUtil;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    // protected abstract Map<String,String> createOutputUrlMap(StageRunResult<T>
    // stageOutput);

    private static int SERIALIZED_TYPE_URLS = 0;
    private static int SERIALIZED_TYPE_VALUE = 1;

    protected int serializedOutputType() {
        return -1;
    };

    /** Keep subclass completion logic inside the same non-database exception boundary. */
    @Override
    public final void handleStageDone(StageRunResult<T> stageRunResult) {
        StageContext stageContext = stageRunResult.getStageContext();
        try {
            doHandleStageDone(stageRunResult);
        } catch (Exception e) {
            if (e instanceof StagePersistenceException || isDatabaseException(e)) {
                logger.error("Database-related stage completion failure, stageId={}; not changing stage to FAIL",
                        stageContext.getRunStageId(), e);
                if (e instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                throw new IllegalStateException("Database-related stage completion failure", e);
            }
            logger.error("Stage completion failed, stageId={}; marking stage FAIL",
                    stageContext.getRunStageId(), e);
            // Output preparation itself may have failed, so do not depend on stageOutput here.
            handleFail(stageContext, stageRunResult.getWorkDir() == null
                    ? null : stageRunResult.getWorkDir().toString(),
                    "Stage completion failed: " + e.getClass().getSimpleName()
                            + (e.getMessage() == null ? "" : ": " + e.getMessage()));
        }
    }

    protected void doHandleStageDone(StageRunResult<T> stageRunResult) throws Exception {

        Pair<Map<String, String>, ? extends StageResult> uploadConfigAndOutputUrlMap = this
                .buildUploadConfigAndOutputUrlMap(stageRunResult);

        Map<String, String> uploadConfig = uploadConfigAndOutputUrlMap.getLeft();
        StageContext stageContext = stageRunResult.getStageContext();

        if (uploadConfig != null && !uploadConfig.isEmpty()) {
            if (!this.batchUploadObjectsFromLocal(uploadConfig)) {
                this.handleFail(stageContext, stageRunResult.getWorkDir() == null
                        ? null : stageRunResult.getWorkDir().toString(), "Failed to upload stage outputs");
                return;
            }
        }

        StageResult outputMap = uploadConfigAndOutputUrlMap.getRight();
        String serializedOutputMap = JsonUtil.toJson(outputMap);

        BioPipelineStage updateStage = new BioPipelineStage();
        setSerializedOutput(updateStage, serializedOutputMap);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        Date endDate = new Date();
        updateStage.setEndTime(endDate);
        updateStage.setVersion(stageContext.getVersion() + 1);

        int updateRes = updateStageFromVersion(updateStage, stageContext.getRunStageId(), stageContext.getVersion());
        boolean updateSuccess = updateRes > 0;
        if (updateSuccess) {
            cleanupAfterCompletion(stageRunResult.getWorkDir() == null
                    ? null : stageRunResult.getWorkDir().toString());
        }
        notifyStageDone(stageContext, updateSuccess);
        return;

    }

    protected abstract Pair<Map<String, String>, ? extends StageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<T> stageRunResult);

    protected void setSerializedOutput(BioPipelineStage updateStage, String serializedOutput) {
        updateStage.setOutputUrl(serializedOutput);
    }

    private void deleteStageResultDir(Path p) {

        try {
            FileUtils.deleteDirectory(p.toFile());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            this.logger.error("delete dir {} exception", p, e);
        }

    }

    // protected abstract boolean batchUploadObjectsFromLocal(StageRunResult<T>
    // stageRunResult);

    protected String createStoreObjectName(StageContext pipelineStage, String name) {
        return bioStageUtil.createStoreObjectName(pipelineStage, name);
    }

    protected String substractFileNameFromPath(String path) {
        return Path.of(path).getFileName().toString();
    }

    protected boolean batchUploadObjectsFromLocal(Map<String, String> params) {
        for (Map.Entry<String, String> objectConfig : params.entrySet()) {
            FileInputStream fileInputStream = null;
            try {
                fileInputStream = new FileInputStream(objectConfig.getKey());
                StorageService.PutResult putResult = this.storageService.putObject(objectConfig.getValue(),
                        fileInputStream);
                if (!putResult.success()) {
                    return false;
                }
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            } finally {
                if (fileInputStream != null) {
                    try {
                        fileInputStream.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

        }
        return true;
    }

    protected boolean deleteStageResultDir(String deleteDir) {
        try {
            FileUtils.deleteDirectory(new File(deleteDir));
            return true;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
    }

    protected void handleFail(StageContext bioPipelineStage, String deleteDir) {
        handleFail(bioPipelineStage, deleteDir, "Stage failed without a failure message");
    }

    protected void handleFail(StageContext bioPipelineStage, String deleteDir, String failMessage) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setVersion(bioPipelineStage.getVersion() + 1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        updateStage.setEndTime(new Date());
        int updateRes = updateStageFromVersion(updateStage, bioPipelineStage.getRunStageId(), bioPipelineStage.getVersion());
        if (updateRes > 0) {
            // A stale callback must not append a failure for a newer execution.
            // Log persistence must not undo FAIL or interrupt cleanup/notification.
            try {
                pipelineService.recordStageFailure(bioPipelineStage, failMessage);
            } catch (Exception e) {
                logger.error("Failed to persist failure log, pipelineId={}, stageId={}, version={}, message={}",
                        bioPipelineStage.getPipelineId(), bioPipelineStage.getRunStageId(),
                        bioPipelineStage.getVersion(), failMessage, e);
            }
            cleanupAfterCompletion(deleteDir);
        }
        notifyStageDone(bioPipelineStage, false);
    }

    protected void handleUnsuccessUpload(StageContext bioPipelineStage, String deleteDir) {
        handleFail(bioPipelineStage, deleteDir, "Failed to upload stage outputs");
    }

    protected int updateStageFinish(StageContext bioPipelineStage, String outputUrl) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(outputUrl);
        updateStage.setVersion(bioPipelineStage.getVersion() + 1);
        updateStage.setEndTime(new Date());
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        return updateStageFromVersion(updateStage, bioPipelineStage.getRunStageId(), bioPipelineStage.getVersion());
    }

    protected int updateStageFromVersion(BioPipelineStage bioPipelineStage, long updateStageId, int currentVersion) {
        try {
            int updateRes = pipelineService
                    .updateStageFromVersion(new UpdateStageCommand(bioPipelineStage, updateStageId, currentVersion));
            if (updateRes <= 0) {
                logger.warn("Stage completion update not committed, stageId={}, version={}, result={}",
                        updateStageId, currentVersion, updateRes);
            }
            return updateRes;
        } catch (RuntimeException e) {
            // Do not retry a failed database write as a second FAIL update.
            throw new StagePersistenceException(updateStageId, e);
        }
    }

    protected void cleanupAfterCompletion(String deleteDir) {
        if (StringUtils.isBlank(deleteDir)) {
            return;
        }
        try {
            deleteStageResultDir(deleteDir);
        } catch (Exception e) {
            logger.error("Stage result cleanup failed after status update, dir={}", deleteDir, e);
        }
    }

    protected void notifyStageDone(StageContext stageContext, boolean success) {
        try {
            pipelineService.pipelineStageDone(stageContext.getRunStageId(), success);
        } catch (Exception e) {
            // The state update has already been attempted; notification must not roll it back.
            logger.error("Stage completion notification failed, stageId={}, success={}",
                    stageContext.getRunStageId(), success, e);
        }
    }

    private static boolean isDatabaseException(Throwable exception) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        for (Throwable cause = exception; cause != null && visited.add(cause); cause = cause.getCause()) {
            if (cause instanceof DataAccessException || cause instanceof SQLException
                    || cause instanceof PersistenceException) {
                return true;
            }
        }
        return false;
    }

    private static final class StagePersistenceException extends RuntimeException {
        private StagePersistenceException(long stageId, RuntimeException cause) {
            super("Failed to persist stage completion, stageId=" + stageId, cause);
        }
    }
}
