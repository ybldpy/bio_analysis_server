package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;

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

    @Override
    public void handleStageDone(StageRunResult<T> stageRunResult) {
        // TODO Auto-generated method stub

        Pair<Map<String, String>, ? extends StageResult> uploadConfigAndOutputUrlMap = this
                .buildUploadConfigAndOutputUrlMap(stageRunResult);

        Map<String, String> uploadConfig = uploadConfigAndOutputUrlMap.getLeft();
        StageContext stageContext = stageRunResult.getStageContext();

        if (uploadConfig != null && !uploadConfig.isEmpty()) {
            if (!this.batchUploadObjectsFromLocal(uploadConfig)) {
                this.handleFail(stageContext, stageRunResult.getStageOutput().getParentPath().toString());
                return;
            }
        }

        StageResult outputMap = uploadConfigAndOutputUrlMap.getRight();
        String serializedOutputMap = null;
        try {
            serializedOutputMap = JsonUtil.toJson(outputMap);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            this.logger.error("stage id = {} parsing {} json exception", stageContext.getRunStageId(), outputMap, e);
            return;
        }

        deleteStageResultDir(stageRunResult.getStageOutput().getParentPath());

        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(serializedOutputMap);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        Date endDate = new Date();
        updateStage.setEndTime(endDate);
        updateStage.setVersion(stageContext.getVersion() + 1);

        int updateRes = this.pipelineService.updateStageFromVersion(
                new UpdateStageCommand(updateStage, stageContext.getRunStageId(), stageContext.getVersion()));
        boolean updateSuccess = updateRes > 0;
        pipelineService.pipelineStageDone(stageContext.getRunStageId(), updateSuccess);
        return;

    }

    protected abstract Pair<Map<String, String>, ? extends StageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<T> stageRunResult);

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
        if (StringUtils.isNotBlank(deleteDir)) {
            this.deleteStageResultDir(deleteDir);
        }
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setVersion(bioPipelineStage.getVersion() + 1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        pipelineService.updateStageFromVersion(
                new UpdateStageCommand(updateStage, bioPipelineStage.getRunStageId(), bioPipelineStage.getVersion()));
        
        pipelineService.pipelineStageDone(bioPipelineStage.getRunStageId(), false);
    }

    protected void handleUnsuccessUpload(StageContext bioPipelineStage, String deleteDir) {
        handleFail(bioPipelineStage, deleteDir);
    }

    protected int updateStageFinish(BioPipelineStage bioPipelineStage, String outputUrl) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(outputUrl);
        updateStage.setVersion(bioPipelineStage.getVersion() + 1);
        updateStage.setEndTime(new Date());
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        return pipelineService.updateStageFromVersion(
                new UpdateStageCommand(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion()));
    }

    protected int updateStageFromVersion(BioPipelineStage bioPipelineStage, long updateStageId, int currentVersion) {
        return pipelineService
                .updateStageFromVersion(new UpdateStageCommand(bioPipelineStage, updateStageId, currentVersion));
    }

}
