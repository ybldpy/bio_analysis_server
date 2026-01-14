package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.utils.BioStageUtil;
import jakarta.annotation.Resource;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_STATUS_FAIL;

public abstract class AbstractStageDoneHandler implements StageDoneHandler{

    protected static final JsonMapper jsonMapper = new JsonMapper();

    @Resource
    protected StorageService storageService;


    @Resource
    @Lazy
    protected PipelineService pipelineService;

    @Resource
    protected BioStageUtil bioStageUtil;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    protected String createStoreObjectName(BioPipelineStage pipelineStage, String name){
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
                StorageService.PutResult putResult = this.storageService.putObject(objectConfig.getValue(), fileInputStream);
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

    protected void handleUnsuccessUpload(BioPipelineStage bioPipelineStage, String deleteDir) {
        this.deleteStageResultDir(deleteDir);
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setVersion(bioPipelineStage.getVersion()+1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        pipelineService.updateStageFromVersion(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion());
    }

    protected int updateStageFromVersion(BioPipelineStage bioPipelineStage, long updateStageId, int currentVersion){
        return pipelineService.updateStageFromVersion(bioPipelineStage, updateStageId, currentVersion);
    }




}
