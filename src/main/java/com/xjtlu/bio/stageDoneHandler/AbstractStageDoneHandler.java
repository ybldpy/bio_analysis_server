package com.xjtlu.bio.stageDoneHandler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.stage.UpdateStageCommand;
import com.xjtlu.bio.taskrunner.StageRunResult;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.utils.BioStageUtil;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;
import org.apache.commons.io.FileUtils;
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

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_STATUS_FAIL;
import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_STATUS_FINISHED;


public abstract class AbstractStageDoneHandler<T extends StageOutput> implements StageDoneHandler<T>{

    protected static final JsonMapper jsonMapper = new JsonMapper();

    @Resource
    protected StorageService storageService;


    @Resource
    @Lazy
    protected PipelineService pipelineService;

    @Resource
    protected BioStageUtil bioStageUtil;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());


    // protected abstract Map<String,String> createOutputUrlMap(StageRunResult<T> stageOutput);


    

    @Override
    public boolean handleStageDone(StageRunResult<T> stageRunResult) {
        // TODO Auto-generated method stub

        
        Pair<Map<String,String>, Map<String,Object>> uploadConfigAndOutputUrlMap = this.buildUploadConfigAndOutputUrlMap(stageRunResult);

        Map<String,String> uploadConfig = uploadConfigAndOutputUrlMap.getLeft();
        

        if(uploadConfig!=null && !uploadConfig.isEmpty()){
            if(!this.batchUploadObjectsFromLocal(uploadConfig)){
                this.handleFail(stageRunResult.getStage(), stageRunResult.getStageOutput().getParentPath().toString());
                return false;
            }
        }

        Map<String,Object> outputMap = uploadConfigAndOutputUrlMap.getRight();
        String serializedOutputMap = null; 
        try {
            serializedOutputMap = JsonUtil.toJson(outputMap);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            this.logger.error("{} parsing {} json exception", stageRunResult.getStage(), outputMap,e);
            return false;
        }

        deleteStageResultDir(stageRunResult.getStageOutput().getParentPath());

        BioPipelineStage runStage = stageRunResult.getStage();
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(serializedOutputMap);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        Date endDate = new Date();
        updateStage.setEndTime(endDate);
        updateStage.setVersion(runStage.getVersion()+1);

        int updateRes = this.pipelineService.updateStageFromVersion(new UpdateStageCommand(updateStage, runStage.getStageId(), runStage.getVersion()))
        runStage.setOutputUrl(serializedOutputMap);
        runStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        runStage.setEndTime(endDate);
        runStage.setVersion(runStage.getVersion()+1);

        return updateRes > 0;

    }


    protected abstract Pair<Map<String,String>, Map<String,Object>> buildUploadConfigAndOutputUrlMap(StageRunResult<T> stageRunResult);


    private void deleteStageResultDir(Path p){

        try {
            FileUtils.deleteDirectory(p.toFile());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            this.logger.error("delete dir {} exception", p, e);
        }

    }



    

    

    // protected abstract boolean batchUploadObjectsFromLocal(StageRunResult<T> stageRunResult);

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

    protected void handleFail(BioPipelineStage bioPipelineStage, String deleteDir){
        this.deleteStageResultDir(deleteDir);
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setVersion(bioPipelineStage.getVersion()+1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
        pipelineService.updateStageFromVersion(new UpdateStageCommand(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion()));
    }

    protected void handleUnsuccessUpload(BioPipelineStage bioPipelineStage, String deleteDir) {
        handleFail(bioPipelineStage, deleteDir);
    }

    protected int updateStageFinish(BioPipelineStage bioPipelineStage, String outputUrl){
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setOutputUrl(outputUrl);
        updateStage.setVersion(bioPipelineStage.getVersion()+1);
        updateStage.setEndTime(new Date());
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        return pipelineService.updateStageFromVersion(new UpdateStageCommand(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion()));
    }


    protected int updateStageFromVersion(BioPipelineStage bioPipelineStage, long updateStageId, int currentVersion){
        return pipelineService.updateStageFromVersion(new UpdateStageCommand(bioPipelineStage, updateStageId, currentVersion));
    }




}
