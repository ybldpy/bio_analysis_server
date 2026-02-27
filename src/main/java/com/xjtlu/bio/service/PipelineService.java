package com.xjtlu.bio.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.json.JsonMapper;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.executor.BatchResult;
import org.mybatis.spring.SqlSessionTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.analysisPipeline.StageOrchestrator;
import com.xjtlu.bio.analysisPipeline.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.StageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.RefSeqConfig;
import com.xjtlu.bio.analysisPipeline.taskrunner.PipelineStageTaskDispatcher;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.*;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.requestParameters.CreateSampleRequest.PipelineStageParameters;
import com.xjtlu.bio.service.StorageService.PutResult;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.BioStageUtil;
import com.xjtlu.bio.utils.JsonUtil;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;


import io.micrometer.common.util.StringUtils;
import jakarta.annotation.Resource;

@Service
public class PipelineService {

    private static final Logger logger = LoggerFactory.getLogger(PipelineService.class);

    @Resource
    private BioAnalysisPipelineMapper analysisPipelineMapper;

    @Resource
    private BioPipelineStageMapper bioPipelineStageMapper;
    @Resource
    private BioAnalysisStageMapperExtension bioAnalysisStageMapperExtension;

    @Resource
    private BioSampleMapper bioSampleMapper;

    @Resource
    private BioAnalysisPipelineMapper bioAnalysisPipelineMapper;

    @Resource
    private StorageService storageService;

    @Resource
    @Lazy
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    @Resource
    private TransactionTemplate rcTransactionTemplate;

    @Resource
    private BioStageUtil bioStageUtil;

    @Resource
    private SqlSessionTemplate batchSqlSessionTemplate;

    @Resource
    private Map<Integer, StageDoneHandler> stageDoneHandlerMap;

    @Resource
    private StageOrchestrator stageOrchestrator;

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;

    //for genetic
    private static final int OK = 0;
    private static final int INTERNAL_FAIL = -1;


    //for genetic update
    private static final int NO_EFFECTIVE_UPDATE = 100;


    //for scedule purpose
    private static final int SCHEDULE_UPSTREAM_NOT_READY = 300;
    

    private boolean isLegalPipelineType(int pipelineType) {
        return pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID
                || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }

    public int startStageExecute(BioPipelineStage pipelineStage) {
        BioPipelineStage updateStage = new BioPipelineStage();
        int currentVersion = pipelineStage.getVersion();
        updateStage.setVersion(currentVersion + 1);
        pipelineStage.setVersion(currentVersion + 1);
        updateStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        pipelineStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        return this.updateStageFromVersion(
                new UpdateStageCommand(updateStage, pipelineStage.getStageId(), currentVersion));
    }

    public int updateStageFromVersion(UpdateStageCommand updateStageCommand) {
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andStageIdEqualTo(updateStageCommand.getStageId())
                .andVersionEqualTo(updateStageCommand.getCurrentVersion());
        BioPipelineStage updateStage = updateStageCommand.getUpdateStage();
        updateStage.setVersion(updateStageCommand.getCurrentVersion() + 1);
        try {
            return this.bioPipelineStageMapper.updateByExampleSelective(updateStage, bioPipelineStageExample);
        } catch (Exception e) {
            logger.error("update stage id {} to {} from version {} exception: ", updateStageCommand.getStageId(),
                    updateStage, updateStageCommand.getCurrentVersion(), e);
            return INTERNAL_FAIL;
        }
    }

    private int mapSampleTypeToPipelineType(int sampleType) {
        if (sampleType == SampleService.SAMPLE_TYPE_VIRUS) {
            return PIPELINE_VIRUS;
        }
        if (sampleType == SampleService.SAMPLE_TYPE_BACTERIA) {
            return PIPELINE_VIRUS_BACKTERIA;
        }
        return PIPELINE_VIRUS_COVID;
    }

    // transaction required
    private int batchInsertStages(List<BioPipelineStage> insertStages) {
        try {
            BioPipelineStageMapper stageMapper = batchSqlSessionTemplate.getMapper(BioPipelineStageMapper.class);

            for (BioPipelineStage bioPipelineStage : insertStages) {
                stageMapper.insertSelective(bioPipelineStage);
            }

            List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();
            int updateSuccessCount = 0;
            for (BatchResult batchResult : batchResults) {
                for (int i : batchResult.getUpdateCounts()) {
                    updateSuccessCount += i;
                }
            }
            return updateSuccessCount;
        } catch (Exception e) {
            logger.error("insert expcetion", e);
            return -1;
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Long> createPipeline(BioSample bioSample,
            PipelineStageParameters pipelineStageParams) {

        BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
        bioAnalysisPipeline.setPipelineType(mapSampleTypeToPipelineType(bioSample.getSampleType()));
        bioAnalysisPipeline.setSampleId(bioSample.getSid());
        int insertRes = this.bioAnalysisPipelineMapper.insert(bioAnalysisPipeline);
        if (insertRes < 1) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        List<BioPipelineStage> stages = this.buildPipelineStages(bioSample, bioAnalysisPipeline, pipelineStageParams);
        if (stages == null || stages.isEmpty()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        for (BioPipelineStage stage : stages) {
            stage.setVersion(0);
        }

        insertRes = this.bioAnalysisStageMapperExtension.batchInsert(stages);
        if (insertRes != stages.size()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        return new Result<Long>(Result.SUCCESS, bioAnalysisPipeline.getPipelineId(), null);
    }

    private List<BioPipelineStage> buildPipelineStages(BioSample bioSample, BioAnalysisPipeline bioAnalysisPipeline,
            PipelineStageParameters pipelineParams) {

        if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS
                || bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {
            try {
                List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(
                        bioAnalysisPipeline.getPipelineId(), bioAnalysisPipeline.getPipelineType(), bioSample,
                        pipelineParams);
                return stages;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                return null;
            }
        } else {
            return null;
        }
    }

    // TODO: this is an quick method for just test. Never Use it in production env
    // or treat it as normal service method
    public Result<Boolean> restartStage(long stageId) {

        List<BioPipelineStage> allStages = this.bioAnalysisStageMapperExtension
                .selectAllPipelineStagesByStageId(stageId);
        if (allStages == null || allStages.isEmpty()) {
            return new Result<>(Result.BUSINESS_FAIL, false, "未能启动");
        }

        BioPipelineStage startStage = null;
        BioPipelineStage lastStage = null;
        for (BioPipelineStage stage : allStages) {
            if (stage.getStageId() == stageId) {
                startStage = stage;
                break;
            }
        }

        if (this.pipelineStageTaskDispatcher.isStageIn(stageId)) {
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }

        if (startStage.getStatus() == PIPELINE_STAGE_STATUS_QUEUING) {
            this.addStageTask(startStage);
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }

        int curStatus = startStage.getStatus();
        if (curStatus != PIPELINE_STAGE_STATUS_PENDING && curStatus != PIPELINE_STAGE_STATUS_FAIL) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "不能启动状态为非等待的分析阶段");
        }



        int res = scheduleStage(startStage, allStages);

        if(res == INTERNAL_FAIL || res == NO_EFFECTIVE_UPDATE){
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "内部错误");
        }

        if(res == SCHEDULE_UPSTREAM_NOT_READY){
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "上游阶段未完成, 无法启动此阶段");
        }

        return new Result<Boolean>(Result.SUCCESS, true, null);

        

    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Boolean> pipelineStart(long sampleId) {

        List<BioPipelineStage> stages = this.bioAnalysisStageMapperExtension.selectStagesBySampleId(sampleId);
        if (stages == null || stages.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线");
        }
        BioPipelineStage firstStage = null;
        for (BioPipelineStage stage : stages) {
            if (stage.getStageIndex() == 0) {
                firstStage = stage;
                break;
            }
        }

        if (firstStage == null) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未能找到初始任务");
        }
        if (firstStage.getStatus() != PIPELINE_STAGE_STATUS_PENDING) {
            return new Result<Boolean>(Result.SUCCESS, null, null);
        }

        int res = scheduleStage(firstStage, stages);

        if(res == OK){
            return new Result<Boolean>(Result.SUCCESS, true, null);
        }

        
        return new Result<Boolean>(Result.INTERNAL_FAIL, false, "内部错误");
        

        
    }

    @Async
    public void pipelineStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        int stageType = bioPipelineStage.getStageType();
        StageDoneHandler stageDoneHandler = stageDoneHandlerMap.get(stageType);

        if (!stageRunResult.isSuccess()) {
            BioPipelineStage updateStage = new BioPipelineStage();
            updateStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            int res = this.updateStageFromVersion(
                    new UpdateStageCommand(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion()));
            logger.info("{} execute failed {}", stageRunResult.getStage(), stageRunResult.getErrorLog());
            return;
        }

        boolean handleRes = stageDoneHandler.handleStageDone(stageRunResult);
        if (!handleRes) {
            return;
        }

        this.scheduleDownstreamStages(bioPipelineStage.getStageId());

    }

    private int scheduleStage(BioPipelineStage stage, List<BioPipelineStage> pipelineAllStages) {

        OrchestratePlan plan = null;

        try {
            plan = stageOrchestrator.makePlan(pipelineAllStages, stage.getStageId());
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error(
                    "Failed to build plan for stage. StageId={}, reason={}",
                    stage.getStageId(),
                    e.getMessage(),
                    e);

            return INTERNAL_FAIL;
        } catch (MissingUpstreamException e) {
            logger.warn(
                    "Downstream orchestration skipped. Upstream not ready. StageId={}, desc={}",
                    stage.getStageId(),
                    e.getDesc());
            return SCHEDULE_UPSTREAM_NOT_READY;
        }

        int res = this.batchUpdateStages(plan.getUpdateStageCommands());
        if (res > 0) {
            for (BioPipelineStage runStage : plan.getRunStages()) {
                this.pipelineStageTaskDispatcher.addTask(runStage);
            }
            return OK;
        }

        return NO_EFFECTIVE_UPDATE;

    }

    private int scheduleDownstreamStages(long finishedStageId) {

        List<BioPipelineStage> allStages = this.bioAnalysisStageMapperExtension
                .selectAllPipelineStagesByStageId(finishedStageId);

        OrchestratePlan plan = null;
        try {

            plan = stageOrchestrator.makeDownstreamPlan(finishedStageId, allStages);

        } catch (JsonProcessingException
                | InvocationTargetException
                | IllegalAccessException
                | NoSuchMethodException e) {

            logger.error(
                    "Failed to build downstream plan. finishedStageId={}, reason={}",
                    finishedStageId,
                    e.getMessage(),
                    e);

            return INTERNAL_FAIL;

        } catch (MissingUpstreamException e) {

            logger.warn(
                    "Downstream orchestration skipped. Upstream not ready. finishedStageId={}, desc={}",
                    finishedStageId,
                    e.getDesc());
            return SCHEDULE_UPSTREAM_NOT_READY;
        }

        if (plan.isNoNextStage()) {
            return 0;
        }

        List<UpdateStageCommand> updateStageCommands = plan.getUpdateStageCommands();
        List<BioPipelineStage> pushToWaittingStages = plan.getRunStages();

        int res = this.batchUpdateStages(updateStageCommands);

        if (res == OK) {
            for (BioPipelineStage stage : pushToWaittingStages) {
                this.pipelineStageTaskDispatcher.addTask(stage);
            }
            return OK;
        }

        return NO_EFFECTIVE_UPDATE;

    }

    public int markStageFinish(BioPipelineStage bioPipelineStage, String outputUrl) {
        BioPipelineStage updateStage = new BioPipelineStage();
        updateStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        updateStage.setOutputUrl(outputUrl);
        updateStage.setEndTime(new Date());
        updateStage.setVersion(bioPipelineStage.getVersion() + 1);
        return this.updateStageFromVersion(
                new UpdateStageCommand(updateStage, bioPipelineStage.getStageId(), bioPipelineStage.getVersion()));
    }

    public List<BioPipelineStage> getStagesFromExample(BioPipelineStageExample bioPipelineStageExample) {

        List<BioPipelineStage> stages = null;
        try {
            stages = bioPipelineStageMapper.selectByExampleWithBLOBs(bioPipelineStageExample);
        } catch (Exception e) {
            logger.error("", e);
        }
        return stages;
    }

    // 如果更新成功的数量!=list.size()回滚
    // 0: fail. 1: success
    // remember to remain current version in stage here
    public int batchUpdateStages(List<UpdateStageCommand> updateStageCommands) {

        int res = 0;

        try {
            res = rcTransactionTemplate.execute((status) -> {
                BioPipelineStageMapper batchUpdateMapper = batchSqlSessionTemplate
                        .getMapper(BioPipelineStageMapper.class);
                for (UpdateStageCommand updateStageCommand : updateStageCommands) {
                    BioPipelineStageExample conditionExample = new BioPipelineStageExample();
                    BioPipelineStage updateStage = updateStageCommand.getUpdateStage();
                    int currentVersion = updateStageCommand.getCurrentVersion();
                    updateStage.setVersion(currentVersion + 1);
                    long stageId = updateStageCommand.getStageId();
                    updateStage.setStageId(null);
                    conditionExample.createCriteria().andStageIdEqualTo(stageId).andVersionEqualTo(currentVersion);
                    batchUpdateMapper.updateByExampleSelective(updateStage, conditionExample);
                }
                List<BatchResult> batchResults = batchSqlSessionTemplate.flushStatements();
                int count = 0;
                for (BatchResult result : batchResults) {
                    for (int i : result.getUpdateCounts()) {
                        count += i;
                    }
                }

                if (count != updateStageCommands.size()) {
                    status.setRollbackOnly();
                    return NO_EFFECTIVE_UPDATE;
                }
                return OK;
            });
        } catch (Exception e) {
            return INTERNAL_FAIL;
        }

        return res;

    }

    public boolean addStageTask(BioPipelineStage stage) {
        return this.pipelineStageTaskDispatcher.addTask(stage);
    }

}
