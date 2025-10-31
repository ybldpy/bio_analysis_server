package com.xjtlu.bio.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.entity.BioSampleExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;

import jakarta.annotation.Resource;



@Service
public class PipelineService {

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
    private SampleService sampleService;

    private Set<Integer> bioAnalysisPipelineLockSet = ConcurrentHashMap.newKeySet();


    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;
    
    

    private boolean isLegalPipelineType(int pipelineType){
        return pipelineType == PIPELINE_VIRUS || pipelineType==PIPELINE_VIRUS_COVID || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }



    private Result<Long> createVirusPipeline(int type, String refSeqAccession, boolean sampleIsPair, int sampleName, int sampleProjectId, int sampleType, Map<String, Object> pipelineStageParams){
        
        BioAnalysisPipeline bAnalysisPipeline = new BioAnalysisPipeline();
        bAnalysisPipeline.setPipelineType(type);
        int res = this.bioAnalysisPipelineMapper.insertSelective(bAnalysisPipeline);
        if(res <= 0){
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "内部错误");
        }

        Result<Long> createSampleResult = sampleService.createSample(sampleIsPair, sampleName, sampleProjectId, bAnalysisPipeline.getPipelineId(), sampleType);
        
        int status = createSampleResult.getStatus();
        if (status!=Result.SUCCESS) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return createSampleResult;
        }

        long sid = createSampleResult.getData();

        List<BioPipelineStage> stages = createVirusPipelineStages(bAnalysisPipeline.getPipelineId(), sid, type, pipelineStageParams);

        
        
        res = bioAnalysisStageMapperExtension.batchInsert(stages);
        if(res != stages.size()){
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "内部错误");
        }

        return createSampleResult;
    }

    private List<BioPipelineStage> createVirusPipelineStages(long pid, long sid,int type, Map<String, Object> pipelineStageParams){


        //todo 
        return null;
    }



    @Transactional(rollbackFor = Exception.class)
    public Result<Long> createPipeline(int pipelineType, String refSeqAccession, boolean sampleIsPair, int sampleName, int sampleProjectId, int sampleType, Map<String, Object> pipelineStageParams,Map<String, Object> parameters){

        if(pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID){
            return this.createVirusPipeline(pipelineType, refSeqAccession, sampleIsPair, sampleName, sampleProjectId, sampleType, parameters);
        }
        return null;
    }


    private static List<BioPipelineStage> buildPipelineStages(int pipelineType){\

    }


    

    @Transactional
    public Result<Boolean> pipelineStart(int pid, int sid){
        if(!this.bioAnalysisPipelineLockSet.add(pid)){
            return new Result<Boolean>(Result.DUPLICATE_OPERATION, false, "流水线不能重复启动");
        }
        BioAnalysisPipeline bioAnalysisPipeline = this.bioAnalysisPipelineMapper.selectByPrimaryKey(pid);
        if (bioAnalysisPipeline == null) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到分析流水线");
        }

        BioSampleExample bioSampleExample = new BioSampleExample();
        bioSampleExample.createCriteria().andSidEqualTo(sid).andPipelineIdEqualTo(pid);
        List<BioSample> bioSamples = this.bioSampleMapper.selectByExample(bioSampleExample);
        if (bioSamples==null || bioSamples.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线对应样本");
        }
        if (bioSamples.size() > 1) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "找到多条流水线对应样本");
        }

        BioSample bioSample = bioSamples.get(0);
        if(bioSample.getRead1Url() == null || (bioSample.getIsPair() && bioSample.getRead2Url() == null)){
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "样本数据未完全上传");
        }

        List<BioPipelineStage> stages = buildPipelineStages(bioAnalysisPipeline.getPipelineType());
        

    }


    public void pipelineStageDone(int pid){
            
    }





}
