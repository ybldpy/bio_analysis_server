package com.xjtlu.bio.service;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.entity.BioSampleExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
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
    private BioSampleMapper bioSampleMapper;

    @Resource
    private BioAnalysisPipelineMapper bioAnalysisPipelineMapper;

    private Set<Integer> bioAnalysisPipelineLockSet = ConcurrentHashMap.newKeySet();


    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;


    
 

    private boolean isLegalPipelineType(int pipelineType){
        return pipelineType == PIPELINE_VIRUS || pipelineType==PIPELINE_VIRUS_COVID || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Integer> createPipeline(int pipelineType){
        if(!this.isLegalPipelineType(pipelineType)){
            return new Result<Integer>(Result.BUSINESS_FAIL, -1, "未知分析流水线类型");
        }

        BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
        bioAnalysisPipeline.setPipelineType(pipelineType);
        int res = analysisPipelineMapper.insertSelective(bioAnalysisPipeline);
        if (res < 1) {
            return new Result<Integer>(Result.INTERNAL_FAIL,-1 , "创建分析流水线失败");
        }
        return new Result<Integer>(Result.SUCCESS, bioAnalysisPipeline.getPipelineId(), null);
    }


    private static List<BioPipelineStage> buildPipelineStages(int pipelineType){

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
