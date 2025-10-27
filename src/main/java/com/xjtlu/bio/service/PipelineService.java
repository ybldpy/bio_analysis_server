package com.xjtlu.bio.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioSample;
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
        return null;

        
    
    }






}
