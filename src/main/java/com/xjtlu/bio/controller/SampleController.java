package com.xjtlu.bio.controller;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.parameters.CreateSampleRequest;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.SampleService;

import jakarta.annotation.Resource;
import jakarta.validation.Valid;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@Controller
@RequestMapping("/sample")
public class SampleController {

    @Resource
    private SampleService sampleService;

    @PostMapping("/createSample")
    public ResponseEntity createSample(@RequestBody @Valid CreateSampleRequest createSampleRequest) {
        
        if(createSampleRequest.isPair() && StringUtils.isBlank(createSampleRequest.getRead2OriginName())){
            return ResponseEntity.badRequest().body(
                "输入为双端时read2不能为空"
            );
        }
        
        Result<BioSample> result = sampleService.createSample(
            createSampleRequest.isPair(), 
            createSampleRequest.getSampleName(),
            createSampleRequest.getProjectId(),
            createSampleRequest.getSampleType(),
            createSampleRequest.getRead1OriginName(),
            createSampleRequest.getRead2OriginName(), 
        createSampleRequest.getPipelineStageParameters());

        if(result.getStatus() == Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }

        if(result.getStatus() == Result.PARAMETER_NOT_VALID){
            return ResponseEntity.badRequest().body(result.getFailMsg());
        }
        return ResponseEntity.ok().body(result);
    }
    
    

}
