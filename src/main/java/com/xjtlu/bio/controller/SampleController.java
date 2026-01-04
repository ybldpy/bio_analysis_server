package com.xjtlu.bio.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.controller.parameters.CreateSampleRequest;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.SampleService;

import jakarta.annotation.Resource;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@Controller
@RequestMapping("/sample")
public class SampleController {


    @Resource
    private SampleService sampleService;

    @PostMapping("/createSample")
    public ResponseEntity createSample(@RequestBody CreateSampleRequest createSampleRequest) {
        Map<String, Object> params = new HashMap<>();

        if(createSampleRequest.getPipelineStageParameters()!=null){
            params.put(PipelineService.PIPELINE_REFSEQ_ACCESSION_KEY, createSampleRequest.getPipelineStageParameters().getRefseq());
            params.put("stages", createSampleRequest.getPipelineStageParameters().getExtraParams());
        }
        
        Result<BioSample> result = sampleService.createSample(
            createSampleRequest.isPair(), 
            createSampleRequest.getSampleName(),
            createSampleRequest.getProjectId(),
            createSampleRequest.getSampleType(),
            createSampleRequest.getRead1OriginName(),
            createSampleRequest.getRead2OriginName(), 
        params);

        if(result.getStatus() == Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }

        if(result.getStatus() == Result.PARAMETER_NOT_VALID){
            return ResponseEntity.badRequest().body(result.getFailMsg());
        }


        return ResponseEntity.ok().body(result);
    }
    
    

}
