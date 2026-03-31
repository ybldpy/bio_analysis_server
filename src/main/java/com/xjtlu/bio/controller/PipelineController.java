package com.xjtlu.bio.controller;


import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.response.CreatePipelineResponse;
import com.xjtlu.bio.service.PipelineService;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/pipeline")
public class PipelineController {

    @Resource
    private PipelineService pipelineService;

    @GetMapping("/start")
    public ResponseEntity start(@Param("pipelineId")long pipelineId){

        Result<Boolean> startResult = this.pipelineService.startPipeline(pipelineId);

        if(startResult.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(startResult.getFailMsg());
        }
        return ResponseEntity.ok().body(startResult);
    }

    @GetMapping("/restart")
    public ResponseEntity restart(@Param("stageId")long stageId){
        Result<Boolean> result = this.pipelineService.restartStage(stageId);

        if(result.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }

        
        return ResponseEntity.ok(result);
    }


    @PostMapping("/create")
    public ResponseEntity create(@RequestBody @Valid CreateAnalysisPipelineRequest createAnalysisPipelineRequest){

            if (createAnalysisPipelineRequest.isPair() && StringUtils.isBlank(createAnalysisPipelineRequest.getRead2OriginName())) {
            return ResponseEntity.badRequest().body(null);
        }

        
        Result<CreatePipelineResponse> result = this.pipelineService.createPipeline(createAnalysisPipelineRequest);
        if(result.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }
        return ResponseEntity.ok(result);

    }
}
