package com.xjtlu.bio.controller;


import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.service.PipelineService;
import jakarta.annotation.Resource;
import org.apache.ibatis.annotations.Param;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/pipeline")
public class PipelineController {

    @Resource
    private PipelineService pipelineService;

    @GetMapping("/start")
    public ResponseEntity start(@Param("sampleId") long sampleId){

        Result<Boolean> startResult = this.pipelineService.pipelineStart(sampleId);

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
}
