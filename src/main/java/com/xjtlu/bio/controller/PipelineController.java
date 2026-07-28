package com.xjtlu.bio.controller;


import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.dto.PipelineState;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.service.PipelineService;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

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

    @GetMapping("/stage/run")
    public ResponseEntity restart(@Param("stageId")long stageId){
        Result<Boolean> result = this.pipelineService.restartStage(stageId);

        if(result.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }

        
        return ResponseEntity.ok(result);
    }

    @PostMapping("/getPipelineStates")
    public ResponseEntity requestPipelineStates(@RequestBody List<Integer> pipelineIds){


        long[] queryPipelineIds = pipelineIds == null?null:new long[pipelineIds.size()];
    

        if(queryPipelineIds!=null){
            for(int i=0;i<pipelineIds.size();i++){
                queryPipelineIds[i] = pipelineIds.get(i);
            }
        }
        Result<List<PipelineState>> result = this.pipelineService.queryPipelineStates(queryPipelineIds);
        return ResponseEntity.ok(result);

    }




    @PostMapping("/checkPipelineNamePrefixAvailable")
    public ResponseEntity checkIfPipelineNamePrefixAvailale(@RequestBody Map<String,Object> request){
        String namePrefix = (String) request.get("namePrefix");
        if(StringUtils.isBlank(namePrefix)){
            return ResponseEntity.ok(new Result(Result.BUSINESS_FAIL, false, "前缀不能为空"));
        }

        if(Objects.isNull(request.get("projectId"))){
            return ResponseEntity.ok(new Result(Result.BUSINESS_FAIL, false, "项目不能为空"));
        }
        long projectId = ((Number) request.get("projectId")).longValue();
        return ResponseEntity.ok(this.pipelineService.checkIfPipelinePrefixCanUse(namePrefix, projectId));
    }


    @GetMapping("/getPipelineStages")
    public ResponseEntity requestPipelineStages(@RequestParam(value="pipelineId", required = true)long pipelineId){

        BioPipelineStageExample query = new BioPipelineStageExample();
        query.createCriteria().andPipelineIdEqualTo(pipelineId);

        List<BioPipelineStage> stages = this.pipelineService.queryStages(query);

        return ResponseEntity.ok(new Result(Result.SUCCESS, stages, null));

    }



    @GetMapping("/getPipeline")
    public ResponseEntity requestPipeline(@RequestParam(value="pipelineId", required=true) long pipelineId){
        Result<BioAnalysisPipeline> bioAnalysisPipelineResult = this.pipelineService.queryPipelineById(pipelineId);
        return ResponseEntity.ok(bioAnalysisPipelineResult);
    }


    @PostMapping("/create")
    public ResponseEntity create(@RequestBody @Valid CreateAnalysisPipelineRequest createAnalysisPipelineRequest){

        

        
        Result<Long> result = this.pipelineService.createPipeline(createAnalysisPipelineRequest);
        if(result.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }
        return ResponseEntity.ok(result);

    }
}
