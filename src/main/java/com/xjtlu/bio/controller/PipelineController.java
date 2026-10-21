package com.xjtlu.bio.controller;


import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.dto.PipelineState;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.requestParameters.BatchCreateAnalysisPipelineRequest;
import com.xjtlu.bio.requestParameters.ConfirmTaxonomyRequest;
import com.xjtlu.bio.requestParameters.CreateAnalysisPipelineRequest;
import com.xjtlu.bio.response.TaxonomyConfirmationResponse;
import com.xjtlu.bio.response.VirusTargetOption;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.PipelineTargetService;
import jakarta.annotation.Resource;
import jakarta.validation.Valid;

import java.security.Principal;
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

    @Resource
    private PipelineTargetService pipelineTargetService;

    @GetMapping("/virusTargets")
    public ResponseEntity<Result<List<VirusTargetOption>>> virusTargets() {
        Result<List<VirusTargetOption>> result = pipelineTargetService.listVirusTargets();
        if (result.getStatus() == Result.INTERNAL_FAIL) {
            return ResponseEntity.internalServerError().body(result);
        }
        return ResponseEntity.ok(result);
    }

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

    @PostMapping("/stage/taxonomy/confirm")
    public ResponseEntity<?> confirmTaxonomy(@RequestBody @Valid ConfirmTaxonomyRequest request,
                                             Principal principal) {
        String confirmedBy = principal == null ? null : principal.getName();
        Result<TaxonomyConfirmationResponse> result =
                this.pipelineService.confirmTaxonomy(request, confirmedBy);
        if (result.getStatus() == Result.INTERNAL_FAIL) {
            return ResponseEntity.internalServerError().body(result);
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



    @PostMapping("/batchCreatePipelines")
    public ResponseEntity batchCreatePipelines(@RequestBody BatchCreateAnalysisPipelineRequest batchCreateAnalysisPipelineRequest){
        if(batchCreateAnalysisPipelineRequest.getPipelines().size() < 1){
            return ResponseEntity.ok(new Result(Result.BUSINESS_FAIL, null, "创建的流水线不能为空"));
        }
        Result<List<Long>> createResult = this.pipelineService.batchCreateAnalysisPipelines(batchCreateAnalysisPipelineRequest);
        return ResponseEntity.ok(createResult);
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
