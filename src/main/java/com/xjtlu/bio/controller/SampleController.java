package com.xjtlu.bio.controller;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.requestParameters.CreateSampleRequest;
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

    @PostMapping("/create")
    public ResponseEntity createSample(@RequestBody @Valid CreateSampleRequest createSampleRequest) {

        if (createSampleRequest.isPair() && StringUtils.isBlank(createSampleRequest.getRead2OriginName())) {
            return ResponseEntity.badRequest().body(
                    "输入为双端时read2不能为空");
        }

        Result<BioSample> result = sampleService.createSample(
                createSampleRequest.isPair(),
                createSampleRequest.getSampleName(),
                createSampleRequest.getProjectId(),
                createSampleRequest.getSampleType(),
                createSampleRequest.getRead1OriginName(),
                createSampleRequest.getRead2OriginName(),
                createSampleRequest.getPipelineStageParameters());

        if (result.getStatus() == Result.INTERNAL_FAIL) {
            return ResponseEntity.internalServerError().body(result.getFailMsg());
        }

        if (result.getStatus() == Result.PARAMETER_NOT_VALID) {
            return ResponseEntity.badRequest().body(result.getFailMsg());
        }

        Result<Long> returnResult = new Result<Long>(result.getStatus(), result.getData()!=null?result.getData().getSid():-1, result.getFailMsg());
        return ResponseEntity.ok().body(returnResult);
    }

    @PostMapping("/upload")
    public ResponseEntity sampleUpload(@RequestParam("sampleId") long sampleId,
            @RequestParam("sampleIndex") int sampleIndex, @RequestParam("sampleFile") MultipartFile sampleFile) {
        // TODO: process POST request

        InputStream inputStream = null;

        if (sampleFile.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("文件上传失败：文件内容为空");
        }


        try {
            inputStream = sampleFile.getInputStream();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("连接错误");
        }

        Result<Boolean> uploadRes = this.sampleService.receiveSampleData(sampleId, sampleIndex,
                inputStream);

        if(uploadRes.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(uploadRes.getFailMsg());
        }
        return ResponseEntity.ok().body(uploadRes);        
    }

}
