package com.xjtlu.bio.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.service.PipelineInputService;

import jakarta.annotation.Resource;

import org.springframework.web.bind.annotation.PostMapping;

@Controller
@RequestMapping("/pipelineInput")
public class PipelineInputController {


    @Resource
    private PipelineInputService pipelineInputService;


    private static final Set<Integer> ALLOWED_INPUT_TYPE = Set.of(
        PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_READ,
        PipelineInputService.PIPELINE_INPUT_TYPE_SEQUENCE_ASSEMBLY,
        PipelineInputService.PIPELINE_INPUT_TYPE_REFSEQ
    );

    @PostMapping("/upload")
    public ResponseEntity sampleUpload(@RequestParam("pipelineId")long pipelineId,@RequestParam("inputKey")String inputKey, @RequestParam("fileName")String fileName, @RequestParam("fileType")int type, @RequestParam("inputFile") MultipartFile sampleFile) {
        // TODO: process POST request

        InputStream inputStream = null;

        if (sampleFile.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("文件上传失败：文件内容为空");
        }

        if(!ALLOWED_INPUT_TYPE.contains(type)){
            Result result = new Result(Result.BUSINESS_FAIL, null, "不正确的输入文件类型");
            return ResponseEntity.ok(result);
        }


        try {
            inputStream = sampleFile.getInputStream();
        } catch (IOException e) {
            // TODO replace with logger later
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("连接错误");
        }

        Result uploadRes = this.pipelineInputService.createInputs(pipelineId, fileName, type, inputKey, inputStream);

        if(uploadRes.getStatus()==Result.INTERNAL_FAIL){
            return ResponseEntity.internalServerError().body(uploadRes.getFailMsg());
        }
        return ResponseEntity.ok().body(uploadRes);      
    }

}
