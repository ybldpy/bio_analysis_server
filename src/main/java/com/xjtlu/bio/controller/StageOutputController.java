package com.xjtlu.bio.controller;

import java.nio.file.Path;

import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.xjtlu.bio.service.StageOutputService;

import jakarta.annotation.Resource;

@Controller
@RequestMapping("/stageOutput")
public class StageOutputController {



    @Resource
    private StageOutputService stageOutputService;

    @GetMapping("/download")
    public ResponseEntity downloadStageOutput(@RequestParam("outputUrl")String outputUrl){

    Path filePath = stageOutputService.getStageOutput(outputUrl);
    org.springframework.core.io.Resource resource =  new FileSystemResource(filePath);
    return ResponseEntity.ok()
        .header(
            HttpHeaders.CONTENT_DISPOSITION,
            "attachment; filename=\"" +
                filePath.getFileName() + "\""
        )
        .contentType(MediaType.APPLICATION_OCTET_STREAM)
        .body(resource);
    }
}
