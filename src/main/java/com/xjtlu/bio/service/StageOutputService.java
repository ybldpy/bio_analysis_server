package com.xjtlu.bio.service;

import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;

@Service
public class StageOutputService {
    @Resource
    private LocalStorageService localStorageService;
    public Path getStageOutput(String stageOutputUrl){
        Path p = localStorageService.getObject(stageOutputUrl);
        if(Files.exists(p)){
            return p;
        }
        return null;
    }
}
