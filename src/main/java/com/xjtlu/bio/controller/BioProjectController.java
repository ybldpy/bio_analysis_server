package com.xjtlu.bio.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.service.ProjectService;

import jakarta.annotation.Resource;

@Controller
@RequestMapping("/project")
public class BioProjectController {

    @Resource
    private ProjectService projectService;


    @GetMapping("/getAll")
    public ResponseEntity getAllProjects(){
        return ResponseEntity.ok(projectService.getAllProjects());
    }

    public static record CreateRequest(String projectName, String description){};



    @PostMapping("/create")
    public ResponseEntity createProject(@RequestBody CreateRequest request){

        if(StringUtils.isBlank(request.projectName)){
            return ResponseEntity.ok(new Result(Result.BUSINESS_FAIL, null, "项目名称不能为空"));
        }
        return ResponseEntity.ok(projectService.createProject(request.projectName, request.description));
    }

}
