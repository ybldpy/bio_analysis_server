package com.xjtlu.bio.service;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioProject;
import com.xjtlu.bio.mapper.BioProjectMapper;

import jakarta.annotation.Resource;

@Service
public class ProjectService {
    


    @Resource
    private BioProjectMapper projectMapper;

    public Result createProject(String projectName){

        BioProject project = new BioProject();
        project.setProjectName(projectName);
        
        try{
            int success = projectMapper.insertSelective(project);
            if(success < 0){
                return new Result(Result.INTERNAL_FAIL, null , "创建项目失败");
            }
            return new Result(Result.SUCCESS, null , "");
        }catch(DuplicateKeyException e){
            return new Result(Result.BUSINESS_FAIL, null , "项目名已存在");
        }
    }
}
