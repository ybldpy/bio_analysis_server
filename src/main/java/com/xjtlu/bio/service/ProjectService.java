package com.xjtlu.bio.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioProject;
import com.xjtlu.bio.entity.BioProjectDTO;
import com.xjtlu.bio.entity.BioProjectExample;
import com.xjtlu.bio.entity.BioUser;
import com.xjtlu.bio.entity.BioUserExample;
import com.xjtlu.bio.mapper.BioProjectMapper;
import com.xjtlu.bio.mapper.BioUserMapper;

import jakarta.annotation.Resource;

@Service
public class ProjectService {
    


    @Resource
    private BioProjectMapper projectMapper;
    @Resource
    private BioUserMapper bioUserMapper;

    public Result<BioProject> createProject(String projectName){

        BioProject project = new BioProject();
        project.setProjectName(projectName);
        
        try{
            int success = projectMapper.insertSelective(project);
            if(success < 0){
                return new Result<>(Result.INTERNAL_FAIL,null , "创建项目失败");
            }
            return new Result<>(Result.SUCCESS, project,null);
        }catch(DuplicateKeyException e){
            return new Result<>(Result.BUSINESS_FAIL, null , "项目名已存在");
        }
    }


    
    public Result<List<BioProjectDTO>> getAllProjects(){
        BioProjectExample bioProjectExample = new BioProjectExample();
        List<BioProject> projects = projectMapper.selectByExample(bioProjectExample);
        HashMap<Long, ArrayList<BioProject>> creatorIdMap = new HashMap<>();

        if(projects.isEmpty()){
            return new Result<List<BioProjectDTO>>(Result.SUCCESS, new ArrayList<>(), null);
        }

        for(BioProject p: projects){
            creatorIdMap.putIfAbsent(p.getCreatedBy(), new ArrayList<>());
            creatorIdMap.get(p.getCreatedBy()).add(p);
        }

        List<Long> creatorIdList = creatorIdMap.keySet().stream().toList();

        BioUserExample bioUserExample = new BioUserExample();
        bioUserExample.createCriteria().andUidIn(creatorIdList);

        List<BioUser> users = bioUserMapper.selectByExample(bioUserExample);

        ArrayList<BioProjectDTO> projectDTOs = new ArrayList<>(projects.size());

        for(BioProject p: projects){
            BioUser creator = null;
            for(BioUser user: users){
                if(user.getUid().equals(p.getCreatedBy())){
                    creator = user;
                    break;
                }
            }
        
            BioProjectDTO bioProjectDTO = new BioProjectDTO(p.getPid(), p.getProjectName(), null, null);
            if(creator!=null){
                bioProjectDTO.setCreatorId(creator.getUid());
                bioProjectDTO.setCreatorName(creator.getName());
            }
            projectDTOs.add(bioProjectDTO);
        }

        return new Result<>(Result.SUCCESS, projectDTOs, null);
        
    }
}
