package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;

import com.xjtlu.bio.entity.BioPipelineInputFile;

public interface BioPipelineInputFileExtensionMapper {


    
    public int batchInsertSeletive(List<BioPipelineInputFile> inputFiles);



}
