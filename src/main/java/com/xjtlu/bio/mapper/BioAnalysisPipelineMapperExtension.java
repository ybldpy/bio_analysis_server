package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import com.xjtlu.bio.entity.BioAnalysisPipeline;

public interface BioAnalysisPipelineMapperExtension {



    
    public int batchInsertSelective(@Param("pipelines") List<BioAnalysisPipeline> pipelines);


}
