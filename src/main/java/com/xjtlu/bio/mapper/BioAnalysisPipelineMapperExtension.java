package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import com.xjtlu.bio.entity.BioAnalysisPipeline;


@Mapper
public interface BioAnalysisPipelineMapperExtension {



    @Select("SELECT * FROM bio_analysis_pipeline WHERE pipeline_id = (SELECT pipeline_id FROM bio_pipeline_stage WHERE stage_id = #{stageId})")
    public BioAnalysisPipeline selectPipelineTypeByStageId(long stageId);


}
