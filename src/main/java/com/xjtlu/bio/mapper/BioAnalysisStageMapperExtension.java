package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import com.xjtlu.bio.entity.BioPipelineStage;

@Mapper
public interface BioAnalysisStageMapperExtension {
    public int batchInsert(List<BioPipelineStage> stages);

    public BioPipelineStage selectByIdForUpdate(Long stageId);


    @Select("SELECT * FROM bio_pipeline_stage t1 JOIN bio_analysis_pipeline t2 ON t1.pipeline_id = t2.pipeline_id WHERE t2.sample_id = #{sampleId}")
    public List<BioPipelineStage> selectStagesBySampleId(Long sampleId);

    @Select("SELECT * FROM bio_pipeline_stage WHERE pipeline_id = (SELECT pipeline_id FROM bio_pipeline_stage WHERE stage_id = #{stageId})")
    public List<BioPipelineStage> selectAllStagesByStageId(Long stageId);

}
