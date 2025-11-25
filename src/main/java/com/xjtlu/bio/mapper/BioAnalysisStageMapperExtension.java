package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;

import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;

@Mapper
public interface BioAnalysisStageMapperExtension {
    public int batchInsert(List<BioPipelineStage> stages);

}
