package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import com.xjtlu.bio.entity.BioPipelineTarget;

@Mapper
public interface BioPipelineTargetMapper {

    @Select("""
            SELECT id, label, tax_id, pipeline_type, enabled, sort_order, created_at, updated_at
            FROM bio_pipeline_target
            WHERE enabled = 1
              AND pipeline_type IN (#{virusPipelineType}, #{covidPipelineType})
            ORDER BY sort_order ASC, id ASC
            """)
    @Results(id = "BioPipelineTargetResult", value = {
            @Result(column = "id", property = "id", id = true),
            @Result(column = "label", property = "label"),
            @Result(column = "tax_id", property = "taxId"),
            @Result(column = "pipeline_type", property = "pipelineType"),
            @Result(column = "enabled", property = "enabled"),
            @Result(column = "sort_order", property = "sortOrder"),
            @Result(column = "created_at", property = "createdAt"),
            @Result(column = "updated_at", property = "updatedAt")
    })
    List<BioPipelineTarget> selectEnabledVirusTargets(
            @Param("virusPipelineType") int virusPipelineType,
            @Param("covidPipelineType") int covidPipelineType);
}
