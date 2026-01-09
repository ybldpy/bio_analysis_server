package com.xjtlu.bio.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.xjtlu.bio.entity.BioSample;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface BioSampleExtensionMapper {


    @Select("SELECT * FROM bio_sample WHERE sid = #{sid} FOR UPDATE")
    public BioSample selectByIdForUpdate(Long sid);

}
