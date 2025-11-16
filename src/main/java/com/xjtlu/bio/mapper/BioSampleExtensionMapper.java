package com.xjtlu.bio.mapper;

import org.apache.ibatis.annotations.Mapper;

import com.xjtlu.bio.entity.BioSample;

@Mapper
public interface BioSampleExtensionMapper {
    public BioSample selectByIdForUpdate(Long sid);

}
