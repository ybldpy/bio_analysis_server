package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

@Mapper
public interface BioReferenceTaxonomyMemberMapper {

    @Select("""
            SELECT member_tax_id
            FROM bio_reference_taxonomy_member
            WHERE tax_id = #{taxId}
            ORDER BY sort_order, id
            """)
    List<Integer> selectMemberTaxIds(@Param("taxId") Integer taxId);
}
