package com.xjtlu.bio.mapper;

import java.util.List;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Insert;
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

    /** Called while the caller holds the shared target-taxId lock. */
    @Select("""
            SELECT COALESCE(MAX(sort_order), -1) + 1
            FROM bio_reference_taxonomy_member
            WHERE tax_id = #{taxId}
            """)
    Long selectNextSortOrder(@Param("taxId") int taxId);

    /** A duplicate is a no-op; other database errors must still abort the transaction. */
    @Insert("""
            INSERT INTO bio_reference_taxonomy_member (tax_id, member_tax_id, sort_order)
            VALUES (#{taxId}, #{memberTaxId}, #{sortOrder})
            ON DUPLICATE KEY UPDATE id = id
            """)
    int insertMemberIfAbsent(@Param("taxId") int taxId,
            @Param("memberTaxId") int memberTaxId, @Param("sortOrder") int sortOrder);
}
