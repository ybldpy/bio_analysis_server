package com.xjtlu.bio.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;

/** Checks the real annotation mapper using MyBatis metadata and a JDBC mock only. */
class BioReferenceTaxonomyMemberMapperMappingTest {

    @Test
    void selectsOnlyBoundTargetMembersWithStableOrderAndIntegerResults() throws Exception {
        Configuration configuration = new Configuration();
        configuration.addMapper(BioReferenceTaxonomyMemberMapper.class);
        MappedStatement statement = configuration.getMappedStatement(
                BioReferenceTaxonomyMemberMapper.class.getName() + ".selectMemberTaxIds");
        Map<String, Object> parameters = Map.of("taxId", 11320);
        BoundSql sql = statement.getBoundSql(parameters);
        String normalized = sql.getSql().replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);

        assertEquals("select member_tax_id from bio_reference_taxonomy_member "
                + "where tax_id = ? order by sort_order, id", normalized);
        assertEquals(SqlCommandType.SELECT, statement.getSqlCommandType());
        assertEquals(List.of("taxId"), sql.getParameterMappings().stream()
                .map(mapping -> mapping.getProperty()).toList());
        assertEquals(Integer.class, statement.getResultMaps().get(0).getType());
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, parameters, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setInt(1, 11320);
        verifyNoMoreInteractions(jdbcStatement);
    }
}
