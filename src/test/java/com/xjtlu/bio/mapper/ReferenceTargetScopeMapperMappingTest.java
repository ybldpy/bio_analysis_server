package com.xjtlu.bio.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultFlag;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;

import com.xjtlu.bio.entity.BioPipelineTarget;

/** Verifies target-scope annotation SQL and bindings without connecting to a database. */
class ReferenceTargetScopeMapperMappingTest {

    @Test
    void selectsTheBoundTargetAndPipelinePairWithoutFilteringDisabledTargets() throws Exception {
        MappedStatement statement = statement(BioPipelineTargetMapper.class, "selectByTaxIdAndPipelineType");
        Object parameters = new ParamNameResolver(statement.getConfiguration(),
                BioPipelineTargetMapper.class.getMethod("selectByTaxIdAndPipelineType", int.class, int.class))
                .getNamedParams(new Object[] {70101, 29});
        BoundSql sql = statement.getBoundSql(parameters);

        assertEquals("select id, label, tax_id, pipeline_type, enabled, sort_order, created_at, updated_at "
                + "from bio_pipeline_target where tax_id = ? and pipeline_type = ?", normalized(sql));
        assertEquals(SqlCommandType.SELECT, statement.getSqlCommandType());
        assertEquals(List.of("taxId", "pipelineType"), parameterNames(sql));
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, parameters, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setInt(1, 70101);
        verify(jdbcStatement).setInt(2, 29);
        verifyNoMoreInteractions(jdbcStatement);
    }

    @Test
    void pairLookupReusesTheExplicitTargetResultMapWithoutGlobalUnderscoreMapping() {
        MappedStatement statement = statement(BioPipelineTargetMapper.class, "selectByTaxIdAndPipelineType");
        ResultMap resultMap = statement.getResultMaps().get(0);

        assertEquals(BioPipelineTargetMapper.class.getName() + ".BioPipelineTargetResult", resultMap.getId());
        assertEquals(BioPipelineTarget.class, resultMap.getType());
        assertEquals(Map.of(
                "id", "id",
                "label", "label",
                "tax_id", "taxId",
                "pipeline_type", "pipelineType",
                "enabled", "enabled",
                "sort_order", "sortOrder",
                "created_at", "createdAt",
                "updated_at", "updatedAt"),
                resultMap.getResultMappings().stream()
                        .collect(Collectors.toMap(ResultMapping::getColumn, ResultMapping::getProperty)));
        assertEquals(List.of("id"), resultMap.getResultMappings().stream()
                .filter(mapping -> mapping.getFlags().contains(ResultFlag.ID))
                .map(ResultMapping::getProperty).toList());
    }

    @Test
    void locksAllTargetRowsForTheBoundTaxIdInIdOrderRegardlessOfPipelineOrEnabledState() throws Exception {
        MappedStatement statement = statement(BioPipelineTargetMapper.class, "lockIdsByTaxId");
        Object parameters = new ParamNameResolver(statement.getConfiguration(),
                BioPipelineTargetMapper.class.getMethod("lockIdsByTaxId", int.class))
                .getNamedParams(new Object[] {70101});
        BoundSql sql = statement.getBoundSql(parameters);

        assertEquals("select id from bio_pipeline_target where tax_id = ? order by id for update",
                normalized(sql));
        assertTrue(statement.isFlushCacheRequired(), "The lock must invalidate pre-lock session reads");
        assertFalse(statement.isUseCache(), "A cached result must never stand in for acquiring locks");
        assertEquals(SqlCommandType.SELECT, statement.getSqlCommandType());
        assertEquals(List.of("taxId"), parameterNames(sql));
        assertEquals(Long.class, statement.getResultMaps().get(0).getType());
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, parameters, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setInt(1, 70101);
        verifyNoMoreInteractions(jdbcStatement);
    }

    @Test
    void insertsOnlyTheBoundMemberAndLeavesExistingMembershipAndSortOrderUntouched() throws Exception {
        MappedStatement statement = statement(BioReferenceTaxonomyMemberMapper.class, "insertMemberIfAbsent");
        Object parameters = new ParamNameResolver(statement.getConfiguration(),
                BioReferenceTaxonomyMemberMapper.class.getMethod("insertMemberIfAbsent",
                        int.class, int.class, int.class))
                .getNamedParams(new Object[] {70101, 70102, 6});
        BoundSql sql = statement.getBoundSql(parameters);

        assertEquals("insert into bio_reference_taxonomy_member (tax_id, member_tax_id, sort_order) "
                + "values (?, ?, ?) on duplicate key update id = id", normalized(sql));
        assertEquals(SqlCommandType.INSERT, statement.getSqlCommandType());
        assertEquals(List.of("taxId", "memberTaxId", "sortOrder"), parameterNames(sql));
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, parameters, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setInt(1, 70101);
        verify(jdbcStatement).setInt(2, 70102);
        verify(jdbcStatement).setInt(3, 6);
        verifyNoMoreInteractions(jdbcStatement);
    }

    @Test
    void computesTheNextSortOrderWithinOnlyTheBoundTargetUsingALongResult() throws Exception {
        MappedStatement statement = statement(BioReferenceTaxonomyMemberMapper.class, "selectNextSortOrder");
        Object parameters = new ParamNameResolver(statement.getConfiguration(),
                BioReferenceTaxonomyMemberMapper.class.getMethod("selectNextSortOrder", int.class))
                .getNamedParams(new Object[] {70101});
        BoundSql sql = statement.getBoundSql(parameters);

        assertEquals("select coalesce(max(sort_order), -1) + 1 from bio_reference_taxonomy_member "
                + "where tax_id = ?", normalized(sql));
        assertEquals(SqlCommandType.SELECT, statement.getSqlCommandType());
        assertEquals(List.of("taxId"), parameterNames(sql));
        assertEquals(Long.class, statement.getResultMaps().get(0).getType());
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, parameters, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setInt(1, 70101);
        verifyNoMoreInteractions(jdbcStatement);
    }

    private static MappedStatement statement(Class<?> mapperType, String methodName) {
        Configuration configuration = new Configuration();
        configuration.setMapUnderscoreToCamelCase(false);
        configuration.setUseActualParamName(false);
        configuration.addMapper(mapperType);
        return configuration.getMappedStatement(mapperType.getName() + "." + methodName);
    }

    private static String normalized(BoundSql sql) {
        return sql.getSql().replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);
    }

    private static List<String> parameterNames(BoundSql sql) {
        return sql.getParameterMappings().stream().map(mapping -> mapping.getProperty()).toList();
    }
}
