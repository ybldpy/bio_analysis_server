package com.xjtlu.bio.mapper;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Date;
import java.util.List;

import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.xjtlu.bio.entity.BioPipelineLog;
import com.xjtlu.bio.entity.BioPipelineLogExample;

/** Verifies the real mapper with JDBC mocks, without starting Spring or a database. */
class BioPipelineLogMapperMappingTest {

    private static final String NAMESPACE = BioPipelineLogMapper.class.getName();
    private static Configuration configuration;

    @BeforeAll
    static void loadMapper() throws Exception {
        configuration = new Configuration();
        String resource = "mapper/BioPipelineLogMapper.xml";
        try (InputStream input = Resources.getResourceAsStream(resource)) {
            new XMLMapperBuilder(input, configuration, resource,
                    configuration.getSqlFragments()).parse();
        }
    }

    @Test
    void insertBindsTheCompletePlainTextMessageAndUsesGeneratedId() throws Exception {
        BioPipelineLog row = event();
        row.setContent("  执行失败：'quoted'\\path\n" + "detail ".repeat(12_000) + "\n  ");
        MappedStatement statement = statement("insert");
        BoundSql sql = statement.getBoundSql(row);
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, row, sql).setParameters(jdbcStatement);

        int contentIndex = parameterIndex(sql, "content");
        assertEquals(JdbcType.LONGVARCHAR, sql.getParameterMappings().get(contentIndex - 1).getJdbcType());
        verify(jdbcStatement).setString(contentIndex, row.getContent());
        assertFalse(sql.getSql().contains(row.getContent()));
        assertInstanceOf(Jdbc3KeyGenerator.class, statement.getKeyGenerator());
        assertArrayEquals(new String[] { "id" }, statement.getKeyProperties());
        assertArrayEquals(new String[] { "id" }, statement.getKeyColumns());
    }

    @Test
    void primaryKeyAndBlobQueriesRetrieveTheCompleteContent() throws Exception {
        String content = "message\n" + "long failure description ".repeat(4_000);
        for (String method : List.of("selectByPrimaryKey", "selectByExampleWithBLOBs")) {
            MappedStatement statement = statement(method);
            BoundSql sql = statement.getBoundSql(
                    method.equals("selectByPrimaryKey") ? 11L : new BioPipelineLogExample());
            assertTrue(sql.getSql().contains("content"), method);
            assertEquals(NAMESPACE + ".ResultMapWithBLOBs", statement.getResultMaps().get(0).getId(), method);
            ResultMapping mapping = statement.getResultMaps().get(0).getResultMappings().stream()
                    .filter(result -> "content".equals(result.getProperty()))
                    .findFirst().orElseThrow();
            assertEquals(JdbcType.LONGVARCHAR, mapping.getJdbcType(), method);
            assertEquals(String.class, mapping.getJavaType(), method);
            ResultSet resultSet = mock(ResultSet.class);
            when(resultSet.getString("content")).thenReturn(content);

            assertEquals(content, mapping.getTypeHandler().getResult(resultSet, "content"), method);
            verify(resultSet).getString("content");
        }
    }

    @Test
    void pipelineLevelEventBindsNullStageAndVersionWithExplicitJdbcTypes() throws Exception {
        BioPipelineLog row = event();
        row.setStageId(null);
        row.setStageVersion(null);
        MappedStatement statement = statement("insert");
        BoundSql sql = statement.getBoundSql(row);
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, row, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setNull(parameterIndex(sql, "stageId"), Types.BIGINT);
        verify(jdbcStatement).setNull(parameterIndex(sql, "stageVersion"), Types.INTEGER);
    }

    @Test
    void selectiveInsertUsesDatabaseDefaultWhileExplicitTimeIsBound() {
        BioPipelineLog row = event();
        BoundSql defaultSql = statement("insertSelective").getBoundSql(row);
        assertFalse(defaultSql.getSql().contains("created_at"));
        row.setCreatedAt(new Date(1_700_000_000_123L));

        BoundSql explicitSql = statement("insertSelective").getBoundSql(row);

        assertTrue(explicitSql.getSql().contains("created_at"));
        assertEquals(JdbcType.TIMESTAMP, explicitSql.getParameterMappings().stream()
                .filter(mapping -> "createdAt".equals(mapping.getProperty()))
                .findFirst().orElseThrow().getJdbcType());
    }

    @Test
    void stageHistoryUsesBoundStageIdAndStableChronologicalOrder() throws Exception {
        BioPipelineLogExample example = new BioPipelineLogExample();
        example.createCriteria().andStageIdEqualTo(42L);
        example.setOrderByClause("created_at asc, id asc");
        MappedStatement statement = statement("selectByExampleWithBLOBs");
        BoundSql sql = statement.getBoundSql(example);
        String normalized = sql.getSql().replaceAll("\\s+", " ").trim();

        assertTrue(normalized.matches(".*[Ww][Hh][Ee][Rr][Ee] \\( stage_id = \\? \\).*"));
        assertTrue(normalized.endsWith("order by created_at asc, id asc"));
        assertEquals(1, sql.getParameterMappings().size());
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, example, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setLong(1, 42L);
    }

    private static MappedStatement statement(String name) {
        return configuration.getMappedStatement(NAMESPACE + "." + name);
    }

    private static int parameterIndex(BoundSql sql, String property) {
        for (int i = 0; i < sql.getParameterMappings().size(); i++) {
            if (property.equals(sql.getParameterMappings().get(i).getProperty())) {
                return i + 1;
            }
        }
        throw new AssertionError("Missing parameter: " + property);
    }

    private static BioPipelineLog event() {
        BioPipelineLog row = new BioPipelineLog();
        row.setPipelineId(7L);
        row.setStageId(42L);
        row.setStageVersion(3);
        row.setLevel("ERROR");
        row.setEventType("STAGE_FAILED");
        row.setContent("fixture failure");
        return row;
    }
}
