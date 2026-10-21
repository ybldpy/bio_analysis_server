package com.xjtlu.bio.mapper;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Locale;

import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ResultMapping;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.BooleanTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.entity.BioReferenceSequenceExample;

/** Exercises the generated XML through real MyBatis metadata and mock JDBC, never a database. */
class BioReferenceSequenceEnabledMappingTest {

    private static final String NAMESPACE = BioReferenceSequenceMapper.class.getName();
    private static Configuration configuration;

    @BeforeAll
    static void readTheActualGeneratedXml() throws Exception {
        configuration = new Configuration();
        configuration.setMapUnderscoreToCamelCase(false);
        configuration.setUseActualParamName(false);
        String resource = "mapper/BioReferenceSequenceMapper.xml";
        try (InputStream input = Resources.getResourceAsStream(resource)) {
            new XMLMapperBuilder(input, configuration, resource, configuration.getSqlFragments()).parse();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readsEnabledAsBooleanFromBothResultMaps(boolean enabled) throws Exception {
        for (String mapId : List.of("BaseResultMap", "ResultMapWithBLOBs")) {
            ResultMapping mapping = resultMapping(mapId, "enabled");
            assertEquals("enabled", mapping.getColumn());
            assertEquals(Boolean.class, mapping.getJavaType());
            assertEquals(JdbcType.BIT, mapping.getJdbcType());
            assertInstanceOf(BooleanTypeHandler.class, mapping.getTypeHandler());
            ResultSet jdbc = mock(ResultSet.class);
            when(jdbc.getBoolean("enabled")).thenReturn(enabled);

            Object value = mapping.getTypeHandler().getResult(jdbc, mapping.getColumn());
            BioReferenceSequence row = new BioReferenceSequence();
            configuration.newMetaObject(row).setValue(mapping.getProperty(), value);

            assertEquals(enabled, row.getEnabled());
            verify(jdbc).getBoolean("enabled");
        }
    }

    @Test
    void preservesNullBooleanInsteadOfConvertingItToFalse() throws Exception {
        ResultMapping mapping = resultMapping("BaseResultMap", "enabled");
        ResultSet jdbc = mock(ResultSet.class);
        when(jdbc.getBoolean("enabled")).thenReturn(false);
        when(jdbc.wasNull()).thenReturn(true);

        assertNull(mapping.getTypeHandler().getResult(jdbc, "enabled"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void bindsEnabledAsBitInEveryGeneratedInsertAndUpdate(boolean enabled) throws Exception {
        BioReferenceSequence row = row(enabled);
        for (String id : List.of("insert", "insertSelective", "updateByPrimaryKey",
                "updateByPrimaryKeySelective", "updateByPrimaryKeyWithBLOBs")) {
            assertEnabledBinding(id, row, enabled);
        }
        BioReferenceSequenceExample example = new BioReferenceSequenceExample();
        example.createCriteria().andReferenceIdEqualTo(row.getReferenceId());
        for (String id : List.of("updateByExample", "updateByExampleSelective", "updateByExampleWithBLOBs")) {
            assertEnabledBinding(id, exampleParameters(id, row, example), enabled);
        }
    }

    @Test
    void selectiveEnabledOnlyUpdateBindsFalseAndKeepsOtherColumnsUntouched() throws Exception {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setReferenceId(77L);
        row.setEnabled(false);
        MappedStatement statement = statement("updateByPrimaryKeySelective");
        BoundSql sql = statement.getBoundSql(row);

        assertEquals("update bio_reference_sequence set enabled = ? where reference_id = ?", normalize(sql));
        PreparedStatement jdbc = mock(PreparedStatement.class);
        new DefaultParameterHandler(statement, row, sql).setParameters(jdbc);

        verify(jdbc).setBoolean(1, false);
        verify(jdbc).setLong(2, 77L);
        verifyNoMoreInteractions(jdbc);
    }

    @Test
    void selectiveNullEnabledIsOmittedRatherThanWrittenAsFalse() {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setAccession("LOCAL-FIXTURE");
        row.setReferenceId(77L);
        for (String id : List.of("insertSelective", "updateByPrimaryKeySelective")) {
            BoundSql sql = statement(id).getBoundSql(row);
            assertFalse(normalize(sql).contains("enabled"));
            assertFalse(sql.getParameterMappings().stream().anyMatch(m -> m.getProperty().endsWith("enabled")));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"insert", "insertSelective"})
    void generatedKeysActuallyPopulateReferenceId(String id) throws Exception {
        MappedStatement statement = statement(id);
        assertEquals(SqlCommandType.INSERT, statement.getSqlCommandType());
        assertInstanceOf(Jdbc3KeyGenerator.class, statement.getKeyGenerator());
        assertArrayEquals(new String[] {"referenceId"}, statement.getKeyProperties());
        assertArrayEquals(new String[] {"reference_id"}, statement.getKeyColumns());
        BioReferenceSequence row = new BioReferenceSequence();
        row.setEnabled(true);
        Statement jdbc = mock(Statement.class);
        ResultSet keys = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(jdbc.getGeneratedKeys()).thenReturn(keys);
        when(keys.getMetaData()).thenReturn(metadata);
        when(metadata.getColumnCount()).thenReturn(1);
        when(metadata.getColumnType(1)).thenReturn(Types.BIGINT);
        when(keys.next()).thenReturn(true, false);
        when(keys.getLong(1)).thenReturn(901L);

        statement.getKeyGenerator().processAfter(null, statement, jdbc, row);

        assertEquals(901L, row.getReferenceId());
        verify(keys).close();
    }

    @Test
    void rawMetadataIsPresentOnlyInBlobResultMapsAndCorrespondingSelects() throws Exception {
        assertFalse(configuration.getResultMap(NAMESPACE + ".BaseResultMap").getResultMappings().stream()
                .anyMatch(m -> "rawMetadata".equals(m.getProperty())));
        ResultMapping metadataMapping = resultMapping("ResultMapWithBLOBs", "rawMetadata");
        assertEquals("raw_metadata", metadataMapping.getColumn());
        assertEquals(JdbcType.LONGVARCHAR, metadataMapping.getJdbcType());
        assertEquals(String.class, metadataMapping.getJavaType());
        ResultSet jdbc = mock(ResultSet.class);
        String metadata = "{\"fixture\":true}";
        when(jdbc.getString("raw_metadata")).thenReturn(metadata);
        assertEquals(metadata, metadataMapping.getTypeHandler().getResult(jdbc, "raw_metadata"));

        BioReferenceSequenceExample example = new BioReferenceSequenceExample();
        assertFalse(normalize(statement("selectByExample").getBoundSql(example)).contains("raw_metadata"));
        for (String id : List.of("selectByExampleWithBLOBs", "selectByPrimaryKey")) {
            BoundSql sql = statement(id).getBoundSql(id.endsWith("PrimaryKey") ? 77L : example);
            assertTrue(normalize(sql).contains("raw_metadata"));
            assertTrue(normalize(sql).contains("enabled"));
            assertEquals(NAMESPACE + ".ResultMapWithBLOBs", statement(id).getResultMaps().get(0).getId());
        }
    }

    @Test
    void rawMetadataWritesRemainLongVarcharAndKeepTheCompleteValue() throws Exception {
        BioReferenceSequence row = row(true);
        row.setRawMetadata("{\"fixture\":\"" + "x".repeat(10000) + "\"}");
        for (String id : List.of("insert", "insertSelective", "updateByPrimaryKeySelective",
                "updateByPrimaryKeyWithBLOBs")) {
            assertMetadataBinding(id, row, row.getRawMetadata());
        }
        BioReferenceSequenceExample example = new BioReferenceSequenceExample();
        example.createCriteria().andReferenceIdEqualTo(row.getReferenceId());
        for (String id : List.of("updateByExampleSelective", "updateByExampleWithBLOBs")) {
            assertMetadataBinding(id, exampleParameters(id, row, example), row.getRawMetadata());
        }
        assertFalse(normalize(statement("updateByPrimaryKey").getBoundSql(row)).contains("raw_metadata"));
    }

    @Test
    void exactTaxIdAndEnabledRemainInOneAndCriteriaGroupWithBoundValues() throws Exception {
        BioReferenceSequenceExample example = new BioReferenceSequenceExample();
        example.createCriteria().andTaxIdEqualTo(123).andEnabledEqualTo(true);
        MappedStatement statement = statement("selectByExampleWithBLOBs");
        BoundSql sql = statement.getBoundSql(example);

        assertTrue(normalize(sql).endsWith("where (tax_id = ? and enabled = ?)"), normalize(sql));
        assertFalse(normalize(sql).contains(" or "));
        PreparedStatement jdbc = mock(PreparedStatement.class);
        new DefaultParameterHandler(statement, example, sql).setParameters(jdbc);

        verify(jdbc).setInt(1, 123);
        verify(jdbc).setBoolean(2, true);
        verifyNoMoreInteractions(jdbc);
    }

    @Test
    void taxIdInAndEnabledRemainInOneAndCriteriaGroupWithBoundValues() throws Exception {
        BioReferenceSequenceExample example = new BioReferenceSequenceExample();
        example.createCriteria().andTaxIdIn(List.of(123, 456)).andEnabledEqualTo(true);
        MappedStatement statement = statement("selectByExample");
        BoundSql sql = statement.getBoundSql(example);

        assertTrue(normalize(sql).endsWith("where (tax_id in (?, ?) and enabled = ?)"), normalize(sql));
        assertFalse(normalize(sql).contains(" or "));
        PreparedStatement jdbc = mock(PreparedStatement.class);
        new DefaultParameterHandler(statement, example, sql).setParameters(jdbc);

        verify(jdbc).setInt(1, 123);
        verify(jdbc).setInt(2, 456);
        verify(jdbc).setBoolean(3, true);
        verifyNoMoreInteractions(jdbc);
    }

    @Test
    void unfilteredManagementExamplesDoNotImplicitlyHideDisabledReferences() {
        BioReferenceSequenceExample example = new BioReferenceSequenceExample();
        for (String id : List.of("selectByExample", "selectByExampleWithBLOBs", "countByExample")) {
            String sql = normalize(statement(id).getBoundSql(example));
            assertFalse(sql.contains(" where "), sql);
            assertFalse(sql.contains("enabled ="), sql);
        }
    }

    private static MappedStatement statement(String id) { return configuration.getMappedStatement(NAMESPACE + "." + id); }

    private static ResultMapping resultMapping(String mapId, String property) {
        return configuration.getResultMap(NAMESPACE + "." + mapId).getResultMappings().stream()
                .filter(mapping -> property.equals(mapping.getProperty())).findFirst().orElseThrow();
    }

    private static Object exampleParameters(String method, BioReferenceSequence row, BioReferenceSequenceExample example)
            throws Exception {
        return new ParamNameResolver(configuration, BioReferenceSequenceMapper.class.getMethod(method,
                BioReferenceSequence.class, BioReferenceSequenceExample.class))
                .getNamedParams(new Object[] {row, example});
    }

    private static void assertEnabledBinding(String id, Object parameter, boolean enabled) throws Exception {
        MappedStatement statement = statement(id);
        BoundSql sql = statement.getBoundSql(parameter);
        int index = parameterIndex(sql, "enabled");
        assertEquals(JdbcType.BIT, sql.getParameterMappings().get(index).getJdbcType(), id);
        PreparedStatement jdbc = mock(PreparedStatement.class);
        new DefaultParameterHandler(statement, parameter, sql).setParameters(jdbc);
        verify(jdbc).setBoolean(index + 1, enabled);
    }

    private static void assertMetadataBinding(String id, Object parameter, String metadata) throws Exception {
        MappedStatement statement = statement(id);
        BoundSql sql = statement.getBoundSql(parameter);
        int index = parameterIndex(sql, "rawMetadata");
        assertEquals(JdbcType.LONGVARCHAR, sql.getParameterMappings().get(index).getJdbcType(), id);
        PreparedStatement jdbc = mock(PreparedStatement.class);
        new DefaultParameterHandler(statement, parameter, sql).setParameters(jdbc);
        verify(jdbc).setString(index + 1, metadata);
    }

    private static int parameterIndex(BoundSql sql, String property) {
        List<ParameterMapping> mappings = sql.getParameterMappings();
        List<ParameterMapping> matches = mappings.stream().filter(m -> m.getProperty().endsWith(property)).toList();
        assertEquals(1, matches.size());
        return mappings.indexOf(matches.get(0));
    }

    private static BioReferenceSequence row(boolean enabled) {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setReferenceId(77L);
        row.setAccession("LOCAL-FIXTURE");
        row.setTaxId(123);
        row.setEnabled(enabled);
        row.setRawMetadata("{\"fixture\":true}");
        return row;
    }

    private static String normalize(BoundSql sql) {
        return sql.getSql().replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT)
                .replace("( ", "(").replace(" )", ")").replace(" ,", ",");
    }
}
