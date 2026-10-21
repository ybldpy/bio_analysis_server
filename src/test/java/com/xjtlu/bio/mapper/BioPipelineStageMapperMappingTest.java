package com.xjtlu.bio.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;

class BioPipelineStageMapperMappingTest {

    private static final String NAMESPACE = BioPipelineStageMapper.class.getName();
    private static Configuration configuration;

    @BeforeAll
    static void loadMappers() throws Exception {
        configuration = new Configuration();
        for (String resource : List.of(
                "mapper/BioPipelineStageMapper.xml",
                "mapper/BioAnalysisStageMapperExtension.xml")) {
            try (InputStream input = Resources.getResourceAsStream(resource)) {
                new XMLMapperBuilder(input, configuration, resource,
                        configuration.getSqlFragments()).parse();
            }
        }
    }

    @Test
    void keepsOutputUrlInOrdinaryResultMappingAndSelects() {
        var outputMapping = configuration.getResultMap(NAMESPACE + ".BaseResultMap")
                .getResultMappings().stream()
                .filter(mapping -> "outputUrl".equals(mapping.getProperty()))
                .findFirst().orElseThrow();

        assertEquals(JdbcType.VARCHAR, outputMapping.getJdbcType());
        assertEquals(String.class, outputMapping.getJavaType());
        assertTrue(configuration.getMappedStatement(NAMESPACE + ".selectByExample")
                .getBoundSql(new BioPipelineStageExample()).getSql().contains("output_url"));
        assertTrue(configuration.getMappedStatement(NAMESPACE + ".selectByPrimaryKey")
                .getBoundSql(1L).getSql().contains("output_url"));
    }

    @Test
    void bindsOutputUrlAsVarcharInEveryInsertAndUpdate() {
        BioPipelineStage row = outputRow();
        BioPipelineStageExample example = new BioPipelineStageExample();
        example.createCriteria().andStageIdEqualTo(row.getStageId());

        for (String statement : List.of("insert", "insertSelective",
                "updateByPrimaryKey", "updateByPrimaryKeySelective",
                "updateByPrimaryKeyWithBLOBs")) {
            assertTextOutputBinding(NAMESPACE + "." + statement, row);
        }
        for (String statement : List.of("updateByExample", "updateByExampleSelective",
                "updateByExampleWithBLOBs")) {
            assertTextOutputBinding(NAMESPACE + "." + statement,
                    Map.of("row", row, "example", example));
        }
    }

    @Test
    void bindsOutputUrlAsVarcharInBatchInsert() {
        assertTextOutputBinding(BioAnalysisStageMapperExtension.class.getName() + ".batchInsert",
                Map.of("stages", List.of(outputRow())));
    }

    @Test
    void storesLongResultsInOutputInlineWithBlobMappings() {
        var inlineMapping = configuration.getResultMap(NAMESPACE + ".ResultMapWithBLOBs")
                .getResultMappings().stream()
                .filter(mapping -> "outputInline".equals(mapping.getProperty()))
                .findFirst().orElseThrow();
        assertEquals(JdbcType.LONGVARCHAR, inlineMapping.getJdbcType());
        assertEquals(String.class, inlineMapping.getJavaType());
        assertTrue(configuration.getMappedStatement(NAMESPACE + ".selectByExampleWithBLOBs")
                .getBoundSql(new BioPipelineStageExample()).getSql().contains("output_inline"));

        BioPipelineStage row = outputRow();
        BioPipelineStageExample example = new BioPipelineStageExample();
        example.createCriteria().andStageIdEqualTo(row.getStageId());
        for (String statement : List.of("insert", "insertSelective",
                "updateByPrimaryKeySelective", "updateByPrimaryKeyWithBLOBs")) {
            assertInlineOutputBinding(NAMESPACE + "." + statement, row);
        }
        for (String statement : List.of("updateByExampleSelective", "updateByExampleWithBLOBs")) {
            assertInlineOutputBinding(NAMESPACE + "." + statement,
                    Map.of("row", row, "example", example));
        }
    }

    @Test
    void generatorDoesNotOverrideOutputUrl() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

        try (InputStream input = Resources.getResourceAsStream(
                "configuration/mybatisGeneratorConfig.xml")) {
            var document = factory.newDocumentBuilder().parse(input);
            Element context = (Element) document.getElementsByTagName("context").item(0);
            assertEquals("MyBatis3", context.getAttribute("targetRuntime"));
            assertEquals("flat", context.getAttribute("defaultModelType"));

            var tables = context.getElementsByTagName("table");
            assertEquals(1, tables.getLength());
            Element table = (Element) tables.item(0);
            assertEquals("bio_pipeline_stage", table.getAttribute("tableName"));
            Element key = (Element) table.getElementsByTagName("generatedKey").item(0);
            assertEquals("stage_id", key.getAttribute("column"));

            var overrides = table.getElementsByTagName("columnOverride");
            assertEquals(0, overrides.getLength());
        }
    }

    private static BioPipelineStage outputRow() {
        BioPipelineStage row = new BioPipelineStage();
        row.setStageId(1L);
        row.setOutputUrl("outputDir/test/result.json");
        row.setOutputInline("{\"result\":\"" + "x".repeat(10_000) + "\"}");
        return row;
    }

    private static void assertTextOutputBinding(String statement, Object parameter) {
        BoundSql sql = configuration.getMappedStatement(statement).getBoundSql(parameter);
        List<ParameterMapping> bindings = sql.getParameterMappings().stream()
                .filter(mapping -> mapping.getProperty().endsWith("outputUrl"))
                .toList();

        assertEquals(1, bindings.size(), statement);
        assertEquals(JdbcType.VARCHAR, bindings.get(0).getJdbcType(), statement);
        assertTrue(sql.getSql().contains("output_url"), statement);
    }

    private static void assertInlineOutputBinding(String statement, Object parameter) {
        BoundSql sql = configuration.getMappedStatement(statement).getBoundSql(parameter);
        List<ParameterMapping> bindings = sql.getParameterMappings().stream()
                .filter(mapping -> mapping.getProperty().endsWith("outputInline"))
                .toList();

        assertEquals(1, bindings.size(), statement);
        assertEquals(JdbcType.LONGVARCHAR, bindings.get(0).getJdbcType(), statement);
        assertTrue(sql.getSql().contains("output_inline"), statement);
    }
}
