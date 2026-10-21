package com.xjtlu.bio.mapper;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.entity.BioPipelineTarget;

/** Verifies the actual mapper and checked-in migration without connecting to a database. */
class BioPipelineTargetMapperMappingTest {

    private static final Path MIGRATION = Path.of("scripts", "migrations",
            "20261020_add_bio_pipeline_target.sql");
    private static final Pattern SEED_ROW = Pattern.compile(
            "(?im)^\\s*(?:UNION\\s+ALL\\s+)?SELECT\\s+'((?:''|[^'])*)'(?:\\s+AS\\s+label)?\\s*,"
                    + "\\s*(\\d+)(?:\\s+AS\\s+tax_id)?\\s*,"
                    + "\\s*(\\d+)(?:\\s+AS\\s+pipeline_type)?\\s*,"
                    + "\\s*(\\d+)(?:\\s+AS\\s+sort_order)?\\s*$");

    @Test
    void selectsOnlyEnabledVirusTypesWithStableOrderingAndBoundParameters() throws Exception {
        MappedStatement statement = statement();
        Object parameters = new ParamNameResolver(statement.getConfiguration(),
                BioPipelineTargetMapper.class.getMethod("selectEnabledVirusTargets", int.class, int.class))
                .getNamedParams(new Object[] {PIPELINE_VIRUS, PIPELINE_VIRUS_COVID});
        BoundSql sql = statement.getBoundSql(parameters);
        String normalized = sql.getSql().replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);

        assertEquals("select id, label, tax_id, pipeline_type, enabled, sort_order, created_at, updated_at "
                + "from bio_pipeline_target where enabled = 1 and pipeline_type in (?, ?) "
                + "order by sort_order asc, id asc", normalized);
        assertEquals(SqlCommandType.SELECT, statement.getSqlCommandType());
        assertEquals(List.of("virusPipelineType", "covidPipelineType"),
                sql.getParameterMappings().stream().map(mapping -> mapping.getProperty()).toList());
        PreparedStatement jdbcStatement = mock(PreparedStatement.class);

        new DefaultParameterHandler(statement, parameters, sql).setParameters(jdbcStatement);

        verify(jdbcStatement).setInt(1, PIPELINE_VIRUS);
        verify(jdbcStatement).setInt(2, PIPELINE_VIRUS_COVID);
        verifyNoMoreInteractions(jdbcStatement);
    }

    @Test
    void explicitlyMapsAllDatabaseColumnsWithoutDependingOnGlobalUnderscoreMapping() {
        ResultMap resultMap = statement().getResultMaps().get(0);

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
    void migrationSeedsMatchAllSixtySevenFrontendOptionsInDisplayOrder() throws Exception {
        JsonNode document = new ObjectMapper().readTree(
                Files.readString(Path.of("docs", "frontend", "virus-taxonomy-options.json")));
        JsonNode options = document.path("options");
        List<Seed> seeds = new ArrayList<>();
        Matcher matcher = SEED_ROW.matcher(Files.readString(MIGRATION));
        while (matcher.find()) {
            seeds.add(new Seed(matcher.group(1).replace("''", "'"),
                    Integer.parseInt(matcher.group(2)),
                    Integer.parseInt(matcher.group(3)),
                    Integer.parseInt(matcher.group(4))));
        }

        assertTrue(options.isArray());
        assertEquals(67, options.size());
        assertEquals(options.size(), seeds.size());
        assertEquals(options.size(), document.path("optionCount").intValue());
        Set<String> uniqueTargets = new HashSet<>();
        for (int index = 0; index < options.size(); index++) {
            JsonNode option = options.get(index);
            Seed actual = seeds.get(index);
            assertEquals(new Seed(option.path("label").asText(), option.path("taxId").intValue(),
                    option.path("pipelineType").intValue(), index), actual, "Seed index " + index);
            assertTrue(uniqueTargets.add(actual.pipelineType() + ":" + actual.taxId()),
                    "Duplicate target seed: " + actual);
            assertTrue(actual.taxId() > 0);
            assertTrue(Set.of(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID).contains(actual.pipelineType()));
        }
    }

    @Test
    void migrationOnlyInsertsMissingKeysWithoutOverwritingMaintainedConfiguration() throws Exception {
        String sql = Files.readString(MIGRATION).replaceAll("(?m)--[^\\r\\n]*", "")
                .replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);

        assertTrue(sql.contains("create table if not exists bio_pipeline_target"));
        assertTrue(sql.contains("unique key uk_pipeline_target_type_tax_id (pipeline_type, tax_id)"));
        assertTrue(sql.contains(
                "insert into bio_pipeline_target (label, tax_id, pipeline_type, enabled, sort_order)"));
        assertTrue(sql.contains("select seed.label, seed.tax_id, seed.pipeline_type, true, seed.sort_order"));
        assertTrue(sql.contains("left join bio_pipeline_target as existing "
                + "on existing.pipeline_type = seed.pipeline_type and existing.tax_id = seed.tax_id "
                + "where existing.id is null"));
        assertFalse(sql.contains("on duplicate key update"));
        assertFalse(sql.contains("replace into"));
        assertFalse(sql.contains("delete from"));
        assertFalse(sql.contains("truncate "));
        assertFalse(sql.contains("member_tax_id"));
    }

    private static MappedStatement statement() {
        Configuration configuration = new Configuration();
        configuration.setMapUnderscoreToCamelCase(false);
        configuration.setUseActualParamName(false);
        configuration.addMapper(BioPipelineTargetMapper.class);
        return configuration.getMappedStatement(
                BioPipelineTargetMapper.class.getName() + ".selectEnabledVirusTargets");
    }

    private record Seed(String label, int taxId, int pipelineType, int sortOrder) {
    }
}
