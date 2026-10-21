package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;

/**
 * Opt-in, read-only verification of the local development database against the
 * reviewed dictionary snapshot. Does not start Spring, scheduling, or analysis tools.
 * Intentional membership changes require updating the corresponding audit snapshot.
 */
@EnabledIfSystemProperty(named = "verifyReferenceTaxonomyDatabase", matches = "true")
class ReferenceTaxonomyDatabaseIntegrationTest {

    @Test
    void allFrontendTargetsResolveThroughRealMyBatisAndHaveReferenceRecords() throws Exception {
        Map<?, ?> configuration;
        try (var input = Files.newInputStream(Path.of("src/main/resources/application-dev.yml"))) {
            configuration = new Yaml().load(input);
        }
        Map<?, ?> datasource = (Map<?, ?>) ((Map<?, ?>) configuration.get("spring")).get("datasource");
        String url = (String) datasource.get("url");
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        assertTrue(List.of("localhost", "127.0.0.1").contains(endpoint.getHost()),
                "This test only permits a local development database");
        assertEquals("/bio", endpoint.getPath(), "This test only permits the bio development schema");
        Properties credentials = new Properties();
        credentials.setProperty("user", String.valueOf(datasource.get("username")));
        credentials.setProperty("password", String.valueOf(datasource.get("password")));
        credentials.setProperty("connectTimeout", "5000");
        credentials.setProperty("socketTimeout", "10000");

        Configuration mybatis = new Configuration();
        mybatis.setLogImpl(NoLoggingImpl.class);
        mybatis.addMapper(BioReferenceTaxonomyMemberMapper.class);
        JsonNode options = new ObjectMapper().readTree(
                Files.readString(Path.of("docs/frontend/virus-taxonomy-options.json"))).path("options");
        assertTrue(options.isArray());
        assertFalse(options.isEmpty());

        try (Connection connection = DriverManager.getConnection(url, credentials)) {
            connection.setReadOnly(true);
            try (SqlSession session = new SqlSessionFactoryBuilder().build(mybatis).openSession(connection)) {
                ReferenceTaxonomyResolver resolver = new ReferenceTaxonomyResolver(
                        session.getMapper(BioReferenceTaxonomyMemberMapper.class));
                for (JsonNode option : options) {
                    int taxId = option.path("taxId").intValue();
                    List<Integer> expected = new ArrayList<>();
                    option.path("memberTaxIds").forEach(member -> expected.add(member.intValue()));
                    List<Integer> actual = resolver.resolveMemberTaxIds(taxId);
                    assertEquals(expected, actual, "Database mapping differs for target " + taxId);

                    String sql = "SELECT COUNT(*) FROM bio_reference_sequence WHERE tax_id IN ("
                            + String.join(",", Collections.nCopies(actual.size(), "?")) + ")";
                    try (var statement = connection.prepareStatement(sql)) {
                        for (int i = 0; i < actual.size(); i++) statement.setInt(i + 1, actual.get(i));
                        try (var result = statement.executeQuery()) {
                            assertTrue(result.next());
                            assertTrue(result.getLong(1) > 0,
                                    "No reference metadata for target " + taxId);
                        }
                    }
                }
            }
        }
    }
}
