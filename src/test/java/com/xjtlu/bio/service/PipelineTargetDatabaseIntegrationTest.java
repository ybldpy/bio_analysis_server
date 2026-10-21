package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.controller.PipelineController;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;

/**
 * Opt-in, read-only verification of the development database and HTTP contract.
 * Does not start the application, scheduling, or analysis tools. The expected
 * options come from current database configuration, not a fixed seed count.
 */
@EnabledIfSystemProperty(named = "verifyPipelineTargetDatabase", matches = "true")
class PipelineTargetDatabaseIntegrationTest {

    @Test
    void endpointMatchesEnabledVirusTargetsFromRealDatabaseInConfiguredOrder() throws Exception {
        Map<?, ?> configuration;
        try (var input = Files.newInputStream(Path.of("src/main/resources/application-dev.yml"))) {
            configuration = new Yaml().load(input);
        }
        Map<?, ?> datasource = (Map<?, ?>) ((Map<?, ?>) configuration.get("spring")).get("datasource");
        String url = (String) datasource.get("url");
        assertTrue(url != null && url.startsWith("jdbc:mysql://"),
                "This test requires a MySQL development datasource");
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
        mybatis.addMapper(BioPipelineTargetMapper.class);
        ObjectMapper json = new ObjectMapper();

        try (Connection connection = DriverManager.getConnection(url, credentials)) {
            connection.setReadOnly(true);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            try (SqlSession session = new SqlSessionFactoryBuilder().build(mybatis).openSession(connection)) {
                ArrayNode expected = json.createArrayNode();
                // Independent SQL defines both filtering and ordering, including
                // exclusion of disabled rows and non-virus pipeline types.
                try (var statement = connection.prepareStatement("""
                        SELECT label, tax_id, pipeline_type
                        FROM bio_pipeline_target
                        WHERE enabled = 1 AND pipeline_type IN (100, 101)
                        ORDER BY sort_order ASC, id ASC
                        """);
                     var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        expected.addObject()
                                .put("label", rows.getString("label"))
                                .put("taxId", rows.getInt("tax_id"))
                                .put("pipelineType", rows.getInt("pipeline_type"));
                    }
                }
                assertFalse(expected.isEmpty(),
                        "The development database must contain enabled virus target options");

                PipelineTargetService service = new PipelineTargetService(
                        session.getMapper(BioPipelineTargetMapper.class));
                PipelineController controller = new PipelineController();
                ReflectionTestUtils.setField(controller, "pipelineTargetService", service);
                MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
                byte[] response = mockMvc.perform(get("/pipeline/virusTargets")
                                .accept(MediaType.APPLICATION_JSON))
                        .andExpect(status().isOk())
                        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                        .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                        .andReturn().getResponse().getContentAsByteArray();

                JsonNode result = json.readTree(response);
                assertTrue(result.path("failMsg").isNull());
                JsonNode options = result.path("data");
                assertTrue(options.isArray());
                for (JsonNode option : options) {
                    assertEquals(3, option.size(), "Only public option fields may be returned");
                    assertTrue(option.path("label").isTextual());
                    assertTrue(option.path("taxId").isIntegralNumber());
                    assertTrue(option.path("pipelineType").isIntegralNumber());
                }
                assertEquals(expected, options,
                        "HTTP options must match current enabled virus rows in sort_order/id order");
            }
        }
    }
}
