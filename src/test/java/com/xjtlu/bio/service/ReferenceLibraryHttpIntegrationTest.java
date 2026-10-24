package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.convention.TestBean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xjtlu.bio.BioAnalysisApplication;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.ReferenceLibraryHttpUploadCases.UploadCase;

/**
 * Opt-in live HTTP integration: starts the complete application on loopback/random port.
 * Production security, multipart handling, controllers, services, mapper/transaction wiring,
 * database and local storage remain real. Only background scheduling/dispatch are disabled.
 * Retains exactly three successful uploads as disabled, explicitly synthetic test records.
 * Run with -Dtest=ReferenceLibraryHttpIntegrationTest -DverifyReferenceLibraryHttp=true.
 * Each enabled rerun creates another uniquely marked set; it never changes existing references.
 */
@EnabledIfSystemProperty(named = "verifyReferenceLibraryHttp", matches = "true")
@SpringBootTest(classes = BioAnalysisApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.address=127.0.0.1",
                "spring.main.banner-mode=off",
                "spring.datasource.hikari.maximum-pool-size=3",
                "spring.datasource.hikari.connection-timeout=5000",
                "logging.level.root=WARN",
                "logging.level.com.xjtlu.bio=WARN",
                "logging.level.org.springframework.boot.web.embedded.tomcat=INFO",
                "logging.level.org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration=ERROR"
        })
@ActiveProfiles("dev")
@ContextConfiguration(initializers = ReferenceLibraryHttpIntegrationTest.DevelopmentOnly.class)
@Import(ReferenceLibraryHttpIntegrationTest.NoScheduling.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ReferenceLibraryHttpIntegrationTest {
    private static final Path STORAGE = Path.of("/home/jcy/bio_app_data/storage");
    private static final String SCHEDULER_BEAN =
            "org.springframework.context.annotation.internalScheduledAnnotationProcessor";
    private final ObjectMapper json = new ObjectMapper();
    private final String marker = "HTTP_UPLOAD_TEST_ONLY_" + UUID.randomUUID();
    private final Path reportDir = Path.of("target/reference-library-http-integration", marker);
    private final List<ObjectNode> caseReports = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER).build();
    @LocalServerPort private int port;
    @Autowired private DataSource dataSource;
    @Autowired private ConfigurableApplicationContext application;
    @TestBean(methodName = "disabledDispatcher", enforceOverride = true)
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;
    private JdbcTemplate jdbc;
    private List<UploadCase> cases;

    @BeforeAll
    void prepare() throws Exception {
        assertTrue(application.getBeansOfType(ScheduledAnnotationBeanPostProcessor.class).isEmpty(),
                "This HTTP test must never register background scheduled jobs");
        assertInstanceOf(NoWorkers.class, pipelineStageTaskDispatcher);
        assertEquals(STORAGE, STORAGE.toRealPath());
        jdbc = new JdbcTemplate(dataSource);
        jdbc.setQueryTimeout(10);
        Integer taxId = jdbc.queryForObject("""
                SELECT tax_id FROM bio_reference_sequence
                WHERE org_type=0 AND tax_id > 0 ORDER BY reference_id LIMIT 1
                """, Integer.class);
        assertNotNull(taxId);
        byte[] fixture;
        try (var input = new ClassPathResource("reference-library/custom-upload-test.fna").getInputStream()) {
            fixture = input.readAllBytes();
        }
        cases = ReferenceLibraryHttpUploadCases.cases(marker, taxId, fixture);
        Files.createDirectories(reportDir);
        System.out.println("Live HTTP upload test: http://127.0.0.1:" + port);
        System.out.println("Retained disabled records marker: " + marker);
        System.out.println("Live HTTP report directory: " + reportDir.toAbsolutePath());
        writeReport();
    }

    @TestFactory
    List<DynamicTest> realMultipartRequests() {
        return cases.stream().map(test -> DynamicTest.dynamicTest(test.name(), () -> runCase(test))).toList();
    }

    private void runCase(UploadCase test) throws Exception {
        ObjectNode report = json.createObjectNode();
        report.put("name", test.name());
        report.put("expectedHttp", test.expectedHttp());
        report.put("expectedBusinessStatus", test.expectedStatus());
        caseReports.add(report);
        try {
            List<Map<String, Object>> beforeRows = testRows();
            Set<String> beforeFiles = customFiles();
            HttpResponse<byte[]> response = upload(test);
            report.put("actualHttp", response.statusCode());
            JsonNode body = json.readTree(response.body());
            report.set("response", body);
            assertEquals(test.expectedHttp(), response.statusCode(), test.name() + ": " + body);
            assertTrue(body.path("status").isIntegralNumber(), "A business status is required");
            assertEquals(test.expectedStatus(), body.path("status").intValue(), body.toString());

            String failMessage = body.path("failMsg").asText("");
            if (failMessage.contains("1（病毒）") || failMessage.contains("2（细菌）")) {
                warnings.add(test.name() + ": stale orgType enum message: " + failMessage);
            }

            if (test.expectedStoredBytes() != null) {
                assertEquals(beforeRows.size() + 1, testRows().size());
                verifySuccessfulUpload(test, body.path("data"), report);
                Set<String> expectedFiles = new TreeSet<>(beforeFiles);
                expectedFiles.add(body.path("data").path("fastaObjectName").asText());
                assertEquals(expectedFiles, customFiles(), "Exactly one new stored object is expected");
            } else {
                assertTrue(body.path("data").isNull());
                assertEquals(beforeRows, testRows(), "Rejected request must not insert or alter test records");
                assertEquals(beforeFiles, customFiles(), "Rejected request must not leave a new object");
                report.put("noDatabaseMutationVerified", true);
                report.put("noNewStoredObjectVerified", true);
            }
            report.put("passed", true);
        } catch (Exception | AssertionError failure) {
            report.put("passed", false);
            report.put("failureType", failure.getClass().getSimpleName());
            report.put("failure", failure.getMessage());
            throw failure;
        } finally {
            writeReport();
        }
    }

    private void verifySuccessfulUpload(UploadCase test, JsonNode item, ObjectNode report) throws Exception {
        long id = item.path("referenceId").longValue();
        assertTrue(id > 0);
        String accession = item.path("accession").asText();
        if (test.fields().containsKey("accession")) {
            assertEquals(test.fields().get("accession"), accession);
        } else {
            assertTrue(accession.matches("CUSTOM_[0-9a-f]{32}"));
        }
        assertEquals("USER_UPLOAD", item.path("sourceDb").asText());
        assertEquals(0, item.path("orgType").intValue());
        assertTrue(item.path("enabled").isBoolean());
        assertFalse(item.path("enabled").booleanValue());
        assertEquals(64, item.path("genomeLength").intValue());
        assertEquals(marker, item.path("organismName").asText());
        // JDBC runs independently from the HTTP request's MyBatis transaction/session.
        Map<String, Object> row = jdbc.queryForMap("""
                SELECT accession, source_db, tax_id, org_type, organism_name, genome_length,
                       enabled + 0 AS enabled_value, is_annotated + 0 AS annotated_value, path, raw_metadata
                FROM bio_reference_sequence WHERE reference_id=?
                """, id);
        assertEquals(accession, row.get("accession"));
        assertEquals("USER_UPLOAD", row.get("source_db"));
        assertEquals(Integer.parseInt(test.fields().get("taxId")), ((Number) row.get("tax_id")).intValue());
        assertEquals(0, ((Number) row.get("org_type")).intValue());
        assertEquals(marker, row.get("organism_name"));
        assertEquals(64, ((Number) row.get("genome_length")).intValue());
        assertEquals(0, ((Number) row.get("enabled_value")).intValue());
        assertEquals(0, ((Number) row.get("annotated_value")).intValue());
        String key = item.path("fastaObjectName").asText();
        assertTrue(key.matches("refseq/custom/[0-9a-f-]{36}/reference\\.fna"));
        assertEquals(key, row.get("path"));
        Path stored = STORAGE.resolve(key).normalize();
        assertTrue(stored.startsWith(STORAGE));
        assertTrue(Files.isRegularFile(stored, LinkOption.NOFOLLOW_LINKS));
        assertTrue(stored.toRealPath().startsWith(STORAGE));
        assertArrayEquals(test.expectedStoredBytes(), Files.readAllBytes(stored));
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(test.expectedStoredBytes()));
        JsonNode metadata = json.readTree((String) row.get("raw_metadata"));
        assertEquals(hash, metadata.path("sha256").asText());
        assertEquals(1, metadata.path("recordCount").intValue());
        assertEquals(test.filename(), metadata.path("originalFilename").asText());

        HttpResponse<byte[]> detail = send(HttpRequest.newBuilder(uri("/referenceGenome/library/" + id)).GET());
        assertEquals(200, detail.statusCode());
        assertEquals(item, json.readTree(detail.body()).path("data"));

        ObjectNode query = json.createObjectNode().put("accession", accession).put("orgType", 0)
                .put("enabled", false).put("start", 0).put("pageSize", 20);
        HttpResponse<byte[]> search = send(HttpRequest.newBuilder(uri("/referenceGenome/library/query"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(query))));
        assertEquals(200, search.statusCode());
        JsonNode page = json.readTree(search.body());
        assertEquals(0, page.path("status").intValue());
        assertEquals(1, page.path("data").path("total").intValue());
        assertEquals(1, page.path("data").path("items").size());
        assertEquals(item, page.path("data").path("items").get(0));

        HttpResponse<byte[]> download = send(HttpRequest.newBuilder(uri("/stageOutput/download?outputUrl="
                + URLEncoder.encode(key, StandardCharsets.UTF_8))).GET());
        assertEquals(200, download.statusCode());
        assertArrayEquals(test.expectedStoredBytes(), download.body());
        report.put("independentDatabaseReadVerified", true);
        report.put("storedBytesAndHashVerified", true);
        report.put("detailAndQueryHttpVerified", true);
        report.put("downloadHttpVerified", true);
        report.put("storedFile", stored.toString());
        report.put("storedBytes", Files.size(stored));
        report.put("sha256", hash);
    }

    private HttpResponse<byte[]> upload(UploadCase test) throws Exception {
        String boundary = "UploadTestBoundary" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        for (var field : test.fields().entrySet()) {
            body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\""
                    + field.getKey() + "\"\r\n\r\n" + field.getValue() + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
        if (test.content() != null) {
            body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\""
                    + test.filename() + "\"\r\nContent-Type: application/octet-stream\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            body.write(test.content());
            body.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        body.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        return send(HttpRequest.newBuilder(uri("/referenceGenome/library/upload"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray())));
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }

    private HttpResponse<byte[]> send(HttpRequest.Builder request) throws Exception {
        return client.send(request.timeout(Duration.ofSeconds(30)).build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private List<Map<String, Object>> testRows() {
        return jdbc.queryForList("""
                SELECT reference_id, accession, source_db, tax_id, org_type, genome_length,
                       enabled + 0 AS enabled_value, path, raw_metadata
                FROM bio_reference_sequence WHERE organism_name=? ORDER BY reference_id
                """, marker);
    }

    private Set<String> customFiles() throws Exception {
        Set<String> files = new TreeSet<>();
        Path root = STORAGE.resolve("refseq/custom");
        if (!Files.exists(root)) return files;
        try (var paths = Files.walk(root, 3)) {
            paths.filter(path -> !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
                    .forEach(path -> files.add(STORAGE.relativize(path).toString()));
        }
        return files;
    }

    @AfterAll
    void finishReport() throws Exception {
        if (jdbc != null) {
            writeReport();
            System.out.println("Live HTTP cases: " + caseReports.size() + ", passed: "
                    + caseReports.stream().filter(row -> row.path("passed").asBoolean()).count());
            System.out.println("Disabled uploads retained: " + testRows().size());
            System.out.println("Live HTTP report: " + reportDir.resolve("result.json").toAbsolutePath());
        }
    }

    private void writeReport() throws Exception {
        ObjectNode report = json.createObjectNode();
        report.put("updatedAt", Instant.now().toString());
        report.put("baseUrl", "http://127.0.0.1:" + port);
        report.put("marker", marker);
        report.put("scope", "Full Spring Boot + real HTTP/security/multipart/database/storage; background scheduling and dispatch disabled only");
        report.put("serverClosesAfterTestClass", true);
        report.put("testRecordsRetainedDisabled", true);
        report.set("cases", json.valueToTree(caseReports));
        report.set("warnings", json.valueToTree(warnings));
        Files.createDirectories(reportDir);
        json.writerWithDefaultPrettyPrinter().writeValue(reportDir.resolve("result.json").toFile(), report);
    }

    static PipelineStageTaskDispatcher disabledDispatcher() {
        return new NoWorkers();
    }

    private static final class NoWorkers extends PipelineStageTaskDispatcher {
        @Override public void init() { /* No permanent worker threads for HTTP-only tests. */ }
        @Override public void run() { throw new AssertionError("No analysis execution in an upload HTTP test"); }
        @Override public boolean addTask(BioPipelineStage stage) {
            throw new AssertionError("No analysis submission in an upload HTTP test");
        }
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class NoScheduling {
        @Bean static BeanFactoryPostProcessor removeScheduledProcessor() {
            return factory -> {
                BeanDefinitionRegistry registry = (BeanDefinitionRegistry) factory;
                if (registry.containsBeanDefinition(SCHEDULER_BEAN)) registry.removeBeanDefinition(SCHEDULER_BEAN);
            };
        }
    }

    /** Validate destinations before the application creates database/storage beans. */
    static class DevelopmentOnly implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override public void initialize(ConfigurableApplicationContext context) {
            var env = context.getEnvironment();
            String url = env.getRequiredProperty("spring.datasource.url");
            if (!url.startsWith("jdbc:mysql://")) throw new IllegalStateException("Expected local MySQL datasource");
            URI endpoint = URI.create(url.substring("jdbc:".length()));
            if (!List.of("localhost", "127.0.0.1").contains(endpoint.getHost()) || !"/bio".equals(endpoint.getPath())) {
                throw new IllegalStateException("Only localhost/bio is permitted for this opt-in test");
            }
            if (!STORAGE.equals(Path.of(env.getRequiredProperty("localstorageService.baseDir")).toAbsolutePath().normalize())) {
                throw new IllegalStateException("Unexpected development storage root");
            }
            if (!"127.0.0.1".equals(env.getProperty("server.address")) || !"0".equals(env.getProperty("server.port"))) {
                throw new IllegalStateException("Only loopback/random-port service startup is permitted");
            }
        }
    }
}
