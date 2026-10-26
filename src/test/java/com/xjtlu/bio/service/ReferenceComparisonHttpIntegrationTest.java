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
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.convention.TestBean;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xjtlu.bio.BioAnalysisApplication;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;

/**
 * Opt-in end-to-end HTTP test using an isolated MySQL process and real minimap2.
 * All fixture strings are seeded software test data, not biological specimens or
 * claimed NCBI references. The private routing taxId is only used in the empty
 * test schema. No rows or files in the development database/storage are changed.
 *
 * Run alone in a forked JVM with -Dcomparison.http.integration=true and
 * -Dtest=ReferenceComparisonHttpIntegrationTest. Production dispatcher queue,
 * execution loop, services, mappers and DoneHandlers remain real. Only periodic
 * scheduling is removed and worker threads are made daemon for fork termination.
 */
@EnabledIfSystemProperty(named = "comparison.http.integration", matches = "true")
@SpringBootTest(classes = BioAnalysisApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "server.address=127.0.0.1",
                "debug=false",
                "spring.main.banner-mode=off",
                "spring.datasource.hikari.maximum-pool-size=4",
                "spring.datasource.hikari.connection-timeout=5000",
                "logging.level.root=WARN",
                "logging.level.com.xjtlu.bio=INFO",
                "logging.level.org.springframework=WARN",
                "logging.level.org.springframework.boot.web.embedded.tomcat=INFO",
                "logging.level.org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration=ERROR"
        })
@ActiveProfiles("dev")
@ContextConfiguration(initializers = ReferenceComparisonHttpIntegrationTest.IsolatedEnvironment.class)
@Import(ReferenceComparisonHttpIntegrationTest.NoScheduling.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class ReferenceComparisonHttpIntegrationTest {
    private static final int TEST_ONLY_TAX_ID = 2_000_000_000;
    private static final int QUERY_LENGTH = 6_000;
    private static final String SCHEDULER_BEAN =
            "org.springframework.context.annotation.internalScheduledAnnotationProcessor";
    private static Path root;
    private static ComparisonIsolatedMysql mysql;

    private final ObjectMapper json = new ObjectMapper();
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build();
    private final String marker = "COMPARISON_TEST_ONLY_" + UUID.randomUUID().toString().replace("-", "");
    private final List<ObjectNode> requests = new ArrayList<>();
    private final List<ObjectNode> rounds = new ArrayList<>();
    private final List<JsonNode> references = new ArrayList<>();
    private final List<Long> startedStageIds = new ArrayList<>();
    private final List<String> shutdownWarnings = new ArrayList<>();

    @LocalServerPort private int port;
    @Autowired private ConfigurableApplicationContext application;
    @Autowired private DataSource dataSource;
    @Autowired private AnalysisPipelineToolsConfig toolConfiguration;
    @TestBean(methodName = "daemonDispatcher", enforceOverride = true)
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    private JdbcTemplate jdbc;
    private long projectId;
    private byte[] queryFasta;
    private byte[] unrelatedQueryFasta;
    private JsonNode baselineMetrics;
    private List<String> baselineArtifactHashes;
    private String baselineSummaryHash;
    private boolean setupPassed;
    private boolean databaseStopped;

    @BeforeAll
    void prepare() throws Exception {
        assertTrue(application.getBeansOfType(ScheduledAnnotationBeanPostProcessor.class).isEmpty(),
                "No background scheduled job may run in this isolated integration test");
        DaemonDispatcher dispatcher = assertInstanceOf(DaemonDispatcher.class, pipelineStageTaskDispatcher);
        // @TestBean supplies an existing singleton; do not rely on @PostConstruct
        // or @Resource processing for that override. Use real Spring collaborators.
        application.getAutowireCapableBeanFactory().autowireBean(dispatcher);
        assertNotNull(ReflectionTestUtils.getField(dispatcher, "pipelineService"));
        assertNotNull(ReflectionTestUtils.getField(dispatcher, "stageExecutorMap"));
        assertNotNull(ReflectionTestUtils.getField(dispatcher, "stageDoneHandlerMap"));
        // Init is guarded if lifecycle already ran.
        dispatcher.init();
        assertEquals(root.resolve("storage").toString(),
                application.getEnvironment().getProperty("localstorageService.baseDir"));
        try (var connection = dataSource.getConnection()) {
            assertEquals(mysql.jdbcUrl().split("\\?", 2)[0],
                    connection.getMetaData().getURL().split("\\?", 2)[0]);
        }
        jdbc = new JdbcTemplate(dataSource);
        jdbc.setQueryTimeout(10);
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM bio_analysis_pipeline", Integer.class));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM bio_reference_sequence", Integer.class));

        ObjectNode project = json.createObjectNode().put("projectName", marker)
                .put("description", "ISOLATED TEST ONLY: seeded strings, no biological samples");
        projectId = postJson("/project/create", project).path("data").asLong();
        assertTrue(projectId > 0);

        String query = randomBases(72_391L, QUERY_LENGTH);
        queryFasta = fasta("TEST_ONLY_query", query);
        unrelatedQueryFasta = fasta("TEST_ONLY_query", randomBases(931_107L, QUERY_LENGTH));
        char[] middle = query.substring(1_000, 5_000).toCharArray();
        for (int offset = 200; offset < middle.length; offset += 450) {
            middle[offset] = middle[offset] == 'A' ? 'C' : 'A';
        }
        String partial = randomBases(103_101L, 2_000) + new String(middle)
                + randomBases(103_102L, 2_000);

        uploadReference("exact", "exact.fna", fasta("TEST_ONLY_exact", query));
        uploadReference("partial", "partial.fna.gz", gzip(fasta("TEST_ONLY_partial", partial)));
        uploadReference("unrelated", "unrelated.fna.zip",
                zip(fasta("TEST_ONLY_unrelated", randomBases(810_233L, QUERY_LENGTH))));
        assertEquals(3, references.size());
        assertEquals(3, jdbc.queryForObject("SELECT COUNT(*) FROM bio_reference_sequence", Integer.class));
        setupPassed = true;
        writeReport();
        System.out.println("Comparison HTTP integration: http://127.0.0.1:" + port);
        System.out.println("Isolated comparison report: " + root.resolve("result.json"));
    }

    @TestFactory
    List<DynamicTest> runRealComparisonPipelines() {
        return List.of(
                DynamicTest.dynamicTest("plain: exact + partial + no-match", () ->
                        runRound("plain", "query.fna", queryFasta, true)),
                DynamicTest.dynamicTest("gzip: repeated exact + partial + no-match", () ->
                        runRound("gzip", "query.fna.gz", gzip(queryFasta), true)),
                DynamicTest.dynamicTest("zip: repeated exact + partial + no-match", () ->
                        runRound("zip", "query.fna.zip", zip(queryFasta), true)),
                DynamicTest.dynamicTest("all-no-match completes without selection failure", () ->
                        runRound("all-no-match", "query.fna", unrelatedQueryFasta, false)));
    }

    private void uploadReference(String name, String filename, byte[] bytes) throws Exception {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("taxId", Integer.toString(TEST_ONLY_TAX_ID));
        fields.put("orgType", "0");
        fields.put("organismName", marker + "_" + name);
        // Deliberately equal segments: Comparison must not group/filter candidates.
        fields.put("segment", "1");
        fields.put("enabled", "true");
        JsonNode item = multipart("/referenceGenome/library/upload", fields, "file", filename, bytes).path("data");
        assertTrue(item.path("referenceId").asLong() > 0);
        assertTrue(item.path("accession").asText().startsWith("CUSTOM_"));
        assertEquals("USER_UPLOAD", item.path("sourceDb").asText());
        assertEquals(TEST_ONLY_TAX_ID, item.path("taxId").asInt());
        assertTrue(item.path("enabled").asBoolean());
        assertEquals(name.equals("partial") ? 8_000 : QUERY_LENGTH, item.path("genomeLength").asInt());
        references.add(item.deepCopy());
        writeReport();
    }

    private void runRound(String name, String filename, byte[] inputBytes, boolean expectMatches) throws Exception {
        ObjectNode round = json.createObjectNode().put("name", name).put("inputFilename", filename)
                .put("uploadedBytes", inputBytes.length).put("uploadedSha256", sha256(inputBytes))
                .put("expectedMatchedCandidateCount", expectMatches ? 2 : 0);
        rounds.add(round);
        long began = System.nanoTime();
        try {
            ObjectNode create = json.createObjectNode().put("projectId", projectId)
                    .put("pipelineType", 100).put("analysisName", marker + "_" + name);
            ObjectNode parameters = create.putObject("pipelineStageParameters");
            parameters.put("taxId", TEST_ONLY_TAX_ID).put("sequencingPlatform", 0);
            ArrayNode whitelist = parameters.putArray("enabledReferenceIds");
            references.forEach(reference -> whitelist.add(reference.path("referenceId").asLong()));
            long pipelineId = postJson("/pipeline/create", create).path("data").asLong();
            assertTrue(pipelineId > 0);
            round.put("pipelineId", pipelineId);

            Map<String, String> uploadFields = Map.of("pipelineId", Long.toString(pipelineId),
                    "inputKey", "sample/0/0", "fileType", "11", "fileName", filename);
            JsonNode upload = multipart("/pipelineInput/upload", uploadFields, "inputFile", filename, inputBytes);
            round.set("uploadResponse", upload);
            round.set("startResponse", getJson("/pipeline/start?pipelineId=" + pipelineId));

            JsonNode stage = awaitCompletion(pipelineId, round);
            assertEquals(43, stage.path("stageType").asInt());
            assertEquals(0, stage.path("stageIndex").asInt());
            assertFalse(stage.path("stageName").asText().isBlank());
            JsonNode storedParameters = json.readTree(stage.path("parameters").asText());
            assertEquals(3, storedParameters.path("candidateReferences").size());
            for (int index = 0; index < references.size(); index++) {
                assertEquals(references.get(index).path("referenceId").asLong(),
                        storedParameters.path("candidateReferences").get(index).path("referenceId").asLong());
            }
            round.put("candidateWhitelistPreserved", true);
            JsonNode output = json.readTree(stage.path("outputUrl").asText());
            round.set("output", output);
            assertEquals(2, output.path("schemaVersion").asInt());
            assertEquals(3, output.path("candidateCount").asInt());
            assertEquals(expectMatches ? 2 : 0, output.path("matchedCandidateCount").asInt());
            assertEquals(expectMatches ? 1 : 3, output.path("unmatchedCandidateCount").asInt());
            assertTrue(stage.path("outputUrl").asText().length() < 1_000,
                    "Only compact report links/counts belong in output_url");

            ArrayNode downloads = round.putArray("downloads");
            byte[] manifestBytes = download(name, "reference_comparison.json",
                    output.path("comparisonJsonUrl").asText(), downloads);
            byte[] summaryBytes = download(name, "reference_comparison.tsv",
                    output.path("comparisonTsvUrl").asText(), downloads);
            JsonNode manifest = json.readTree(manifestBytes);
            round.set("manifest", manifest);
            assertEquals(2, manifest.path("schemaVersion").asInt());
            assertEquals(output.path("candidateCount"), manifest.path("candidateCount"));
            assertEquals(output.path("matchedCandidateCount"), manifest.path("matchedCandidateCount"));
            assertEquals(output.path("unmatchedCandidateCount"), manifest.path("unmatchedCandidateCount"));
            List<String> summaryLines = new String(summaryBytes, StandardCharsets.UTF_8).lines().toList();
            assertEquals(4, summaryLines.size());
            assertEquals(15, summaryLines.get(0).split("\t", -1).length);
            assertTrue(summaryLines.get(0).startsWith("reference_id\treference_accession\t"));

            List<String> artifactHashes = verifyCandidates(name, manifest, downloads, expectMatches);
            JsonNode normalized = normalizedMetrics(manifest);
            round.set("normalizedMetrics", normalized);
            String summaryHash = sha256(summaryBytes);
            if (expectMatches) {
                if (baselineMetrics == null) {
                    baselineMetrics = normalized.deepCopy();
                    baselineArtifactHashes = List.copyOf(artifactHashes);
                    baselineSummaryHash = summaryHash;
                    round.put("crossFormatComparison", "BASELINE");
                } else {
                    assertEquals(baselineMetrics, normalized,
                            "The same decompressed query must produce the same metrics");
                    assertEquals(baselineArtifactHashes, artifactHashes,
                            "PAF and difference TSV content must be stable across input formats");
                    assertEquals(baselineSummaryHash, summaryHash);
                    round.put("crossFormatComparison", "IDENTICAL_METRICS_AND_ARTIFACTS");
                }
            }
            round.put("databasePersistenceVerified", true);
            round.put("allArtifactsDownloadedOverHttp", true);
            round.put("passed", true);
        } catch (Exception | AssertionError failure) {
            round.put("passed", false).put("failureType", failure.getClass().getSimpleName())
                    .put("failure", failure.getMessage());
            throw failure;
        } finally {
            round.put("elapsedMillis", (System.nanoTime() - began) / 1_000_000L);
            writeReport();
        }
    }

    private JsonNode awaitCompletion(long pipelineId, ObjectNode round) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        while (System.nanoTime() < deadline) {
            JsonNode stages = getJson("/pipeline/getPipelineStages?pipelineId=" + pipelineId).path("data");
            assertEquals(1, stages.size(), "FASTA route must contain exactly one comparison stage");
            JsonNode stage = stages.get(0);
            long stageId = stage.path("stageId").asLong();
            assertTrue(stageId > 0);
            if (!startedStageIds.contains(stageId)) startedStageIds.add(stageId);
            round.put("stageId", stageId).put("lastStageStatus", stage.path("status").asInt());
            JsonNode pipeline = getJson("/pipeline/getPipeline?pipelineId=" + pipelineId).path("data");
            round.put("lastPipelineStatus", pipeline.path("status").asInt());
            if (stage.path("status").asInt() == 3) {
                round.set("failureLogs", json.valueToTree(jdbc.queryForList(
                        "SELECT stage_id, event_type, content FROM bio_pipeline_log WHERE stage_id=?", stageId)));
                fail("Comparison stage failed: " + round);
            }
            if (stage.path("status").asInt() == 4 && pipeline.path("status").asInt() == 2) {
                round.set("finalStage", stage);
                round.set("finalPipeline", pipeline);
                Map<String, Object> stored = jdbc.queryForMap(
                        "SELECT stage_type, status, output_url FROM bio_pipeline_stage WHERE stage_id=?", stageId);
                assertEquals(43, ((Number) stored.get("stage_type")).intValue());
                assertEquals(4, ((Number) stored.get("status")).intValue());
                assertEquals(stage.path("outputUrl").asText(), stored.get("output_url"));
                assertEquals(2, jdbc.queryForObject(
                        "SELECT status FROM bio_analysis_pipeline WHERE pipeline_id=?", Integer.class, pipelineId));
                return stage;
            }
            Thread.sleep(200);
        }
        fail("Comparison pipeline did not complete within 120 seconds: " + round);
        throw new AssertionError("unreachable");
    }

    private List<String> verifyCandidates(String roundName, JsonNode manifest, ArrayNode downloads,
            boolean expectMatches) throws Exception {
        JsonNode comparisons = manifest.path("comparisons");
        assertEquals(3, comparisons.size());
        List<String> hashes = new ArrayList<>();
        for (int index = 0; index < comparisons.size(); index++) {
            JsonNode candidate = comparisons.get(index);
            JsonNode reference = references.get(index);
            assertEquals(reference.path("referenceId"), candidate.path("referenceId"));
            assertEquals(reference.path("accession"), candidate.path("referenceAccession"));
            assertEquals("USER_UPLOAD", candidate.path("sourceDb").asText());
            assertEquals(TEST_ONLY_TAX_ID, candidate.path("taxId").asInt());
            assertEquals("1", candidate.path("segment").asText());
            assertEquals(index == 1 ? 8_000 : QUERY_LENGTH, candidate.path("referenceBases").asLong());
            boolean matched = expectMatches && index < 2;
            assertEquals(matched ? "MATCHED" : "NO_MATCH", candidate.path("status").asText());

            byte[] paf = download(roundName, "candidate_" + (index + 1) + ".paf",
                    candidate.path("alignmentPafUrl").asText(), downloads);
            byte[] differences = download(roundName, "candidate_" + (index + 1) + ".differences.tsv",
                    candidate.path("differenceTsvUrl").asText(), downloads);
            hashes.add(sha256(paf));
            hashes.add(sha256(differences));
            List<String> diffLines = new String(differences, StandardCharsets.UTF_8).lines().toList();
            assertFalse(diffLines.isEmpty());
            assertTrue(diffLines.get(0).startsWith("reference_name\tquery_name\t"));
            if (!matched) {
                assertEquals(0, paf.length);
                assertEquals(1, diffLines.size());
                assertEquals(0.0, candidate.path("referenceCoverage").asDouble());
                assertEquals(0.0, candidate.path("sequenceIdentity").asDouble());
                assertEquals(0, candidate.path("alignedReferenceBases").asLong());
                assertEquals(0, candidate.path("alignmentCount").asInt());
            } else {
                assertTrue(paf.length > 0);
                assertTrue(candidate.path("alignedReferenceBases").asLong() > 0);
                assertTrue(candidate.path("alignmentCount").asInt() > 0);
                if (index == 0) {
                    assertEquals(100.0, candidate.path("sequenceIdentity").asDouble(), 1e-9);
                    assertEquals(1.0, candidate.path("referenceCoverage").asDouble(), 1e-9);
                    assertEquals(QUERY_LENGTH, candidate.path("alignedReferenceBases").asLong());
                    assertEquals(1, diffLines.size());
                } else {
                    assertTrue(candidate.path("sequenceIdentity").asDouble() > 95.0);
                    assertTrue(candidate.path("sequenceIdentity").asDouble() < 100.0);
                    assertTrue(candidate.path("referenceCoverage").asDouble() > 0.3);
                    assertTrue(candidate.path("referenceCoverage").asDouble() < 0.8);
                    assertTrue(diffLines.size() > 1, "Controlled substitutions must appear in the differences report");
                }
            }
        }
        return hashes;
    }

    private JsonNode normalizedMetrics(JsonNode manifest) {
        ArrayNode result = json.createArrayNode();
        manifest.path("comparisons").forEach(item -> {
            ObjectNode copy = item.deepCopy();
            copy.remove(List.of("alignmentPafUrl", "differenceTsvUrl"));
            result.add(copy);
        });
        return result;
    }

    private byte[] download(String roundName, String filename, String key, ArrayNode downloads) throws Exception {
        assertFalse(key.isBlank());
        assertTrue(key.startsWith("stageOutput/"));
        HttpResponse<byte[]> response = send(HttpRequest.newBuilder(uri("/stageOutput/download?outputUrl="
                + URLEncoder.encode(key, StandardCharsets.UTF_8))).GET());
        assertEquals(200, response.statusCode(), key);
        Path destination = Files.createDirectories(root.resolve("downloads").resolve(roundName)).resolve(filename);
        Files.write(destination, response.body());
        Path stored = root.resolve("storage").resolve(key).normalize();
        assertTrue(stored.startsWith(root.resolve("storage")));
        assertTrue(Files.isRegularFile(stored), "Downloaded artifact must remain in persistent test storage");
        assertArrayEquals(Files.readAllBytes(stored), response.body());
        downloads.addObject().put("objectKey", key).put("httpStatus", response.statusCode())
                .put("bytes", response.body().length).put("sha256", sha256(response.body()))
                .put("downloadedFile", destination.toString());
        return response.body();
    }

    private JsonNode postJson(String path, JsonNode body) throws Exception {
        return requestJson(HttpRequest.newBuilder(uri(path)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(body))));
    }

    private JsonNode getJson(String path) throws Exception {
        return requestJson(HttpRequest.newBuilder(uri(path)).GET());
    }

    private JsonNode multipart(String path, Map<String, String> fields, String fileField,
            String filename, byte[] bytes) throws Exception {
        String boundary = "ComparisonBoundary" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            payload.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\""
                    + field.getKey() + "\"\r\n\r\n" + field.getValue() + "\r\n").getBytes(StandardCharsets.UTF_8));
        }
        payload.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\""
                + fileField + "\"; filename=\"" + filename + "\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n").getBytes(StandardCharsets.UTF_8));
        payload.write(bytes);
        payload.write(("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        return requestJson(HttpRequest.newBuilder(uri(path))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(payload.toByteArray())));
    }

    private JsonNode requestJson(HttpRequest.Builder builder) throws Exception {
        HttpRequest request = builder.timeout(Duration.ofSeconds(30)).build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        ObjectNode record = json.createObjectNode().put("method", request.method())
                .put("path", request.uri().getRawPath())
                .put("query", request.uri().getRawQuery())
                .put("httpStatus", response.statusCode());
        requests.add(record);
        JsonNode body;
        try {
            body = json.readTree(response.body());
        } catch (Exception error) {
            record.put("responseText", new String(response.body(), StandardCharsets.UTF_8));
            writeReport();
            throw error;
        }
        record.put("businessStatus", body.path("status").asInt(-1));
        if (body.path("data").isIntegralNumber()) record.set("id", body.path("data"));
        if (!body.path("failMsg").isNull()) record.set("failMsg", body.path("failMsg"));
        writeReport();
        assertEquals(200, response.statusCode(), request.uri() + ": " + body);
        assertEquals(0, body.path("status").asInt(-1), request.uri() + ": " + body);
        return body;
    }

    private HttpResponse<byte[]> send(HttpRequest.Builder request) throws Exception {
        return client.send(request.timeout(Duration.ofSeconds(30)).build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private URI uri(String path) {
        return URI.create("http://127.0.0.1:" + port + path);
    }

    private void writeReport() throws Exception {
        if (root == null) return;
        ObjectNode report = json.createObjectNode().put("updatedAt", Instant.now().toString())
                .put("marker", marker).put("baseUrl", "http://127.0.0.1:" + port)
                .put("isolatedDatabase", true).put("developmentDataChanged", false)
                .put("fixtureKind", "SEEDED_RANDOM_SOFTWARE_TEST_STRINGS_NOT_BIOLOGICAL_SAMPLES")
                .put("privateTestRoutingTaxId", TEST_ONLY_TAX_ID).put("projectId", projectId)
                .put("setupPassed", setupPassed).put("isolatedMysqlStopped", databaseStopped)
                .put("passedRounds", rounds.stream().filter(item -> item.path("passed").asBoolean()).count())
                .put("scope", "Real HTTP, MySQL, storage, dispatcher queue/run, minimap2, DoneHandler; scheduling disabled");
        if (toolConfiguration != null) report.set("minimap2CommandPrefix", json.valueToTree(toolConfiguration.getMinimap2()));
        if (queryFasta != null) report.put("decompressedQuerySha256", sha256(queryFasta));
        report.set("references", json.valueToTree(references));
        report.set("requests", json.valueToTree(requests));
        report.set("rounds", json.valueToTree(rounds));
        report.set("shutdownWarnings", json.valueToTree(shutdownWarnings));
        Files.createDirectories(root);
        json.writerWithDefaultPrettyPrinter().writeValue(root.resolve("result.json").toFile(), report);
    }

    @AfterAll
    void finish() throws Exception {
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (pipelineStageTaskDispatcher != null
                    && startedStageIds.stream().anyMatch(pipelineStageTaskDispatcher::isStageIn)
                    && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            if (pipelineStageTaskDispatcher != null
                    && startedStageIds.stream().anyMatch(pipelineStageTaskDispatcher::isStageIn)) {
                shutdownWarnings.add("A test dispatcher task was still active at the bounded shutdown deadline");
            }
            writeReport();
            assertTrue(shutdownWarnings.isEmpty(), "Test work must finish before shutting down its database");
        } finally {
            // Spring's afterTestClass listeners still need an active context.
            // @DirtiesContext owns context shutdown; only stop our database here.
            try {
                if (mysql != null) {
                    mysql.close();
                    databaseStopped = true;
                }
            } finally {
                writeReport();
            }
        }
        System.out.println("Comparison HTTP rounds: " + rounds.size() + ", passed: "
                + rounds.stream().filter(item -> item.path("passed").asBoolean()).count());
        System.out.println("Comparison HTTP report retained at: " + root.resolve("result.json"));
    }

    private static String randomBases(long seed, int length) {
        Random random = new Random(seed);
        char[] alphabet = {'A', 'C', 'G', 'T'};
        char[] bases = new char[length];
        for (int index = 0; index < length; index++) bases[index] = alphabet[random.nextInt(alphabet.length)];
        return new String(bases);
    }

    private static byte[] fasta(String name, String sequence) {
        StringBuilder result = new StringBuilder(">" + name + "\n");
        for (int offset = 0; offset < sequence.length(); offset += 80) {
            result.append(sequence, offset, Math.min(offset + 80, sequence.length())).append('\n');
        }
        return result.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] gzip(byte[] bytes) throws Exception {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(result)) { gzip.write(bytes); }
        return result.toByteArray();
    }

    private static byte[] zip(byte[] bytes) throws Exception {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(result)) {
            ZipEntry entry = new ZipEntry("fixture.fna");
            entry.setTime(0L);
            zip.putNextEntry(entry);
            zip.write(bytes);
            zip.closeEntry();
        }
        return result.toByteArray();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    static PipelineStageTaskDispatcher daemonDispatcher() { return new DaemonDispatcher(); }

    /** No overrides of addTask/run: the production queue and execution loop are exercised. */
    static final class DaemonDispatcher extends PipelineStageTaskDispatcher {
        private final AtomicBoolean started = new AtomicBoolean();

        @Override public void init() {
            if (!started.compareAndSet(false, true)) return;
            for (int index = 0; index < 4; index++) {
                Thread worker = new Thread(this, "comparison-http-test-worker-" + index);
                worker.setDaemon(true);
                worker.start();
            }
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

    static class IsolatedEnvironment implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override public void initialize(ConfigurableApplicationContext context) {
            if (!Boolean.getBoolean("comparison.http.integration")) {
                throw new IllegalStateException("Explicit comparison.http.integration=true is required");
            }
            try {
                Path parent = Files.createDirectories(Path.of("target/comparison-http-integration").toAbsolutePath());
                root = Files.createTempDirectory(parent, "r-").toRealPath();
                // The helper creates root/mysql and owns an idempotent shutdown hook.
                mysql = ComparisonIsolatedMysql.start(root);
                TestPropertyValues.of(
                        "spring.datasource.url=" + mysql.jdbcUrl(),
                        "spring.datasource.username=" + mysql.username(),
                        "spring.datasource.password=" + mysql.password(),
                        "localstorageService.baseDir=" + root.resolve("storage"),
                        "localstorageService.temporaryPath=" + root.resolve("temporary"),
                        "analysis-pipeline.stage.baseInputDir=" + root.resolve("stage/input"),
                        "analysis-pipeline.stage.baseWorkDir=" + root.resolve("stage/work"),
                        "refSeqService.base=" + root.resolve("refseq-service"),
                        "refSeqService.temporaryPath=" + root.resolve("refseq-service/temp"),
                        "refSeqService.nonInnerRefSeqDir=" + root.resolve("refseq-service/nonInnerRefSeqDir"),
                        "refSeqService.refSeqIndexDir=" + root.resolve("refseq-service/index"),
                        "refSeqService.virusRefseqsDir=" + root.resolve("refseq-service/virus/refseqs"),
                        "refSeqService.virusIndexPath=" + root.resolve("refseq-service/virus/index"))
                        .applyTo(context.getEnvironment());
                Files.createDirectories(root.resolve("storage"));
                System.out.println("Starting isolated Comparison HTTP test under " + root);
            } catch (Exception error) {
                if (mysql != null) {
                    try { mysql.close(); }
                    catch (Exception cleanup) { error.addSuppressed(cleanup); }
                }
                throw new IllegalStateException("Unable to initialize isolated Comparison HTTP test", error);
            }
        }
    }
}
