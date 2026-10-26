package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xjtlu.bio.controller.PipelineInputBatchController;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineInputFileExample;
import com.xjtlu.bio.entity.BioProject;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioPipelineInputFileMapper;
import com.xjtlu.bio.mapper.BioProjectMapper;

/**
 * Opt-in upload-only integration: real HTTP/Tomcat, MyBatis, MySQL transactions
 * and local storage. No analysis services, workers, schedulers or external tools
 * are loaded. The existing helper reads development DDL only, then owns a new
 * loopback database under target/batch-http; no development rows are changed.
 *
 * Run separately with -Dbatch.upload.http.integration=true
 * -Dtest=PipelineInputBatchHttpIntegrationTest. Retains isolated evidence files.
 */
@EnabledIfSystemProperty(named = "batch.upload.http.integration", matches = "true")
@SpringBootTest(classes = PipelineInputBatchHttpIntegrationTest.UploadTestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"server.address=127.0.0.1", "spring.main.banner-mode=off", "debug=false",
                "spring.datasource.hikari.maximum-pool-size=4",
                "spring.datasource.hikari.connection-timeout=5000", "logging.level.root=WARN",
                "logging.level.com.xjtlu.bio=INFO"})
@ActiveProfiles("batch-http-test")
@ContextConfiguration(initializers = PipelineInputBatchHttpIntegrationTest.IsolatedEnvironment.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
class PipelineInputBatchHttpIntegrationTest {

    private static final byte[] R1 = "@fixture/1\nACGT\n+\nIIII\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] R2 = "@fixture/2\nTGCA\n+\nIIII\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ASSEMBLY = ">software_fixture\nACGTACGT\n".getBytes(StandardCharsets.US_ASCII);
    private static Path root;
    private static ComparisonIsolatedMysql mysql;

    @LocalServerPort private int port;
    @Autowired private ObjectMapper json;
    @Autowired private DataSource dataSource;
    @Autowired private ConfigurableApplicationContext application;
    @Autowired private BioAnalysisPipelineMapper pipelines;
    @Autowired private BioPipelineInputFileMapper inputs;
    @Autowired private BioProjectMapper projects;
    @Autowired private FaultInjectingStorage storage;

    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final List<JsonNode> requests = new ArrayList<>();
    private JdbcTemplate jdbc;
    private long projectId;

    @BeforeAll
    void verifyIsolation() throws Exception {
        try (var connection = dataSource.getConnection()) {
            assertEquals(mysql.jdbcUrl().split("\\?", 2)[0],
                    connection.getMetaData().getURL().split("\\?", 2)[0]);
        }
        jdbc = new JdbcTemplate(dataSource);
        jdbc.setQueryTimeout(10);
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM bio_analysis_pipeline", Integer.class));
        assertEquals(0, application.getBeanNamesForType(PipelineStageTaskDispatcher.class).length);
        assertEquals(0, application.getBeanNamesForType(PipelineService.class).length);
        BioProject project = new BioProject();
        project.setProjectName("BATCH_HTTP_TEST_" + UUID.randomUUID());
        project.setDescription("Isolated upload-only software fixture");
        assertEquals(1, projects.insertSelective(project));
        projectId = project.getPid();
        System.out.println("Batch upload HTTP integration evidence: " + root);
    }

    @AfterEach
    void resetFaultsAndVerifyNoExecution() throws Exception {
        storage.resetFaults();
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM bio_pipeline_stage", Integer.class));
        writeReport(false);
    }

    @Test
    void acceptsInitialPairedSingleAndAssemblyUploadsAndPreservesReferenceInputs() throws Exception {
        long pairedId = pipeline(100, 3, null);
        BioPipelineInputFile reference = seedFile(pairedId, 20, "reference", "reference.fna", ASSEMBLY);
        Reply pair = upload(pairedId, 0, file("sample/0/0", "r1", "reads.fastq", R1),
                file("sample/0/1", "r2", "reads.fastq", R2));
        assertSuccess(pair, pairedId, 0, 2);
        List<BioPipelineInputFile> paired = sampleFiles(pairedId);
        assertEquals(2, paired.stream().map(BioPipelineInputFile::getFilePath).distinct().count());
        assertArrayEquals(R1, bytes(paired.get(0)));
        assertArrayEquals(R2, bytes(paired.get(1)));
        assertNotNull(inputs.selectByPrimaryKey(reference.getInputFileId()));
        assertArrayEquals(ASSEMBLY, bytes(reference));

        long singleId = pipeline(101, 3, null);
        Reply single = upload(singleId, 0, file("sample/0/0", "single", "single.fastq", R1));
        assertSuccess(single, singleId, 0, 1);
        assertArrayEquals(R1, bytes(sampleFiles(singleId).get(0)));
        assertEquals(List.of("sample/0/0"), sampleFiles(singleId).stream().map(BioPipelineInputFile::getInputKey).toList());

        long assemblyId = pipeline(200, 3, null);
        Reply assembly = upload(assemblyId, 1, file("sample/0/0", "contigs", "contigs.fasta", ASSEMBLY));
        assertSuccess(assembly, assemblyId, 1, 1);
        assertEquals(11, sampleFiles(assemblyId).get(0).getFileRole());
        assertArrayEquals(ASSEMBLY, bytes(sampleFiles(assemblyId).get(0)));
        for (long id : List.of(pairedId, singleId, assemblyId)) {
            assertEquals(3, pipelines.selectByPrimaryKey(id).getStatus());
        }
    }

    @Test
    void storesPlainGzipAndZipWithoutChangingBytesForAllInitialPipelineTypes() throws Exception {
        List<UploadFile> variants = List.of(file("sample/0/0", "input", "fixture.fasta", ASSEMBLY),
                file("sample/0/0", "input", "fixture.fasta.gz", gzip(ASSEMBLY)),
                file("sample/0/0", "input", "fixture.fasta.zip", zip(ASSEMBLY)));
        int[] types = {100, 101, 200};
        for (int index = 0; index < variants.size(); index++) {
            long id = pipeline(types[index], 3, null);
            UploadFile file = variants.get(index);
            assertSuccess(upload(id, 1, file), id, 1, 1);
            BioPipelineInputFile saved = sampleFiles(id).get(0);
            assertArrayEquals(file.bytes(), bytes(saved));
            assertTrue(saved.getFilePath().startsWith("pipelineInput/p" + id + "/batches/"));
            assertTrue(saved.getFilePath().endsWith("/" + file.filename()));
        }
    }

    @Test
    void acceptsBrowserFormDataJsonBlobManifest() throws Exception {
        long id = pipeline(100, 3, null);
        UploadFile file = file("sample/0/0", "r1", "reads.fastq", R1);
        assertSuccess(send(manifest(id, 0, file), List.of(file), true), id, 0, 1);
        assertArrayEquals(R1, bytes(sampleFiles(id).get(0)));
    }

    @Test
    void secondStorageFailureRollsBackAndAllowsRetryButNotReplacementAfterSuccess() throws Exception {
        long id = pipeline(100, 3, null);
        Set<Path> before = storedFiles();
        storage.failWriteNumber(2);
        Reply reply = upload(id, 0, file("sample/0/0", "r1", "r1.fastq", R1),
                file("sample/0/1", "r2", "r2.fastq", R2));
        assertFailure(reply, 500, 2);
        assertTrue(sampleFiles(id).isEmpty());
        assertEquals(before, storedFiles());

        storage.resetFaults();
        assertSuccess(upload(id, 0, file("sample/0/0", "r1", "r1.fastq", R1),
                file("sample/0/1", "r2", "r2.fastq", R2)), id, 0, 2);
        List<BioPipelineInputFile> committed = sampleFiles(id);
        Set<Path> afterSuccess = storedFiles();
        assertFailure(upload(id, 0, file("sample/0/0", "r1", "r1.fastq", R1),
                file("sample/0/1", "r2", "r2.fastq", R2)), 200, 1);
        assertOldPairUnchanged(id, committed);
        assertEquals(afterSuccess, storedFiles());
    }

    @Test
    void existingPairRejectsRepeatUploadAndLayoutChangesWithoutWritingNewObjects() throws Exception {
        long id = pipeline(200, 3, null);
        List<BioPipelineInputFile> old = seedPair(id);
        storage.resetFaults();
        Set<Path> before = storedFiles();
        assertFailure(upload(id, 0, file("sample/0/0", "r1", "new_R1.fastq", R2),
                file("sample/0/1", "r2", "new_R2.fastq", R1)), 200, 1);
        assertFailure(upload(id, 0, file("sample/0/0", "single", "new_single.fastq", R2)), 200, 1);
        assertFailure(upload(id, 1, file("sample/0/0", "assembly", "new.fna", ASSEMBLY)), 200, 1);
        assertOldPairUnchanged(id, old);
        assertEquals(before, storedFiles());
        assertEquals(0, storage.writeCount());
    }

    @Test
    void secondDatabaseInsertFailureRollsBackEntireInitialUpload() throws Exception {
        long id = pipeline(100, 3, null);
        Set<Path> before = storedFiles();
        jdbc.execute("CREATE TRIGGER batch_test_reject_r2 BEFORE INSERT ON bio_pipeline_input_file "
                + "FOR EACH ROW BEGIN IF NEW.pipeline_id = " + id
                + " AND NEW.input_key = 'sample/0/1' THEN SIGNAL SQLSTATE '45000' "
                + "SET MESSAGE_TEXT = 'injected batch R2 insert failure'; END IF; END");
        try {
            assertFailure(upload(id, 0, file("sample/0/0", "r1", "next_R1.fastq", R2),
                    file("sample/0/1", "r2", "next_R2.fastq", R1)), 500, 2);
            assertTrue(sampleFiles(id).isEmpty());
            assertEquals(before, storedFiles());
        } finally {
            jdbc.execute("DROP TRIGGER batch_test_reject_r2");
        }
    }

    @Test
    void legacySingleReadInputBlocksBatchAppendOrTypeChange() throws Exception {
        long id = pipeline(101, 3, null);
        BioPipelineInputFile old = seedFile(id, 10, "sample/0/0", "old_R1.fastq", R1);
        storage.resetFaults();
        Set<Path> before = storedFiles();
        assertFailure(upload(id, 0, file("sample/0/0", "r1", "next_R1.fastq", R2),
                file("sample/0/1", "r2", "next_R2.fastq", R1)), 200, 1);
        assertFailure(upload(id, 1, file("sample/0/0", "assembly", "next.fna", ASSEMBLY)), 200, 1);
        assertEquals(List.of(old.getInputFileId()), sampleFiles(id).stream().map(BioPipelineInputFile::getInputFileId).toList());
        assertEquals(10, sampleFiles(id).get(0).getFileRole());
        assertArrayEquals(R1, bytes(old));
        assertEquals(before, storedFiles());
        assertEquals(0, storage.writeCount());
    }

    @Test
    void existingAssemblyInputBlocksRepeatedAssemblyOrReadUpload() throws Exception {
        long id = pipeline(100, 3, null);
        BioPipelineInputFile old = seedFile(id, 11, "sample/0/0", "old.fna", ASSEMBLY);
        storage.resetFaults();
        Set<Path> before = storedFiles();
        assertFailure(upload(id, 1, file("sample/0/0", "assembly", "next.fna", ASSEMBLY)), 200, 1);
        assertFailure(upload(id, 0, file("sample/0/0", "r1", "next.fastq", R1)), 200, 1);
        assertEquals(List.of(old.getInputFileId()), sampleFiles(id).stream().map(BioPipelineInputFile::getInputFileId).toList());
        assertEquals(11, sampleFiles(id).get(0).getFileRole());
        assertArrayEquals(ASSEMBLY, bytes(old));
        assertEquals(before, storedFiles());
        assertEquals(0, storage.writeCount());
    }

    @Test
    void inputInsertedWhileFilesAreStoredIsPreservedByTransactionalRecheck() throws Exception {
        long id = pipeline(100, 3, null);
        Set<Path> before = storedFiles();
        BioPipelineInputFile[] concurrent = new BioPipelineInputFile[1];
        storage.afterWriteNumber(1, () -> concurrent[0] = seedFile(id, 10, "sample/0/0", "concurrent.fastq", R1));
        assertFailure(upload(id, 0, file("sample/0/0", "r1", "next_R1.fastq", R2),
                file("sample/0/1", "r2", "next_R2.fastq", R1)), 200, 3);
        assertNotNull(concurrent[0]);
        assertEquals(List.of(concurrent[0].getInputFileId()),
                sampleFiles(id).stream().map(BioPipelineInputFile::getInputFileId).toList());
        assertArrayEquals(R1, bytes(concurrent[0]));
        Set<Path> expected = new java.util.HashSet<>(before);
        expected.add(root.resolve("storage").resolve(concurrent[0].getFilePath()));
        assertEquals(expected, storedFiles());
    }

    @Test
    void rejectsStartedUnsupportedAndDerivedPipelinesWithoutWrites() throws Exception {
        long parent = pipeline(100, 3, null);
        List<Long> rejected = List.of(pipeline(100, 1, null), pipeline(200, 2, null),
                pipeline(100, 3, parent), pipeline(301, 3, null), pipeline(102, 3, null));
        Set<Path> before = storedFiles();
        for (long id : rejected) {
            assertFailure(upload(id, 0, file("sample/0/0", "r1", "r1.fastq", R1)), 200, 1);
            assertTrue(sampleFiles(id).isEmpty());
        }
        assertEquals(before, storedFiles());
    }

    @Test
    void invalidWholeRequestDoesNotPublishPartialOrSingleEndInputs() throws Exception {
        long id = pipeline(100, 3, null);
        Set<Path> before = storedFiles();
        UploadFile first = file("sample/0/0", "r1", "r1.fastq", R1);
        UploadFile second = file("sample/0/1", "r2", "r2.fastq", R2);
        assertParameterFailure(send(manifest(id, 0, first, second), List.of(first)));
        assertParameterFailure(upload(id, 0, first, file("sample/0/1", "r2", "r2.fastq", new byte[0])));
        assertParameterFailure(upload(id, 0, second));
        assertParameterFailure(upload(id, 1, file("sample/0/0", "r1", "contigs.fna", ASSEMBLY),
                file("sample/0/1", "r2", "more.fna", ASSEMBLY)));
        ObjectNode missingLevel = manifest(id, 0, first);
        missingLevel.remove("sequenceLevel");
        assertParameterFailure(send(missingLevel, List.of(first)));
        ObjectNode oldFileType = manifest(id, 0, first);
        ((ObjectNode) oldFileType.path("inputs").get(0)).put("fileType", 10);
        assertParameterFailure(send(oldFileType, List.of(first)));
        assertTrue(sampleFiles(id).isEmpty());
        assertEquals(before, storedFiles());
    }

    private long pipeline(int type, int status, Long parent) {
        BioAnalysisPipeline pipeline = new BioAnalysisPipeline();
        pipeline.setPipelineType(type);
        pipeline.setStatus(status);
        pipeline.setAnalysisPipelineName("BATCH_TEST_" + UUID.randomUUID());
        pipeline.setProjectId(projectId);
        pipeline.setParentPipelineId(parent);
        pipeline.setCreateTime(new Date());
        pipeline.setPipelineParameters("{}");
        assertEquals(1, pipelines.insertSelective(pipeline));
        return pipeline.getPipelineId();
    }

    private List<BioPipelineInputFile> seedPair(long id) {
        return List.of(seedFile(id, 10, "sample/0/0", "old_R1.fastq", R1),
                seedFile(id, 10, "sample/0/1", "old_R2.fastq", R2));
    }

    private BioPipelineInputFile seedFile(long id, int role, String key, String name, byte[] bytes) {
        BioPipelineInputFile row = new BioPipelineInputFile();
        row.setPipelineId(id);
        row.setFileRole(role);
        row.setInputKey(key);
        row.setFileName(name);
        row.setOriginalFilename(name);
        row.setStatus(1);
        row.setCreateTime(new Date());
        row.setFilePath("pipelineInput/p" + id + "/fixture/" + UUID.randomUUID() + "/" + name);
        assertTrue(storage.putObject(row.getFilePath(), new ByteArrayInputStream(bytes),
                StorageService.PutOptions.builder().overwrite(false).build()).success());
        assertEquals(1, inputs.insertSelective(row));
        return row;
    }

    private List<BioPipelineInputFile> sampleFiles(long id) {
        BioPipelineInputFileExample query = new BioPipelineInputFileExample();
        query.createCriteria().andPipelineIdEqualTo(id).andFileRoleIn(List.of(10, 11));
        query.setOrderByClause("input_key");
        return inputs.selectByExample(query);
    }

    private void assertOldPairUnchanged(long id, List<BioPipelineInputFile> old) throws Exception {
        assertEquals(old.stream().map(BioPipelineInputFile::getInputFileId).toList(),
                sampleFiles(id).stream().map(BioPipelineInputFile::getInputFileId).toList());
        assertArrayEquals(R1, bytes(old.get(0)));
        assertArrayEquals(R2, bytes(old.get(1)));
        assertTrue(sampleFiles(id).stream().allMatch(row -> row.getFileRole() == 10));
    }

    private byte[] bytes(BioPipelineInputFile row) throws IOException {
        return Files.readAllBytes(root.resolve("storage").resolve(row.getFilePath()));
    }

    private Set<Path> storedFiles() throws IOException {
        try (var files = Files.walk(root.resolve("storage"))) {
            return files.filter(Files::isRegularFile).collect(Collectors.toSet());
        }
    }

    private UploadFile file(String inputKey, String part, String filename, byte[] bytes) {
        return new UploadFile(inputKey, part, filename, bytes);
    }

    private ObjectNode manifest(long id, int level, UploadFile... files) {
        ObjectNode manifest = json.createObjectNode().put("pipelineId", id).put("sequenceLevel", level);
        ArrayNode list = manifest.putArray("inputs");
        for (UploadFile file : files) list.addObject().put("inputKey", file.inputKey()).put("filePart", file.part());
        return manifest;
    }

    private Reply upload(long id, int level, UploadFile... files) throws Exception {
        return send(manifest(id, level, files), List.of(files));
    }

    private Reply send(ObjectNode manifest, List<UploadFile> files) throws Exception {
        return send(manifest, files, false);
    }

    private Reply send(ObjectNode manifest, List<UploadFile> files, boolean blobManifest) throws Exception {
        String boundary = "BatchTestBoundary" + UUID.randomUUID().toString().replace("-", "");
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"manifest\""
                + (blobManifest ? "; filename=\"blob\"" : "") + "\r\n"
                + "Content-Type: application/json\r\n\r\n" + manifest + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (UploadFile file : files) {
            body.write(("--" + boundary + "\r\nContent-Disposition: form-data; name=\"" + file.part()
                    + "\"; filename=\"" + file.filename() + "\"\r\nContent-Type: application/octet-stream\r\n\r\n")
                    .getBytes(StandardCharsets.UTF_8));
            body.write(file.bytes());
            body.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        body.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/pipelineInput/uploadBatch"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .timeout(Duration.ofSeconds(30)).POST(HttpRequest.BodyPublishers.ofByteArray(body.toByteArray())).build();
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        JsonNode parsed = json.readTree(response.body());
        ObjectNode entry = json.createObjectNode().put("httpStatus", response.statusCode());
        entry.set("manifest", manifest.deepCopy());
        entry.set("response", parsed);
        requests.add(entry);
        return new Reply(response.statusCode(), parsed);
    }

    private void assertSuccess(Reply reply, long id, int level, int count) {
        assertEquals(200, reply.httpStatus(), reply.body().toString());
        assertEquals(0, reply.body().path("status").asInt(-1), reply.body().toString());
        JsonNode data = reply.body().path("data");
        assertEquals(id, data.path("pipelineId").asLong());
        assertEquals(level, data.path("sequenceLevel").asInt(-1));
        assertEquals(count, data.path("inputs").size());
        assertFalse(data.has("removedInputFileIds"), "Initial upload must not expose replacement result fields");
        assertEquals(count, sampleFiles(id).size());
        for (JsonNode input : data.path("inputs")) assertTrue(input.path("inputFileId").asLong() > 0);
    }

    private void assertFailure(Reply reply, int httpStatus, int status) {
        assertEquals(httpStatus, reply.httpStatus(), reply.body().toString());
        assertEquals(status, reply.body().path("status").asInt(-1), reply.body().toString());
        assertTrue(reply.body().path("data").isNull(), reply.body().toString());
        assertFalse(reply.body().path("failMsg").asText().isBlank());
    }

    private void assertParameterFailure(Reply reply) {
        assertTrue(reply.httpStatus() == 200 || reply.httpStatus() == 400, reply.body().toString());
        assertEquals(4, reply.body().path("status").asInt(-1), reply.body().toString());
        assertTrue(reply.body().path("data").isNull());
    }

    private static byte[] gzip(byte[] bytes) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(result)) { gzip.write(bytes); }
        return result.toByteArray();
    }

    private static byte[] zip(byte[] bytes) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(result)) {
            ZipEntry entry = new ZipEntry("fixture.fasta");
            entry.setTime(0L);
            zip.putNextEntry(entry);
            zip.write(bytes);
            zip.closeEntry();
        }
        return result.toByteArray();
    }

    private void writeReport(boolean stopped) throws Exception {
        ObjectNode report = json.createObjectNode().put("updatedAt", Instant.now().toString())
                .put("isolatedMysqlStopped", stopped).put("developmentRowsChanged", false)
                .put("scope", "Upload HTTP + MySQL transactions + LocalStorage; no analysis tools")
                .put("requestCount", requests.size());
        report.set("requests", json.valueToTree(requests));
        json.writerWithDefaultPrettyPrinter().writeValue(root.resolve("result.json").toFile(), report);
    }

    @AfterAll
    void stopIsolatedDatabase() throws Exception {
        try {
            if (mysql != null) mysql.close();
        } finally {
            if (root != null) writeReport(true);
        }
    }

    private record UploadFile(String inputKey, String part, String filename, byte[] bytes) { }
    private record Reply(int httpStatus, JsonNode body) { }

    @Configuration(proxyBeanMethods = false)
    @EnableAutoConfiguration(exclude = {SecurityAutoConfiguration.class, UserDetailsServiceAutoConfiguration.class})
    @MapperScan("com.xjtlu.bio.mapper")
    @Import({PipelineInputBatchController.class, PipelineInputBatchService.class, PipelineInputOperationCoordinator.class})
    static class UploadTestApplication {
        @Bean FaultInjectingStorage storageService() { return new FaultInjectingStorage(); }
    }

    static final class FaultInjectingStorage extends LocalStorageService {
        private final AtomicInteger writes = new AtomicInteger();
        private volatile int failWrite = -1;
        private volatile int callbackWrite = -1;
        private volatile Runnable afterWrite;

        void failWriteNumber(int number) { writes.set(0); failWrite = number; }
        int writeCount() { return writes.get(); }
        void afterWriteNumber(int number, Runnable callback) {
            writes.set(0);
            callbackWrite = number;
            afterWrite = callback;
        }
        void resetFaults() { failWrite = -1; writes.set(0); callbackWrite = -1; afterWrite = null; }

        @Override public PutResult putObject(String key, InputStream data, PutOptions options) {
            PutResult stored = super.putObject(key, data, options);
            int write = stored.success() ? writes.incrementAndGet() : -1;
            if (stored.success() && write == callbackWrite) {
                Runnable callback = afterWrite;
                callbackWrite = -1;
                afterWrite = null;
                if (callback != null) callback.run();
            }
            if (stored.success() && write == failWrite) {
                // A provider can report failure after publishing; compensation must
                // include the current key, not only earlier successful objects.
                return new PutResult(false, new IOException("Injected storage failure after second object write"));
            }
            return stored;
        }
    }

    static class IsolatedEnvironment implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override public void initialize(ConfigurableApplicationContext context) {
            if (!Boolean.getBoolean("batch.upload.http.integration")) {
                throw new IllegalStateException("Explicit batch.upload.http.integration=true is required");
            }
            try {
                Path parent = Files.createDirectories(Path.of("target/batch-http").toAbsolutePath());
                root = Files.createTempDirectory(parent, "r-").toRealPath();
                mysql = ComparisonIsolatedMysql.start(root);
                Files.createDirectories(root.resolve("storage"));
                Files.createDirectories(root.resolve("multipart"));
                TestPropertyValues.of("spring.datasource.url=" + mysql.jdbcUrl(),
                        "spring.datasource.username=" + mysql.username(),
                        "spring.datasource.password=" + mysql.password(),
                        "localstorageService.baseDir=" + root.resolve("storage"),
                        "spring.servlet.multipart.location=" + root.resolve("multipart"))
                        .applyTo(context.getEnvironment());
            } catch (Exception failure) {
                if (mysql != null) {
                    try { mysql.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                }
                throw new IllegalStateException("Unable to initialize isolated batch upload HTTP test", failure);
            }
        }
    }
}
