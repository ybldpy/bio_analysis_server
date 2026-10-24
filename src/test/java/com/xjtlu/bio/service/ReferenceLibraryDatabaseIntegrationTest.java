package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.yaml.snakeyaml.Yaml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.controller.ReferenceLibraryController;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;

/**
 * Explicitly opt-in: commits ONE disabled test reference and retains its file for inspection.
 * Only localhost/bio and the existing development storage root are permitted.
 * Uses real SQL, transactions and storage, but a standalone MockMvc HTTP harness;
 * does not start Spring Boot, schedulers, analysis tools, or a network HTTP server.
 * Omitted target scope means no target membership configuration is modified.
 * Run with -DverifyReferenceLibraryUpload=true and this test's name explicitly selected.
 */
@EnabledIfSystemProperty(named = "verifyReferenceLibraryUpload", matches = "true")
class ReferenceLibraryDatabaseIntegrationTest {

    private static final Path DEVELOPMENT_STORAGE = Path.of("/home/jcy/bio_app_data/storage");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void customUploadCommitsDisabledRecordAndStoresExactFileBytes() throws Exception {
        Map<?, ?> config;
        try (var input = Files.newInputStream(Path.of("src/main/resources/application-dev.yml"))) {
            config = new Yaml().load(input);
        }
        DriverManagerDataSource datasource = developmentDatasource(config);
        Map<?, ?> storageConfig = (Map<?, ?>) config.get("localstorageService");
        Path storageRoot = Path.of((String) storageConfig.get("baseDir")).toAbsolutePath().normalize();
        assertEquals(DEVELOPMENT_STORAGE, storageRoot, "Unexpected development storage root");
        assertTrue(Files.isDirectory(storageRoot, LinkOption.NOFOLLOW_LINKS));
        assertEquals(storageRoot, storageRoot.toRealPath(), "Do not redirect this test through a symlink");

        // taxId=562/orgType=2 are ONLY request metadata for this disabled format fixture.
        // The artificial bytes are not an actual reference for that organism.
        // Do not depend on legacy rows' org_type values or modify them for this test.
        final int taxId = 562;
        final int orgType = 2;
        long referenceCountBefore;
        try (var connection = datasource.getConnection()) {
            connection.setReadOnly(true);
            try (var statement = connection.prepareStatement("SELECT COUNT(*) FROM bio_reference_sequence");
                    var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                referenceCountBefore = rows.getLong(1);
            }
        }
        System.out.println("Development library rows before upload: " + referenceCountBefore);

        Configuration mybatis = new Configuration();
        mybatis.setLogImpl(NoLoggingImpl.class);
        mybatis.addMapper(BioPipelineTargetMapper.class);
        mybatis.addMapper(BioReferenceTaxonomyMemberMapper.class);
        SqlSessionFactoryBean factory = new SqlSessionFactoryBean();
        factory.setDataSource(datasource);
        factory.setConfiguration(mybatis);
        factory.setMapperLocations(new ClassPathResource("mapper/BioReferenceSequenceMapper.xml"));
        SqlSessionTemplate session = new SqlSessionTemplate(factory.getObject());
        BioReferenceTaxonomyMemberMapper members = session.getMapper(BioReferenceTaxonomyMemberMapper.class);
        ReferenceTargetScopeService scopes = new ReferenceTargetScopeService(
                session.getMapper(BioPipelineTargetMapper.class), members, new ReferenceTaxonomyResolver(members));
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        ReferenceLibraryService service = new ReferenceLibraryService(
                session.getMapper(BioReferenceSequenceMapper.class), storage,
                new DataSourceTransactionManager(datasource), scopes);
        MockMvc http = MockMvcBuilders.standaloneSetup(new ReferenceLibraryController(service)).build();

        String marker = "UPLOAD_INTEGRATION_TEST_ONLY_" + UUID.randomUUID();
        String filename = marker + ".fna";
        byte[] fixture;
        try (var input = new ClassPathResource("reference-library/custom-upload-test.fna").getInputStream()) {
            fixture = input.readAllBytes();
        }
        String expectedHash = sha256(fixture);
        // Print this before writing, so even a failed verification can be reconciled by marker.
        System.out.println("Custom upload integration marker: " + marker);
        byte[] responseBytes = http.perform(multipart("/referenceGenome/library/upload")
                        .file(new MockMultipartFile("file", filename, "text/plain", fixture))
                        .param("taxId", Integer.toString(taxId))
                        .param("orgType", Integer.toString(orgType))
                        .param("organismName", marker)
                        .param("enabled", "false"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andReturn().getResponse().getContentAsByteArray();
        JsonNode response = JSON.readTree(responseBytes);
        JsonNode item = response.path("data");
        long referenceId = item.path("referenceId").longValue();
        assertTrue(referenceId > 0);
        String accession = item.path("accession").asText();
        assertTrue(accession.matches("CUSTOM_[0-9a-f]{32}"));
        assertEquals("USER_UPLOAD", item.path("sourceDb").asText());
        assertTrue(item.path("enabled").isBoolean());
        assertFalse(item.path("enabled").booleanValue());
        assertEquals(taxId, item.path("taxId").intValue());
        assertEquals(orgType, item.path("orgType").intValue());
        assertEquals(marker, item.path("organismName").asText());
        assertEquals(64, item.path("genomeLength").intValue());
        String objectKey = item.path("fastaObjectName").asText();
        assertTrue(objectKey.matches("refseq/custom/[0-9a-f-]{36}/reference\\.fna"));

        // A new JDBC connection, not the insert session/entity, proves the transaction committed.
        try (var connection = datasource.getConnection()) {
            connection.setReadOnly(true);
            try (var statement = connection.prepareStatement("""
                    SELECT reference_id, accession, source_db, tax_id, org_type, organism_name,
                           genome_length, is_annotated, enabled, path, raw_metadata
                    FROM bio_reference_sequence WHERE organism_name = ?
                    """)) {
                statement.setString(1, marker);
                try (var row = statement.executeQuery()) {
                    assertTrue(row.next(), "Committed test record was not found by an independent connection");
                    assertEquals(referenceId, row.getLong("reference_id"));
                    assertEquals(accession, row.getString("accession"));
                    assertEquals("USER_UPLOAD", row.getString("source_db"));
                    assertEquals(taxId, row.getInt("tax_id"));
                    assertEquals(orgType, row.getInt("org_type"));
                    assertEquals(marker, row.getString("organism_name"));
                    assertEquals(64, row.getInt("genome_length"));
                    assertNotNull(row.getObject("enabled"));
                    assertFalse(row.getBoolean("enabled"));
                    assertNotNull(row.getObject("is_annotated"));
                    assertFalse(row.getBoolean("is_annotated"));
                    assertEquals(objectKey, row.getString("path"));
                    JsonNode metadata = JSON.readTree(row.getString("raw_metadata"));
                    assertEquals(filename, metadata.path("originalFilename").asText());
                    assertEquals(1, metadata.path("recordCount").intValue());
                    assertEquals(expectedHash, metadata.path("sha256").asText());
                    Instant.parse(metadata.path("importedAt").asText());
                    assertFalse(row.next(), "Exactly one test record should be inserted");
                }
            }
        }

        Path storedFile = storage.getObject(objectKey).toAbsolutePath().normalize();
        assertTrue(storedFile.startsWith(storageRoot));
        assertTrue(Files.isRegularFile(storedFile, LinkOption.NOFOLLOW_LINKS));
        assertTrue(storedFile.toRealPath().startsWith(storageRoot));
        assertTrue(storage.isReadableObject(objectKey));
        assertEquals(fixture.length, Files.size(storedFile));
        byte[] storedBytes = Files.readAllBytes(storedFile);
        assertArrayEquals(fixture, storedBytes);
        assertEquals(expectedHash, sha256(storedBytes));

        JsonNode detail = JSON.readTree(http.perform(get("/referenceGenome/library/{id}", referenceId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value(Result.SUCCESS))
                .andReturn().getResponse().getContentAsByteArray());
        assertEquals(item, detail.path("data"), "Detail API must read back the persisted upload");

        Path reportDir = Path.of("target/reference-library-upload-integration", marker);
        Files.createDirectories(reportDir);
        Path downloaded = reportDir.resolve("downloaded-reference.fna").toAbsolutePath();
        StorageService.GetObjectResult download = storage.getObject(objectKey, downloaded.toString());
        assertTrue(download.success(), "Stored file must be retrievable through StorageService");
        assertArrayEquals(fixture, Files.readAllBytes(downloaded));

        var report = JSON.createObjectNode();
        report.put("verifiedAt", Instant.now().toString());
        report.put("testMarker", marker);
        report.put("referenceCountBefore", referenceCountBefore);
        report.set("uploadResponse", response);
        report.put("filePath", storedFile.toString());
        report.put("fileBytes", storedBytes.length);
        report.put("sha256", expectedHash);
        report.put("independentDatabaseReadVerified", true);
        report.put("exactStoredBytesVerified", true);
        report.put("detailApiVerified", true);
        report.put("storageDownloadVerified", true);
        report.put("retainedDisabledTestRecord", true);
        report.put("note", "Synthetic storage fixture only; not a biological reference; target membership unchanged.");
        Path reportPath = reportDir.resolve("result.json");
        JSON.writerWithDefaultPrettyPrinter().writeValue(reportPath.toFile(), report);
        System.out.println("Custom upload verified: referenceId=" + referenceId + ", accession=" + accession);
        System.out.println("Stored file: " + storedFile);
        System.out.println("Integration report: " + reportPath.toAbsolutePath());
    }

    private static DriverManagerDataSource developmentDatasource(Map<?, ?> config) {
        Map<?, ?> datasourceConfig = (Map<?, ?>) ((Map<?, ?>) config.get("spring")).get("datasource");
        String url = (String) datasourceConfig.get("url");
        assertTrue(url != null && url.startsWith("jdbc:mysql://"), "A MySQL development datasource is required");
        URI endpoint = URI.create(url.substring("jdbc:".length()));
        assertTrue(List.of("localhost", "127.0.0.1").contains(endpoint.getHost()), "Only localhost is permitted");
        assertEquals("/bio", endpoint.getPath(), "Only the bio development schema is permitted");
        DriverManagerDataSource datasource = new DriverManagerDataSource();
        datasource.setUrl(url);
        datasource.setUsername(String.valueOf(datasourceConfig.get("username")));
        datasource.setPassword(String.valueOf(datasourceConfig.get("password")));
        Properties options = new Properties();
        options.setProperty("connectTimeout", "5000");
        options.setProperty("socketTimeout", "10000");
        datasource.setConnectionProperties(options);
        return datasource;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
