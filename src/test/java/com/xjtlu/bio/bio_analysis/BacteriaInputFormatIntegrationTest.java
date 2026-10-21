package com.xjtlu.bio.bio_analysis;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;

/**
 * Real bacterial DAG, with one fixed paired-read fixture wrapped in three formats.
 * Requires explicitly supplied benign fixture paths; no downloads or database writes.
 * The default 100,000-pair prefix bounds runtime; this tests transport equivalence,
 * not the biological accuracy of a reduced-coverage sample.
 */
@EnabledIfSystemProperty(named = "bacteria.formats.integration", matches = "true")
class BacteriaInputFormatIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @Timeout(value = 90, unit = TimeUnit.MINUTES)
    void identicalReadsHaveEquivalentOutputsAcrossPlainGzipAndZip() throws Exception {
        Path r1Source = requiredPath("bacteria.formats.r1");
        Path r2Source = requiredPath("bacteria.formats.r2");
        int pairs = Integer.getInteger("bacteria.formats.pairs", 100_000);
        assertTrue(pairs > 0);
        Path parent = Files.createDirectories(Path.of("target/bacteria-input-format-integration")).toAbsolutePath();
        Path root = Files.createTempDirectory(parent, "run-");
        Path fixtures = Files.createDirectory(root.resolve("fixtures"));
        byte[] r1 = prefix(r1Source, pairs);
        byte[] r2 = prefix(r2Source, pairs);
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("scope", "Same paired input; real complete bacterial DAG; in-memory persistence only");
        report.put("sourceR1", r1Source.toString());
        report.put("sourceR2", r2Source.toString());
        report.put("pairs", pairs);
        report.put("decodedR1Sha256", hash(r1));
        report.put("decodedR2Sha256", hash(r2));
        List<Map<String, String>> cases = new ArrayList<>();
        for (String format : List.of("plain", "gzip", "zip")) {
            Path dir = Files.createDirectory(fixtures.resolve(format));
            Path first = wrap(dir, "sample_R1.fastq", r1, format);
            Path second = wrap(dir, "sample_R2.fastq", r2, format);
            assertEquals(hash(r1), decodedHash(first));
            assertEquals(hash(r2), decodedHash(second));
            cases.add(Map.of("name", format, "r1", first.toString(), "r2", second.toString()));
        }
        Path manifest = root.resolve("manifest.json");
        JSON.writerWithDefaultPrettyPrinter().writeValue(manifest.toFile(), Map.of("cases", cases));
        report.put("manifest", manifest.toString());
        report.put("status", "RUNNING");
        writeReport(root, report);
        System.out.println("Full pipeline format comparison: " + root);

        String oldManifest = System.getProperty("bacteria.integration.manifest");
        String oldReportDir = System.getProperty("bacteria.integration.reportDir");
        try {
            System.setProperty("bacteria.integration.manifest", manifest.toString());
            System.setProperty("bacteria.integration.reportDir", root.resolve("pipelines").toString());
            new BacteriaPipelineStageIntegrationTest().runCompleteReadLevelBacterialPipelines();
            Path pipelineRoot;
            try (var dirs = Files.list(root.resolve("pipelines"))) {
                pipelineRoot = dirs.filter(Files::isDirectory).findFirst().orElseThrow();
            }
            report.put("pipelineSummary", pipelineRoot.resolve("summary.json").toString());
            List<Map<String, Object>> comparisons = new ArrayList<>();
            report.put("comparisons", comparisons);
            JsonNode baseline = JSON.readTree(pipelineRoot.resolve("plain/report.json").toFile());
            for (String format : List.of("plain", "gzip", "zip")) {
                JsonNode actual = JSON.readTree(pipelineRoot.resolve(format + "/report.json").toFile());
                comparisons.add(compare(baseline, actual, format));
                writeReport(root, report);
            }
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL");
            report.put("error", failure.toString());
            throw failure;
        } finally {
            restore("bacteria.integration.manifest", oldManifest);
            restore("bacteria.integration.reportDir", oldReportDir);
            writeReport(root, report);
        }
    }

    private static Map<String, Object> compare(JsonNode baseline, JsonNode actual, String format) throws Exception {
        assertEquals("COMPLETE", actual.path("status").asText(), "Workflow incomplete: " + format);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("format", format);
        result.put("pipelineStatus", actual.path("status").asText());
        result.put("durationSeconds", actual.path("durationSeconds").asDouble());
        for (JsonNode stage : actual.path("trace")) {
            assertEquals("FINISHED", stage.path("status").asText(), stage.path("name").asText());
        }
        Path baseStorage = Path.of(baseline.path("storageRoot").asText());
        Path storage = Path.of(actual.path("storageRoot").asText());
        JsonNode baseQc = output(baseline, PIPELINE_STAGE_QC);
        JsonNode qc = output(actual, PIPELINE_STAGE_QC);
        for (String mate : List.of("cleanedR1", "cleanedR2")) {
            String expected = decodedHash(baseStorage.resolve(baseQc.path(mate).asText()));
            String observed = decodedHash(storage.resolve(qc.path(mate).asText()));
            result.put(mate + "Sha256", observed);
            assertEquals(expected, observed, format + ": decoded QC output differs for " + mate);
        }
        JsonNode qcJson = JSON.readTree(storage.resolve(qc.path("reportJSON").asText()).toFile());
        JsonNode baseQcJson = JSON.readTree(baseStorage.resolve(baseQc.path("reportJSON").asText()).toFile());
        assertEquals(baseQcJson.path("filtering_result"), qcJson.path("filtering_result"));
        for (String field : List.of("before_filtering", "after_filtering")) {
            assertEquals(baseQcJson.path("summary").path(field), qcJson.path("summary").path(field));
        }
        result.put("qcInputReads", qcJson.path("summary").path("before_filtering").path("total_reads").asLong());
        result.put("qcRetainedReads", qcJson.path("summary").path("after_filtering").path("total_reads").asLong());
        JsonNode assembly = output(actual, PIPELINE_STAGE_ASSEMBLY);
        JsonNode baseAssembly = output(baseline, PIPELINE_STAGE_ASSEMBLY);
        for (String artifact : List.of("contigsUrl", "scaffoldUrl")) {
            assertEquals(baseAssembly.hasNonNull(artifact), assembly.hasNonNull(artifact));
            if (assembly.hasNonNull(artifact)) {
                String observed = decodedHash(storage.resolve(assembly.get(artifact).asText()));
                result.put(artifact + "Sha256", observed);
                assertEquals(decodedHash(baseStorage.resolve(baseAssembly.get(artifact).asText())), observed,
                        format + ": assembly differs for " + artifact);
            }
        }
        ObjectNode taxonomy = (ObjectNode) output(actual, PIPELINE_STAGE_TAXONOMY).deepCopy();
        ObjectNode baseTaxonomy = (ObjectNode) output(baseline, PIPELINE_STAGE_TAXONOMY).deepCopy();
        taxonomy.remove(List.of("mainBinFastaUrl", "contigAssignmentReportUrl"));
        baseTaxonomy.remove(List.of("mainBinFastaUrl", "contigAssignmentReportUrl"));
        assertEquals(baseTaxonomy, taxonomy, format + ": taxonomy evidence differs");
        result.put("taxonomyBest", taxonomy.path("best"));
        for (var artifact : Map.of(PIPELINE_STAGE_AMR, "amrResult", PIPELINE_STAGE_VIRULENCE, "vfTsv",
                PIPELINE_STAGE_MLST, "mlst", PIPELINE_STAGE_SEROTYPE, "serotypeResultUrl").entrySet()) {
            String key = artifact.getValue();
            String baselineUrl = output(baseline, artifact.getKey()).path(key).asText();
            String actualUrl = output(actual, artifact.getKey()).path(key).asText();
            String expected = normalizedReport(baseStorage.resolve(baselineUrl), baseline);
            String observed = normalizedReport(storage.resolve(actualUrl), actual);
            assertEquals(expected, observed, format + ": downstream report differs for " + key);
            result.put(key + "NormalizedSha256", hash(observed.getBytes(StandardCharsets.UTF_8)));
        }
        result.put("equivalent", true);
        return result;
    }

    private static String normalizedReport(Path path, JsonNode report) throws IOException {
        return Files.readString(path).replace(report.path("runRoot").asText(), "<RUN>")
                .replaceAll("/(input|work|stageOutput)/[0-9]+/", "/$1/<STAGE>/");
    }

    private static JsonNode output(JsonNode report, int type) throws IOException {
        for (JsonNode event : report.path("trace")) {
            if (event.path("stageType").asInt() == type) {
                JsonNode json = event.hasNonNull("outputInline") ? event.get("outputInline") : event.get("outputUrl");
                assertNotNull(json, "Missing stage result: " + type);
                return JSON.readTree(json.asText());
            }
        }
        throw new AssertionError("Missing stage: " + type);
    }

    private static byte[] prefix(Path source, int records) throws IOException {
        StringBuilder contents = new StringBuilder();
        try (BufferedReader reader = SequenceFileUtil.getReader(source)) {
            for (int i = 0; i < records * 4; i++) {
                String line = reader.readLine();
                assertNotNull(line, "Fixture has fewer requested records: " + source);
                contents.append(line).append('\n');
            }
        }
        return contents.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static Path wrap(Path dir, String name, byte[] bytes, String format) throws IOException {
        Path path = dir.resolve(name + (format.equals("gzip") ? ".gz" : format.equals("zip") ? ".zip" : ""));
        if (format.equals("gzip")) {
            try (var out = new GZIPOutputStream(Files.newOutputStream(path))) { out.write(bytes); }
        } else if (format.equals("zip")) {
            try (var out = new ZipOutputStream(Files.newOutputStream(path))) {
                out.putNextEntry(new ZipEntry(name));
                out.write(bytes);
                out.closeEntry();
            }
        } else {
            Files.write(path, bytes);
        }
        return path;
    }

    private static String decodedHash(Path path) throws Exception {
        try (InputStream raw = Files.newInputStream(path)) {
            InputStream decoded = raw;
            if (path.toString().endsWith(".gz")) decoded = new GZIPInputStream(raw);
            if (path.toString().endsWith(".zip")) {
                ZipInputStream zip = new ZipInputStream(raw);
                assertNotNull(zip.getNextEntry());
                decoded = zip;
            }
            try (InputStream contents = decoded) {
                MessageDigest hash = MessageDigest.getInstance("SHA-256");
                byte[] buffer = new byte[128 * 1024];
                int size;
                while ((size = contents.read(buffer)) != -1) hash.update(buffer, 0, size);
                return HexFormat.of().formatHex(hash.digest());
            }
        }
    }

    private static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
    private static Path requiredPath(String key) {
        assertNotNull(System.getProperty(key), "Set -D" + key);
        Path path = Path.of(System.getProperty(key)).toAbsolutePath();
        assertTrue(Files.isRegularFile(path), "Missing fixture: " + path);
        return path;
    }
    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key); else System.setProperty(key, value);
    }
    private static void writeReport(Path root, Map<String, Object> report) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(root.resolve("comparison.json").toFile(), report);
    }
}
