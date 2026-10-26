package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.domain.TaxonomyContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.SeroTypeStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.SeroTypingStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real stage-level format equivalence on explicitly supplied benign E. coli K-12 MG1655 files.
 * No Spring context, database service, download, mock process or biological parameter override.
 * Known fixture taxonomy is supplied directly; classification and reference truth are not tested.
 * Run serially after other ECTyper jobs; outputs/logs survive under target/assembly-input-format-integration.
 */
@EnabledIfSystemProperty(named = "assembly.formats.integration", matches = "true")
class AssemblyInputFormatIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path DEFAULT_ECTYPER_DATA = Path.of(
            "/home/jcy/.conda/envs/ectyper/lib/python3.14/site-packages/ectyper/Data");

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void realAssemblyStagesAgreeAcrossPlainGzipAndZip() throws Exception {
        Path source = requiredPath("assembly.formats.fasta");
        Path reference = requiredPath("assembly.formats.reference");
        Path cache = Path.of(System.getProperty("assembly.formats.ectyperData", DEFAULT_ECTYPER_DATA.toString()));
        Path parent = Files.createDirectories(Path.of("target/assembly-input-format-integration")).toAbsolutePath();
        Path root = Files.createTempDirectory(parent, "run-");
        Map<String, Object> report = new LinkedHashMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        report.put("scope", "Real ECTyper and reference-comparison executors; packaging equivalence, not biological accuracy");
        report.put("source", source.toString()); report.put("reference", reference.toString());
        report.put("cases", cases); report.put("status", "RUNNING");
        writeReport(root, report);
        System.out.println("Assembly-stage format comparison: " + root);
        try {
            byte[] fasta = decodedBytes(source), referenceFasta = decodedBytes(reference);
            String sourceHash = fileHash(source), referenceHash = fileHash(reference);
            report.put("sourceSha256", sourceHash); report.put("referenceSha256", referenceHash);
            report.put("decodedInputSha256", hash(fasta));
            Map<String, Object> resourceState = cacheState(cache);
            report.put("externalResourcesBefore", resourceState);
            AnalysisPipelineToolsConfig tools = tools();
            Map<String, String> baseline = null;
            for (String format : List.of("plain", "gzip", "zip")) {
                assertEquals(resourceState, cacheState(cache), "ECTyper resources changed before " + format);
                Path run = Files.createDirectory(root.resolve(format));
                Path storageRoot = Files.createDirectories(run.resolve("storage/fixture")).getParent();
                Path input = wrap(storageRoot.resolve("fixture"), fasta, format);
                Files.write(storageRoot.resolve("fixture/reference.fasta"), referenceFasta);
                assertEquals(hash(fasta), hash(decodedBytes(input)), "Input wrapping changed FASTA");
                LocalStorageService storage = new LocalStorageService();
                ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
                BioStageUtil dirs = new BioStageUtil();
                ReflectionTestUtils.setField(dirs, "stageWorkDirPath", run.resolve("work").toString());
                ReflectionTestUtils.setField(dirs, "stageInputDirPath", run.resolve("input").toString());
                Recorder recorder = new Recorder(run.resolve("processes"));
                Map<String, Object> row = new LinkedHashMap<>(); cases.add(row);
                row.put("format", format); row.put("runRoot", run.toString()); row.put("processes", recorder.records);
                row.put("status", "RUNNING"); row.put("wrappedInputSha256", fileHash(input));
                writeReport(root, report);
                SeroTypingStageExectuor serotype = new SeroTypingStageExectuor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException {
                        return recorder.run(c, w);
                    }
                };
                ReferenceComparsionStageExecutor comparison = new ReferenceComparsionStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException {
                        return recorder.run(c, w);
                    }
                };
                for (Object executor : List.of(serotype, comparison)) {
                    ReflectionTestUtils.setField(executor, "storageService", storage);
                    ReflectionTestUtils.setField(executor, "bioStageUtil", dirs);
                    ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
                }
                String key = "fixture/" + input.getFileName();
                SeroTypingStageParameters serotypeParams = serotypeParameters();
                var typed = serotype.execute(stage(101, PIPELINE_STAGE_SEROTYPE, new SeroTypeStageInputUrls(key), serotypeParams));
                row.put("serotypeSuccess", typed.isSuccess());
                assertTrue(typed.isSuccess(), typed.getErrorLog());
                ReferenceSequence sequence = new ReferenceSequence();
                sequence.setAccession("MG1655_fixture_reference"); sequence.setPath("fixture/reference.fasta");
                ReferenceComparisonStageParameters comparisonParams = new ReferenceComparisonStageParameters();
                comparisonParams.setReferenceGenome(ReferenceGenome.fromSingleSequence(sequence));
                var aligned = comparison.execute(stage(102, PIPELINE_STAGE_REFERENCE_COMPARISON,
                        new ReferenceComparisonStageInputUrls(key), comparisonParams));
                row.put("referenceComparisonSuccess", aligned.isSuccess());
                assertTrue(aligned.isSuccess(), aligned.getErrorLog());
                assertEquals(1, aligned.getStageOutput().getComparisons().size());
                var compared = aligned.getStageOutput().getComparisons().get(0);
                Map<String, Path> artifacts = Map.of("serotypeTsv", typed.getStageOutput().getResultPath(),
                        "alignmentPaf", compared.alignmentPafPath(),
                        "differenceTsv", compared.differenceTsvPath(),
                        "comparisonTsv", aligned.getStageOutput().getComparisonTsvPath());
                Map<String, String> normalized = new LinkedHashMap<>(), hashes = new LinkedHashMap<>();
                for (var artifact : artifacts.entrySet()) {
                    assertTrue(Files.size(artifact.getValue()) > 0, artifact.getKey());
                    String contents = Files.readString(artifact.getValue()).replace(run.toString(), "<RUN>");
                    normalized.put(artifact.getKey(), contents); hashes.put(artifact.getKey(), hash(contents.getBytes(StandardCharsets.UTF_8)));
                    row.put(artifact.getKey(), artifact.getValue().toString());
                }
                row.put("normalizedSha256", hashes);
                assertFalse(Files.exists(run.resolve("input/101"))); assertFalse(Files.exists(run.resolve("input/102")));
                assertEquals(hash(fasta), hash(decodedBytes(input)), "Executor modified the fixture object");
                assertEquals(resourceState, cacheState(cache), "ECTyper resources changed during " + format);
                if (baseline == null) baseline = normalized;
                boolean equivalent = baseline.equals(normalized);
                row.put("equivalent", equivalent); row.put("status", equivalent ? "PASS" : "MISMATCH");
                writeReport(root, report);
            }
            report.put("externalResourcesAfter", cacheState(cache));
            assertEquals(sourceHash, fileHash(source)); assertEquals(referenceHash, fileHash(reference));
            assertTrue(cases.stream().allMatch(row -> Boolean.TRUE.equals(row.get("equivalent"))), "Output differences; inspect comparison.json");
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL"); report.put("error", failure.toString()); throw failure;
        } finally { writeReport(root, report); }
    }

    private static SeroTypingStageParameters serotypeParameters() {
        TaxonomyResult taxonomy = new TaxonomyResult(); taxonomy.setStatus(STATUS_CONFIDENT);
        TaxonomyResult.Taxon best = new TaxonomyResult.Taxon(); best.setTaxid(562); best.setName("Escherichia coli"); best.setRank("S");
        taxonomy.setBest(best);
        SeroTypingStageParameters params = new SeroTypingStageParameters();
        params.setTaxonomyContext(TaxonomyContext.of(taxonomy));
        SequenceMeta meta = new SequenceMeta(); meta.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY); params.setReadMeta(meta);
        return params;
    }

    private static BioPipelineStage stage(long id, int type, Object input, Object parameters) throws Exception {
        BioPipelineStage stage = new BioPipelineStage(); stage.setStageId(id); stage.setPipelineId(1L);
        stage.setStageType(type); stage.setStageIndex(0); stage.setVersion(0); stage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        stage.setInputUrl(JsonUtil.toJson(input)); stage.setParameters(JsonUtil.toJson(parameters)); return stage;
    }

    private static AnalysisPipelineToolsConfig tools() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean(); yaml.setResources(new ClassPathResource("application-dev.yml"));
        Properties properties = yaml.getObject(); assertNotNull(properties);
        Map<String, Object> selected = new LinkedHashMap<>();
        properties.forEach((key, value) -> {
            String name = key.toString();
            if (name.equals("analysis-pipeline.conda") || name.startsWith("analysis-pipeline.tools.ectyper[")
                    || name.startsWith("analysis-pipeline.tools.minimap2[")) selected.put(name, value);
        });
        StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().addFirst(new MapPropertySource("assemblyFormatToolConfig", selected));
        return Binder.get(env).bind("analysis-pipeline.tools", Bindable.of(AnalysisPipelineToolsConfig.class)).get();
    }

    private static Map<String, Object> cacheState(Path directory) throws IOException {
        assertFalse(Files.exists(directory.resolve(".lock")), "Do not run alongside an ECTyper cache operation");
        Map<String, Object> state = new LinkedHashMap<>();
        for (String name : List.of("ectyper_alleles_db.json", "ectyper_patho_stx_toxin_typing_database.json",
                "EnteroRef_GTDBSketch_20231003_V2.msh", "EnteroRef_GTDBSketch_20231003_V2.msh.txt")) {
            Path file = directory.resolve(name); assertTrue(Files.isRegularFile(file) && Files.isReadable(file), "Missing cached resource: " + file);
            assertTrue(Files.size(file) > 0, "Empty cached resource: " + file);
            state.put(file.toAbsolutePath().toString(), Map.of("size", Files.size(file), "modified", Files.getLastModifiedTime(file).toMillis()));
        }
        return state;
    }

    private static Path wrap(Path directory, byte[] bytes, String format) throws IOException {
        Path file = directory.resolve("assembly.fasta" + (format.equals("gzip") ? ".gz" : format.equals("zip") ? ".zip" : ""));
        if (format.equals("gzip")) {
            try (var out = new GZIPOutputStream(Files.newOutputStream(file))) { out.write(bytes); }
        } else if (format.equals("zip")) {
            try (var out = new ZipOutputStream(Files.newOutputStream(file))) {
                out.putNextEntry(new ZipEntry("assembly.fasta")); out.write(bytes); out.closeEntry();
            }
        } else Files.write(file, bytes);
        return file;
    }

    private static byte[] decodedBytes(Path path) throws IOException {
        try (InputStream raw = Files.newInputStream(path)) {
            InputStream decoded = raw;
            if (SequenceFileUtil.isGzip(path)) decoded = new GZIPInputStream(raw);
            if (SequenceFileUtil.isZip(path.toString())) {
                ZipInputStream zip = new ZipInputStream(raw); assertNotNull(zip.getNextEntry()); decoded = zip;
            }
            try (InputStream contents = decoded) { return contents.readAllBytes(); }
        }
    }
    private static Path requiredPath(String key) {
        assertNotNull(System.getProperty(key), "Set -D" + key + " to the existing benign MG1655 fixture");
        Path path = Path.of(System.getProperty(key)).toAbsolutePath(); assertTrue(Files.isRegularFile(path), path.toString()); return path;
    }
    private static String fileHash(Path path) throws Exception { return hash(Files.readAllBytes(path)); }
    private static String hash(byte[] bytes) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
    private static void writeReport(Path root, Map<String, Object> report) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(root.resolve("comparison.json").toFile(), report);
    }

    private static final class Recorder {
        final Path logs; final List<Map<String, Object>> records = new ArrayList<>();
        Recorder(Path directory) throws IOException { logs = Files.createDirectories(directory); }
        int run(List<String> command, Path work) throws IOException, InterruptedException {
            int id = records.size() + 1; long started = System.nanoTime();
            Path stdout = logs.resolve(id + ".stdout.log"), stderr = logs.resolve(id + ".stderr.log");
            Map<String, Object> row = new LinkedHashMap<>(); records.add(row);
            row.put("command", List.copyOf(command)); row.put("workDir", work.toString());
            row.put("stdout", stdout.toString()); row.put("stderr", stderr.toString());
            Set<ProcessHandle> owned = new LinkedHashSet<>(); Process process = null;
            try {
                ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile());
                builder.environment().put("PYTHONDONTWRITEBYTECODE", "1");
                process = builder.redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
                owned.add(process.toHandle());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(Long.getLong("assembly.formats.processTimeoutSeconds", 300L));
                while (true) {
                    process.descendants().forEach(owned::add);
                    if (process.waitFor(1, TimeUnit.SECONDS)) { row.put("exitCode", process.exitValue()); return process.exitValue(); }
                    if (System.nanoTime() >= deadline) { row.put("timeout", true); throw new IOException("Tool timeout: " + command); }
                }
            } finally {
                if (process != null) process.descendants().forEach(owned::add);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (owned.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline && !Thread.currentThread().isInterrupted()) {
                    try { Thread.sleep(50); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
                row.put("survivingProcessIds", owned.stream().filter(ProcessHandle::isAlive).map(ProcessHandle::pid).toList());
                row.put("durationSeconds", (System.nanoTime() - started) / 1e9);
                JSON.writerWithDefaultPrettyPrinter().writeValue(logs.resolve(id + ".json").toFile(), row);
            }
        }
    }
}
