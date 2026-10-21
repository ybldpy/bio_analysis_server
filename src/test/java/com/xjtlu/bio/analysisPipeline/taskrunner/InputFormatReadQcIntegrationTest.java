package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
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
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.AbstractStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.FailStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.QcStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.ReadInspectStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls.SequenceInputMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/**
 * Opt-in format-only integration, without Spring, database, scheduler or subprocess mocks.
 * Run with -Dinput.formats.integration=true; optional executable overrides use
 * input.formats.bbmapPath, input.formats.fastp and input.formats.fastplong.
 */
@EnabledIfSystemProperty(named = "input.formats.integration", matches = "true")
class InputFormatReadQcIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int THREADS = 1;

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void publishedReadsAndQcResultsAreEquivalentAcrossFormats() throws Exception {
        Path parent = Files.createDirectories(Path.of("target/input-format-integration").toAbsolutePath());
        Path root = Files.createTempDirectory(parent, "run-");
        Map<String, Object> summary = new LinkedHashMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        summary.put("root", root.toString());
        summary.put("startedAt", Instant.now().toString());
        summary.put("fixture", "Deterministic arbitrary ACGT records; one sequence file per ZIP input slot");
        summary.put("threads", THREADS);
        summary.put("cases", cases);
        summary.put("failures", failures);
        try {
            String bbmap = System.getProperty("input.formats.bbmapPath", "/home/jcy/test/bio_test/bbmap");
            String fastp = System.getProperty("input.formats.fastp", "/usr/conda/envs/bio-fastp/bin/fastp");
            String fastplong = System.getProperty("input.formats.fastplong", "/home/jcy/.conda/envs/fastplong/bin/fastplong");
            for (String executable : List.of(Path.of(bbmap, "reformat.sh").toString(), fastp, fastplong)) {
                assertTrue(Files.isExecutable(Path.of(executable)), "Missing real executable: " + executable);
            }
            summary.put("executables", Map.of("bbmap", bbmap, "fastp", fastp, "fastplong", fastplong));
            AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
            tools.setFastp(List.of(fastp, "--thread", Integer.toString(THREADS)));
            tools.setFastplong(List.of(fastplong, "--thread", Integer.toString(THREADS)));

            for (String layout : List.of("single", "interleaved", "paired", "direct-qc", "long-single")) {
                Fixture fixture = fixture(layout.equals("long-single"));
                Outcome baseline = null;
                List<Encoding> encodings = layout.equals("paired")
                        ? List.of(new Encoding("plain", "plain"), new Encoding("gzip", "gzip"),
                                new Encoding("zip", "zip"), new Encoding("zip", "gzip"), new Encoding("gzip", "zip"))
                        : layout.equals("direct-qc")
                                ? List.of(new Encoding("plain", "plain"), new Encoding("zip", "gzip"), new Encoding("gzip", "zip"))
                                : List.of(new Encoding("plain", null), new Encoding("gzip", null), new Encoding("zip", null));
                for (Encoding encoding : encodings) {
                    String name = layout + "-" + encoding.first + (encoding.second == null ? "" : "-" + encoding.second);
                    Path caseRoot = Files.createDirectory(root.resolve(name));
                    Map<String, Object> report = new LinkedHashMap<>();
                    cases.add(report);
                    report.put("name", name);
                    report.put("root", caseRoot.toString());
                    report.put("startedAt", Instant.now().toString());
                    report.put("status", "RUNNING");
                    try {
                        Outcome outcome = runCase(caseRoot, layout, encoding, fixture, bbmap, tools, report);
                        if (encoding.first.equals("plain")) {
                            baseline = outcome;
                            report.put("comparison", "BASELINE");
                        } else {
                            assertNotNull(baseline, "Plain-input baseline failed for " + layout);
                            assertEquals(baseline.r1, outcome.r1, "Decoded retained R1 differs from plain baseline");
                            assertEquals(baseline.r2, outcome.r2, "Decoded retained R2 differs from plain baseline");
                            assertEquals(baseline.qcSections, outcome.qcSections, "Stable QC report sections differ");
                            report.put("comparison", "MATCHES_PLAIN");
                        }
                        report.put("status", "PASSED");
                    } catch (Exception | AssertionError failure) {
                        report.put("status", "FAILED");
                        report.put("error", failure.toString());
                        failures.add(name + ": " + failure);
                    } finally {
                        report.put("endedAt", Instant.now().toString());
                        writeJson(caseRoot.resolve("report.json"), report);
                        writeJson(root.resolve("summary.json"), summary);
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException("Format integration interrupted; reports at " + root);
                    }
                }
            }
            assertTrue(failures.isEmpty(), "Format integration failures: " + failures + "; reports: " + root);
            summary.put("status", "PASSED");
        } catch (Exception | AssertionError failure) {
            summary.put("status", "FAILED");
            summary.put("error", failure.toString());
            throw failure;
        } finally {
            summary.put("endedAt", Instant.now().toString());
            writeJson(root.resolve("summary.json"), summary);
            System.out.println("Input format integration reports: " + root);
        }
    }

    private Outcome runCase(Path root, String layout, Encoding encoding, Fixture fixture, String bbmap,
            AnalysisPipelineToolsConfig tools, Map<String, Object> report) throws Exception {
        Path storageRoot = Files.createDirectory(root.resolve("storage"));
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", root.resolve("work").toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", root.resolve("input").toString());
        boolean paired = layout.equals("paired") || layout.equals("direct-qc");
        String sourceR1 = layout.equals("interleaved") ? fixture.interleaved : fixture.r1;
        String r1 = storeInput(storageRoot, "read1", encoding.first, sourceR1);
        String r2 = paired ? storeInput(storageRoot, "read2", encoding.second, fixture.r2) : null;
        Map<String, String> inputHashes = new LinkedHashMap<>();
        inputHashes.put(r1, sha256(Files.readAllBytes(storageRoot.resolve(r1))));
        if (r2 != null) inputHashes.put(r2, sha256(Files.readAllBytes(storageRoot.resolve(r2))));
        report.put("sourceObjectSha256", inputHashes);
        report.put("sourceDecodedR1Sha256", sha256(sourceR1.getBytes(StandardCharsets.UTF_8)));
        if (r2 != null) report.put("sourceDecodedR2Sha256", sha256(fixture.r2.getBytes(StandardCharsets.UTF_8)));
        MemoryPipelineService persistence = new MemoryPipelineService();
        report.put("stages", persistence.stages);
        report.put("notifications", persistence.notifications);
        ProcessRecorder recorder = new ProcessRecorder(Files.createDirectory(root.resolve("processes")));
        report.put("processes", recorder.records);
        ReadInspectStageExecutor inspect = new ReadInspectStageExecutor() {
            @Override protected int runSubProcess(List<String> command, Path work, Path out, Path err, boolean append)
                    throws IOException, InterruptedException {
                return recorder.run(command, work, out, err, append);
            }
        };
        QcStageExecutor qc = new QcStageExecutor() {
            @Override protected int runSubProcess(List<String> command, Path work) throws IOException, InterruptedException {
                return recorder.run(command, work, null, null, false);
            }
        };
        for (Object executor : List.of(inspect, qc)) {
            ReflectionTestUtils.setField(executor, "storageService", storage);
            ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
            ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        }
        ReflectionTestUtils.setField(inspect, "bbmapPath", bbmap);
        ReadInspectStageDoneHandler inspectDone = new ReadInspectStageDoneHandler();
        QcStageDoneHandler qcDone = new QcStageDoneHandler();
        FailStageDoneHandler failDone = new FailStageDoneHandler();
        for (Object handler : List.of(inspectDone, qcDone, failDone)) {
            ReflectionTestUtils.setField(handler, "storageService", storage);
            ReflectionTestUtils.setField(handler, "bioStageUtil", directories);
            ReflectionTestUtils.setField(handler, "pipelineService", persistence);
        }
        ReflectionTestUtils.setField(qcDone, "jsonFileName", "cleaned.json");
        ReflectionTestUtils.setField(qcDone, "htmlFileName", "cleaned.html");

        SequenceMeta meta = new SequenceMeta();
        meta.setSequenceLevel(SEQUENCE_LEVEL_READ);
        meta.setReadLenType(layout.equals("long-single") ? READ_LEN_TYPE_LONG : READ_LEN_TYPE_SHORT);
        meta.setQualityEncoding(QUALITY_ENCODING_33);
        meta.setSequencingPlatform(layout.equals("long-single") ? Constants.SequencingPlatform.ONT : Constants.SequencingPlatform.ILLUMINA);
        if (!layout.equals("direct-qc")) {
            ReadInspectStageInputUrls input = new ReadInspectStageInputUrls();
            input.setRead1Url(r1);
            input.setRead2Url(r2);
            SequenceInputMeta declaration = new SequenceInputMeta();
            declaration.setSequencingPlatform(meta.getSequencingPlatform());
            input.setSequenceInputMeta(declaration);
            BioPipelineStage stage = stage(1L, PIPELINE_STAGE_READ_INSPECT, input, new BaseStageParams(), persistence);
            execute(stage, inspect, inspectDone, failDone, root);
            ReadInspectStageResult published = JsonUtil.toObject(stage.getOutputUrl(), ReadInspectStageResult.class);
            assertEquals(JSON.valueToTree(meta), JSON.valueToTree(published.getReadMeta()), "Resolved read metadata differs");
            r1 = published.getR1Url();
            r2 = published.getR2Url();
            assertEquals(fixture.r1, decoded(storageRoot.resolve(r1)), "Published inspected R1 changed");
            if (paired || layout.equals("interleaved")) {
                assertNotNull(r2);
                assertEquals(fixture.r2, decoded(storageRoot.resolve(r2)), "Published inspected R2 changed");
            } else assertNull(r2);
            meta = published.getReadMeta();
        }
        QcStageInputUrls qcInput = new QcStageInputUrls();
        qcInput.setRead1(r1);
        qcInput.setRead2(r2);
        QcParameters parameters = new QcParameters();
        parameters.setReadMeta(meta);
        BioPipelineStage qcStage = stage(2L, PIPELINE_STAGE_QC, qcInput, parameters, persistence);
        execute(qcStage, qc, qcDone, failDone, root);
        QcResult published = JsonUtil.toObject(qcStage.getOutputUrl(), QcResult.class);
        String cleanedR1 = decoded(storageRoot.resolve(published.getCleanedR1()));
        String cleanedR2 = published.getCleanedR2() == null ? null : decoded(storageRoot.resolve(published.getCleanedR2()));
        assertFalse(cleanedR1.isBlank(), "Fixture must retain reads, not pass via empty output");
        if (r2 != null) assertNotNull(cleanedR2);
        JsonNode qcReport = JSON.readTree(storageRoot.resolve(published.getReportJSON()).toFile());
        Map<String, JsonNode> stable = new LinkedHashMap<>();
        stable.put("filtering_result", qcReport.path("filtering_result"));
        stable.put("before_filtering", qcReport.path("summary").path("before_filtering"));
        stable.put("after_filtering", qcReport.path("summary").path("after_filtering"));
        stable.values().forEach(section -> assertFalse(section.isMissingNode(), "Missing stable QC report section"));
        assertTrue(stable.get("after_filtering").path("total_reads").asLong() > 0);
        report.put("qcStableSections", stable);
        report.put("cleanedR1DecodedSha256", sha256(cleanedR1.getBytes(StandardCharsets.UTF_8)));
        if (cleanedR2 != null) report.put("cleanedR2DecodedSha256", sha256(cleanedR2.getBytes(StandardCharsets.UTF_8)));
        for (Map.Entry<String, String> input : inputHashes.entrySet()) {
            assertEquals(input.getValue(), sha256(Files.readAllBytes(storageRoot.resolve(input.getKey()))), "Original object changed");
        }
        for (Map<String, Object> process : recorder.records) {
            // BBMap can legitimately exit nonzero for the single-end diagnostic,
            // but a watchdog or process-start failure must never count as a pass.
            assertFalse(process.containsKey("error"), "Real process failed: " + process);
            assertFalse(process.containsKey("timeout"), "Real process timed out: " + process);
            assertNotNull(process.get("exitCode"));
            assertEquals(List.of(), process.get("survivingProcessIds"), "Owned child process survived cleanup");
        }
        return new Outcome(cleanedR1, cleanedR2, stable);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void execute(BioPipelineStage stage, PipelineStageExecutor executor, AbstractStageDoneHandler done,
            FailStageDoneHandler failed, Path root) {
        stage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
        stage.setVersion(stage.getVersion() + 1);
        StageRunResult result = executor.execute(stage);
        assertNotNull(result);
        (result.isSuccess() ? done : failed).handleStageDone(result);
        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, stage.getStatus(), "DoneHandler did not commit FINISHED");
        assertFalse(Files.exists(root.resolve("input").resolve(stage.getStageId().toString())));
        assertFalse(Files.exists(root.resolve("work").resolve(stage.getStageId().toString())));
    }

    private static BioPipelineStage stage(long id, int type, Object input, Object params, MemoryPipelineService service)
            throws Exception {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(id);
        stage.setPipelineId(1L);
        stage.setStageType(type);
        stage.setVersion(0);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(params));
        service.stages.put(id, stage);
        return stage;
    }

    private static String storeInput(Path root, String slot, String encoding, String content) throws Exception {
        String key = "fixture/" + slot + (encoding.equals("zip") ? ".zip" : encoding.equals("gzip") ? ".fastq.gz" : ".fastq");
        Path path = root.resolve(key);
        Files.createDirectories(path.getParent());
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (encoding.equals("zip")) {
            try (ZipOutputStream stream = new ZipOutputStream(Files.newOutputStream(path))) {
                stream.putNextEntry(new ZipEntry("reads.fastq"));
                stream.write(bytes);
                stream.closeEntry();
            }
        } else if (encoding.equals("gzip")) {
            try (GZIPOutputStream stream = new GZIPOutputStream(Files.newOutputStream(path))) { stream.write(bytes); }
        } else Files.write(path, bytes);
        assertEquals(content, decoded(path), "Encoded fixture does not reproduce original bytes");
        return key;
    }

    private static String decoded(Path path) throws IOException {
        try (InputStream raw = Files.newInputStream(path)) {
            if (path.toString().endsWith(".gz")) {
                try (GZIPInputStream gzip = new GZIPInputStream(raw)) { return new String(gzip.readAllBytes(), StandardCharsets.UTF_8); }
            }
            if (path.toString().endsWith(".zip")) {
                try (ZipInputStream zip = new ZipInputStream(raw)) {
                    assertNotNull(zip.getNextEntry(), "ZIP fixture must contain its sequence file");
                    return new String(zip.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
            return new String(raw.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static Fixture fixture(boolean longReads) {
        Random random = new Random(46231L);
        StringBuilder r1 = new StringBuilder(), r2 = new StringBuilder(), interleaved = new StringBuilder();
        for (int index = 0; index < 32; index++) {
            int length = (longReads ? 700 : 120) + index % 4;
            for (int mate = 1; mate <= 2; mate++) {
                StringBuilder bases = new StringBuilder();
                for (int pos = 0; pos < length; pos++) bases.append("ACGT".charAt(random.nextInt(4)));
                String record = "@format_fixture_" + index + "/" + mate + "\n" + bases + "\n+\n"
                        + (index == 31 ? "!" : "I").repeat(length) + "\n";
                (mate == 1 ? r1 : r2).append(record);
                interleaved.append(record);
            }
        }
        return new Fixture(r1.toString(), r2.toString(), interleaved.toString());
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void writeJson(Path path, Object value) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
    }

    private record Encoding(String first, String second) { }
    private record Fixture(String r1, String r2, String interleaved) { }
    private record Outcome(String r1, String r2, Map<String, JsonNode> qcSections) { }

    private static final class MemoryPipelineService extends PipelineService {
        final Map<Long, BioPipelineStage> stages = new LinkedHashMap<>();
        @Override public void recordStageFailure(com.xjtlu.bio.analysisPipeline.context.runtime.StageContext context,
                String message) { /* Failure details are retained in the test report; no database writes. */ }
        final List<Map<String, Object>> notifications = new ArrayList<>();
        @Override public BioPipelineStage queryStageById(long id) { return stages.get(id); }
        @Override public void pipelineStageDone(long id, boolean success) {
            notifications.add(Map.of("stageId", id, "success", success));
        }
        @Override public int updateStageFromVersion(UpdateStageCommand command) {
            BioPipelineStage target = stages.get(command.getStageId());
            if (target == null || !Objects.equals(target.getVersion(), command.getCurrentVersion())) return 0;
            BioPipelineStage patch = command.getUpdateStage();
            if (patch.getStatus() != null) target.setStatus(patch.getStatus());
            if (patch.getOutputUrl() != null) target.setOutputUrl(patch.getOutputUrl());
            if (patch.getOutputInline() != null) target.setOutputInline(patch.getOutputInline());
            if (patch.getEndTime() != null) target.setEndTime(patch.getEndTime());
            target.setVersion(command.getCurrentVersion() + 1);
            return 1;
        }
    }

    /** Executes every real command, adding only resource limits, bounded waits and retained logs. */
    private static final class ProcessRecorder {
        final Path logs;
        final List<Map<String, Object>> records = new ArrayList<>();
        ProcessRecorder(Path logs) { this.logs = logs; }
        int run(List<String> command, Path work, Path stdout, Path stderr, boolean append)
                throws IOException, InterruptedException {
            int index = records.size() + 1;
            Map<String, Object> record = new LinkedHashMap<>();
            records.add(record);
            List<String> actual = new ArrayList<>(command);
            if (actual.get(0).endsWith("reformat.sh")) actual.add("threads=" + THREADS);
            Path durableOut = logs.resolve(index + ".stdout.log"), durableErr = logs.resolve(index + ".stderr.log");
            Path out = stdout == null ? durableOut : stdout, err = stderr == null ? durableErr : stderr;
            record.put("requestedCommand", command);
            record.put("executedCommand", actual);
            record.put("workDir", work.toString());
            record.put("stdout", durableOut.toString());
            record.put("stderr", durableErr.toString());
            record.put("startedAt", Instant.now().toString());
            long started = System.nanoTime();
            Process process = null;
            Set<ProcessHandle> owned = new LinkedHashSet<>();
            try {
                ProcessBuilder builder = new ProcessBuilder(actual).directory(work.toFile());
                for (String key : List.of("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"))
                    builder.environment().put(key, Integer.toString(THREADS));
                Files.createDirectories(out.getParent());
                Files.createDirectories(err.getParent());
                builder.redirectOutput(append ? ProcessBuilder.Redirect.appendTo(out.toFile()) : ProcessBuilder.Redirect.to(out.toFile()));
                builder.redirectError(append ? ProcessBuilder.Redirect.appendTo(err.toFile()) : ProcessBuilder.Redirect.to(err.toFile()));
                process = builder.start();
                owned.add(process.toHandle());
                long deadline = started + TimeUnit.SECONDS.toNanos(120);
                while (true) {
                    process.descendants().forEach(owned::add);
                    if (process.waitFor(100, TimeUnit.MILLISECONDS)) {
                        record.put("exitCode", process.exitValue());
                        return process.exitValue();
                    }
                    if (System.nanoTime() >= deadline) {
                        record.put("timeout", true);
                        throw new IOException("Real tool exceeded 120 seconds; logs: " + logs);
                    }
                }
            } catch (IOException | InterruptedException failure) {
                record.put("error", failure.toString());
                if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
                throw failure;
            } finally {
                if (process != null) process.descendants().forEach(owned::add);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (owned.stream().anyMatch(ProcessHandle::isAlive)
                        && System.nanoTime() < stopDeadline && !Thread.currentThread().isInterrupted()) {
                    try { Thread.sleep(20); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                }
                record.put("survivingProcessIds", owned.stream().filter(ProcessHandle::isAlive).map(ProcessHandle::pid).toList());
                record.put("durationSeconds", (System.nanoTime() - started) / 1_000_000_000.0);
                if (!out.equals(durableOut) && Files.exists(out)) Files.copy(out, durableOut, StandardCopyOption.REPLACE_EXISTING);
                if (!err.equals(durableErr) && Files.exists(err)) Files.copy(err, durableErr, StandardCopyOption.REPLACE_EXISTING);
                writeJson(logs.resolve(index + ".json"), record);
            }
        }
    }
}
