package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Uses already downloaded public interleaved files and official, separately published mate oracles.
 * This test never constructs an interleaved input, downloads data, or starts Spring/database services.
 * Enable with -DreadInspect.publicManifest=/absolute/manifest.json; plain/gzip inputs are accepted.
 */
@EnabledIfSystemProperty(named = "readInspect.publicManifest", matches = ".+")
class ReadInspectPublicInterleavedIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int FORCED_EXIT = 17;

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void publishedInterleavedFilesSplitExactlyLikeTheirOfficialOracles() throws Exception {
        Path root = Files.createTempDirectory(Files.createDirectories(
                Path.of("target/readinspect-public-interleaved").toAbsolutePath()), "run-");
        Map<String, Object> report = new LinkedHashMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        report.put("status", "RUNNING"); report.put("cases", cases); report.put("failures", failures);
        report.put("scope", "Public native interleaved FASTQ transport/splitting; no biological accuracy claim");
        System.out.println("Public interleaved comparison: " + root);
        write(root.resolve("summary.json"), report);
        try {
            Path manifest = Path.of(System.getProperty("readInspect.publicManifest")).toAbsolutePath();
            report.put("manifest", manifest.toString()); report.put("manifestSha256", sha(Files.readAllBytes(manifest)));
            JsonNode fixtures = JSON.readTree(manifest.toFile()).get("fixtures");
            assertNotNull(fixtures, "Manifest needs fixtures");
            assertTrue(fixtures.isArray() && !fixtures.isEmpty(), "Manifest fixtures must be a nonempty array");
            Path bbmap = Path.of(System.getProperty("readInspect.bbmapPath", "/home/jcy/test/bio_test/bbmap")).toAbsolutePath();
            report.put("bbmapPath", bbmap.toString());
            int fixtureIndex = 0;
            for (JsonNode fixture : fixtures) {
                for (String mode : List.of("normal", "fallback")) {
                    for (int round = 1; round <= 2; round++) {
                        Path run = Files.createDirectory(root.resolve("fixture-" + fixtureIndex + "-" + mode + "-" + round));
                        Map<String, Object> row = new LinkedHashMap<>(); cases.add(row);
                        row.put("fixture", fixture); row.put("mode", mode); row.put("round", round);
                        row.put("runRoot", run.toString()); row.put("status", "RUNNING");
                        row.put("exitCode", null); // No exit status exists when preflight or process startup fails.
                        write(root.resolve("summary.json"), report);
                        try {
                            runCase(fixture, mode, bbmap, run, row);
                            row.put("status", "PASS");
                        } catch (Exception | AssertionError failure) {
                            row.put("status", "FAIL"); row.put("error", failure.toString());
                            failures.add(run.getFileName() + ": " + failure);
                        } finally {
                            write(run.resolve("case.json"), row);
                            write(root.resolve("summary.json"), report);
                        }
                        if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Test interrupted");
                    }
                }
                fixtureIndex++;
            }
            assertTrue(failures.isEmpty(), () -> "Public interleaved failures; inspect " + root + ": " + failures);
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL"); report.put("error", failure.toString()); throw failure;
        } finally { write(root.resolve("summary.json"), report); }
    }

    private static void runCase(JsonNode fixture, String mode, Path bbmap, Path run, Map<String, Object> row)
            throws Exception {
        String oracleComparison = fixture.path("oracleComparison").asText("exact");
        assertTrue(List.of("exact", "header-sequence-quality").contains(oracleComparison), "Unknown oracle comparison mode");
        boolean ignoreOraclePlusComment = oracleComparison.equals("header-sequence-quality");
        row.put("oracleComparison", oracleComparison);
        for (String field : List.of("id", "sourceUrl", "expectedR1Url", "expectedR2Url", "evidenceUrl", "commit")) {
            assertTrue(fixture.path(field).isTextual() && !fixture.path(field).asText().isBlank(), "Missing " + field);
        }
        Map<String, Path> sources = new LinkedHashMap<>();
        for (String name : List.of("input", "expectedR1", "expectedR2")) {
            Path path = Path.of(fixture.path(name).asText());
            assertTrue(path.isAbsolute() && Files.isRegularFile(path), "Missing absolute " + name + ": " + path);
            sources.put(name, path);
        }
        Map<String, String> before = rawHashes(sources);
        row.put("originalDownloadSha256", before);
        try {
            Fastq input = readFastq(sources.get("input"));
            Fastq expected1 = readFastq(sources.get("expectedR1"));
            Fastq expected2 = readFastq(sources.get("expectedR2"));
            row.put("inputRecords", input.records.size()); row.put("pairCount", expected1.records.size());
            row.put("inputDecodedSha256", sha(input.bytes));
            row.put("expectedR1", describe(expected1)); row.put("expectedR2", describe(expected2));
            assertFalse(expected1.records.isEmpty(), "Empty oracle");
            assertEquals(fixture.path("expectedPairs").asInt(), expected1.records.size(), "Documented pair count");
            assertEquals(expected1.records.size(), expected2.records.size(), "Oracle mate counts differ");
            assertEquals(2 * expected1.records.size(), input.records.size(), "Published input/oracle counts differ");
            // Verify the supplied independent oracle, without constructing or rewriting an interleaved file.
            for (int pair = 0; pair < expected1.records.size(); pair++) {
                assertRecord(expected1.records.get(pair), input.records.get(2 * pair), "Published input R1", pair, ignoreOraclePlusComment);
                assertRecord(expected2.records.get(pair), input.records.get(2 * pair + 1), "Published input R2", pair, ignoreOraclePlusComment);
            }
            row.put("publishedInputMatchesExpectedRecords", true);
            // A separate preservation check retains every original byte, including '+' comments.
            // These are in-memory expectations from the downloaded input, NOT generated test inputs.
            Fastq preserved1 = originalMate(input, 0);
            Fastq preserved2 = originalMate(input, 1);
            Path storageRoot = Files.createDirectories(run.resolve("storage"));
            Path stored = storageRoot.resolve(sources.get("input").getFileName());
            Files.copy(sources.get("input"), stored);
            LocalStorageService storage = new LocalStorageService();
            ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
            BioStageUtil directories = new BioStageUtil();
            ReflectionTestUtils.setField(directories, "stageWorkDirPath", run.resolve("work").toString());
            ReflectionTestUtils.setField(directories, "stageInputDirPath", run.resolve("input").toString());
            ProcessProbe process = new ProcessProbe(mode.equals("fallback"), run.resolve("process"));
            row.put("process", process.record);
            ReadInspectStageExecutor executor = new ReadInspectStageExecutor() {
                @Override protected int runSubProcess(List<String> command, Path work, Path out, Path err, boolean append)
                        throws IOException, InterruptedException {
                    return process.run(command, work, out, err, append);
                }
            };
            ReflectionTestUtils.setField(executor, "storageService", storage);
            ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
            ReflectionTestUtils.setField(executor, "bbmapPath", bbmap.toString());
            BioPipelineStage stage = new BioPipelineStage();
            stage.setStageId(42L); stage.setPipelineId(1L); stage.setVersion(0);
            stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
            stage.setInputUrl(JsonUtil.toJson(new ReadInspectStageInputUrls(stored.getFileName().toString(), null)));
            stage.setParameters(JsonUtil.toJson(new BaseStageParams()));
            StageRunResult<ReadInspectStageOutput> result = executor.execute(stage);
            row.put("stageSuccess", result.isSuccess()); row.put("stageError", result.getErrorLog());
            row.put("exitCode", process.record.get("exitCode"));
            assertEquals(1, process.calls, "Expected exactly one external-process attempt");
            assertEquals(mode.equals("normal") ? 0 : FORCED_EXIT, process.record.get("exitCode"),
                    "Normal path must really exit zero, not silently enter fallback");
            assertTrue(result.isSuccess(), result.getErrorLog());
            ReadInspectStageOutput output = result.getStageOutput();
            assertNotNull(output); assertFalse(output.isUseOriginalSequence(), "Expected paired split output");
            assertNotNull(output.getR1Path()); assertNotNull(output.getR2Path());
            assertFalse(Files.exists(run.resolve("input/42")), "Staged input was not cleaned up");
            Map<String, Object> first = compareOutput("R1", output.getR1Path(), expected1, run, ignoreOraclePlusComment);
            Map<String, Object> second = compareOutput("R2", output.getR2Path(), expected2, run, ignoreOraclePlusComment);
            row.put("outputR1", first); row.put("outputR2", second);
            row.put("byteEqual", Boolean.TRUE.equals(first.get("byteEqual")) && Boolean.TRUE.equals(second.get("byteEqual")));
            Map<String, Object> preservation1 = compareOutput("Original R1", output.getR1Path(), preserved1, run, false);
            Map<String, Object> preservation2 = compareOutput("Original R2", output.getR2Path(), preserved2, run, false);
            row.put("inputR1Preservation", preservation1); row.put("inputR2Preservation", preservation2);
            row.put("inputBytesPreserved", Boolean.TRUE.equals(preservation1.get("byteEqual"))
                    && Boolean.TRUE.equals(preservation2.get("byteEqual")));
            // Evaluate both mates and retain their evidence before failing a case.
            assertTrue(Boolean.TRUE.equals(first.get("recordsEqual")), "R1: " + first.get("firstMismatch"));
            assertTrue(Boolean.TRUE.equals(second.get("recordsEqual")), "R2: " + second.get("firstMismatch"));
            if (!ignoreOraclePlusComment) {
                assertEquals(true, row.get("byteEqual"), "Published mate bytes differ; see case.json");
            }
            assertEquals(true, row.get("inputBytesPreserved"), "Original record bytes changed (including '+' comments/newlines)");
            assertEquals(before.get("input"), sha(Files.readAllBytes(stored)), "Stored source object was modified");
        } finally {
            Map<String, String> after = rawHashes(sources);
            row.put("originalDownloadSha256After", after); row.put("sourcesUnchanged", before.equals(after));
            assertEquals(before, after, "Original downloaded input/oracles changed");
        }
    }

    private static Map<String, Object> compareOutput(String mate, Path path, Fastq expected, Path run,
            boolean ignoreOraclePlusComment) throws Exception {
        assertTrue(path.startsWith(run.resolve("work/42")) && Files.isRegularFile(path), "Output must survive input cleanup: " + path);
        Fastq actual = readFastq(path);
        Map<String, Object> comparison = describe(actual);
        comparison.put("path", path.toString()); comparison.put("expectedDecodedSha256", sha(expected.bytes));
        comparison.put("byteEqual", Arrays.equals(expected.bytes, actual.bytes));
        String mismatch = null;
        try {
            assertEquals(expected.records.size(), actual.records.size(), mate + " record count");
            for (int i = 0; i < expected.records.size(); i++) {
                assertRecord(expected.records.get(i), actual.records.get(i), mate, i, ignoreOraclePlusComment);
            }
        } catch (AssertionError failure) { mismatch = failure.getMessage(); }
        comparison.put("recordsEqual", mismatch == null); comparison.put("firstMismatch", mismatch);
        return comparison;
    }

    private static void assertRecord(List<String> expected, List<String> actual, String mate, int index,
            boolean ignoreOraclePlusComment) {
        String[] fields = {"header", "sequence", "plus", "quality"};
        for (int field = 0; field < fields.length; field++) {
            if (field == 2 && ignoreOraclePlusComment) continue;
            assertEquals(expected.get(field), actual.get(field), mate + " record " + (index + 1) + " " + fields[field]);
        }
    }

    private record Fastq(byte[] bytes, List<List<String>> records, long lf, long crlf, boolean finalNewline) { }

    private static Fastq originalMate(Fastq input, int parity) throws IOException {
        ByteArrayOutputStream selected = new ByteArrayOutputStream();
        int start = 0, line = 0;
        for (int i = 0; i < input.bytes.length; i++) {
            if (input.bytes[i] != '\n') continue;
            line++;
            if (line % 4 == 0) {
                if ((line / 4 - 1) % 2 == parity) selected.write(input.bytes, start, i + 1 - start);
                start = i + 1;
            }
        }
        if (start < input.bytes.length && (input.records.size() - 1) % 2 == parity) {
            selected.write(input.bytes, start, input.bytes.length - start);
        }
        return parseFastq(selected.toByteArray(), "original mate " + parity);
    }

    private static Fastq readFastq(Path path) throws IOException {
        byte[] bytes;
        try (InputStream raw = Files.newInputStream(path); var buffered = new java.io.BufferedInputStream(raw)) {
            buffered.mark(2); int first = buffered.read(), second = buffered.read(); buffered.reset();
            if (first == 0x1f && second == 0x8b) {
                try (GZIPInputStream gzip = new GZIPInputStream(buffered)) { bytes = gzip.readAllBytes(); }
            } else bytes = buffered.readAllBytes();
        }
        return parseFastq(bytes, path.toString());
    }

    private static Fastq parseFastq(byte[] bytes, String description) throws IOException {
        List<String> lines = new ArrayList<>();
        long lf = 0, crlf = 0; int start = 0;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] != '\n') continue;
            int end = i;
            if (end > start && bytes[end - 1] == '\r') { end--; crlf++; } else lf++;
            lines.add(new String(bytes, start, end - start, StandardCharsets.ISO_8859_1)); start = i + 1;
        }
        if (start < bytes.length) lines.add(new String(bytes, start, bytes.length - start, StandardCharsets.ISO_8859_1));
        if (lines.size() % 4 != 0) throw new IOException("Incomplete four-line FASTQ: " + description);
        List<List<String>> records = new ArrayList<>();
        for (int i = 0; i < lines.size(); i += 4) {
            List<String> record = List.copyOf(lines.subList(i, i + 4));
            if (!record.get(0).startsWith("@") || !record.get(2).startsWith("+") || record.get(1).length() != record.get(3).length()) {
                throw new IOException("Invalid FASTQ record " + (i / 4 + 1) + ": " + description);
            }
            records.add(record);
        }
        return new Fastq(bytes, records, lf, crlf, bytes.length > 0 && bytes[bytes.length - 1] == '\n');
    }

    private static Map<String, Object> describe(Fastq value) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("decodedSha256", sha(value.bytes)); result.put("decodedBytes", value.bytes.length);
        result.put("records", value.records.size()); result.put("lfLines", value.lf); result.put("crlfLines", value.crlf);
        result.put("finalNewline", value.finalNewline); return result;
    }

    private static Map<String, String> rawHashes(Map<String, Path> paths) throws Exception {
        Map<String, String> hashes = new LinkedHashMap<>();
        for (var entry : paths.entrySet()) hashes.put(entry.getKey(), sha(Files.readAllBytes(entry.getValue())));
        return hashes;
    }

    private static String sha(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void write(Path path, Object value) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
    }

    private static final class ProcessProbe {
        final boolean forced; final Path directory; final Map<String, Object> record = new LinkedHashMap<>();
        int calls;
        ProcessProbe(boolean forced, Path directory) throws IOException {
            this.forced = forced; this.directory = Files.createDirectories(directory);
        }
        int run(List<String> original, Path work, Path out, Path err, boolean append) throws IOException, InterruptedException {
            calls++;
            List<String> command = new ArrayList<>(original); command.add("threads=1");
            Path stdout = out == null ? directory.resolve("stdout.log") : out;
            Path stderr = err == null ? directory.resolve("stderr.log") : err;
            record.put("command", command); record.put("workDir", work.toString()); record.put("forcedNonzero", forced);
            record.put("stdout", stdout.toString()); record.put("stderr", stderr.toString()); record.put("timeoutSeconds", 120);
            record.put("exitCode", null);
            long began = System.nanoTime();
            Process process = null; Set<ProcessHandle> owned = new LinkedHashSet<>();
            try {
                if (forced) {
                    Files.writeString(stdout, "");
                    Files.writeString(stderr, "Integration test: intentionally forced process exit 17 for Java fallback.\n",
                            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                    record.put("exitCode", FORCED_EXIT); return FORCED_EXIT;
                }
                ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile());
                builder.redirectOutput(append ? ProcessBuilder.Redirect.appendTo(stdout.toFile()) : ProcessBuilder.Redirect.to(stdout.toFile()));
                builder.redirectError(append ? ProcessBuilder.Redirect.appendTo(stderr.toFile()) : ProcessBuilder.Redirect.to(stderr.toFile()));
                process = builder.start(); owned.add(process.toHandle());
                while (true) {
                    process.descendants().forEach(owned::add);
                    if (process.waitFor(200, TimeUnit.MILLISECONDS)) break;
                    if (System.nanoTime() - began > TimeUnit.SECONDS.toNanos(120)) {
                        record.put("timedOut", true); throw new IOException("BBMap exceeded 120-second watchdog");
                    }
                }
                record.put("exitCode", process.exitValue()); return process.exitValue();
            } catch (IOException | InterruptedException failure) {
                record.put("error", failure.toString()); throw failure;
            } finally {
                if (process != null) process.descendants().forEach(owned::add);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                boolean interrupted = Thread.interrupted();
                long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (owned.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < cleanupDeadline) {
                    try { Thread.sleep(20); } catch (InterruptedException ignored) { interrupted = true; }
                }
                if (interrupted) Thread.currentThread().interrupt();
                record.put("durationSeconds", (System.nanoTime() - began) / 1e9);
                record.put("survivingProcessIds", owned.stream().filter(ProcessHandle::isAlive).map(ProcessHandle::pid).toList());
                write(directory.resolve("process.json"), record);
            }
        }
    }
}
