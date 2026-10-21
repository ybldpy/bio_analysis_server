package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
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

/** Opt-in, bounded-memory transport test. Never downloads or constructs an interleaved input.
 * Manifest: input/expectedR1/expectedR2 absolute paths, expectedPairs, sourceUrl, evidenceUrl,
 * referenceVersion and referenceCommand. Expected mates are external khmer results, not public goldens.
 */
@EnabledIfSystemProperty(named = "readInspect.largeManifest", matches = ".+")
class ReadInspectLargeInterleavedIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int BUFFER = 1 << 16, FORCED_EXIT = 17, WATCHDOG_SECONDS = 1200;
    private static final String[] FIELDS = {"header", "sequence", "plus", "quality"};

    @Test
    @Timeout(value = 60, unit = TimeUnit.MINUTES)
    void largePublicInputMatchesExternalReferenceAndPreservesEveryRecordByte() throws Exception {
        Path root = Files.createTempDirectory(Files.createDirectories(
                Path.of("target/readinspect-large-interleaved").toAbsolutePath()), "run-temp-");
        Map<String, Object> report = new LinkedHashMap<>();
        List<Map<String, Object>> cases = new ArrayList<>();
        List<String> failures = new ArrayList<>();
        report.put("status", "RUNNING"); report.put("cases", cases); report.put("failures", failures);
        report.put("scope", "File transport/splitting only; external khmer reference, not public golden mates or biological accuracy");
        System.out.println("Large interleaved evidence: " + root);
        try {
            Path manifestPath = Path.of(System.getProperty("readInspect.largeManifest")).toAbsolutePath();
            String manifestText = Files.readString(manifestPath);
            JsonNode manifest = JSON.readTree(manifestText);
            report.put("manifest", manifestPath.toString()); report.put("provenanceVerbatim", manifestText);
            report.put("provenance", manifest); report.put("manifestSha256", hash(manifestPath, false));
            for (String key : List.of("sourceUrl", "evidenceUrl", "referenceVersion", "referenceCommand"))
                assertTrue(manifest.path(key).isTextual() && !manifest.path(key).asText().isBlank(), "Missing " + key);
            long pairs = manifest.path("expectedPairs").asLong();
            assertEquals(2_500_000L, pairs, "Documented full-size fixture pair count");
            Map<String, Path> sources = new LinkedHashMap<>();
            Map<String, Map<String, Object>> sourceEvidence = new LinkedHashMap<>();
            report.put("sources", sourceEvidence);
            long preflight = System.nanoTime();
            for (String key : List.of("input", "expectedR1", "expectedR2")) {
                Path path = Path.of(manifest.path(key).asText());
                assertTrue(path.isAbsolute() && Files.isRegularFile(path), "Missing absolute file: " + key);
                sources.put(key, path);
                Map<String, Object> description = fingerprint(path);
                try (FastqReader reader = new FastqReader(path)) {
                    while (reader.next()) { /* Validate/count the entire source, without retaining records. */ }
                    description.putAll(reader.description());
                }
                sourceEvidence.put(key, description);
            }
            report.put("preflightSeconds", seconds(preflight));
            Path bbmap = Path.of(System.getProperty("readInspect.bbmapPath", "/home/jcy/test/bio_test/bbmap")).toAbsolutePath();
            report.put("bbmapPath", bbmap.toString());
            for (int round = 1; round <= 2; round++) for (String mode : List.of("normal", "fallback")) {
                Path run = Files.createDirectory(root.resolve(mode + "-" + round));
                Map<String, Object> row = new LinkedHashMap<>(); cases.add(row);
                row.put("mode", mode); row.put("round", round); row.put("runRoot", run.toString());
                row.put("provenance", manifest); row.put("sources", sourceEvidence);
                row.put("status", "RUNNING"); write(root.resolve("summary.json"), report);
                try {
                    runCase(sources, sourceEvidence, pairs, mode, bbmap, run, row);
                    row.put("status", "PASS");
                } catch (Exception | AssertionError failure) {
                    row.put("status", "FAIL"); row.put("error", failure.toString());
                    failures.add(run.getFileName() + ": " + failure);
                } finally {
                    write(run.resolve("case.json"), row); write(root.resolve("summary.json"), report);
                }
                if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Test interrupted");
            }
            assertTrue(failures.isEmpty(), () -> "Large interleaved failures; inspect " + root + ": " + failures);
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL"); report.put("error", failure.toString()); throw failure;
        } finally { write(root.resolve("summary.json"), report); }
    }

    private static void runCase(Map<String, Path> sources, Map<String, Map<String, Object>> sourceEvidence,
            long pairs, String mode, Path bbmap,
            Path run, Map<String, Object> row) throws Exception {
        Map<String, String> before = rawHashes(sources); row.put("rawSha256Before", before);
        List<String> errors = new ArrayList<>(); row.put("checksFailed", errors);
        try {
            LocalStorageService storage = new LocalStorageService();
            ReflectionTestUtils.setField(storage, "base", sources.get("input").getParent().toString());
            BioStageUtil directories = new BioStageUtil();
            ReflectionTestUtils.setField(directories, "stageWorkDirPath", run.resolve("work").toString());
            ReflectionTestUtils.setField(directories, "stageInputDirPath", run.resolve("input").toString());
            ProcessProbe process = new ProcessProbe(mode.equals("fallback"), run.resolve("process"));
            row.put("process", process.record);
            ReadInspectStageExecutor executor = new ReadInspectStageExecutor() {
                @Override protected int runSubProcess(List<String> command, Path work, Path out, Path err, boolean append)
                        throws IOException, InterruptedException { return process.run(command, work, out, err, append); }
            };
            ReflectionTestUtils.setField(executor, "storageService", storage);
            ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
            ReflectionTestUtils.setField(executor, "bbmapPath", bbmap.toString());
            BioPipelineStage stage = new BioPipelineStage();
            stage.setStageId(42L); stage.setPipelineId(1L); stage.setVersion(0);
            stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
            stage.setInputUrl(JsonUtil.toJson(new ReadInspectStageInputUrls(sources.get("input").getFileName().toString(), null)));
            stage.setParameters(JsonUtil.toJson(new BaseStageParams()));
            long started = System.nanoTime();
            StageRunResult<ReadInspectStageOutput> result;
            try { result = executor.execute(stage); } finally { row.put("stageRuntimeSeconds", seconds(started)); }
            row.put("stageSuccess", result.isSuccess()); row.put("stageError", result.getErrorLog());
            check(process.calls == 1, "Exactly one subprocess attempt required", errors);
            check(Integer.valueOf(mode.equals("normal") ? 0 : FORCED_EXIT).equals(process.record.get("exitCode")),
                    "Unexpected exit: normal mode must not silently fall back", errors);
            check(Boolean.FALSE.equals(process.record.get("timedOut")), "Subprocess timed out", errors);
            check(List.of().equals(process.record.get("survivingProcessIds")), "Owned subprocess survived cleanup", errors);
            check(result.isSuccess(), "Stage unsuccessful", errors);
            ReadInspectStageOutput output = result.getStageOutput();
            assertNotNull(output, "Missing stage output");
            row.put("stageOutput", JSON.valueToTree(output));
            check(!output.isUseOriginalSequence(), "Expected paired split outputs", errors);
            check(output.getReadMeta() != null && output.getReadMeta().getSequenceLevel()
                    == Constants.SequenceInput.SEQUENCE_LEVEL_READ, "Missing/incorrect read metadata", errors);
            check(!Files.exists(run.resolve("input/42")), "Staged input not cleaned up", errors);
            started = System.nanoTime();
            try {
                Path[] outputs = {output.getR1Path(), output.getR2Path()};
                for (int mate = 0; mate < 2; mate++) {
                    String label = "R" + (mate + 1);
                    Map<String, Object> evidence = new LinkedHashMap<>(); row.put("output" + label, evidence);
                    try {
                        Path path = outputs[mate];
                        assertNotNull(path, "Missing " + label);
                        assertTrue(path.toAbsolutePath().normalize().startsWith(run.resolve("work/42"))
                                && Files.isRegularFile(path), "Output must survive input cleanup");
                        evidence.putAll(fingerprint(path));
                        Object expectedHash = sourceEvidence.get("expected" + label).get("decodedSha256");
                        evidence.put("expectedDecodedSha256", expectedHash);
                        evidence.put("decodedSha256Equal", expectedHash.equals(evidence.get("decodedSha256")));
                        evidence.putAll(compareReference(sources.get("expected" + label), path, pairs));
                        check(Boolean.TRUE.equals(evidence.get("referenceByteEqual")), label + " external reference mismatch", errors);
                        check(Boolean.TRUE.equals(evidence.get("decodedSha256Equal")), label + " external reference hash mismatch", errors);
                        check(Boolean.TRUE.equals(evidence.get("validReference")), label + " invalid external reference", errors);
                    } catch (Exception | AssertionError failure) {
                        evidence.put("error", failure.toString()); errors.add(label + ": " + failure);
                    }
                }
                if (outputs[0] != null && outputs[1] != null && Files.isRegularFile(outputs[0]) && Files.isRegularFile(outputs[1])) {
                    Map<String, Object> preservation = compareOriginal(sources.get("input"), outputs, pairs);
                    row.put("originalRecordPreservation", preservation);
                    check(Boolean.TRUE.equals(preservation.get("equal")), "Original record bytes/order/format changed", errors);
                } else errors.add("Cannot compare original records: missing output mate");
            } finally { row.put("comparisonSeconds", seconds(started)); }
        } finally {
            Map<String, String> after = rawHashes(sources); row.put("rawSha256After", after);
            row.put("sourcesUnchanged", before.equals(after));
            check(before.equals(after), "Original source/reference files changed", errors);
        }
        assertTrue(errors.isEmpty(), () -> String.join("; ", errors));
    }

    private static Map<String, Object> compareReference(Path expected, Path actual, long pairs) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        long offset = 0, firstMismatch = -1;
        try (InputStream a = decoded(expected); InputStream b = decoded(actual)) {
            byte[] left = new byte[BUFFER], right = new byte[BUFFER];
            while (true) {
                int n = a.readNBytes(left, 0, left.length), m = b.readNBytes(right, 0, right.length);
                if (n == 0 && m == 0) break;
                int mismatch = Arrays.mismatch(left, 0, n, right, 0, m);
                if (firstMismatch < 0 && mismatch >= 0) firstMismatch = offset + mismatch;
                offset += Math.max(n, m);
            }
        }
        result.put("referenceByteEqual", firstMismatch < 0); result.put("firstMismatchByteOffset", firstMismatch);
        try (FastqReader reference = new FastqReader(expected)) {
            while (reference.next()) { }
            result.put("reference", reference.description());
            result.put("validReference", reference.records == pairs && reference.invalid == 0);
        }
        return result;
    }

    private static Map<String, Object> compareOriginal(Path source, Path[] outputs, long pairs) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>(); List<String> mismatches = new ArrayList<>();
        long differences = 0, pairedNamesChecked = 0, pairNameErrors = 0;
        try (FastqReader input = new FastqReader(source); FastqReader r1 = new FastqReader(outputs[0]);
                FastqReader r2 = new FastqReader(outputs[1])) {
            FastqReader[] mates = {r1, r2}; byte[] firstHeader = new byte[1024]; int firstLength = 0;
            while (input.next()) {
                int mate = (int) ((input.records - 1) % 2); FastqReader output = mates[mate];
                if (mate == 0) {
                    firstLength = input.contentLength(0);
                    if (firstHeader.length < firstLength) firstHeader = new byte[firstLength];
                    System.arraycopy(input.lines[0], 0, firstHeader, 0, firstLength);
                } else if (slashMate(firstHeader, firstLength) || slashMate(input.lines[0], input.contentLength(0))) {
                    pairedNamesChecked++;
                    int length = input.contentLength(0);
                    if (firstLength != length || firstLength < 2 || firstHeader[firstLength - 1] != '1'
                            || input.lines[0][length - 1] != '2'
                            || !Arrays.equals(firstHeader, 0, firstLength - 1, input.lines[0], 0, length - 1)) pairNameErrors++;
                }
                if (!output.next()) {
                    differences++; detail(mismatches, "R" + (mate + 1) + " missing record " + ((input.records + 1) / 2));
                    continue;
                }
                for (int field = 0; field < 4; field++) {
                    if (input.lengths[field] != output.lengths[field]
                            || !Arrays.equals(input.lines[field], 0, input.lengths[field], output.lines[field], 0, output.lengths[field])) {
                        differences++;
                        if (mismatches.size() < 8) detail(mismatches, "R" + (mate + 1) + " record " + output.records
                                + " " + FIELDS[field] + " inputSha256=" + digest(input.lines[field], input.lengths[field])
                                + " outputSha256=" + digest(output.lines[field], output.lengths[field]));
                    }
                }
            }
            for (FastqReader mate : mates) while (mate.next()) differences++;
            result.put("source", input.description()); result.put("R1", r1.description()); result.put("R2", r2.description());
            result.put("equal", differences == 0 && input.records == pairs * 2 && r1.records == pairs && r2.records == pairs
                    && input.invalid == 0 && r1.invalid == 0 && r2.invalid == 0 && pairNameErrors == 0);
        }
        result.put("differingFieldsOrMissingRecords", differences); result.put("firstMismatches", mismatches);
        result.put("slashMatePairsChecked", pairedNamesChecked); result.put("slashMatePairErrors", pairNameErrors);
        return result;
    }

    private static boolean slashMate(byte[] header, int length) {
        return length >= 2 && header[length - 2] == '/' && (header[length - 1] == '1' || header[length - 1] == '2');
    }

    /** Reuses four line buffers. Newline bytes remain present for exact record-boundary comparisons. */
    private static final class FastqReader implements Closeable {
        final InputStream stream; final byte[][] lines = {new byte[1024], new byte[1024], new byte[1024], new byte[1024]};
        final int[] lengths = new int[4]; final List<String> errors = new ArrayList<>();
        long records, invalid, bytes, lf, crlf; boolean finalNewline;
        FastqReader(Path path) throws IOException { stream = decoded(path); }
        boolean next() throws IOException {
            if ((records & 0xffff) == 0 && Thread.currentThread().isInterrupted()) throw new IOException("FASTQ scan interrupted");
            Arrays.fill(lengths, 0);
            for (int field = 0; field < 4; field++) {
                int value;
                while ((value = stream.read()) != -1) {
                    if (lengths[field] == lines[field].length) {
                        if (lines[field].length >= 16 * 1024 * 1024) throw new IOException("FASTQ line exceeds safety bound at record " + (records + 1));
                        lines[field] = Arrays.copyOf(lines[field], lines[field].length * 2);
                    }
                    lines[field][lengths[field]++] = (byte) value; bytes++; finalNewline = value == '\n';
                    if (value == '\n') {
                        if (lengths[field] > 1 && lines[field][lengths[field] - 2] == '\r') crlf++; else lf++;
                        break;
                    }
                }
                if (field == 0 && lengths[0] == 0) return false;
            }
            records++;
            if (contentLength(0) <= 1 || lines[0][0] != '@' || lengths[2] == 0 || lines[2][0] != '+'
                    || contentLength(1) == 0 || contentLength(1) != contentLength(3)) {
                invalid++; detail(errors, "Invalid header/plus/sequence-quality length at record " + records);
            }
            return true;
        }
        int contentLength(int field) {
            int n = lengths[field];
            if (n > 0 && lines[field][n - 1] == '\n') { n--; if (n > 0 && lines[field][n - 1] == '\r') n--; }
            return n;
        }
        Map<String, Object> description() {
            return Map.of("records", records, "invalidRecords", invalid, "decodedBytes", bytes,
                    "lfLines", lf, "crlfLines", crlf, "finalNewline", finalNewline, "validationErrors", errors);
        }
        @Override public void close() throws IOException { stream.close(); }
    }

    private static InputStream decoded(Path path) throws IOException {
        BufferedInputStream raw = new BufferedInputStream(Files.newInputStream(path), BUFFER);
        try {
            raw.mark(2); int first = raw.read(), second = raw.read(); raw.reset();
            return first == 0x1f && second == 0x8b ? new BufferedInputStream(new GZIPInputStream(raw, BUFFER), BUFFER) : raw;
        } catch (IOException failure) { raw.close(); throw failure; }
    }
    private static Map<String, Object> fingerprint(Path path) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("path", path.toString()); result.put("rawBytes", Files.size(path));
        result.put("rawSha256", hash(path, false)); result.put("decodedSha256", hash(path, true)); return result;
    }
    private static Map<String, String> rawHashes(Map<String, Path> sources) throws Exception {
        Map<String, String> result = new LinkedHashMap<>();
        for (var entry : sources.entrySet()) result.put(entry.getKey(), hash(entry.getValue(), false));
        return result;
    }
    private static String hash(Path path, boolean decode) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256"); byte[] buffer = new byte[BUFFER];
        try (InputStream stream = decode ? decoded(path) : new BufferedInputStream(Files.newInputStream(path), BUFFER)) {
            int n; while ((n = stream.read(buffer)) != -1) digest.update(buffer, 0, n);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
    private static String digest(byte[] bytes, int length) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256"); digest.update(bytes, 0, length);
        return HexFormat.of().formatHex(digest.digest());
    }
    private static void check(boolean condition, String message, List<String> errors) { if (!condition) errors.add(message); }
    private static void detail(List<String> details, String message) { if (details.size() < 8) details.add(message); }
    private static double seconds(long started) { return (System.nanoTime() - started) / 1e9; }
    private static void write(Path path, Object value) throws IOException { JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value); }

    private static final class ProcessProbe {
        final boolean forced; final Path directory; final Map<String, Object> record = new LinkedHashMap<>(); int calls;
        ProcessProbe(boolean forced, Path directory) throws IOException {
            this.forced = forced; this.directory = Files.createDirectories(directory);
            record.put("exitCode", null); record.put("timedOut", false); record.put("survivingProcessIds", List.of());
        }
        int run(List<String> original, Path work, Path out, Path err, boolean append) throws IOException, InterruptedException {
            calls++; List<String> command = new ArrayList<>(original); if (!forced) command.add("threads=2");
            Path stdout = out == null ? directory.resolve("stdout.log") : out;
            Path stderr = err == null ? directory.resolve("stderr.log") : err;
            record.put("attempts", calls); record.put("command", command); record.put("workDir", work.toString());
            record.put("forcedNonzero", forced); record.put("stdout", stdout.toString()); record.put("stderr", stderr.toString());
            record.put("timeoutSeconds", WATCHDOG_SECONDS); record.put("append", append);
            long began = System.nanoTime(); Process process = null; Set<ProcessHandle> owned = new LinkedHashSet<>();
            try {
                if (forced) {
                    Files.writeString(stdout, "");
                    Files.writeString(stderr, "Integration test intentionally forced exit 17 for Java fallback; no subprocess launched.\n");
                    record.put("exitCode", FORCED_EXIT); return FORCED_EXIT;
                }
                ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile());
                builder.redirectOutput(append ? ProcessBuilder.Redirect.appendTo(stdout.toFile()) : ProcessBuilder.Redirect.to(stdout.toFile()));
                builder.redirectError(append ? ProcessBuilder.Redirect.appendTo(stderr.toFile()) : ProcessBuilder.Redirect.to(stderr.toFile()));
                process = builder.start(); owned.add(process.toHandle()); record.put("pid", process.pid());
                while (true) {
                    process.descendants().forEach(owned::add);
                    if (process.waitFor(200, TimeUnit.MILLISECONDS)) break;
                    if (seconds(began) > WATCHDOG_SECONDS) {
                        record.put("timedOut", true); throw new IOException("BBMap exceeded 1200-second watchdog");
                    }
                }
                record.put("exitCode", process.exitValue()); return process.exitValue();
            } catch (IOException | InterruptedException failure) {
                record.put("error", failure.toString()); throw failure;
            } finally {
                if (process != null) process.descendants().forEach(owned::add);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
                owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                boolean interrupted = Thread.interrupted(); long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (owned.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline) {
                    try { Thread.sleep(20); } catch (InterruptedException ignored) { interrupted = true; }
                }
                if (interrupted) Thread.currentThread().interrupt();
                record.put("durationSeconds", seconds(began));
                record.put("survivingProcessIds", owned.stream().filter(ProcessHandle::isAlive).map(ProcessHandle::pid).toList());
                write(directory.resolve("process.json"), record);
            }
        }
    }
}
