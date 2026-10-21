package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/**
 * Opt-in real-tool verification: -DreadInspect.bbmapPath=/path/to/bbmap.
 * Uses the public executor, real local storage and real reformat.sh, without
 * starting Spring, workers, schedules or database connections. All fixtures and
 * executor directories are private temporary files. Assertions describe intended
 * input-validation/output-preservation contracts, not current buggy behavior.
 */
@EnabledIfSystemProperty(named = "readInspect.bbmapPath", matches = ".+")
@Timeout(60)
class ReadInspectStageVerificationTest {

    @TempDir
    Path tempDir;
    private Path storage;
    private WatchedExecutor executor;
    private long stageId;

    @BeforeEach
    void setUp(TestInfo testInfo) throws IOException {
        System.out.println("ReadInspect verification: " + testInfo.getDisplayName());
        Path bbmap = Path.of(System.getProperty("readInspect.bbmapPath"));
        assertTrue(Files.isExecutable(bbmap.resolve("reformat.sh")), "reformat.sh must be executable");
        storage = Files.createDirectories(tempDir.resolve("storage"));
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", tempDir.resolve("work").toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", tempDir.resolve("input").toString());
        LocalStorageService localStorage = new LocalStorageService();
        ReflectionTestUtils.setField(localStorage, "base", storage.toString());
        executor = new WatchedExecutor();
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "storageService", localStorage);
        ReflectionTestUtils.setField(executor, "bbmapPath", bbmap.toString());
    }

    @AfterEach
    void externalToolsMustHaveTerminatedNormally() {
        assertTrue(executor.timedOutCommands.isEmpty(),
                "Real tool hung on this input; test watchdog terminated it (not a successful rejection): "
                        + executor.timedOutCommands);
    }

    @ParameterizedTest
    @ValueSource(strings = {"fastq", "fq", "fastq.gz", "fq.gz"})
    void singleEndKeepsEveryInputByteAndCorrectMetadata(String extension) throws Exception {
        String key = write("single." + extension, singles(100, 101, 102));
        byte[] before = Files.readAllBytes(storage.resolve(key));
        assertOriginal(run(key, null), key, READ_LEN_TYPE_SHORT, QUALITY_ENCODING_33);
        assertArrayEquals(before, Files.readAllBytes(storage.resolve(key)));
    }

    @ParameterizedTest
    @CsvSource({"499,0", "500,1", "501,1", "1500,1"})
    void readLengthThresholdIsAppliedToActualSingleReadLengths(int length, int expected) throws Exception {
        String key = write("length.fastq", singles(length, length, length));
        assertOriginal(run(key, null), key, expected, QUALITY_ENCODING_33);
    }

    static Stream<Arguments> medians() {
        return Stream.of(
                Arguments.of(new int[]{1000, 100, 110}, READ_LEN_TYPE_SHORT),
                Arguments.of(new int[]{100, 1000, 1100}, READ_LEN_TYPE_LONG),
                // Current documented implementation uses the lower middle value.
                Arguments.of(new int[]{100, 900}, READ_LEN_TYPE_SHORT),
                Arguments.of(new int[]{500}, READ_LEN_TYPE_LONG));
    }

    @ParameterizedTest
    @MethodSource("medians")
    void classifiesBySampledLowerMedianRatherThanFirstRecord(int[] lengths, int expected) throws Exception {
        String key = write("median.fastq", singles(lengths));
        assertOriginal(run(key, null), key, expected, QUALITY_ENCODING_33);
    }

    @ParameterizedTest
    @CsvSource({"!,0", "I,0", "h,1"})
    void followsDocumentedQualityEncodingHeuristic(char quality, int expected) throws Exception {
        // 'I' is ambiguous; expecting 33 validates the declared default, not
        // proof that the encoding can be uniquely recovered from its characters.
        String key = write("quality.fastq", record("singleA", 100, quality) + record("singleB", 100, quality));
        assertOriginal(run(key, null), key, READ_LEN_TYPE_SHORT, expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "\t"})
    void blankSecondInputUsesSingleEndInspection(String second) throws Exception {
        String key = write("single.fastq", singles(100, 100));
        assertOriginal(run(key, second), key, READ_LEN_TYPE_SHORT, QUALITY_ENCODING_33);
    }

    @Test
    void validSeparatePairPreservesBothSources() throws Exception {
        String r1 = write("paired_R1.fastq.gz", record("pair/1", 150, 'I'));
        String r2 = write("paired_R2.fastq.gz", record("pair/2", 150, 'I'));
        byte[] before1 = Files.readAllBytes(storage.resolve(r1));
        byte[] before2 = Files.readAllBytes(storage.resolve(r2));
        assertOriginal(run(r1, r2), r1, READ_LEN_TYPE_SHORT, QUALITY_ENCODING_33);
        assertArrayEquals(before1, Files.readAllBytes(storage.resolve(r1)));
        assertArrayEquals(before2, Files.readAllBytes(storage.resolve(r2)));
    }

    @Test
    void separateInputsDoNotForceLongRecordsToShort() throws Exception {
        String r1 = write("long_R1.fastq", record("pair/1", 1000, 'I'));
        String r2 = write("long_R2.fastq", record("pair/2", 1000, 'I'));
        StageRunResult<ReadInspectStageOutput> result = run(r1, r2);
        // Rejecting an unsupported long paired layout is also safe; silently
        // labelling actual long records as short is not.
        assertTrue(!result.isSuccess() || result.getStageOutput().getReadLenType() == READ_LEN_TYPE_LONG,
                "Separate input paths must not silently force long records to SHORT");
    }

    @Test
    void separateInputsDoNotForceLegacyQualityToPhred33() throws Exception {
        String r1 = write("legacy_R1.fastq", record("pair/1", 100, 'h'));
        String r2 = write("legacy_R2.fastq", record("pair/2", 100, 'h'));
        StageRunResult<ReadInspectStageOutput> result = run(r1, r2);
        assertTrue(!result.isSuccess() || result.getStageOutput().getQualityEncoding() == QUALITY_ENCODING_64,
                "Pair path must inspect encoding or reject unsupported input instead of assuming 33");
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-both", "missing-r1", "missing-r2", "empty-r1", "empty-r2",
            "truncated-r1", "truncated-r2", "different-names", "unequal-record-counts", "corrupt-r2-gzip"})
    void rejectsInvalidSeparateInputs(String problem) throws Exception {
        String r1 = "paired_R1.fastq";
        String r2 = "paired_R2.fastq";
        if (!problem.equals("missing-both") && !problem.equals("missing-r1")) {
            write(r1, problem.equals("empty-r1") ? "" : problem.equals("truncated-r1")
                    ? "@pair/1\nACGT\n+\n" : record("pair/1", 100, 'I'));
        }
        if (!problem.equals("missing-both") && !problem.equals("missing-r2")) {
            write(r2, problem.equals("empty-r2") ? "" : problem.equals("truncated-r2")
                    ? "@pair/2\nACGT\n+\n" : record(problem.equals("different-names") ? "other/2" : "pair/2", 100, 'I')
                    + (problem.equals("unequal-record-counts") ? record("extra/2", 100, 'I') : ""));
        }
        if (problem.equals("corrupt-r2-gzip")) {
            r2 = "bad.fastq.gz";
            Files.writeString(storage.resolve(r2), "not a gzip stream");
        }
        assertRejected(run(r1, r2), problem);
    }

    static Stream<Arguments> layouts() {
        return Stream.of("slash", "illumina", "identical")
                .flatMap(style -> Stream.of("fastq", "fastq.gz").map(ext -> Arguments.of(style, ext)));
    }

    @ParameterizedTest
    @MethodSource("layouts")
    void deinterleavesWithoutDroppingReorderingOrChangingRecords(String style, String extension) throws Exception {
        StringBuilder joined = new StringBuilder(), expectedR1 = new StringBuilder(), expectedR2 = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            String id = "pair" + i;
            String suffix1 = style.equals("slash") ? "/1" : style.equals("illumina") ? " 1:N:0:ACGT" : "";
            String suffix2 = style.equals("slash") ? "/2" : style.equals("illumina") ? " 2:N:0:ACGT" : "";
            String first = record(id + suffix1, 100 + i, 'I');
            String second = record(id + suffix2, 110 + i, 'I');
            joined.append(first).append(second);
            expectedR1.append(first);
            expectedR2.append(second);
        }
        String key = write("interleaved." + extension, joined.toString());
        StageRunResult<ReadInspectStageOutput> result = run(key, null);
        assertTrue(result.isSuccess(), result::getErrorLog);
        ReadInspectStageOutput output = result.getStageOutput();
        assertFalse(output.isUseOriginalSequence());
        assertNotNull(output.getR1Path());
        assertNotNull(output.getR2Path());
        assertEquals(expectedR1.toString(), read(output.getR1Path()));
        assertEquals(expectedR2.toString(), read(output.getR2Path()));
        assertEquals(QUALITY_ENCODING_33, output.getQualityEncoding());
        assertEquals(READ_LEN_TYPE_SHORT, output.getReadLenType());
    }

    static Stream<Arguments> splitQualityFixtures() {
        return Stream.of("fastq", "fq", "fastq.gz", "fq.gz")
                // Include the reported case, then Q0/Q1/Q2/Q42 and a nonzero N quality.
                .flatMap(extension -> Stream.of("!!!!I", "!\"#KI")
                        .map(quality -> Arguments.of(extension, quality)));
    }

    @ParameterizedTest
    @MethodSource("splitQualityFixtures")
    void splittingPreservesLowAndAmbiguousBaseQualityCharacters(String extension, String quality) throws Exception {
        String first = "@pair/1\nACGTN\n+\n" + quality + "\n";
        String second = "@pair/2\nNACGT\n+\n" + new StringBuilder(quality).reverse() + "\n";
        String key = write("quality-pair." + extension, first + second);
        byte[] original = Files.readAllBytes(storage.resolve(key));
        StageRunResult<ReadInspectStageOutput> result = run(key, null);
        assertTrue(result.isSuccess(), result::getErrorLog);
        assertFalse(result.getStageOutput().isUseOriginalSequence());
        assertEquals(QUALITY_ENCODING_33, result.getStageOutput().getQualityEncoding());
        assertEquals(first, read(result.getStageOutput().getR1Path()), "Inspection must not silently rewrite quality scores");
        assertEquals(second, read(result.getStageOutput().getR2Path()));
        assertArrayEquals(original, Files.readAllBytes(storage.resolve(key)), "Source file must remain unchanged");
    }

    static Stream<Arguments> malformed() {
        return Stream.of(
                Arguments.of("empty", ""),
                Arguments.of("truncated", "@single\nACGT\n+\n"),
                Arguments.of("empty-sequence", "@single\n\n+\n\n"),
                Arguments.of("different-lengths", "@single\nACGT\n+\nIII\n"),
                Arguments.of("bad-header", "single\nACGT\n+\nIIII\n"),
                Arguments.of("bad-plus", "@single\nACGT\nwrong\nIIII\n"));
    }

    @ParameterizedTest
    @MethodSource("malformed")
    void rejectsMalformedSingleFastq(String label, String content) throws Exception {
        assertRejected(run(write("bad.fastq", content), null), label);
    }

    @Test
    void rejectsCorruptSingleGzip() throws Exception {
        Files.writeString(storage.resolve("corrupt.fastq.gz"), "not gzip");
        assertRejected(run("corrupt.fastq.gz", null), "invalid gzip header");
    }

    @Test
    void rejectsMalformedTailBeyondInspectionPrefix() throws Exception {
        StringBuilder content = new StringBuilder();
        for (int i = 0; i < 2100; i++) content.append(record("single" + i, 100, 'I'));
        content.append("@brokenTail\nACGT\n+\n");
        assertRejected(run(write("late-error.fastq", content.toString()), null),
                "A valid 2000-record prefix must not certify a malformed whole file");
    }

    @Test
    void doesNotSilentlyDowngradeAnInterleavedFileWithAnOrphan() throws Exception {
        String content = record("pairA/1", 100, 'I') + record("pairA/2", 100, 'I')
                + record("pairB/1", 100, 'I') + record("other/2", 100, 'I');
        assertRejected(run(write("broken-interleaved.fastq", content), null),
                "Mixed paired/orphan records must not silently become ordinary single-end reads");
    }

    @Test
    @EnabledIfSystemProperty(named = "readInspect.realR1", matches = ".+")
    void checksExistingRealShortReadFixtureAgainstAnIndependentFastqReader() throws Exception {
        Path source1 = Path.of(System.getProperty("readInspect.realR1"));
        Path source2 = Path.of(System.getProperty("readInspect.realR2"));
        String contents1 = read(source1), contents2 = read(source2);
        List<String> ids1 = validateRealFastq(contents1), ids2 = validateRealFastq(contents2);
        assertFalse(ids1.isEmpty());
        assertEquals(ids1, ids2, "Paired real input identifiers/counts must agree");
        Files.copy(source1, storage.resolve("real_R1.fastq.gz"));
        Files.copy(source2, storage.resolve("real_R2.fastq.gz"));
        assertOriginal(run("real_R1.fastq.gz", "real_R2.fastq.gz"), "real_R1.fastq.gz", READ_LEN_TYPE_SHORT, QUALITY_ENCODING_33);
        assertOriginal(run("real_R1.fastq.gz", null), "real_R1.fastq.gz", READ_LEN_TYPE_SHORT, QUALITY_ENCODING_33);
        assertEquals(contents1, read(storage.resolve("real_R1.fastq.gz")));
        assertEquals(contents2, read(storage.resolve("real_R2.fastq.gz")));
    }

    private static List<String> validateRealFastq(String content) {
        String[] lines = content.split("\\R");
        assertEquals(0, lines.length % 4);
        List<String> ids = new ArrayList<>();
        int minimumQuality = Integer.MAX_VALUE;
        for (int i = 0; i < lines.length; i += 4) {
            assertTrue(lines[i].startsWith("@"));
            assertTrue(lines[i + 2].startsWith("+"));
            assertEquals(lines[i + 1].length(), lines[i + 3].length());
            assertTrue(lines[i + 1].length() > 0 && lines[i + 1].length() < 500);
            String id = lines[i].split("\\s+")[0].replaceFirst("/[12]$", "");
            ids.add(id);
            for (char quality : lines[i + 3].toCharArray()) {
                assertTrue(quality >= 33 && quality <= 126);
                minimumQuality = Math.min(minimumQuality, quality);
            }
        }
        assertTrue(minimumQuality < 64, "This real fixture must provide unambiguous +33 evidence");
        return ids;
    }

    private StageRunResult<ReadInspectStageOutput> run(String r1, String r2) throws Exception {
        ReadInspectStageInputUrls inputs = new ReadInspectStageInputUrls();
        inputs.setRead1Url(r1);
        inputs.setRead2Url(r2);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(0L);
        stage.setStageId(++stageId);
        stage.setVersion(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        stage.setParameters(JsonUtil.toJson(new BaseStageParams()));
        stage.setInputUrl(JsonUtil.toJson(inputs));
        return executor.execute(stage);
    }

    private static void assertOriginal(StageRunResult<ReadInspectStageOutput> result, String url, int length, int encoding) {
        assertTrue(result.isSuccess(), result::getErrorLog);
        ReadInspectStageOutput output = result.getStageOutput();
        assertNotNull(output);
        assertTrue(output.isUseOriginalSequence());
        assertEquals(url, output.getOriginalSequenceUrl());
        assertEquals(length, output.getReadLenType());
        assertEquals(encoding, output.getQualityEncoding());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
    }

    private static void assertRejected(StageRunResult<ReadInspectStageOutput> result, String problem) {
        assertFalse(result.isSuccess(), "Invalid input was accepted: " + problem);
        assertNotNull(result.getFailReason());
        assertFalse(result.getFailReason().isBlank());
    }

    private String write(String key, String content) throws IOException {
        Path path = storage.resolve(key);
        try (OutputStream stream = key.toLowerCase(Locale.ROOT).endsWith(".gz")
                ? new GZIPOutputStream(Files.newOutputStream(path)) : Files.newOutputStream(path)) {
            stream.write(content.getBytes(StandardCharsets.UTF_8));
        }
        return key;
    }

    private static String read(Path path) throws IOException {
        if (path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".gz")) {
            try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(path))) {
                return new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return Files.readString(path);
    }

    private static String singles(int... lengths) {
        StringBuilder out = new StringBuilder();
        for (int i = 0; i < lengths.length; i++) out.append(record("single" + i, lengths[i], 'I'));
        return out.toString();
    }

    private static String record(String id, int length, char quality) {
        return "@" + id + "\n" + "ACGT".repeat((length + 3) / 4).substring(0, length)
                + "\n+\n" + String.valueOf(quality).repeat(length) + "\n";
    }

    /** Run the original command unchanged, with a test-only process-tree watchdog. */
    private static final class WatchedExecutor extends ReadInspectStageExecutor {
        private final List<String> timedOutCommands = new ArrayList<>();

        @Override
        protected int runSubProcess(List<String> command, Path workDir, Path stdout, Path stderr, boolean append)
                throws IOException, InterruptedException {
            ProcessBuilder builder = new ProcessBuilder(command).directory(workDir.toFile());
            builder.redirectOutput(stdout == null ? ProcessBuilder.Redirect.INHERIT
                    : append ? ProcessBuilder.Redirect.appendTo(stdout.toFile()) : ProcessBuilder.Redirect.to(stdout.toFile()));
            builder.redirectError(stderr == null ? ProcessBuilder.Redirect.INHERIT
                    : append ? ProcessBuilder.Redirect.appendTo(stderr.toFile()) : ProcessBuilder.Redirect.to(stderr.toFile()));
            Process process = builder.start();
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    timedOutCommands.add(String.join(" ", command));
                    if (stderr != null && Files.isRegularFile(stderr)) {
                        System.out.println("Timed-out tool diagnostic:\n" + Files.readString(stderr));
                    }
                    throw new IOException("Test watchdog: real reformat.sh did not exit within 10 seconds");
                }
                return process.exitValue();
            } finally {
                if (process.isAlive()) {
                    process.descendants().toList().forEach(ProcessHandle::destroyForcibly);
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                }
            }
        }
    }
}
