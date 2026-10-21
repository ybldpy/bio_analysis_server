package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls.SequenceInputMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real storage/parser lifecycle with arbitrary ACGT records, not biological fixtures. */
class ReadInspectFallbackTest {

    private static final String NOT_PAIRED = "Names do not appear to be correctly paired.\n";
    private static final String ONT_HEADER = "01234567-89ab-4cde-8fab-0123456789ab"
            + " runid=0123456789abcdef0123456789abcdef01234567 ch=123";
    private static final String PACBIO_HEADER = "m64011_190830_220126/123/ccs";

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "{0}: {1} pairs")
    @MethodSource("splitCases")
    void nonzeroExitSplitsSmallAndLargeInputsWithoutChangingAnyRecords(String format, int pairCount)
            throws Exception {
        Reads reads = pairs(pairCount);
        StubExecutor executor = failedProcess();

        Probe probe = execute(executor, format, reads.interleaved(), Constants.SequencingPlatform.ILLUMINA);

        assertSplit(probe, reads);
        assertMetadata(probe.result().getStageOutput(), Constants.SequencingPlatform.ILLUMINA,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64);
        assertEquals(1, executor.commandCount, "Fallback must not retry the external tool");
    }

    private static Stream<Arguments> splitCases() {
        return Stream.of("plain", "gzip", "zip")
                .flatMap(format -> Stream.of(2, 2100).map(count -> Arguments.of(format, count)));
    }

    @Test
    void leadingOrphanIsSkippedWithoutAliasingOrDuplicatingSubsequentPairs() throws Exception {
        Reads reads = pairs(12); // 24 / 25 > 0.9: retain the existing orphan-skipping policy.
        String orphan = record("unmatched-leading-record", 13, 'h', 3);

        Probe probe = execute(failedProcess(), "plain", orphan + reads.interleaved(), null);

        assertSplit(probe, reads);
        assertFalse(decoded(probe.result().getStageOutput().getR1Path()).contains("unmatched-leading-record"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"plain", "gzip", "zip"})
    void unpairedFallbackPreservesSingleInputIncludingZipMaterialization(String format) throws Exception {
        String content = singles(3);

        Probe probe = execute(failedProcess(), format, content, null);

        ReadInspectStageOutput output = successful(probe);
        assertNull(output.getR2Path());
        assertEquals(probe.inputObject(), output.getOriginalSequenceUrl());
        if (format.equals("zip")) {
            assertFalse(output.isUseOriginalSequence());
            assertNotNull(output.getR1Path());
            assertTrue(output.getR1Path().startsWith(tempDir.resolve("work/42")));
            assertEquals(content, decoded(output.getR1Path()));
        } else {
            assertTrue(output.isUseOriginalSequence());
            assertNull(output.getR1Path());
        }
        assertMetadata(output, Constants.SequencingPlatform.UNKNOWN,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64);
    }

    @ParameterizedTest(name = "paired reads={0}, singleton reads={1}")
    @MethodSource("ambiguousCases")
    void ambiguousInputsAndExactThresholdsRemainFailures(int pairCount, int singletonCount) throws Exception {
        String content = pairs(pairCount).interleaved() + singles(singletonCount);

        Probe probe = execute(failedProcess(), "plain", content, null);

        assertFailure(probe);
    }

    private static Stream<Arguments> ambiguousCases() {
        return Stream.of(Arguments.of(1, 2), Arguments.of(1, 18), Arguments.of(9, 2));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Names appear to be correctly paired.\n", NOT_PAIRED})
    void startupIOExceptionUsesFileLayoutEvenWhenTheLogSuggestsAnotherOutcome(String log) throws Exception {
        Reads reads = pairs(2);
        StubExecutor executor = new StubExecutor(-1, new IOException("Synthetic process startup failure"), log);

        Probe probe = execute(executor, "plain", reads.interleaved(), null);

        assertSplit(probe, reads);
        assertEquals(1, executor.commandCount);
    }

    @Test
    void explicitUnpairedDiagnosticWithoutExceptionRetainsTheExistingSingleEndPath() throws Exception {
        // Deliberately pairable names distinguish the diagnostic path from Java fallback.
        Probe probe = execute(new StubExecutor(1, null, NOT_PAIRED), "plain", pairs(2).interleaved(), null);

        ReadInspectStageOutput output = successful(probe);
        assertTrue(output.isUseOriginalSequence());
        assertEquals(probe.inputObject(), output.getOriginalSequenceUrl());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
    }

    @Test
    void interruptedExecutionDoesNotFallbackEvenWithAnUnpairedDiagnostic() throws Exception {
        try {
            Probe probe = execute(new StubExecutor(-1, new InterruptedException("Synthetic interruption"), NOT_PAIRED),
                    "plain", pairs(2).interleaved(), null);

            assertFailure(probe);
        } finally {
            Thread.interrupted(); // Do not leak a restored interrupt into the JUnit worker.
        }
    }

    @Test
    void alreadyInterruptedThreadDoesNotFallbackOnAPlainNonzeroResult() throws Exception {
        StubExecutor executor = failedProcess();
        executor.interruptOnReturn = true;
        try {
            Probe probe = execute(executor, "plain", pairs(2).interleaved(), null);

            assertFailure(probe);
        } finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest(name = "header={0}, declared platform={1}, expected platform={2}")
    @MethodSource("longReadPlatforms")
    void fallbackKeepsDetectedQualityLengthAndDeclaredOrDetectedPlatform(
            String header, Integer declared, int expected)
            throws Exception {
        String content = record(header, 600, 'h', 0);

        Probe probe = execute(failedProcess(), "gzip", content, declared);

        ReadInspectStageOutput output = successful(probe);
        assertTrue(output.isUseOriginalSequence());
        assertNull(output.getR2Path());
        assertMetadata(output, expected, Constants.SequenceInput.READ_LEN_TYPE_LONG,
                Constants.SequenceInput.QUALITY_ENCODING_64);
    }

    private static Stream<Arguments> longReadPlatforms() {
        return Stream.of(
                Arguments.of(ONT_HEADER, null, Constants.SequencingPlatform.ONT),
                Arguments.of(ONT_HEADER, Constants.SequencingPlatform.UNKNOWN, Constants.SequencingPlatform.ONT),
                Arguments.of(PACBIO_HEADER, null, Constants.SequencingPlatform.PACBIO),
                Arguments.of(PACBIO_HEADER, Constants.SequencingPlatform.UNKNOWN, Constants.SequencingPlatform.PACBIO),
                Arguments.of("renamed-read-1", null, Constants.SequencingPlatform.UNKNOWN),
                Arguments.of("renamed-read-1", Constants.SequencingPlatform.UNKNOWN,
                        Constants.SequencingPlatform.UNKNOWN),
                Arguments.of(ONT_HEADER, Constants.SequencingPlatform.PACBIO, Constants.SequencingPlatform.PACBIO));
    }

    @Test
    void mainSampleLengthOverridesTheLegacyThousandRecordLengthDecision() throws Exception {
        StringBuilder content = new StringBuilder();
        for (int index = 0; index < 2000; index++) {
            content.append(record("length-fixture_" + index, index < 1000 ? 600 : 8, 'h', index));
        }

        Probe probe = execute(failedProcess(), "plain", content.toString(), null);

        ReadInspectStageOutput output = successful(probe);
        assertTrue(output.isUseOriginalSequence());
        assertMetadata(output, Constants.SequencingPlatform.UNKNOWN,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64);
    }

    @ParameterizedTest
    @ValueSource(strings = {"header", "plus", "quality-length"})
    void malformedRecordBeyondInitialSamplingCannotBeAcceptedByFallback(String invalidField) throws Exception {
        String record = record("tail-pair/1", 8, 'h', 0);
        record = switch (invalidField) {
            case "header" -> record.substring(1);
            case "plus" -> record.replace("\n+\n", "\ninvalid-plus\n");
            default -> record.replace("hhhhhhhh", "hhh");
        };
        String content = pairs(1000).interleaved() + record + record("tail-pair/2", 8, 'h', 1);

        Probe probe = execute(failedProcess(), "plain", content, null);

        assertFailure(probe);
    }

    @Test
    void nonexistentBbmapScriptTriggersRealProcessStartupIOExceptionAndFallback() throws Exception {
        assertFalse(Files.exists(tempDir.resolve("missing-bbmap/reformat.sh")));
        Reads reads = pairs(2);

        // No process override: the production ProcessBuilder fails before any tool can execute.
        Probe probe = execute(new ReadInspectStageExecutor(), "zip", reads.interleaved(), null);

        assertSplit(probe, reads);
    }

    private Probe execute(ReadInspectStageExecutor executor, String format, String content, Integer platform)
            throws Exception {
        Path storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        String inputObject = switch (format) {
            case "gzip" -> "reads.fastq.gz";
            case "zip" -> "reads.zip";
            default -> "reads.fastq";
        };
        Path source = storageRoot.resolve(inputObject);
        writeInput(source, content);
        byte[] original = Files.readAllBytes(source);
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", tempDir.resolve("work").toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", tempDir.resolve("input").toString());
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "bbmapPath", tempDir.resolve("missing-bbmap").toString());

        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls(inputObject, null);
        if (platform != null) {
            SequenceInputMeta declaration = new SequenceInputMeta();
            declaration.setSequencingPlatform(platform);
            input.setSequenceInputMeta(declaration);
        }
        SequenceMeta staleMeta = new SequenceMeta();
        staleMeta.setSequencingPlatform(Constants.SequencingPlatform.SOLID);
        staleMeta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY);
        BaseStageParams parameters = new BaseStageParams();
        parameters.setReadMeta(staleMeta);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(42L);
        stage.setPipelineId(1L);
        stage.setVersion(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(parameters));

        StageRunResult<ReadInspectStageOutput> result = executor.execute(stage);

        assertNotNull(result);
        if (executor instanceof StubExecutor stub) {
            assertEquals(1, stub.commandCount, "The real lifecycle must reach the injected process outcome once");
        }
        // Read source bytes after clearing the artificial interrupt, if one was set.
        boolean interrupted = Thread.interrupted();
        try {
            assertArrayEquals(original, Files.readAllBytes(source), "Input objects must never be rewritten");
            assertFalse(Files.exists(tempDir.resolve("input/42")), "Real lifecycle must clean staged input");
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return new Probe(result, inputObject);
    }

    private ReadInspectStageOutput successful(Probe probe) {
        assertTrue(probe.result().isSuccess(), probe.result().getErrorLog());
        assertNotNull(probe.result().getStageOutput());
        return probe.result().getStageOutput();
    }

    private void assertSplit(Probe probe, Reads expected) throws IOException {
        ReadInspectStageOutput output = successful(probe);
        assertFalse(output.isUseOriginalSequence());
        assertNotNull(output.getR1Path());
        assertNotNull(output.getR2Path());
        for (Path path : List.of(output.getR1Path(), output.getR2Path())) {
            assertTrue(path.startsWith(tempDir.resolve("work/42")));
            assertTrue(Files.isRegularFile(path), "Split outputs must survive staged-input cleanup");
        }
        assertEquals(expected.r1(), decoded(output.getR1Path()));
        assertEquals(expected.r2(), decoded(output.getR2Path()));
    }

    private static void assertFailure(Probe probe) {
        assertFalse(probe.result().isSuccess());
        assertNull(probe.result().getStageOutput());
        assertNotNull(probe.result().getErrorLog());
    }

    private static void assertMetadata(ReadInspectStageOutput output, int platform, int length, int quality) {
        assertNotNull(output.getReadMeta());
        assertEquals(platform, output.getReadMeta().getSequencingPlatform());
        assertEquals(length, output.getReadMeta().getReadLenType());
        assertEquals(quality, output.getReadMeta().getQualityEncoding());
        assertEquals(Constants.SequenceInput.SEQUENCE_LEVEL_READ, output.getReadMeta().getSequenceLevel());
    }

    private static String decoded(Path path) throws IOException {
        try (BufferedReader reader = SequenceFileUtil.getReader(path); StringWriter output = new StringWriter()) {
            reader.transferTo(output);
            return output.toString();
        }
    }

    private static void writeInput(Path path, String content) throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (path.toString().endsWith(".zip")) {
            try (ZipOutputStream archive = new ZipOutputStream(Files.newOutputStream(path))) {
                archive.putNextEntry(new ZipEntry("arbitrary.fastq"));
                archive.write(bytes);
                archive.closeEntry();
            }
        } else if (path.toString().endsWith(".gz")) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(Files.newOutputStream(path))) {
                gzip.write(bytes);
            }
        } else {
            Files.write(path, bytes);
        }
    }

    private static Reads pairs(int count) {
        StringBuilder all = new StringBuilder();
        StringBuilder first = new StringBuilder();
        StringBuilder second = new StringBuilder();
        for (int index = 0; index < count; index++) {
            String base = "fixture_" + index;
            // Preserve all three existing ID conventions, including the complete original headers.
            String firstId = base + switch (index % 3) {
                case 0 -> "/1";
                case 1 -> ".1";
                default -> " 1:N:0:arbitrary";
            };
            String secondId = base + switch (index % 3) {
                case 0 -> "/2";
                case 1 -> ".2";
                default -> " 2:N:0:arbitrary";
            };
            String r1 = record(firstId, 8 + index % 5, 'h', index);
            String r2 = record(secondId, 9 + index % 5, 'h', index + 1);
            first.append(r1);
            second.append(r2);
            all.append(r1).append(r2);
        }
        return new Reads(all.toString(), first.toString(), second.toString());
    }

    private static String singles(int count) {
        StringBuilder content = new StringBuilder();
        for (int index = 0; index < count; index++) {
            content.append(record("singleton_" + index, 8 + index % 5, 'h', index));
        }
        return content.toString();
    }

    private static String record(String id, int length, char quality, int offset) {
        String sequence = "ACGT".repeat((length + 7) / 4).substring(offset % 4, offset % 4 + length);
        return "@" + id + "\n" + sequence + "\n+\n" + String.valueOf(quality).repeat(length) + "\n";
    }

    private static StubExecutor failedProcess() {
        return new StubExecutor(2, null, "Synthetic BBMap runtime failure\n");
    }

    private record Reads(String interleaved, String r1, String r2) { }
    private record Probe(StageRunResult<ReadInspectStageOutput> result, String inputObject) { }

    private static final class StubExecutor extends ReadInspectStageExecutor {
        private final int exitCode;
        private final Exception exception;
        private final String diagnosticLog;
        private int commandCount;
        private boolean interruptOnReturn;

        private StubExecutor(int exitCode, Exception exception, String diagnosticLog) {
            this.exitCode = exitCode;
            this.exception = exception;
            this.diagnosticLog = diagnosticLog;
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir, Path stdout, Path stderr) {
            commandCount++;
            try {
                Files.writeString(stderr, diagnosticLog);
            } catch (IOException exception) {
                throw new AssertionError("Cannot create isolated diagnostic log", exception);
            }
            if (interruptOnReturn) {
                Thread.currentThread().interrupt();
            }
            return new ExecuteResult(exitCode, exception);
        }
    }
}
