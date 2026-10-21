package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls.SequenceInputMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.LongReadPlatformDetector;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Platform inference contracts using arbitrary repeated-base fixtures and a stub pairing process. */
class ReadInspectPlatformDetectionTest {

    private static final String ONT_HEADER = "@01234567-89ab-4cde-8fab-0123456789ab"
            + " runid=0123456789abcdef0123456789abcdef01234567"
            + " ch=123 start_time=2025-01-01T00:00:00Z";
    private static final String PACBIO_CCS_HEADER = "@m64011_190830_220126/123/ccs";
    private static final String PACBIO_SUBREAD_HEADER = "@m64011_190830_220126/456/0_1000";

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(ints = {Constants.SequencingPlatform.ILLUMINA, Constants.SequencingPlatform.BGI_MGI_DNBSEQ,
            Constants.SequencingPlatform.ONT, Constants.SequencingPlatform.PACBIO})
    void everyKnownDeclarationSkipsDetectionEvenWhenNativeHeadersDisagree(int platform) throws Exception {
        String contradictoryHeader = platform == Constants.SequencingPlatform.ONT
                ? PACBIO_CCS_HEADER : ONT_HEADER;
        SequenceInputMeta declaration = new SequenceInputMeta();
        declaration.setSequencingPlatform(platform);

        try (MockedStatic<LongReadPlatformDetector> detector = mockStatic(LongReadPlatformDetector.class)) {
            Probe probe = execute(List.of(contradictoryHeader), 600, declaration,
                    false, false, ProcessOutcome.UNPAIRED);

            assertSuccessfulPlatform(probe, platform, Constants.SequenceInput.READ_LEN_TYPE_LONG);
            detector.verifyNoInteractions();
        }
    }

    @ParameterizedTest(name = "{0}: detected platform={3}")
    @MethodSource("undeclaredNativeHeaderCases")
    void absentEmptyNullAndUnknownDeclarationsAllowLongReadDetection(
            String description, SequenceInputMeta declaration, String header, int expectedPlatform)
            throws Exception {
        List<String> headers = List.of(header);
        try (MockedStatic<LongReadPlatformDetector> detector = mockStatic(
                LongReadPlatformDetector.class, CALLS_REAL_METHODS)) {
            Probe probe = execute(headers, 600, declaration, false, false, ProcessOutcome.UNPAIRED);

            assertSuccessfulPlatform(probe, expectedPlatform,
                    Constants.SequenceInput.READ_LEN_TYPE_LONG);
            detector.verify(() -> LongReadPlatformDetector.detect(headers));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shortReadAndPairedShortcutNeverInvokeLongReadDetector(boolean paired) throws Exception {
        try (MockedStatic<LongReadPlatformDetector> detector = mockStatic(LongReadPlatformDetector.class)) {
            // A paired-slot input follows the existing short-read contract, regardless of header style.
            Probe probe = execute(List.of(ONT_HEADER), paired ? 600 : 150,
                    null, paired, false, ProcessOutcome.UNPAIRED);

            assertSuccessfulPlatform(probe, Constants.SequencingPlatform.UNKNOWN,
                    Constants.SequenceInput.READ_LEN_TYPE_SHORT);
            assertEquals(paired ? 0 : 1, probe.executor.commandCount);
            detector.verifyNoInteractions();
        }
    }

    @ParameterizedTest(name = "platform={1}, gzip={2}, split={3}")
    @MethodSource("nativeHeaderCases")
    void nativeHeaderInferenceSurvivesPlainOrCompressedInputAndBothSuccessPaths(
            String header, int expectedPlatform, boolean gzip, boolean split) throws Exception {
        List<String> headers = List.of(header);
        try (MockedStatic<LongReadPlatformDetector> detector = mockStatic(
                LongReadPlatformDetector.class, CALLS_REAL_METHODS)) {
            Probe probe = execute(headers, 600, null, false, gzip,
                    split ? ProcessOutcome.SPLIT : ProcessOutcome.UNPAIRED);

            ReadInspectStageOutput output = assertSuccessfulPlatform(probe, expectedPlatform,
                    Constants.SequenceInput.READ_LEN_TYPE_LONG);
            assertEquals(!split, output.isUseOriginalSequence());
            assertEquals(1, probe.executor.commandCount);
            if (split) {
                assertTrue(Files.isRegularFile(output.getR1Path()));
                assertTrue(Files.isRegularFile(output.getR2Path()));
            } else {
                assertEquals(gzip ? "reads/input.fastq.gz" : "reads/input.fastq",
                        output.getOriginalSequenceUrl());
                assertNull(output.getR1Path());
                assertNull(output.getR2Path());
            }
            detector.verify(() -> LongReadPlatformDetector.detect(headers));
        }
    }

    @ParameterizedTest(name = "{0}: ambiguous evidence={2}")
    @MethodSource("ambiguousHeaderCases")
    void unknownOrMixedHeadersRemainUnknownInsteadOfUsingStaleStageParameters(
            String declarationDescription, SequenceInputMeta declaration, String headerDescription,
            List<String> headers) throws Exception {
        try (MockedStatic<LongReadPlatformDetector> detector = mockStatic(
                LongReadPlatformDetector.class, CALLS_REAL_METHODS)) {
            Probe probe = execute(headers, 600, declaration, false, false, ProcessOutcome.UNPAIRED);

            assertSuccessfulPlatform(probe, Constants.SequencingPlatform.UNKNOWN,
                    Constants.SequenceInput.READ_LEN_TYPE_LONG);
            detector.verify(() -> LongReadPlatformDetector.detect(headers));
        }
    }

    @Test
    void detectionUsesOnlyHeadersFromTheExistingFirstTwoThousandRecordSample() throws Exception {
        List<String> headers = new ArrayList<>();
        for (int index = 0; index < 2000; index++) {
            headers.add(ONT_HEADER + " read=" + index);
        }
        headers.add(PACBIO_CCS_HEADER);
        List<String> expectedSample = List.copyOf(headers.subList(0, 2000));

        try (MockedStatic<LongReadPlatformDetector> detector = mockStatic(
                LongReadPlatformDetector.class, CALLS_REAL_METHODS)) {
            Probe probe = execute(headers, 600, null, false, false, ProcessOutcome.UNPAIRED);

            assertSuccessfulPlatform(probe, Constants.SequencingPlatform.ONT,
                    Constants.SequenceInput.READ_LEN_TYPE_LONG);
            detector.verify(() -> LongReadPlatformDetector.detect(expectedSample));
        }
    }

    @Test
    void localFallbackAfterPairingFailurePreservesDetectedMetadata() throws Exception {
        Probe probe = execute(List.of(ONT_HEADER), 600, null,
                false, false, ProcessOutcome.FAILURE);

        ReadInspectStageOutput output = assertSuccessfulPlatform(probe,
                Constants.SequencingPlatform.ONT, Constants.SequenceInput.READ_LEN_TYPE_LONG);
        assertTrue(output.isUseOriginalSequence());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
        assertEquals(1, probe.executor.commandCount);
    }

    private static List<Declaration> undeclaredMetadata() {
        SequenceInputMeta explicitNull = new SequenceInputMeta();
        explicitNull.setSequencingPlatform(null);
        SequenceInputMeta unknown = new SequenceInputMeta();
        unknown.setSequencingPlatform(Constants.SequencingPlatform.UNKNOWN);
        return List.of(
                new Declaration("absent object", null),
                new Declaration("empty object", new SequenceInputMeta()),
                new Declaration("explicit null platform", explicitNull),
                new Declaration("explicit UNKNOWN platform", unknown));
    }

    private static Stream<Arguments> undeclaredNativeHeaderCases() {
        return undeclaredMetadata().stream().flatMap(declaration ->
                Stream.of(ONT_HEADER, PACBIO_CCS_HEADER, PACBIO_SUBREAD_HEADER)
                        .map(header -> Arguments.of(declaration.description(), declaration.inputMeta(), header,
                                header.equals(ONT_HEADER) ? Constants.SequencingPlatform.ONT
                                        : Constants.SequencingPlatform.PACBIO)));
    }

    private static Stream<Arguments> nativeHeaderCases() {
        List<Arguments> cases = new ArrayList<>();
        for (String header : List.of(ONT_HEADER, PACBIO_CCS_HEADER, PACBIO_SUBREAD_HEADER)) {
            int platform = header.equals(ONT_HEADER)
                    ? Constants.SequencingPlatform.ONT : Constants.SequencingPlatform.PACBIO;
            for (boolean gzip : List.of(false, true)) {
                for (boolean split : List.of(false, true)) {
                    cases.add(Arguments.of(header, platform, gzip, split));
                }
            }
        }
        return cases.stream();
    }

    private static Stream<Arguments> ambiguousHeaderCases() {
        return undeclaredMetadata().stream().flatMap(declaration -> Stream.of(
                Arguments.of(declaration.description(), declaration.inputMeta(), "renamed reads",
                        List.of("@renamed-read-1", "@renamed-read-2")),
                Arguments.of(declaration.description(), declaration.inputMeta(), "conflicting native platforms",
                        List.of(ONT_HEADER, PACBIO_CCS_HEADER))));
    }

    private record Declaration(String description, SequenceInputMeta inputMeta) { }

    private static ReadInspectStageOutput assertSuccessfulPlatform(Probe probe, int platform, int readType) {
        assertNotNull(probe.result);
        assertTrue(probe.result.isSuccess(), probe.result.getErrorLog());
        ReadInspectStageOutput output = probe.result.getStageOutput();
        assertNotNull(output);
        assertNotNull(output.getReadMeta());
        assertEquals(platform, output.getReadMeta().getSequencingPlatform());
        assertEquals(readType, output.getReadMeta().getReadLenType());
        assertEquals(Constants.SequenceInput.QUALITY_ENCODING_33, output.getReadMeta().getQualityEncoding());
        assertEquals(Constants.SequenceInput.SEQUENCE_LEVEL_READ, output.getReadMeta().getSequenceLevel());
        return output;
    }

    private Probe execute(List<String> headers, int length, SequenceInputMeta declaration,
            boolean paired, boolean gzip, ProcessOutcome outcome) throws Exception {
        String inputObject = gzip ? "reads/input.fastq.gz" : "reads/input.fastq";
        Path storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        Path source = storageRoot.resolve(inputObject);
        Files.createDirectories(source.getParent());
        StringBuilder content = new StringBuilder();
        for (String header : headers) {
            content.append(fastqRecord(header, length));
        }
        writeFastq(source, content.toString());
        String secondObject = paired ? "reads/input_2.fastq" : null;
        if (paired) {
            writeFastq(storageRoot.resolve(secondObject), content.toString());
        }

        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil stageUtil = new BioStageUtil();
        ReflectionTestUtils.setField(stageUtil, "stageWorkDirPath", tempDir.resolve("work").toString());
        ReflectionTestUtils.setField(stageUtil, "stageInputDirPath", tempDir.resolve("input").toString());
        StubProcessExecutor executor = new StubProcessExecutor(outcome);
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", stageUtil);
        ReflectionTestUtils.setField(executor, "bbmapPath", tempDir.resolve("unused-bbmap").toString());

        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls(inputObject, secondObject);
        input.setSequenceInputMeta(declaration);
        BaseStageParams parameters = new BaseStageParams();
        SequenceMeta staleMeta = new SequenceMeta();
        staleMeta.setSequencingPlatform(Constants.SequencingPlatform.SOLID);
        staleMeta.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        staleMeta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY);
        parameters.setReadMeta(staleMeta);

        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(1L);
        stage.setStageId(1L);
        stage.setVersion(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        stage.setStageIndex(0);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(parameters));
        return new Probe(executor, executor.execute(stage));
    }

    private static String fastqRecord(String header, int length) {
        return header + "\n" + "A".repeat(length) + "\n+\n" + "!".repeat(length) + "\n";
    }

    private static void writeFastq(Path path, String content) throws IOException {
        if (path.getFileName().toString().endsWith(".gz")) {
            try (OutputStream stream = new GZIPOutputStream(Files.newOutputStream(path))) {
                stream.write(content.getBytes(StandardCharsets.UTF_8));
            }
        } else {
            Files.writeString(path, content);
        }
    }

    private enum ProcessOutcome {
        UNPAIRED, SPLIT, FAILURE
    }

    private record Probe(StubProcessExecutor executor, StageRunResult<ReadInspectStageOutput> result) {
    }

    private static final class StubProcessExecutor extends ReadInspectStageExecutor {
        private final ProcessOutcome outcome;
        private int commandCount;

        private StubProcessExecutor(ProcessOutcome outcome) {
            this.outcome = outcome;
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir, Path stdout, Path stderr) {
            commandCount++;
            try {
                if (outcome == ProcessOutcome.UNPAIRED) {
                    Files.writeString(stderr, "Names do not appear to be correctly paired.\n");
                    return new ExecuteResult(1, null);
                }
                if (outcome == ProcessOutcome.FAILURE) {
                    Files.writeString(stderr, "Fatal error: could not process input.\n");
                    return new ExecuteResult(1, null);
                }
                for (String argument : command) {
                    if (argument.startsWith("out=") || argument.startsWith("out2=")) {
                        Path output = Path.of(argument.substring(argument.indexOf('=') + 1));
                        writeFastq(output, fastqRecord("@arbitrary-split-read", 600));
                    }
                }
                Files.writeString(stderr, "Split complete.\n");
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                throw new AssertionError("Cannot create isolated subprocess fixtures", e);
            }
        }
    }
}
