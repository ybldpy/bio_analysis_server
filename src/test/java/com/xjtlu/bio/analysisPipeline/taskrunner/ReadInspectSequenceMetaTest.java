package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls.SequenceInputMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Metadata contracts through the real parser/lifecycle, with no external tools or database. */
class ReadInspectSequenceMetaTest {

    private static final String INPUT_OBJECT = "reads/input.fastq";
    private static final String SECOND_OBJECT = "reads/input_2.fastq";

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "paired input: {0}")
    @MethodSource("declarations")
    void pairedShortcutResolvesInputDeclarationWithoutUsingStaleParams(
            String description, SequenceInputMeta declaration, int expectedPlatform) throws Exception {
        Probe probe = execute(declaration, true, 8, '!', false);

        ReadInspectStageOutput output = successfulOutput(probe);
        assertMeta(output, expectedPlatform, Constants.SequenceInput.READ_LEN_TYPE_SHORT,
                Constants.SequenceInput.QUALITY_ENCODING_33);
        assertEquals(0, probe.executor.commandCount);
        assertTrue(output.isUseOriginalSequence());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
    }

    @ParameterizedTest(name = "single input: length={0}, platform={2}")
    @MethodSource("singleReadCases")
    void singleReadParserCombinesDetectedPropertiesWithDeclaredPlatform(
            int readLength, char quality, Integer platform, int expectedReadType, int expectedQuality)
            throws Exception {
        SequenceInputMeta declaration = new SequenceInputMeta();
        declaration.setSequencingPlatform(platform);
        Probe probe = execute(declaration, false, readLength, quality, false);

        ReadInspectStageOutput output = successfulOutput(probe);
        assertMeta(output, platform == null ? Constants.SequencingPlatform.UNKNOWN : platform,
                expectedReadType, expectedQuality);
        assertEquals(1, probe.executor.commandCount,
                "Use the production FASTQ parser, replacing only the external pairing check");
        assertTrue(output.isUseOriginalSequence());
        assertEquals(INPUT_OBJECT, output.getOriginalSequenceUrl());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
    }

    @Test
    void splitReadOutputAlsoRetainsTheDeclaredPlatformAndDetectedMetadata() throws Exception {
        SequenceInputMeta declaration = declaration(Constants.SequencingPlatform.ILLUMINA);
        Probe probe = execute(declaration, false, 8, 'h', true);

        ReadInspectStageOutput output = successfulOutput(probe);
        assertMeta(output, Constants.SequencingPlatform.ILLUMINA,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64);
        assertEquals(1, probe.executor.commandCount);
        assertFalse(output.isUseOriginalSequence());
        assertTrue(Files.isRegularFile(output.getR1Path()));
        assertTrue(Files.isRegularFile(output.getR2Path()));
    }

    @Test
    void fallbackAfterProcessFailurePreservesDeclaredMetadata() throws Exception {
        Probe probe = execute(declaration(Constants.SequencingPlatform.ILLUMINA),
                false, 8, '!', false, true);

        ReadInspectStageOutput output = successfulOutput(probe);
        assertMeta(output, Constants.SequencingPlatform.ILLUMINA,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_33);
        assertTrue(output.isUseOriginalSequence());
        assertEquals(INPUT_OBJECT, output.getOriginalSequenceUrl());
        assertEquals(1, probe.executor.commandCount);
    }

    @Test
    void failedFormatInspectionDoesNotInventAnOutputOrTryToMergeMetadata() throws Exception {
        Probe probe = execute(declaration(Constants.SequencingPlatform.ILLUMINA),
                false, 0, '!', false);

        assertNotNull(probe.result);
        assertFalse(probe.result.isSuccess());
        assertNotNull(probe.result.getErrorLog());
        assertNull(probe.result.getStageOutput());
        assertEquals(0, probe.executor.commandCount);
    }

    @Test
    void inputDeclarationSerializationPreservesMissingPlatformAndExplicitUnknown() throws Exception {
        ReadInspectStageInputUrls absent = JsonUtil.toObject(
                "{\"read1Url\":\"reads/input.fastq\"}", ReadInspectStageInputUrls.class);
        assertNull(absent.getSequenceInputMeta());

        ReadInspectStageInputUrls unspecified = JsonUtil.toObject(
                "{\"read1Url\":\"reads/input.fastq\",\"sequenceInputMeta\":{}}",
                ReadInspectStageInputUrls.class);
        assertNotNull(unspecified.getSequenceInputMeta());
        assertNull(unspecified.getSequenceInputMeta().getSequencingPlatform());

        unspecified.setSequenceInputMeta(declaration(Constants.SequencingPlatform.UNKNOWN));
        ReadInspectStageInputUrls restored = JsonUtil.toObject(
                JsonUtil.toJson(unspecified), ReadInspectStageInputUrls.class);
        assertEquals(Integer.valueOf(Constants.SequencingPlatform.UNKNOWN),
                restored.getSequenceInputMeta().getSequencingPlatform());
    }

    @Test
    void legacyOutputPropertiesAndFullMetadataShareOneSourceOfTruth() {
        ReadInspectStageOutput output = new ReadInspectStageOutput(
                Constants.SequenceInput.QUALITY_ENCODING_64, Constants.SequenceInput.READ_LEN_TYPE_LONG,
                true, INPUT_OBJECT, null, null, tempDir);

        assertNotNull(output.getReadMeta());
        assertEquals(output.getQualityEncoding(), output.getReadMeta().getQualityEncoding());
        assertEquals(output.getReadLenType(), output.getReadMeta().getReadLenType());

        output.setQualityEncoding(Constants.SequenceInput.QUALITY_ENCODING_33);
        output.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        assertEquals(Constants.SequenceInput.QUALITY_ENCODING_33, output.getReadMeta().getQualityEncoding());
        assertEquals(Constants.SequenceInput.READ_LEN_TYPE_SHORT, output.getReadMeta().getReadLenType());

        SequenceMeta replacement = new SequenceMeta();
        replacement.setQualityEncoding(Constants.SequenceInput.QUALITY_ENCODING_64);
        replacement.setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_LONG);
        replacement.setSequencingPlatform(Constants.SequencingPlatform.PACBIO);
        replacement.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_READ);
        output.setReadMeta(replacement);

        assertSame(replacement, output.getReadMeta());
        assertEquals(Constants.SequenceInput.QUALITY_ENCODING_64, output.getQualityEncoding());
        assertEquals(Constants.SequenceInput.READ_LEN_TYPE_LONG, output.getReadLenType());
        output.getReadMeta().setReadLenType(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        assertEquals(Constants.SequenceInput.READ_LEN_TYPE_SHORT, output.getReadLenType());
    }

    private static Stream<Arguments> declarations() {
        return Stream.of(
                Arguments.of("no metadata", null, Constants.SequencingPlatform.UNKNOWN),
                Arguments.of("platform omitted", new SequenceInputMeta(), Constants.SequencingPlatform.UNKNOWN),
                Arguments.of("explicit unknown", declaration(Constants.SequencingPlatform.UNKNOWN),
                        Constants.SequencingPlatform.UNKNOWN),
                Arguments.of("explicit platform", declaration(Constants.SequencingPlatform.ILLUMINA),
                        Constants.SequencingPlatform.ILLUMINA));
    }

    private static Stream<Arguments> singleReadCases() {
        return Stream.of(
                Arguments.of(8, 'h', Constants.SequencingPlatform.ILLUMINA,
                        Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64),
                Arguments.of(600, '!', Constants.SequencingPlatform.ONT,
                        Constants.SequenceInput.READ_LEN_TYPE_LONG, Constants.SequenceInput.QUALITY_ENCODING_33),
                Arguments.of(600, '!', Constants.SequencingPlatform.PACBIO,
                        Constants.SequenceInput.READ_LEN_TYPE_LONG, Constants.SequenceInput.QUALITY_ENCODING_33),
                Arguments.of(600, '!', null,
                        Constants.SequenceInput.READ_LEN_TYPE_LONG, Constants.SequenceInput.QUALITY_ENCODING_33));
    }

    private static SequenceInputMeta declaration(int platform) {
        SequenceInputMeta inputMeta = new SequenceInputMeta();
        inputMeta.setSequencingPlatform(platform);
        return inputMeta;
    }

    private static ReadInspectStageOutput successfulOutput(Probe probe) {
        assertNotNull(probe.result);
        assertTrue(probe.result.isSuccess(), probe.result.getErrorLog());
        assertNotNull(probe.result.getStageOutput());
        return probe.result.getStageOutput();
    }

    private static void assertMeta(ReadInspectStageOutput output, int platform, int length, int quality) {
        SequenceMeta meta = output.getReadMeta();
        assertNotNull(meta);
        assertEquals(platform, meta.getSequencingPlatform());
        assertEquals(Constants.SequenceInput.SEQUENCE_LEVEL_READ, meta.getSequenceLevel());
        assertEquals(length, meta.getReadLenType());
        assertEquals(quality, meta.getQualityEncoding());
        assertEquals(length, output.getReadLenType());
        assertEquals(quality, output.getQualityEncoding());
    }

    private Probe execute(SequenceInputMeta declaration, boolean paired, int readLength, char quality,
            boolean split) throws Exception {
        return execute(declaration, paired, readLength, quality, split, false);
    }

    private Probe execute(SequenceInputMeta declaration, boolean paired, int readLength, char quality,
            boolean split, boolean genericFailure) throws Exception {
        Path storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        Path source = storageRoot.resolve(INPUT_OBJECT);
        Files.createDirectories(source.getParent());
        Files.writeString(source, fastqRecord("arbitrary-a", readLength, quality)
                + fastqRecord("arbitrary-b", readLength, quality));
        if (paired) {
            Files.writeString(storageRoot.resolve(SECOND_OBJECT), fastqRecord("arbitrary-c", readLength, quality));
        }

        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil stageUtil = new BioStageUtil();
        ReflectionTestUtils.setField(stageUtil, "stageWorkDirPath", tempDir.resolve("work").toString());
        ReflectionTestUtils.setField(stageUtil, "stageInputDirPath", tempDir.resolve("input").toString());
        StubProcessExecutor executor = new StubProcessExecutor(split, genericFailure);
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", stageUtil);
        ReflectionTestUtils.setField(executor, "bbmapPath", tempDir.resolve("unused-bbmap").toString());

        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls();
        input.setRead1Url(INPUT_OBJECT);
        input.setRead2Url(paired ? SECOND_OBJECT : null);
        input.setSequenceInputMeta(declaration);

        SequenceMeta stale = new SequenceMeta();
        stale.setSequencingPlatform(Constants.SequencingPlatform.SOLID);
        stale.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY);
        stale.setReadLenType(readLength >= 500 ? Constants.SequenceInput.READ_LEN_TYPE_SHORT
                : Constants.SequenceInput.READ_LEN_TYPE_LONG);
        stale.setQualityEncoding(quality == 'h' ? Constants.SequenceInput.QUALITY_ENCODING_33
                : Constants.SequenceInput.QUALITY_ENCODING_64);
        BaseStageParams parameters = new BaseStageParams();
        parameters.setReadMeta(stale);

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

    private static String fastqRecord(String name, int length, char quality) {
        return "@" + name + "\n" + "A".repeat(length) + "\n+\n"
                + String.valueOf(quality).repeat(length) + "\n";
    }

    private record Probe(StubProcessExecutor executor, StageRunResult<ReadInspectStageOutput> result) {
    }

    private static final class StubProcessExecutor extends ReadInspectStageExecutor {
        private final boolean split;
        private final boolean genericFailure;
        private int commandCount;

        private StubProcessExecutor(boolean split, boolean genericFailure) {
            this.split = split;
            this.genericFailure = genericFailure;
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir, Path stdout, Path stderr) {
            commandCount++;
            try {
                if (genericFailure) {
                    Files.writeString(stderr, "Fatal error: could not process input.\n");
                    return new ExecuteResult(1, null);
                }
                if (!split) {
                    Files.writeString(stderr, "Names do not appear to be correctly paired.\n");
                    return new ExecuteResult(1, null);
                }
                for (String argument : command) {
                    if (argument.startsWith("out=") || argument.startsWith("out2=")) {
                        Files.writeString(Path.of(argument.substring(argument.indexOf('=') + 1)),
                                fastqRecord("arbitrary-pair", 8, 'h'));
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
