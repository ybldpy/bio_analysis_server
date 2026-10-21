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

import org.junit.jupiter.api.BeforeEach;
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
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real storage, archive handling and executor lifecycle; only BBMap is replaced. */
class ReadInspectZipInputTest {

    @TempDir
    Path tempDir;
    private Path storageRoot;
    private Path inputDir;
    private Path workDir;
    private PairingStubExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        inputDir = tempDir.resolve("input/42");
        workDir = tempDir.resolve("work/42");
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageInputDirPath", inputDir.getParent().toString());
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", workDir.getParent().toString());
        executor = new PairingStubExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "bbmapPath", tempDir.resolve("unused-bbmap").toString());
    }

    @ParameterizedTest
    @MethodSource("singleZipCases")
    void singleZipPublishesInspectedBytesAndMetadataAfterInputCleanup(
            String name, int length, char quality, int platform) throws Exception {
        String content = record("arbitrary-a", length, quality) + record("arbitrary-b", length, quality);
        String source = writeArchive(name, content);
        byte[] original = Files.readAllBytes(storageRoot.resolve(source));

        ReadInspectStageOutput output = run(source, null, platform);

        assertFalse(output.isUseOriginalSequence());
        assertEquals(source, output.getOriginalSequenceUrl());
        assertEquals(workDir.resolve("r1.fastq"), output.getR1Path());
        assertNull(output.getR2Path());
        assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), Files.readAllBytes(output.getR1Path()));
        assertEquals(content, executor.inspectedContent);
        assertFalse(executor.inspectedPath.getFileName().toString().endsWith(".zip"));
        assertTrue(executor.inspectedPath.getFileName().toString().endsWith(".fastq"));
        assertEquals(1, executor.commandCount);
        assertMetadata(output, platform,
                length >= 500 ? Constants.SequenceInput.READ_LEN_TYPE_LONG : Constants.SequenceInput.READ_LEN_TYPE_SHORT,
                quality == 'h' ? Constants.SequenceInput.QUALITY_ENCODING_64 : Constants.SequenceInput.QUALITY_ENCODING_33);
        assertArrayEquals(original, Files.readAllBytes(storageRoot.resolve(source)));
    }

    private static Stream<Arguments> singleZipCases() {
        return Stream.of(
                Arguments.of("single.fastq.zip", 8, 'h', Constants.SequencingPlatform.ILLUMINA),
                Arguments.of("reads.zip", 8, 'h', Constants.SequencingPlatform.UNKNOWN),
                Arguments.of("long-reads.zip", 600, '!', Constants.SequencingPlatform.ONT));
    }

    @ParameterizedTest
    @MethodSource("pairedCompression")
    void pairedSlotsMaterializeOnlyZipInputsAndKeepOtherSlotsOriginal(String first, String second)
            throws Exception {
        String r1Content = record("arbitrary-pair/1", 8, '!');
        String r2Content = record("arbitrary-pair/2", 12, '!');
        String r1 = writeArchive("first." + first, r1Content);
        String r2 = writeArchive("second." + second, r2Content);
        byte[] originalR1 = Files.readAllBytes(storageRoot.resolve(r1));
        byte[] originalR2 = Files.readAllBytes(storageRoot.resolve(r2));

        ReadInspectStageOutput output = run(r1, r2, Constants.SequencingPlatform.ILLUMINA);

        assertEquals(!first.equals("zip") && !second.equals("zip"), output.isUseOriginalSequence());
        assertEquals(r1, output.getOriginalSequenceUrl());
        assertSlot(output.getR1Path(), first.equals("zip"), "r1.fastq", r1Content);
        assertSlot(output.getR2Path(), second.equals("zip"), "r2.fastq", r2Content);
        assertEquals(0, executor.commandCount, "Separate paired files retain the existing no-BBMap shortcut");
        assertMetadata(output, Constants.SequencingPlatform.ILLUMINA,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_33);
        assertArrayEquals(originalR1, Files.readAllBytes(storageRoot.resolve(r1)));
        assertArrayEquals(originalR2, Files.readAllBytes(storageRoot.resolve(r2)));
    }

    private static Stream<Arguments> pairedCompression() {
        return Stream.of(Arguments.of("zip", "zip"), Arguments.of("zip", "gz"),
                Arguments.of("gz", "zip"), Arguments.of("gz", "gz"));
    }

    @Test
    void nativeSingleGzipStillRetainsItsOriginalObject() throws Exception {
        String content = record("arbitrary-a", 8, 'h') + record("arbitrary-b", 8, 'h');
        String source = writeArchive("single.fastq.gz", content);
        byte[] original = Files.readAllBytes(storageRoot.resolve(source));

        ReadInspectStageOutput output = run(source, null, Constants.SequencingPlatform.ILLUMINA);

        assertTrue(output.isUseOriginalSequence());
        assertEquals(source, output.getOriginalSequenceUrl());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
        assertEquals(1, executor.commandCount);
        assertEquals(content, executor.inspectedContent);
        assertMetadata(output, Constants.SequencingPlatform.ILLUMINA,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64);
        assertArrayEquals(original, Files.readAllBytes(storageRoot.resolve(source)));
    }

    @Test
    void interleavedZipKeepsBothSplitOutputsInsteadOfReplacingFirstWithUnsplitInput() throws Exception {
        executor.splitR1 = record("arbitrary-pair/1", 8, 'h');
        executor.splitR2 = record("arbitrary-pair/2", 8, 'h');
        String content = executor.splitR1 + executor.splitR2;
        String source = writeArchive("interleaved.zip", content);

        ReadInspectStageOutput output = run(source, null, Constants.SequencingPlatform.ILLUMINA);

        assertFalse(output.isUseOriginalSequence());
        assertSlot(output.getR1Path(), true, "r1.fastq", executor.splitR1);
        assertSlot(output.getR2Path(), true, "r2.fastq", executor.splitR2);
        assertEquals(content, executor.inspectedContent);
        assertEquals(1, executor.commandCount);
        assertMetadata(output, Constants.SequencingPlatform.ILLUMINA,
                Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.QUALITY_ENCODING_64);
    }

    private void assertSlot(Path path, boolean materialized, String name, String content) throws IOException {
        if (!materialized) {
            assertNull(path);
            return;
        }
        assertEquals(workDir.resolve(name), path);
        assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), Files.readAllBytes(path));
    }

    private ReadInspectStageOutput run(String r1, String r2, int platform) throws Exception {
        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls();
        input.setRead1Url(r1);
        input.setRead2Url(r2);
        SequenceInputMeta declaration = new SequenceInputMeta();
        declaration.setSequencingPlatform(platform);
        input.setSequenceInputMeta(declaration);
        BaseStageParams parameters = new BaseStageParams();
        SequenceMeta stale = new SequenceMeta();
        stale.setSequencingPlatform(Constants.SequencingPlatform.SOLID);
        stale.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY);
        parameters.setReadMeta(stale);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(42L);
        stage.setPipelineId(1L);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        stage.setVersion(0);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(parameters));

        StageRunResult<ReadInspectStageOutput> result = executor.execute(stage);

        assertNotNull(result);
        assertTrue(result.isSuccess(), result.getErrorLog());
        assertNotNull(result.getStageOutput());
        assertFalse(Files.exists(inputDir), "The real lifecycle must remove staged inputs before publication");
        assertTrue(Files.isDirectory(workDir));
        return result.getStageOutput();
    }

    private static void assertMetadata(ReadInspectStageOutput output, int platform, int length, int quality) {
        SequenceMeta meta = output.getReadMeta();
        assertNotNull(meta);
        assertEquals(platform, meta.getSequencingPlatform());
        assertEquals(Constants.SequenceInput.SEQUENCE_LEVEL_READ, meta.getSequenceLevel());
        assertEquals(length, meta.getReadLenType());
        assertEquals(quality, meta.getQualityEncoding());
    }

    private String writeArchive(String name, String content) throws IOException {
        Path target = storageRoot.resolve(name);
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        if (name.endsWith(".zip")) {
            try (ZipOutputStream archive = new ZipOutputStream(Files.newOutputStream(target))) {
                archive.putNextEntry(new ZipEntry("reads.fastq"));
                archive.write(bytes);
                archive.closeEntry();
            }
        } else {
            try (GZIPOutputStream archive = new GZIPOutputStream(Files.newOutputStream(target))) {
                archive.write(bytes);
            }
        }
        return name;
    }

    private static String record(String name, int length, char quality) {
        return "@" + name + "\n" + "ACGT".repeat((length + 3) / 4).substring(0, length)
                + "\n+\n" + String.valueOf(quality).repeat(length) + "\n";
    }

    private static final class PairingStubExecutor extends ReadInspectStageExecutor {
        private int commandCount;
        private Path inspectedPath;
        private String inspectedContent;
        private String splitR1;
        private String splitR2;

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir, Path stdout, Path stderr) {
            commandCount++;
            try {
                inspectedPath = argumentPath(command, "in=");
                try (BufferedReader reader = SequenceFileUtil.getReader(inspectedPath);
                        StringWriter content = new StringWriter()) {
                    reader.transferTo(content);
                    inspectedContent = content.toString();
                }
                if (splitR1 == null) {
                    Files.writeString(stderr, "Names do not appear to be correctly paired.\n");
                    return new ExecuteResult(1, null);
                }
                Files.writeString(argumentPath(command, "out="), splitR1);
                Files.writeString(argumentPath(command, "out2="), splitR2);
                Files.writeString(stderr, "Synthetic split complete.\n");
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                throw new AssertionError("Cannot prepare isolated pairing-check fixture", e);
            }
        }

        private static Path argumentPath(List<String> command, String prefix) {
            return Path.of(command.stream().filter(arg -> arg.startsWith(prefix))
                    .findFirst().orElseThrow().substring(prefix.length()));
        }
    }
}
