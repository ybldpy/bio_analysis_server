package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_SHORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
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
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.QcStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.QCStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.SequenceFileUtil;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real parameter serialization and executor lifecycle; external QC commands are replaced. */
class QcStageExecutorParametersTest {

    private static final String R1 = "reads/input one.fastq";
    private static final String R2 = "reads/input two.fastq";
    private static final String READ = "@arbitrary_fixture\nACGT\n+\nIIII\n";
    private static final List<String> FASTP = List.of("mock-runtime", "mock-fastp");
    private static final List<String> FASTPLONG = List.of("mock-runtime", "mock-fastplong");

    @TempDir
    Path tempDir;
    private Path storageRoot;
    private Path inputDir;
    private Path workDir;
    private StubQcExecutor executor;
    private AnalysisPipelineToolsConfig tools;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        Files.createDirectories(storageRoot.resolve("reads"));
        Files.writeString(storageRoot.resolve(R1), READ);
        Files.writeString(storageRoot.resolve(R2), READ);
        inputDir = tempDir.resolve("input with spaces/42");
        workDir = tempDir.resolve("work with spaces/42");
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageInputDirPath", inputDir.getParent().toString());
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", workDir.getParent().toString());
        tools = new AnalysisPipelineToolsConfig();
        tools.setFastp(FASTP);
        tools.setFastplong(FASTPLONG);
        executor = new StubQcExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
    }

    @ParameterizedTest
    @MethodSource("inputModes")
    void explicitDefaultsKeepToolAndReadRouting(int readType, boolean paired, List<String> tool) throws Exception {
        QcParameters parameters = parameters(readType);

        StageRunResult<QCStageOutput> result = execute(JsonUtil.toJson(parameters), paired);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(1, executor.commands.size());
        List<String> command = executor.commands.get(0);
        assertEquals(tool, command.subList(0, tool.size()));
        assertEquals(inputDir.resolve("input one.fastq").toString(), option(command, "-i"));
        assertEquals(workDir.resolve("input one.fastq").toString(), option(command, "-o"));
        assertFilterArguments(command, readType == READ_LEN_TYPE_LONG, 15, 40, 0,
                readType == READ_LEN_TYPE_LONG ? 20 : 15, 0, readType == READ_LEN_TYPE_LONG ? 1_000_000 : 5, 10);
        assertEquals(paired, command.contains("-I"));
        assertEquals(paired, command.contains("-O"));
        if (paired) {
            assertEquals(inputDir.resolve("input two.fastq").toString(), option(command, "-I"));
            assertEquals(workDir.resolve("input two.fastq").toString(), option(command, "-O"));
            assertEquals(READ, Files.readString(Path.of(result.getStageOutput().getR2Path())));
        } else {
            assertNull(result.getStageOutput().getR2Path());
        }
        assertEquals(READ, executor.loadedRead);
        assertEquals(READ, Files.readString(Path.of(result.getStageOutput().getR1Path())));
        assertEquals(READ, Files.readString(storageRoot.resolve(R1)), "Original input remains unchanged");
        assertTrue(Files.isRegularFile(Path.of(result.getStageOutput().getJsonPath())));
        assertTrue(Files.isRegularFile(Path.of(result.getStageOutput().getHtmlPath())));
        assertFalse(Files.exists(inputDir), "Executor cleans only its staged inputs");
        assertEquals(FASTP, tools.getFastp());
        assertEquals(FASTPLONG, tools.getFastplong());
    }

    private static Stream<Arguments> inputModes() {
        return Stream.of(Arguments.of(READ_LEN_TYPE_SHORT, false, FASTP),
                Arguments.of(READ_LEN_TYPE_SHORT, true, FASTP),
                Arguments.of(READ_LEN_TYPE_LONG, false, FASTPLONG));
    }

    @ParameterizedTest
    @MethodSource("compressedInputModes")
    void zipSlotsReachQcAsFastqAndGzipSlotsKeepTheirEncoding(
            String firstFormat, String secondFormat, int readType) throws Exception {
        String read2 = READ.replace("arbitrary_fixture", "second_fixture");
        String first = compressedInput("first", firstFormat, READ);
        String second = secondFormat == null ? null : compressedInput("second", secondFormat, read2);
        byte[] originalFirst = Files.readAllBytes(storageRoot.resolve(first));

        StageRunResult<QCStageOutput> result = execute(JsonUtil.toJson(parameters(readType)), first, second);

        assertTrue(result.isSuccess(), result.getErrorLog());
        List<String> command = executor.commands.get(0);
        assertEquals(READ, executor.loadedRead);
        assertFalse(option(command, "-i").endsWith(".zip"));
        assertEquals(firstFormat.equals("gz"), option(command, "-i").endsWith(".gz"));
        assertEquals(READ, readContents(Path.of(result.getStageOutput().getR1Path())));
        assertEquals(firstFormat.equals("gz"), result.getStageOutput().getR1Path().endsWith(".gz"));
        assertArrayEquals(originalFirst, Files.readAllBytes(storageRoot.resolve(first)));
        if (second != null) {
            assertFalse(option(command, "-I").endsWith(".zip"));
            assertEquals(secondFormat.equals("gz"), option(command, "-I").endsWith(".gz"));
            assertEquals(read2, readContents(Path.of(result.getStageOutput().getR2Path())));
        } else {
            assertNull(result.getStageOutput().getR2Path());
        }
    }

    private static Stream<Arguments> compressedInputModes() {
        return Stream.of(
                Arguments.of("zip", null, READ_LEN_TYPE_SHORT),
                Arguments.of("zip", "zip", READ_LEN_TYPE_SHORT),
                Arguments.of("zip", "gz", READ_LEN_TYPE_SHORT),
                Arguments.of("gz", "zip", READ_LEN_TYPE_SHORT),
                Arguments.of("gz", "gz", READ_LEN_TYPE_SHORT),
                Arguments.of("zip", null, READ_LEN_TYPE_LONG));
    }

    private String compressedInput(String name, String format, String content) throws IOException {
        String key = "reads/" + name + ".fastq." + format;
        Path source = storageRoot.resolve(key);
        if (format.equals("zip")) {
            try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(source))) {
                zip.putNextEntry(new ZipEntry("sequence.fastq"));
                zip.write(content.getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
        } else {
            try (var writer = SequenceFileUtil.getWriter(source)) {
                writer.write(content);
            }
        }
        return key;
    }

    private static String readContents(Path path) throws IOException {
        try (var reader = SequenceFileUtil.getReader(path)) {
            StringWriter result = new StringWriter();
            reader.transferTo(result);
            return result.toString();
        }
    }

    @Test
    void explicitlyConfiguredShortFiltersDoNotReadLongFilters() throws Exception {
        QcParameters parameters = parameters(READ_LEN_TYPE_SHORT);
        setFilters(parameters.getShortReadFilters(), 18, 22, 7, 35, 500, 9);
        setFilters(parameters.getLongReadFilters(), 11, 60, 4, 400, 20_000, 100);
        parameters.getLongReadFilters().setNPercentLimit(3);

        StageRunResult<QCStageOutput> result = execute(JsonUtil.toJson(parameters), true);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertFilterArguments(executor.commands.get(0), false, 18, 22, 7, 35, 500, 9, 0);
    }

    @Test
    void explicitlyConfiguredLongFiltersDoNotReadShortFilters() throws Exception {
        QcParameters parameters = parameters(READ_LEN_TYPE_LONG);
        setFilters(parameters.getShortReadFilters(), 18, 22, 7, 35, 500, 9);
        setFilters(parameters.getLongReadFilters(), 11, 60, 4, 400, 20_000, 100);
        parameters.getLongReadFilters().setNPercentLimit(3);

        StageRunResult<QCStageOutput> result = execute(JsonUtil.toJson(parameters), false);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertFilterArguments(executor.commands.get(0), true, 11, 60, 4, 400, 20_000, 100, 3);
    }

    @ParameterizedTest
    @MethodSource("legacyParameterJson")
    void oldOrNullFilterSnapshotsStillRunWithDefaults(int readType, String filterJson) throws Exception {
        String json = "{\"readMeta\":{\"readLenType\":" + readType + "}" + filterJson + "}";

        StageRunResult<QCStageOutput> result = execute(json, false);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertFilterArguments(executor.commands.get(0), readType == READ_LEN_TYPE_LONG, 15, 40, 0,
                readType == READ_LEN_TYPE_LONG ? 20 : 15, 0, readType == READ_LEN_TYPE_LONG ? 1_000_000 : 5, 10);
    }

    private static Stream<Arguments> legacyParameterJson() {
        return Stream.of(Arguments.of(READ_LEN_TYPE_SHORT, ""), Arguments.of(READ_LEN_TYPE_LONG, ""),
                Arguments.of(READ_LEN_TYPE_SHORT, ",\"shortReadFilters\":null"),
                Arguments.of(READ_LEN_TYPE_LONG, ",\"longReadFilters\":null"));
    }

    private static QcParameters parameters(int readType) {
        SequenceMeta meta = new SequenceMeta();
        meta.setReadLenType(readType);
        meta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_READ);
        QcParameters parameters = new QcParameters();
        parameters.setReadMeta(meta);
        return parameters;
    }

    private StageRunResult<QCStageOutput> execute(String json, boolean paired) throws Exception {
        return execute(json, R1, paired ? R2 : null);
    }

    private StageRunResult<QCStageOutput> execute(String json, String first, String second) throws Exception {
        QcStageInputUrls input = new QcStageInputUrls();
        input.setRead1(first);
        input.setRead2(second);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(1L);
        stage.setStageId(42L);
        stage.setVersion(0);
        stage.setStageIndex(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_QC);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(json);
        return executor.execute(stage);
    }

    private static void setFilters(QcParameters.ReadFilters filters, int quality, int unqualified,
            int mean, int minimumLength, int maximumLength, int nBaseLimit) {
        filters.setQualifiedQualityPhred(quality);
        filters.setUnqualifiedPercentLimit(unqualified);
        filters.setMeanQuality(mean);
        filters.setMinimumLength(minimumLength);
        filters.setMaximumLength(maximumLength);
        filters.setNBaseLimit(nBaseLimit);
    }

    private static void assertFilterArguments(List<String> command, boolean longRead, int quality, int unqualified,
            int mean, int minimumLength, int maximumLength, int nBaseLimit, int nPercentLimit) {
        assertEquals(Integer.toString(quality), option(command, "--qualified_quality_phred"));
        assertEquals(Integer.toString(unqualified), option(command, "--unqualified_percent_limit"));
        assertEquals(Integer.toString(mean), option(command, longRead ? "--mean_qual" : "--average_qual"));
        assertFalse(command.contains(longRead ? "--average_qual" : "--mean_qual"));
        assertEquals(Integer.toString(minimumLength), option(command, "--length_required"));
        assertEquals(Integer.toString(maximumLength), option(command, "--length_limit"));
        assertEquals(Integer.toString(nBaseLimit), option(command, "--n_base_limit"));
        if (longRead) {
            assertEquals(Integer.toString(nPercentLimit), option(command, "--n_percent_limit"));
        } else {
            assertFalse(command.contains("--n_percent_limit"));
        }
        assertFalse(command.contains("--disable_quality_filtering"));
        assertFalse(command.contains("--disable_length_filtering"));
    }

    private static String option(List<String> command, String flag) {
        int index = command.indexOf(flag);
        assertTrue(index >= 0 && index + 1 < command.size(), "Missing option " + flag + ": " + command);
        assertEquals(1L, command.stream().filter(flag::equals).count(), "Option must occur once: " + flag);
        return command.get(index + 1);
    }

    private static final class StubQcExecutor extends QcStageExecutor {
        private final List<List<String>> commands = new ArrayList<>();
        private String loadedRead;

        @Override
        protected int runSubProcess(List<String> command, Path workDir) throws IOException {
            commands.add(List.copyOf(command));
            loadedRead = readContents(Path.of(option(command, "-i")));
            try (var writer = SequenceFileUtil.getWriter(Path.of(option(command, "-o")))) {
                writer.write(loadedRead);
            }
            if (command.contains("-I")) {
                try (var writer = SequenceFileUtil.getWriter(Path.of(option(command, "-O")))) {
                    writer.write(readContents(Path.of(option(command, "-I"))));
                }
            }
            boolean longRead = command.contains("mock-fastplong");
            Files.writeString(Path.of(option(command, longRead ? "-j" : "--json")), "{\"stub\":true}\n");
            Files.writeString(Path.of(option(command, longRead ? "-h" : "--html")), "<html>stub</html>\n");
            assertNotNull(workDir);
            return 0;
        }
    }
}
