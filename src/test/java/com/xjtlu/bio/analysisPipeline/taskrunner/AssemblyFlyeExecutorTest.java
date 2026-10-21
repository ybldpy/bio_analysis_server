package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_SHORT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.ONT;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.PACBIO;
import static com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AssemblyStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real executor lifecycle/local storage; only the external assembler process is replaced. */
class AssemblyFlyeExecutorTest {

    private static final String R1 = "reads/input reads.fastq";
    private static final String R2 = "reads/second.fastq";
    private static final String READ = "@arbitrary_fixture\nACGT\n+\nIIII\n";
    private static final String FASTA = ">arbitrary_contig\nACGT\n";
    private static final String CONTIGS = "configured.contigs.fasta";
    private static final String SCAFFOLDS = "configured.scaffolds.fasta";
    private static final List<String> FLYE = List.of("mock-runtime", "mock-flye");

    @TempDir
    Path tempDir;
    private Path storageRoot;
    private Path workDir;
    private Path inputDir;
    private StubToolExecutor executor;
    private AnalysisPipelineToolsConfig tools;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        Files.createDirectories(storageRoot.resolve("reads"));
        Files.writeString(storageRoot.resolve(R1), READ);
        Files.writeString(storageRoot.resolve(R2), READ);
        workDir = tempDir.resolve("work with spaces/42");
        inputDir = tempDir.resolve("input with spaces/42");
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", workDir.getParent().toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", inputDir.getParent().toString());
        tools = new AnalysisPipelineToolsConfig();
        tools.setSpades(List.of("mock-runtime", "mock-spades"));
        tools.setFlye(FLYE);
        executor = new StubToolExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        ReflectionTestUtils.setField(executor, "contigsFileName", CONTIGS);
        ReflectionTestUtils.setField(executor, "scaffoldsFileName", SCAFFOLDS);
    }

    @ParameterizedTest
    @MethodSource("defaultModes")
    void routesResolvedLongReadPlatformAndMapsFlyeAssembly(int platform, String mode) throws Exception {
        StageRunResult<AssemblyStageOutput> result = run(READ_LEN_TYPE_LONG, platform, null);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(List.of("mock-runtime", "mock-flye", "--" + mode,
                inputDir.resolve("input reads.fastq").toString(), "--threads", "3", "--out-dir",
                workDir.toString()), executor.commands.get(0));
        assertEquals(1, executor.commands.size());
        assertEquals(workDir, executor.executedWorkDir);
        assertEquals(READ, executor.loadedRead);
        assertEquals(workDir.resolve("assembly.fasta").toString(), result.getStageOutput().getContigPath());
        assertNull(result.getStageOutput().getScaffoldPath(), "Do not reuse a configured SPAdes scaffold");
        assertEquals(FASTA, Files.readString(Path.of(result.getStageOutput().getContigPath())));
        assertTrue(Files.isRegularFile(workDir.resolve(SCAFFOLDS)), "Fixture includes an unrelated scaffold");
        assertEquals(READ, Files.readString(storageRoot.resolve(R1)));
        assertFalse(Files.exists(inputDir), "The lifecycle must remove only staged inputs");
        assertEquals(FLYE, tools.getFlye(), "Command construction must not mutate tool configuration");
    }

    private static Stream<Arguments> defaultModes() {
        return Stream.of(Arguments.of(ONT, "nano-raw"), Arguments.of(UNKNOWN, "nano-raw"),
                Arguments.of(PACBIO, "pacbio-raw"));
    }

    @ParameterizedTest
    @MethodSource("configuredModes")
    void configuredFamilyModeOverridesDefault(int platform, String field, String mode) throws Exception {
        ReflectionTestUtils.setField(executor, field, mode);

        StageRunResult<AssemblyStageOutput> result = run(READ_LEN_TYPE_LONG, platform, null);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(inputDir.resolve("input reads.fastq").toString(), option(executor.commands.get(0), "--" + mode));
    }

    private static Stream<Arguments> configuredModes() {
        return Stream.of(Arguments.of(ONT, "flyeOntInputMode", "nano-hq"),
                Arguments.of(ONT, "flyeOntInputMode", "nano-corr"),
                Arguments.of(UNKNOWN, "flyeOntInputMode", "nano-hq"),
                Arguments.of(PACBIO, "flyePacbioInputMode", "pacbio-hifi"),
                Arguments.of(PACBIO, "flyePacbioInputMode", "pacbio-corr"));
    }

    @ParameterizedTest
    @MethodSource("invalidModes")
    void invalidOrWrongFamilyModeFailsBeforeTool(int platform, String field, String mode) throws Exception {
        ReflectionTestUtils.setField(executor, field, mode);
        assertRejectedBeforeTool(run(READ_LEN_TYPE_LONG, platform, null), "Invalid Flye input mode");
    }

    private static Stream<Arguments> invalidModes() {
        return Stream.of(Arguments.of(ONT, "flyeOntInputMode", "pacbio-hifi"),
                Arguments.of(PACBIO, "flyePacbioInputMode", "nano-raw"),
                Arguments.of(UNKNOWN, "flyeOntInputMode", "--nano-raw"),
                Arguments.of(ONT, "flyeOntInputMode", ""),
                Arguments.of(PACBIO, "flyePacbioInputMode", (String) null));
    }

    @ParameterizedTest
    @ValueSource(ints = {Constants.SequencingPlatform.ILLUMINA, -1, 9999})
    void unsupportedExplicitPlatformDoesNotSilentlyFallBackToOnt(int platform) throws Exception {
        assertRejectedBeforeTool(run(READ_LEN_TYPE_LONG, platform, null), "Unsupported sequencingPlatform");
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty", "blank-token", "null-token"})
    void missingOrMalformedFlyeCommandHasClearConfigurationFailure(String kind) throws Exception {
        tools.setFlye(switch (kind) {
            case "missing" -> null;
            case "empty" -> List.of();
            case "blank-token" -> List.of("mock-flye", " ");
            default -> Arrays.asList("mock-flye", null);
        });
        assertRejectedBeforeTool(run(READ_LEN_TYPE_LONG, ONT, null), "analysis-pipeline.tools.flye");
    }

    @Test
    void pairedSlotsAreRejectedForLongReadsBeforeTool() throws Exception {
        assertRejectedBeforeTool(run(READ_LEN_TYPE_LONG, ONT, R2), "single read input");
    }

    @Test
    void blankOptionalR2IsAbsentOnFlyeRoute() throws Exception {
        StageRunResult<AssemblyStageOutput> result = run(READ_LEN_TYPE_LONG, ONT, " ");
        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(1, executor.commands.size());
        assertFalse(executor.commands.get(0).contains("-2"));
    }

    @ParameterizedTest
    @ValueSource(ints = {UNKNOWN, ONT, PACBIO, Constants.SequencingPlatform.ILLUMINA})
    void resolvedShortReadsKeepSpadesWithoutRequiringFlyeConfiguration(int platform) throws Exception {
        tools.setFlye(null);
        StageRunResult<AssemblyStageOutput> result = run(READ_LEN_TYPE_SHORT, platform, null);
        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(List.of("mock-runtime", "mock-spades"), executor.commands.get(0).subList(0, 2));
        assertEquals(inputDir.resolve("input reads.fastq").toString(), option(executor.commands.get(0), "-s"));
        assertEquals(workDir.resolve(CONTIGS).toString(), result.getStageOutput().getContigPath());
    }

    @ParameterizedTest
    @ValueSource(strings = {"nonzero", "io"})
    void flyeProcessFailureDoesNotPublishOutput(String kind) throws Exception {
        executor.exitCode = 7;
        if (kind.equals("io")) {
            executor.processException = new IOException("Injected launch failure");
        }
        StageRunResult<AssemblyStageOutput> result = run(READ_LEN_TYPE_LONG, ONT, null);
        assertFailedAfterTool(result);
        assertTrue(result.getFailReason().contains("Flye"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty"})
    void legacyArtifactsCannotReplaceMissingRequiredFlyeAssembly(String kind) throws Exception {
        executor.assemblyContent = kind.equals("missing") ? null : "";
        assertFailedAfterTool(run(READ_LEN_TYPE_LONG, ONT, null));
        assertTrue(Files.isRegularFile(workDir.resolve(CONTIGS)));
        assertTrue(Files.isRegularFile(workDir.resolve(SCAFFOLDS)));
    }

    @Test
    void flyeConfigurationGetterReturnsANullSafeDefensiveCopy() {
        tools.getFlye().clear();
        assertEquals(FLYE, tools.getFlye());
        tools.setFlye(null);
        assertNull(tools.getFlye());
    }

    private StageRunResult<AssemblyStageOutput> run(int lengthType, int platform, String r2) throws Exception {
        AssemblyInputUrls input = new AssemblyInputUrls();
        input.setRead1Url(R1);
        input.setRead2Url(r2);
        SequenceMeta meta = new SequenceMeta();
        meta.setReadLenType(lengthType);
        meta.setSequencingPlatform(platform);
        meta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_READ);
        AssemblyStageParameters parameters = new AssemblyStageParameters();
        parameters.setReadMeta(meta);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(1L);
        stage.setStageId(42L);
        stage.setVersion(0);
        stage.setStageIndex(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_ASSEMBLY);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(parameters));
        return executor.execute(stage);
    }

    private void assertRejectedBeforeTool(StageRunResult<AssemblyStageOutput> result, String message) {
        assertFailure(result);
        assertTrue(result.getFailReason().contains(message), result.getErrorLog());
        assertTrue(executor.commands.isEmpty());
    }

    private void assertFailedAfterTool(StageRunResult<AssemblyStageOutput> result) {
        assertFailure(result);
        assertEquals(1, executor.commands.size());
    }

    private static void assertFailure(StageRunResult<AssemblyStageOutput> result) {
        assertNotNull(result);
        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
        assertNotNull(result.getFailReason());
    }

    private static String option(List<String> command, String flag) {
        int index = command.indexOf(flag);
        assertTrue(index >= 0 && index + 1 < command.size(), "Missing option: " + flag);
        return command.get(index + 1);
    }

    private static final class StubToolExecutor extends AssemblyExecutor {
        private final List<List<String>> commands = new ArrayList<>();
        private int exitCode;
        private Exception processException;
        private String assemblyContent = FASTA;
        private Path executedWorkDir;
        private String loadedRead;

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            commands.add(List.copyOf(command));
            executedWorkDir = workDir;
            try {
                String inputFlag = command.contains("mock-flye") ? command.get(2) : "-s";
                loadedRead = Files.readString(Path.of(option(command, inputFlag)));
                if (exitCode == 0 && processException == null) {
                    Files.writeString(workDir.resolve(CONTIGS), FASTA);
                    Files.writeString(workDir.resolve(SCAFFOLDS), FASTA);
                    if (assemblyContent != null) {
                        Files.writeString(workDir.resolve("assembly.fasta"), assemblyContent);
                    }
                }
                return new ExecuteResult(exitCode, processException);
            } catch (IOException exception) {
                throw new AssertionError("Cannot prepare isolated assembler output fixture", exception);
            }
        }
    }
}
