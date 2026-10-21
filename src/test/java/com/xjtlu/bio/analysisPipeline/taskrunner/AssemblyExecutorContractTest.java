package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_ASSEMBLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real executor lifecycle and local storage, with only the external tool replaced. */
class AssemblyExecutorContractTest {

    private static final String R1 = "reads/source_R1.fastq";
    private static final String R2 = "reads/source_R2.fastq";
    private static final String FIRST_READ = "@pair/1\nACGT\n+\nIIII\n";
    private static final String SECOND_READ = "@pair/2\nTGCA\n+\nIIII\n";
    private static final String CONTIGS = "assembled.contigs.fasta";
    private static final String SCAFFOLDS = "assembled.scaffolds.fasta";

    @TempDir
    Path tempDir;
    private Path storageRoot;
    private Path workDir;
    private Path inputDir;
    private StubToolExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        Files.createDirectories(storageRoot.resolve("reads"));
        Files.writeString(storageRoot.resolve(R1), FIRST_READ);
        Files.writeString(storageRoot.resolve(R2), SECOND_READ);
        workDir = tempDir.resolve("work with spaces/42");
        inputDir = tempDir.resolve("input with spaces/42");

        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", workDir.getParent().toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", inputDir.getParent().toString());
        AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
        tools.setSpades(List.of("mock-runtime", "mock-spades"));

        executor = new StubToolExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        ReflectionTestUtils.setField(executor, "contigsFileName", CONTIGS);
        ReflectionTestUtils.setField(executor, "scaffoldsFileName", SCAFFOLDS);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void routesSingleAndPairedInputsAndReturnsConfiguredArtifacts(boolean paired) throws Exception {
        StageRunResult<AssemblyStageOutput> result = run(paired ? R2 : null);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(1, executor.commands.size());
        List<String> command = executor.commands.get(0);
        assertEquals(List.of("mock-runtime", "mock-spades"), command.subList(0, 2));
        assertEquals(inputDir.resolve("source_R1.fastq").toString(), option(command, paired ? "-1" : "-s"));
        assertEquals(workDir.toString(), option(command, "-o"));
        assertEquals(workDir, executor.executedWorkDir);
        assertEquals(FIRST_READ, executor.loadedFirstRead);
        if (paired) {
            assertEquals(inputDir.resolve("source_R2.fastq").toString(), option(command, "-2"));
            assertEquals(SECOND_READ, executor.loadedSecondRead);
            assertFalse(command.contains("-s"));
        } else {
            assertFalse(command.contains("-1"));
            assertFalse(command.contains("-2"));
            assertNull(executor.loadedSecondRead);
        }

        AssemblyStageOutput output = result.getStageOutput();
        assertNotNull(output);
        assertEquals(workDir.resolve(CONTIGS).toString(), output.getContigPath());
        assertEquals(workDir.resolve(SCAFFOLDS).toString(), output.getScaffoldPath());
        assertEquals(executor.contigContents, Files.readString(Path.of(output.getContigPath())));
        assertEquals(executor.scaffoldContents, Files.readString(Path.of(output.getScaffoldPath())));
        assertEquals(FIRST_READ, Files.readString(storageRoot.resolve(R1)));
        assertEquals(SECOND_READ, Files.readString(storageRoot.resolve(R2)));
        assertFalse(Files.exists(inputDir), "The production lifecycle must clean only its staged input copy");
    }

    @ParameterizedTest
    @ValueSource(strings = {"r1", "r2"})
    void missingRequiredInputFailsBeforeStartingTheTool(String missing) throws Exception {
        StageRunResult<AssemblyStageOutput> result = run(
                missing.equals("r1") ? "missing-first.fastq" : R1,
                missing.equals("r2") ? "missing-second.fastq" : R2);

        assertFalse(result.isSuccess());
        assertTrue(executor.commands.isEmpty());
    }

    @Test
    void nonzeroToolExitFailsWithoutClaimingOutputs() throws Exception {
        executor.exitCode = 7;
        assertFailedAfterTool(run(null));
    }

    @Test
    void processIOExceptionFailsWithoutClaimingOutputs() throws Exception {
        executor.exitCode = -1;
        executor.processException = new IOException("Unable to launch the configured executable");
        assertFailedAfterTool(run(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty"})
    void rejectsMissingOrEmptyRequiredContigsEvenAfterSuccessfulExit(String state) throws Exception {
        executor.contigContents = state.equals("missing") ? null : "";
        assertFailedAfterTool(run(null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty"})
    void permitsAbsentOptionalScaffoldsAndDoesNotReturnAnInvalidPath(String state) throws Exception {
        executor.scaffoldContents = state.equals("missing") ? null : "";

        StageRunResult<AssemblyStageOutput> result = run(null);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertNotNull(result.getStageOutput());
        assertNull(result.getStageOutput().getScaffoldPath());
        assertEquals(executor.contigContents,
                Files.readString(Path.of(result.getStageOutput().getContigPath())));
    }

    @Test
    @EnabledIfSystemProperty(named = "assembly.contract.audit", matches = "true")
    void auditRejectsNonFastaTextInsteadOfTreatingNonemptyBytesAsAssembly() throws Exception {
        executor.contigContents = "tool diagnostic text, not a FASTA record\n";

        StageRunResult<AssemblyStageOutput> result = run(null);

        assertEquals(1, executor.commands.size());
        assertFalse(result.isSuccess(),
                "Assembly success must not certify an arbitrary nonempty text file as contigs");
    }

    private StageRunResult<AssemblyStageOutput> run(String r2) throws Exception {
        return run(R1, r2);
    }

    private StageRunResult<AssemblyStageOutput> run(String r1, String r2) throws Exception {
        AssemblyInputUrls input = new AssemblyInputUrls();
        input.setRead1Url(r1);
        input.setRead2Url(r2);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(1L);
        stage.setStageId(42L);
        stage.setVersion(3);
        stage.setStageIndex(0);
        stage.setStageType(PIPELINE_STAGE_ASSEMBLY);
        stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(new BaseStageParams()));
        StageRunResult<AssemblyStageOutput> result = executor.execute(stage);
        assertNotNull(result);
        return result;
    }

    private void assertFailedAfterTool(StageRunResult<AssemblyStageOutput> result) {
        assertEquals(1, executor.commands.size());
        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
        assertNotNull(result.getFailReason());
        assertFalse(result.getFailReason().isBlank());
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
        private String contigContents = ">fixture_contig\nACGT\n";
        private String scaffoldContents = ">fixture_scaffold\nACGTNNACGT\n";
        private String loadedFirstRead;
        private String loadedSecondRead;
        private Path executedWorkDir;

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            commands.add(List.copyOf(command));
            executedWorkDir = workDir;
            try {
                loadedFirstRead = Files.readString(Path.of(option(command, command.contains("-1") ? "-1" : "-s")));
                if (command.contains("-2")) {
                    loadedSecondRead = Files.readString(Path.of(option(command, "-2")));
                }
                if (exitCode == 0 && processException == null) {
                    if (contigContents != null) {
                        Files.writeString(workDir.resolve(CONTIGS), contigContents);
                    }
                    if (scaffoldContents != null) {
                        Files.writeString(workDir.resolve(SCAFFOLDS), scaffoldContents);
                    }
                }
                return new ExecuteResult(exitCode, processException);
            } catch (IOException e) {
                throw new AssertionError("Cannot prepare isolated tool output fixture", e);
            }
        }
    }
}
