package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Exercises the production lifecycle and parser while replacing only the external process. */
class ReadInspectExecutionFailureTest {

    private static final String INPUT_OBJECT = "reads/input.fastq";
    private static final String NOT_PAIRED_LOG =
            "Names do not appear to be correctly paired.\nread-a\nread-b\n";
    private static final String SINGLE_INPUT =
            "@read-a\nACGT\n+\nIIII\n@read-b\nTGCA\n+\nIIII\n";
    private static final String FIRST_MATE = "@pair/1\nACGT\n+\n!!!!\n";
    private static final String SECOND_MATE = "@pair/2\nTGCA\n+\nIIII\n";
    private static final String PAIRED_INPUT = FIRST_MATE + SECOND_MATE;

    @TempDir
    Path tempDir;

    @Test
    void positivePairingMessageUsesLocalInspectionInsteadOfSingleEndShortcut() throws Exception {
        StageRunResult<ReadInspectStageOutput> result = execute(
                1, null, "Input is correctly paired\nOutput write failed\n", PAIRED_INPUT);

        assertSplitOutput(result);
    }

    @Test
    void processIOExceptionUsesLocalInspectionInsteadOfTrustingUnpairedLog() throws Exception {
        StageRunResult<ReadInspectStageOutput> result = execute(
                -1, new IOException("Cannot start the external process"), NOT_PAIRED_LOG, PAIRED_INPUT);

        assertSplitOutput(result);
    }

    @Test
    void successfulExitWithoutSplitOutputsFails() throws Exception {
        StageRunResult<ReadInspectStageOutput> result = execute(
                0, null, "Names appear to be correctly paired.\n");

        assertFalse(result.isSuccess(),
                "A zero exit code does not establish that both split FASTQ outputs exist");
    }

    @ParameterizedTest
    @MethodSource("unpairedDiagnostics")
    void genuineUnpairedDiagnosticWithoutExceptionPreservesSingleInput(String diagnosticLog) throws Exception {
        StageRunResult<ReadInspectStageOutput> result = execute(1, null, diagnosticLog);

        assertTrue(result.isSuccess(), result.getErrorLog());
        ReadInspectStageOutput output = result.getStageOutput();
        assertNotNull(output);
        assertTrue(output.isUseOriginalSequence());
        assertEquals(INPUT_OBJECT, output.getOriginalSequenceUrl());
        assertEquals(Constants.SequenceInput.READ_LEN_TYPE_SHORT, output.getReadLenType());
        assertEquals(Constants.SequenceInput.QUALITY_ENCODING_33, output.getQualityEncoding());
        assertNull(output.getR1Path());
        assertNull(output.getR2Path());
    }

    private static List<String> unpairedDiagnostics() {
        return List.of(
                NOT_PAIRED_LOG,
                "Input is being processed as paired\n" + NOT_PAIRED_LOG,
                "  NAMES DO NOT APPEAR TO BE CORRECTLY PAIRED.  \r\nread-a\r\nread-b\r\n",
                "\tNames\t do  not\tappear to be\tcorrectly paired  .\t\n",
                "Names do not appear to be correctly paired",
                "Names do not appear to be correctly paired!\n",
                "Names do not appear to be paired.\n");
    }

    @ParameterizedTest
    @MethodSource("nonDiagnosticMessages")
    void unrelatedOrPositiveTextDoesNotBypassLocalPairingInspection(String diagnosticLog) throws Exception {
        StageRunResult<ReadInspectStageOutput> result = execute(1, null, diagnosticLog, PAIRED_INPUT);

        assertSplitOutput(result);
    }

    private static List<String> nonDiagnosticMessages() {
        return List.of(
                "",
                "Names appear to be correctly paired.\nOutput write failed\n",
                "Input is correctly paired\nOutput write failed\n",
                "Input is being processed as paired\nFatal error: cannot open output\n",
                "Cannot read /input/not paired.fastq\n",
                "File not paired.fastq is missing\n",
                "Cannot open file: Names do not appear to be correctly paired.fastq\n",
                "Hint: Names do not appear to be correctly paired.\n",
                "Names do not appear to be correctly paired. Output write failed\n",
                "Names do not\nappear to be correctly paired.\n",
                "Names do not appear to be correctly pairedness\n",
                "The phrase 'Names do not appear to be correctly paired.' is only an example\n");
    }

    @Test
    void genericExternalProcessFailureRemainsFailureWhenLocalLayoutIsAmbiguous() throws Exception {
        StageRunResult<ReadInspectStageOutput> result = execute(
                1, null, "Fatal error: cannot read input\n", PAIRED_INPUT + SINGLE_INPUT);

        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorLog());
        assertTrue(result.getErrorLog().contains("本地判断也失败"));
    }

    private static void assertSplitOutput(StageRunResult<ReadInspectStageOutput> result) throws IOException {
        assertTrue(result.isSuccess(), result.getErrorLog());
        ReadInspectStageOutput output = result.getStageOutput();
        assertFalse(output.isUseOriginalSequence(), "A log message must not bypass local interleaved inspection");
        assertNotNull(output.getR1Path());
        assertNotNull(output.getR2Path());
        assertEquals(FIRST_MATE, Files.readString(output.getR1Path()));
        assertEquals(SECOND_MATE, Files.readString(output.getR2Path()));
    }

    private StageRunResult<ReadInspectStageOutput> execute(
            int exitCode, Exception exception, String diagnosticLog) throws Exception {
        return execute(exitCode, exception, diagnosticLog, SINGLE_INPUT);
    }

    private StageRunResult<ReadInspectStageOutput> execute(
            int exitCode, Exception exception, String diagnosticLog, String inputContent) throws Exception {
        Path storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        Path source = storageRoot.resolve(INPUT_OBJECT);
        Files.createDirectories(source.getParent());
        Files.writeString(source, inputContent);

        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil stageUtil = new BioStageUtil();
        ReflectionTestUtils.setField(stageUtil, "stageWorkDirPath", tempDir.resolve("work").toString());
        ReflectionTestUtils.setField(stageUtil, "stageInputDirPath", tempDir.resolve("input").toString());

        StubProcessExecutor executor = new StubProcessExecutor(exitCode, exception, diagnosticLog);
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", stageUtil);
        ReflectionTestUtils.setField(executor, "bbmapPath", tempDir.resolve("unused-bbmap").toString());

        ReadInspectStageInputUrls input = new ReadInspectStageInputUrls();
        input.setRead1Url(INPUT_OBJECT);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(1L);
        stage.setStageId(1L);
        stage.setVersion(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        stage.setStageIndex(0);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(new BaseStageParams()));

        StageRunResult<ReadInspectStageOutput> result = executor.execute(stage);

        assertNotNull(result);
        assertEquals(1, executor.commandCount,
                "The real lifecycle and FASTQ parser must reach the injected subprocess outcome");
        return result;
    }

    private static final class StubProcessExecutor extends ReadInspectStageExecutor {
        private final int exitCode;
        private final Exception exception;
        private final String diagnosticLog;
        private int commandCount;

        private StubProcessExecutor(int exitCode, Exception exception, String diagnosticLog) {
            this.exitCode = exitCode;
            this.exception = exception;
            this.diagnosticLog = diagnosticLog;
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir, Path stdout, Path stderr) {
            commandCount++;
            assertNotNull(stderr, "The inspector must capture the subprocess diagnostic log");
            try {
                Files.writeString(stderr, diagnosticLog);
            } catch (IOException e) {
                throw new AssertionError("Could not create isolated diagnostic fixture", e);
            }
            return new ExecuteResult(exitCode, exception);
        }
    }
}
