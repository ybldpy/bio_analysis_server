package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MappingInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.MappingParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.utils.JsonUtil;

class MappingStageExecutorTest {

    @TempDir
    Path tempDir;

    @Test
    void buildsDepthCommandWithBaseAndMappingQualityThresholds() {
        MappingParameters parameters = new MappingParameters();
        parameters.setMinimumMappingQuality(31);
        parameters.setMinimumBaseQuality(27);

        List<String> command = MappingStageExecutor.buildCoverageDepthCommand(
                List.of("conda", "run", "-n", "bio-map", "samtools"),
                Path.of("/work/aln_sorted.bam"),
                parameters);

        assertEquals(List.of(
                "conda", "run", "-n", "bio-map", "samtools",
                "depth", "-aa",
                "-q", "27",
                "-Q", "31",
                "/work/aln_sorted.bam"), command);
    }

    @Test
    void rejectsNegativeDepthQualityThreshold() {
        MappingParameters parameters = new MappingParameters();
        parameters.setMinimumBaseQuality(-1);

        assertThrows(
                IllegalArgumentException.class,
                () -> MappingStageExecutor.buildCoverageDepthCommand(
                        List.of("samtools"),
                        Path.of("/work/aln_sorted.bam"),
                        parameters));
    }

    @ParameterizedTest
    @ValueSource(strings = {"bt2", "bt2l"})
    void routesPairedShortReadsThroughBowtie2AndPreservesOutputContract(String indexExtension) throws Exception {
        StubExecutor executor = new StubExecutor();
        executor.indexExtension = indexExtension;
        MappingParameters parameters = parameters(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        parameters.setMinimumMappingQuality(31);
        parameters.setMinimumBaseQuality(27);

        StageRunResult<MappingStageOutput> result = executor.run(input("qc/r2.fastq.gz"), parameters, tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals("mock-bowtie2-build", executor.commands.get(0).get(0));
        List<String> command = executor.command("mock-bowtie2");
        assertTrue(command.containsAll(List.of("--local", "--very-sensitive-local")));
        assertEquals("1", option(command, "--seed"));
        assertEquals(tempDir.resolve("work/bowtie2_reference").toString(), option(command, "-x"));
        assertEquals(tempDir.resolve("work/aln.sam").toString(), option(command, "-S"));
        assertFalse(executor.redirectedStdoutTools.contains("mock-bowtie2"));
        assertEquals(tempDir.resolve("input/r1.fastq.gz").toString(), option(command, "-1"));
        assertEquals(tempDir.resolve("input/r2.fastq.gz").toString(), option(command, "-2"));
        assertFalse(command.contains("-U"));
        assertFalse(executor.called("mock-minimap2"));
        assertEquals(Set.of("qc/r1.fastq.gz", "qc/r2.fastq.gz"), executor.loaded);

        MappingStageOutput output = result.getStageOutput();
        assertNotNull(output);
        assertEquals(tempDir.resolve("work/aln_sorted.bam"), output.getBamPath());
        assertEquals(tempDir.resolve("work/aln_sorted.bam.bai"), output.getBamIndexPath());
        assertEquals(tempDir.resolve("work/coverage_depth.tsv"), output.getCoverageDepthPath());
        assertEquals(tempDir.resolve("work").resolve(MappingStageOutput.METRICS_JSON), output.getMetricsJsonPath());
        for (Path artifact : List.of(output.getBamPath(), output.getBamIndexPath(),
                output.getCoverageDepthPath(), output.getMetricsJsonPath())) {
            assertTrue(Files.isRegularFile(artifact));
            assertTrue(Files.size(artifact) > 0);
        }
        assertEquals(4L, output.getMetrics().getTotalReferenceBases());
        assertEquals("TEST_REFERENCE", output.getMetrics().getReferences().get(0).getReferenceName());
        MappingResultMetrics persisted = JsonUtil.toObject(
                Files.readString(output.getMetricsJsonPath()), MappingResultMetrics.class);
        assertEquals(output.getMetrics().getTotalReferenceBases(), persisted.getTotalReferenceBases());
        List<String> depth = executor.commands.stream().filter(c -> c.contains("depth")).findFirst().orElseThrow();
        assertEquals("27", option(depth, "-q"));
        assertEquals("31", option(depth, "-Q"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void routesSingleShortReadsWithUnpairedInputOption(String r2) throws Exception {
        StubExecutor executor = new StubExecutor();

        StageRunResult<MappingStageOutput> result = executor.run(
                input(r2), parameters(Constants.SequenceInput.READ_LEN_TYPE_SHORT), tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        List<String> command = executor.command("mock-bowtie2");
        assertEquals(tempDir.resolve("work/aln.sam").toString(), option(command, "-S"));
        assertFalse(executor.redirectedStdoutTools.contains("mock-bowtie2"));
        assertEquals(tempDir.resolve("input/r1.fastq.gz").toString(), option(command, "-U"));
        assertFalse(command.contains("-1"));
        assertFalse(command.contains("-2"));
        assertEquals(Set.of("qc/r1.fastq.gz"), executor.loaded);
    }

    @Test
    void routesLongReadsThroughMinimap2WithoutBuildingBowtie2Index() throws Exception {
        StubExecutor executor = new StubExecutor();

        StageRunResult<MappingStageOutput> result = executor.run(
                input(null), parameters(Constants.SequenceInput.READ_LEN_TYPE_LONG), tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        List<String> command = executor.command("mock-minimap2");
        assertEquals("map-ont", option(command, "-ax"));
        assertTrue(command.contains(tempDir.resolve("input/r1.fastq.gz").toString()));
        assertTrue(executor.redirectedStdoutTools.contains("mock-minimap2"));
        assertFalse(executor.called("mock-bowtie2-build"));
        assertFalse(executor.called("mock-bowtie2"));
        assertNotNull(result.getStageOutput().getMetrics());
    }

    @Test
    void rejectsMissingReadMetadataBeforeLoadingOrExecuting() throws Exception {
        StubExecutor executor = new StubExecutor();
        MappingParameters parameters = parameters(Constants.SequenceInput.READ_LEN_TYPE_SHORT);
        parameters.setReadMeta(null);

        assertRejectedBeforeLoading(executor, executor.run(input(null), parameters, tempDir));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 2, 99})
    void rejectsUnknownReadLengthBeforeLoadingOrExecuting(int readLengthType) throws Exception {
        StubExecutor executor = new StubExecutor();

        assertRejectedBeforeLoading(executor,
                executor.run(input(null), parameters(readLengthType), tempDir));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void rejectsMissingR1BeforeLoadingOrExecuting(String r1) throws Exception {
        StubExecutor executor = new StubExecutor();
        MappingInputUrls input = input(null);
        input.setR1Url(r1);

        assertRejectedBeforeLoading(executor,
                executor.run(input, parameters(Constants.SequenceInput.READ_LEN_TYPE_SHORT), tempDir));
    }

    @Test
    void stopsBeforeAlignmentWhenIndexBuildFails() throws Exception {
        StubExecutor executor = new StubExecutor();
        executor.failedTool = "mock-bowtie2-build";

        StageRunResult<MappingStageOutput> result = executor.run(
                input(null), parameters(Constants.SequenceInput.READ_LEN_TYPE_SHORT), tempDir);

        assertFalse(result.isSuccess());
        assertEquals(1, executor.commands.size());
        assertFalse(executor.called("mock-bowtie2"));
        assertFalse(executor.called("mock-samtools"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty", "mixed"})
    void rejectsIncompleteIndexEvenWhenBuilderReportsSuccess(String indexProblem) throws Exception {
        StubExecutor executor = new StubExecutor();
        executor.indexProblem = indexProblem;

        StageRunResult<MappingStageOutput> result = executor.run(
                input(null), parameters(Constants.SequenceInput.READ_LEN_TYPE_SHORT), tempDir);

        assertFalse(result.isSuccess());
        assertEquals(1, executor.commands.size());
        assertFalse(executor.called("mock-bowtie2"));
        assertFalse(executor.called("mock-samtools"));
    }

    @ParameterizedTest
    @ValueSource(ints = {Constants.SequenceInput.READ_LEN_TYPE_SHORT, Constants.SequenceInput.READ_LEN_TYPE_LONG})
    void stopsBeforeBamProcessingWhenMapperFails(int readLengthType) throws Exception {
        StubExecutor executor = new StubExecutor();
        executor.failedTool = readLengthType == Constants.SequenceInput.READ_LEN_TYPE_SHORT
                ? "mock-bowtie2" : "mock-minimap2";

        StageRunResult<MappingStageOutput> result = executor.run(
                input(null), parameters(readLengthType), tempDir);

        assertFalse(result.isSuccess());
        assertTrue(executor.called(executor.failedTool));
        assertFalse(executor.called("mock-samtools"));
    }

    private static void assertRejectedBeforeLoading(StubExecutor executor, StageRunResult<?> result) {
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorLog());
        assertTrue(executor.commands.isEmpty());
        assertTrue(executor.loaded.isEmpty());
        assertFalse(executor.materializedReference);
    }

    private static String option(List<String> command, String option) {
        assertTrue(command.contains(option), "Missing option: " + option + " in " + command);
        return command.get(command.indexOf(option) + 1);
    }

    private static MappingInputUrls input(String r2) {
        MappingInputUrls input = new MappingInputUrls();
        input.setR1Url("qc/r1.fastq.gz");
        input.setR2Url(r2);
        return input;
    }

    private static MappingParameters parameters(int readLengthType) {
        MappingParameters parameters = new MappingParameters();
        SequenceMeta metadata = new SequenceMeta();
        metadata.setReadLenType(readLengthType);
        parameters.setReadMeta(metadata);
        ReferenceSequence reference = new ReferenceSequence();
        reference.setAccession("TEST_REFERENCE");
        reference.setPath("references/test.fa");
        parameters.setReferenceGenome(ReferenceGenome.fromSingleSequence(reference));
        return parameters;
    }

    private static class StubExecutor extends MappingStageExecutor {
        final List<List<String>> commands = new ArrayList<>();
        final Set<String> loaded = new HashSet<>();
        final Set<String> redirectedStdoutTools = new HashSet<>();
        String indexExtension = "bt2";
        String indexProblem;
        String failedTool;
        boolean materializedReference;

        StubExecutor() {
            analysisPipelineToolsConfig = new AnalysisPipelineToolsConfig();
            analysisPipelineToolsConfig.setBowtie2(List.of("mock-bowtie2"));
            analysisPipelineToolsConfig.setBowtie2Build(List.of("mock-bowtie2-build"));
            analysisPipelineToolsConfig.setMinimap2(List.of("mock-minimap2"));
            analysisPipelineToolsConfig.setSamtools(List.of("mock-samtools"));
        }

        StageRunResult<MappingStageOutput> run(MappingInputUrls input,
                MappingParameters parameters, Path root) throws Exception {
            StageExecutionInput execution = new StageExecutionInput();
            execution.input = input;
            execution.stageParameters = parameters;
            execution.stageContext = new StageContext(1L, 0, Constants.StageType.PIPELINE_STAGE_MAPPING, 1);
            execution.inputDir = Files.createDirectories(root.resolve("input"));
            execution.workDir = Files.createDirectories(root.resolve("work"));
            return _execute(execution);
        }

        boolean called(String tool) {
            return commands.stream().anyMatch(c -> tool.equals(c.get(0)));
        }

        List<String> command(String tool) {
            return commands.stream().filter(c -> tool.equals(c.get(0))).findFirst().orElseThrow();
        }

        @Override
        protected Path materializeReferenceGenome(ReferenceGenome referenceGenome, Path inputDir)
                throws LoadFailException {
            materializedReference = true;
            Path reference = inputDir.resolve("reference.fa");
            try {
                Files.writeString(reference, ">TEST_REFERENCE\nACGT\n");
                return reference;
            } catch (IOException e) {
                throw new LoadFailException(e);
            }
        }

        @Override
        protected void loadInput(Map<String, Path> paths) throws LoadFailException {
            try {
                for (Map.Entry<String, Path> entry : paths.entrySet()) {
                    loaded.add(entry.getKey());
                    Files.writeString(entry.getValue(), "@test\nACGT\n+\nIIII\n");
                }
            } catch (IOException e) {
                throw new LoadFailException(e);
            }
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            return _execute(command, workDir, (Path) null, (Path) null);
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir, Path stdout, Path stderr) {
            commands.add(List.copyOf(command));
            if (stdout != null) {
                redirectedStdoutTools.add(command.get(0));
            }
            if (command.get(0).equals(failedTool)) {
                return new ExecuteResult(9, null);
            }
            try {
                switch (command.get(0)) {
                    case "mock-bowtie2-build" -> writeIndex(command.get(command.size() - 1));
                    case "mock-bowtie2", "mock-minimap2" -> Files.writeString(
                            stdout == null ? Path.of(option(command, "-S")) : stdout,
                            "@HD\tVN:1.6\n@SQ\tSN:TEST_REFERENCE\tLN:4\n");
                    case "mock-samtools" -> {
                        switch (command.get(1)) {
                            case "view", "sort", "index" -> Files.writeString(
                                    Path.of(option(command, "-o")), "stub binary artifact\n");
                            case "idxstats" -> Files.writeString(stdout, "TEST_REFERENCE\t4\t2\t0\n*\t0\t0\t1\n");
                            case "depth" -> Files.writeString(stdout,
                                    "TEST_REFERENCE\t1\t2\nTEST_REFERENCE\t2\t2\n"
                                            + "TEST_REFERENCE\t3\t0\nTEST_REFERENCE\t4\t0\n");
                            default -> throw new AssertionError("Unexpected samtools command: " + command);
                        }
                    }
                    default -> throw new AssertionError("Unexpected command: " + command);
                }
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                return new ExecuteResult(1, e);
            }
        }

        private void writeIndex(String prefix) throws IOException {
            List<String> parts = List.of("1", "2", "3", "4", "rev.1", "rev.2");
            for (int i = 0; i < parts.size(); i++) {
                if ("missing".equals(indexProblem) && i == parts.size() - 1) {
                    continue;
                }
                String extension = "mixed".equals(indexProblem) && i >= 3 ? "bt2l" : indexExtension;
                String contents = "empty".equals(indexProblem) && i == parts.size() - 1 ? "" : "stub index\n";
                Files.writeString(Path.of(prefix + "." + parts.get(i) + "." + extension), contents);
            }
        }
    }
}
