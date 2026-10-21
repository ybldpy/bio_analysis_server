package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MetagenomicsAnalysisStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;

/** Input routing tests only: all external commands are captured, never executed. */
class MetagenomicZipInputTest {
    private static final byte[] R1 = "@fixture/1\nACGT\n+\nIIII\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] R2 = "@fixture/2\nTGCA\n+\nIIII\n".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @CsvSource({
            "sample_R1.fastq.zip,sample_R2.fq.zip,sample_R1.fastq,sample_R2.fq",
            "sample_R1.zip,sample_R2.zip,sample_R1.fastq,sample_R2.fastq"
    })
    void ampliconUnpacksEachMateBeforeCommandAndSampleName(String first, String second,
            String normalizedFirst, String normalizedSecond) throws Exception {
        Map<String, byte[]> objects = Map.of(first, zip(R1), second, zip(R2));
        AmpliconProbe executor = new AmpliconProbe(objects);

        executor.run(first, second, tempDir);

        List<String> command = executor.commands.get(0);
        assertFile(command, "--r1", tempDir.resolve("input/" + normalizedFirst), R1);
        assertFile(command, "--r2", tempDir.resolve("input/" + normalizedSecond), R2);
        assertEquals("sample", option(command, "--sample-name"));
        assertOriginals(objects, tempDir);
    }

    @Test
    void ampliconUnpacksSingleZipWithoutInventingSecondMate() throws Exception {
        AmpliconProbe executor = new AmpliconProbe(Map.of("single.zip", zip(R1)));

        executor.run("single.zip", null, tempDir);

        List<String> command = executor.commands.get(0);
        assertFile(command, "--r1", tempDir.resolve("input/single.fastq"), R1);
        assertFalse(command.contains("--r2"));
        assertEquals("single", option(command, "--sample-name"));
    }

    @Test
    void ampliconPreservesNativeGzipPathsAndBytes() throws Exception {
        byte[] first = gzip(R1);
        byte[] second = gzip(R2);
        AmpliconProbe executor = new AmpliconProbe(Map.of("sample_R1.fastq.gz", first,
                "sample_R2.fastq.gz", second));

        executor.run("sample_R1.fastq.gz", "sample_R2.fastq.gz", tempDir);

        List<String> command = executor.commands.get(0);
        assertFile(command, "--r1", tempDir.resolve("input/sample_R1.fastq.gz"), first);
        assertFile(command, "--r2", tempDir.resolve("input/sample_R2.fastq.gz"), second);
        assertEquals("sample", option(command, "--sample-name"));
    }

    @ParameterizedTest
    @CsvSource({
            "sample_R1.fastq.zip,sample_R2.fq.zip,sample_R1.fastq,sample_R2.fq",
            "sample_R1.zip,sample_R2.zip,sample_R1.fastq,sample_R2.fastq"
    })
    void shotgunUnpacksMatesAndUsesGzipNamedHostOutputs(String first, String second,
            String normalizedFirst, String normalizedSecond) throws Exception {
        Map<String, byte[]> objects = Map.of(first, zip(R1), second, zip(R2));
        ShotgunProbe executor = new ShotgunProbe(objects);

        executor.run(first, second, tempDir);

        List<String> bowtie = executor.commands.get(0);
        assertFile(bowtie, "-1", tempDir.resolve("input/" + normalizedFirst), R1);
        assertFile(bowtie, "-2", tempDir.resolve("input/" + normalizedSecond), R2);
        assertGzipHostOutputs(executor, normalizedFirst + ".gz", normalizedSecond + ".gz");
        assertOriginals(objects, tempDir);
    }

    @Test
    void shotgunSingleZipReachesFirstCommandWithExtractedContent() throws Exception {
        ShotgunProbe executor = new ShotgunProbe(Map.of("single.zip", zip(R1)));

        executor.run("single.zip", null, tempDir);

        // Stop at the first command: this does not exercise the existing single-end host-removal workflow.
        assertEquals(1, executor.commands.size());
        List<String> command = executor.commands.get(0);
        assertFile(command, "-U", tempDir.resolve("input/single.fastq"), R1);
        assertEquals(tempDir.resolve("work/hostRemoval/nohost_single.fastq.gz").toString(),
                option(command, "--un-gz"));
        assertFalse(command.contains("-2"));
    }

    @Test
    void shotgunPreservesNativeGzipInputsAndOutputNames() throws Exception {
        byte[] first = gzip(R1);
        byte[] second = gzip(R2);
        ShotgunProbe executor = new ShotgunProbe(Map.of("sample_R1.fastq.gz", first,
                "sample_R2.fastq.gz", second));

        executor.run("sample_R1.fastq.gz", "sample_R2.fastq.gz", tempDir);

        List<String> command = executor.commands.get(0);
        assertFile(command, "-1", tempDir.resolve("input/sample_R1.fastq.gz"), first);
        assertFile(command, "-2", tempDir.resolve("input/sample_R2.fastq.gz"), second);
        assertGzipHostOutputs(executor, "sample_R1.fastq.gz", "sample_R2.fastq.gz");
    }

    @Test
    void zipExtractionFailureStopsBothExecutorsBeforeCommands() throws Exception {
        Map<String, byte[]> objects = Map.of("broken.zip", new byte[] {1, 2, 3});
        AmpliconProbe amplicon = new AmpliconProbe(objects);
        ShotgunProbe shotgun = new ShotgunProbe(objects);

        StageRunResult<?> ampliconResult = amplicon.run("broken.zip", null, tempDir.resolve("amplicon"));
        StageRunResult<?> shotgunResult = shotgun.run("broken.zip", null, tempDir.resolve("shotgun"));

        assertFalse(ampliconResult.isSuccess());
        assertFalse(shotgunResult.isSuccess());
        assertTrue(ampliconResult.getErrorLog().contains("Failed to unpack amplicon reads ZIP"));
        assertTrue(shotgunResult.getErrorLog().contains("Failed to unpack shotgun reads ZIP"));
        assertTrue(amplicon.commands.isEmpty());
        assertTrue(shotgun.commands.isEmpty());
    }

    private void assertGzipHostOutputs(ShotgunProbe executor, String first, String second) throws IOException {
        List<String> fastq = executor.commands.stream()
                .filter(command -> command.contains("fastq")).findFirst().orElseThrow();
        Path r1Output = tempDir.resolve("work/hostRemoval/nohost_" + first);
        Path r2Output = tempDir.resolve("work/hostRemoval/nohost_" + second);
        assertEquals(r1Output.toString(), option(fastq, "-1"));
        assertEquals(r2Output.toString(), option(fastq, "-2"));
        try (GZIPInputStream firstInput = new GZIPInputStream(Files.newInputStream(r1Output));
                GZIPInputStream secondInput = new GZIPInputStream(Files.newInputStream(r2Output))) {
            assertArrayEquals(R1, firstInput.readAllBytes());
            assertArrayEquals(R2, secondInput.readAllBytes());
        }
        List<String> kraken = executor.commands.get(executor.commands.size() - 1);
        assertEquals("mock-kraken2", kraken.get(0));
        assertTrue(kraken.containsAll(List.of("--gzip-compressed", "--paired",
                r1Output.toString(), r2Output.toString())));
    }

    private static void assertFile(List<String> command, String option, Path expected, byte[] contents)
            throws IOException {
        assertEquals(expected.toString(), option(command, option));
        assertArrayEquals(contents, Files.readAllBytes(Path.of(option(command, option))));
    }

    private static void assertOriginals(Map<String, byte[]> objects, Path root) throws IOException {
        for (Map.Entry<String, byte[]> object : objects.entrySet()) {
            assertArrayEquals(object.getValue(), Files.readAllBytes(root.resolve("input/" + object.getKey())));
        }
    }

    private static String option(List<String> command, String option) {
        assertTrue(command.contains(option), "Missing option " + option);
        return command.get(command.indexOf(option) + 1);
    }

    private static byte[] zip(byte[] contents) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream output = new ZipOutputStream(bytes)) {
            output.putNextEntry(new ZipEntry("reads.fastq"));
            output.write(contents);
            output.closeEntry();
        }
        return bytes.toByteArray();
    }

    private static byte[] gzip(byte[] contents) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream output = new GZIPOutputStream(bytes)) {
            output.write(contents);
        }
        return bytes.toByteArray();
    }

    private static MetagenomicsAnalysisStageInputUrls input(String first, String second) {
        MetagenomicsAnalysisStageInputUrls input = new MetagenomicsAnalysisStageInputUrls();
        input.setR1Url(first);
        input.setR2Url(second);
        return input;
    }

    private static void load(Map<String, byte[]> objects, Map<String, Path> destinations)
            throws AbstractPipelineStageExector.LoadFailException {
        try {
            for (Map.Entry<String, Path> destination : destinations.entrySet()) {
                Files.write(destination.getValue(), objects.get(destination.getKey()));
            }
        } catch (IOException e) {
            throw new AbstractPipelineStageExector.LoadFailException(e);
        }
    }

    private static class AmpliconProbe extends Amplicon16SAnalysisStageExecutor {
        final Map<String, byte[]> objects;
        final List<List<String>> commands = new ArrayList<>();

        AmpliconProbe(Map<String, byte[]> objects) {
            this.objects = objects;
            analysisPipelineToolsConfig = new AnalysisPipelineToolsConfig();
            analysisPipelineToolsConfig.setDada2(List.of("mock-dada2"));
        }

        StageRunResult<?> run(String first, String second, Path root) throws Exception {
            StageExecutionInput execution = new StageExecutionInput();
            execution.input = input(first, second);
            execution.stageParameters = new BaseStageParams();
            execution.stageContext = new StageContext(1L, 0,
                    Constants.StageType.PIPELINE_STAGE_METAGENOMICS_AMPLICON16S, 1);
            execution.inputDir = Files.createDirectories(root.resolve("input"));
            execution.workDir = Files.createDirectories(root.resolve("work"));
            return _execute(execution);
        }

        @Override
        protected void loadInput(Map<String, Path> paths) throws LoadFailException {
            load(objects, paths);
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            commands.add(List.copyOf(command));
            return new ExecuteResult(9, null); // Deliberately stop before running any analysis.
        }
    }

    private static class ShotgunProbe extends MetagenomicShotgunStageExecutor {
        final Map<String, byte[]> objects;
        final List<List<String>> commands = new ArrayList<>();

        ShotgunProbe(Map<String, byte[]> objects) throws Exception {
            this.objects = objects;
            analysisPipelineToolsConfig = new AnalysisPipelineToolsConfig();
            analysisPipelineToolsConfig.setBowtie2(List.of("mock-bowtie2"));
            analysisPipelineToolsConfig.setSamtools(List.of("mock-samtools"));
            analysisPipelineToolsConfig.setKraken2(List.of("mock-kraken2"));
            for (String fieldName : List.of("humanBowtie2IndexDir", "kraken2DBDir")) {
                Field field = MetagenomicShotgunStageExecutor.class.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(this, "/unused-test-db");
            }
        }

        StageRunResult<?> run(String first, String second, Path root) throws Exception {
            StageExecutionInput execution = new StageExecutionInput();
            execution.input = input(first, second);
            execution.stageParameters = new BaseStageParams();
            execution.stageContext = new StageContext(1L, 0,
                    Constants.StageType.PIPELINE_STAGE_METAGENOMICS_SHORTGUN, 1);
            execution.inputDir = Files.createDirectories(root.resolve("input"));
            execution.workDir = Files.createDirectories(root.resolve("work"));
            return _execute(execution);
        }

        @Override
        protected void loadInput(Map<String, Path> paths) throws LoadFailException {
            load(objects, paths);
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            commands.add(List.copyOf(command));
            if (command.contains("-U") || command.get(0).equals("mock-kraken2")) {
                return new ExecuteResult(9, null); // Capture input wiring, then stop the workflow.
            }
            try {
                if (command.contains("fastq")) {
                    Files.write(Path.of(option(command, "-1")), gzip(R1));
                    Files.write(Path.of(option(command, "-2")), gzip(R2));
                }
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                return new ExecuteResult(1, e);
            }
        }
    }
}
