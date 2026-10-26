package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Real storage, decompression and PAF parsing; no external analysis process is started. */
class ReferenceComparisonCompressedInputTest {

    private static final String QUERY = ">query\nACGT\n";
    private static final String REFERENCE = ">reference\nACGT\n";
    private static final String PAF = "query\t4\t0\t4\t+\treference\t4\t0\t4\t4\t4\t60\tcs:Z::4\n";

    @TempDir
    Path tempDir;

    private Path storageRoot;
    private Path inputDir;
    private Path workDir;
    private StubExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectories(tempDir.resolve("storage"));
        inputDir = tempDir.resolve("input with spaces/42");
        workDir = tempDir.resolve("work with spaces/42");
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageInputDirPath", inputDir.getParent().toString());
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", workDir.getParent().toString());
        AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
        tools.setMinimap2(List.of("mock-runtime", "mock-minimap2"));
        executor = new StubExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
    }

    @ParameterizedTest
    @CsvSource({".zip, .zip", ".ZIP, .ZIP", ".gz, .gz", ".gzip, .gzip", "'', ''",
            ".gz, ''", "'', .gz", ".zip, ''", "'', .zip", ".gz, .zip", ".zip, .gz"})
    void queryAndReferenceArePlainFastaAtTheToolBoundaryAndSourcesAreUnchanged(
            String querySuffix, String referenceSuffix) throws Exception {
        String queryKey = writeObject("samples/source with spaces.fasta" + querySuffix, QUERY);
        String referenceKey = writeObject("references/source with spaces.fasta" + referenceSuffix, REFERENCE);
        byte[] originalQuery = Files.readAllBytes(storageRoot.resolve(queryKey));
        byte[] originalReference = Files.readAllBytes(storageRoot.resolve(referenceKey));

        StageRunResult<ReferenceComparisonStageOutput> result = run(queryKey, referenceKey);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(1, executor.commands.size());
        List<String> command = executor.commands.get(0);
        assertEquals(List.of("mock-runtime", "mock-minimap2"), command.subList(0, 2));
        assertEquals("asm5", option(command, "-x"));
        assertTrue(command.contains("--cs=long"));
        assertTrue(command.contains("--secondary=no"));
        assertEquals(inputDir.resolve("query_source with spaces.fasta"), executor.queryPath);
        assertEquals(inputDir.resolve("candidate_1/reference_source with spaces.fasta"), executor.referencePath);
        assertEquals(QUERY, executor.queryContents);
        assertEquals(REFERENCE, executor.referenceContents);
        assertEquals(workDir.resolve("candidate_1"), executor.processDirectory);
        assertNotNull(result.getStageOutput());
        assertEquals(1, result.getStageOutput().getComparisons().size());
        var comparison = result.getStageOutput().getComparisons().get(0);
        assertEquals(workDir.resolve("candidate_1/alignment.paf"), comparison.alignmentPafPath());
        assertEquals(workDir.resolve("candidate_1/reference_difference.tsv"), comparison.differenceTsvPath());
        assertEquals(PAF, Files.readString(comparison.alignmentPafPath()));
        assertTrue(Files.readString(comparison.differenceTsvPath()).startsWith("reference_name\tquery_name\t"));
        assertEquals("fixture_reference", comparison.metrics().referenceAccession());
        assertEquals(100.0d, comparison.metrics().sequenceIdentity());
        assertEquals(1.0d, comparison.metrics().referenceCoverage());
        assertEquals(workDir.resolve("reference_comparison.tsv"), result.getStageOutput().getComparisonTsvPath());
        assertTrue(Files.isRegularFile(result.getStageOutput().getComparisonTsvPath()));
        assertArrayEquals(originalQuery, Files.readAllBytes(storageRoot.resolve(queryKey)));
        assertArrayEquals(originalReference, Files.readAllBytes(storageRoot.resolve(referenceKey)));
        assertFalse(Files.exists(inputDir), "Only staged copies should be removed by the executor lifecycle");
    }

    private StageRunResult<ReferenceComparisonStageOutput> run(String queryKey, String referenceKey) throws Exception {
        ReferenceSequence reference = new ReferenceSequence();
        reference.setAccession("fixture_reference");
        reference.setPath(referenceKey);
        ReferenceComparisonStageParameters parameters = new ReferenceComparisonStageParameters();
        parameters.setCandidateReferences(List.of(reference));
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(42L);
        stage.setPipelineId(1L);
        stage.setStageType(PIPELINE_STAGE_REFERENCE_COMPARISON);
        stage.setStageIndex(0);
        stage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        stage.setVersion(3);
        stage.setInputUrl(JsonUtil.toJson(new ReferenceComparisonStageInputUrls(queryKey)));
        stage.setParameters(JsonUtil.toJson(parameters));
        return executor.execute(stage);
    }

    private String writeObject(String key, String contents) throws IOException {
        Path destination = storageRoot.resolve(key);
        Files.createDirectories(destination.getParent());
        if (key.toLowerCase(Locale.ROOT).endsWith(".zip")) {
            try (ZipOutputStream archive = new ZipOutputStream(Files.newOutputStream(destination))) {
                archive.putNextEntry(new ZipEntry("payload/"));
                archive.closeEntry();
                archive.putNextEntry(new ZipEntry("payload/sequence.fasta"));
                archive.write(contents.getBytes(StandardCharsets.UTF_8));
                archive.closeEntry();
            }
        } else if (key.endsWith(".gz") || key.endsWith(".gzip")) {
            try (OutputStream compressed = new GZIPOutputStream(Files.newOutputStream(destination))) {
                compressed.write(contents.getBytes(StandardCharsets.UTF_8));
            }
        } else {
            Files.writeString(destination, contents);
        }
        return key;
    }

    private static String option(List<String> command, String flag) {
        int index = command.indexOf(flag);
        assertTrue(index >= 0 && index + 1 < command.size(), "Missing option " + flag + ": " + command);
        return command.get(index + 1);
    }

    private static final class StubExecutor extends ReferenceComparsionStageExecutor {

        private final List<List<String>> commands = new ArrayList<>();
        private Path queryPath;
        private Path referencePath;
        private String queryContents;
        private String referenceContents;
        private Path processDirectory;

        @Override
        protected ExecuteResult _execute(List<String> command, Path directory) {
            commands.add(List.copyOf(command));
            processDirectory = directory;
            int referenceIndex = command.indexOf("--secondary=no") + 1;
            referencePath = Path.of(command.get(referenceIndex));
            queryPath = Path.of(command.get(referenceIndex + 1));
            try {
                referenceContents = Files.readString(referencePath);
                queryContents = Files.readString(queryPath);
                Files.writeString(Path.of(option(command, "-o")), PAF);
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                throw new AssertionError("Cannot prepare isolated reference comparison fixture", e);
            }
        }
    }
}
