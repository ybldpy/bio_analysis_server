package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_ASSEMBLY;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.ReferenceComparisonStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceComparisonResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.command.UpdateStageCommand;
import com.xjtlu.bio.utils.JsonUtil;

/**
 * Opt-in real minimap2, Builder, Executor, local storage and DoneHandler integration.
 * Fixtures are fixed-seed random text, not sequences taken from biological datasets.
 * Only PipelineService persistence/callbacks are replaced by an in-memory recorder:
 * this deliberately does not claim to test HTTP, database transactions or polling.
 *
 * Run with -Dcomparison.real.integration=true; optionally supply
 * -Dcomparison.real.minimap2=/absolute/path/to/minimap2. Evidence is retained under
 * target/reference-comparison-real-tool-integration/run-* (never @TempDir-cleaned).
 */
@EnabledIfSystemProperty(named = "comparison.real.integration", matches = "true")
class ReferenceComparisonRealToolIntegrationTest {

    private static final JsonMapper JSON = new JsonMapper();
    private static final long QUERY_SEED = 2026102501L;
    private static final List<String> FORMATS = List.of("plain", "gzip", "zip");

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void repeatedRealComparisonsPersistStableReportsAcrossPlainGzipAndZip() throws Exception {
        Path executable = Path.of(System.getProperty("comparison.real.minimap2",
                "/usr/conda/envs/bio-map/bin/minimap2")).toAbsolutePath();
        assertTrue(Files.isExecutable(executable), "Explicitly enabled integration requires minimap2: " + executable);
        Path parent = Files.createDirectories(Path.of("target/reference-comparison-real-tool-integration"))
                .toAbsolutePath();
        Path root = Files.createTempDirectory(parent, "run-");
        Path storageRoot = Files.createDirectory(root.resolve("storage"));
        List<Map<String, Object>> cases = new ArrayList<>();
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("scope", "Real Builder/Executor/minimap2/LocalStorage/DoneHandler; in-memory PipelineService; no HTTP/DB");
        summary.put("fixtureOrigin", "Fixed-seed pseudorandom A/C/G/T software fixtures, no biological source data");
        summary.put("querySeed", QUERY_SEED);
        summary.put("rounds", 3);
        summary.put("formats", FORMATS);
        summary.put("minimap2", executable.toString());
        summary.put("cases", cases);
        summary.put("status", "RUNNING");
        writeJson(root.resolve("summary.json"), summary);
        System.out.println("Reference Comparison real-tool evidence: " + root);

        try {
            summary.put("minimap2Version", version(executable, root));
            String query = randomSequence(QUERY_SEED, 12_000);
            String middle = mutate(query.substring(3_000, 9_000));
            List<String> mixedReferences = List.of(query,
                    randomSequence(21L, 3_000) + middle + randomSequence(22L, 3_000),
                    randomSequence(23L, 12_000));
            List<String> unmatchedReferences = List.of(randomSequence(31L, 12_000),
                    randomSequence(32L, 12_000), randomSequence(33L, 12_000));

            LocalStorageService storage = new LocalStorageService();
            ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
            BioStageUtil directories = new BioStageUtil();
            ReflectionTestUtils.setField(directories, "stageInputDirPath", root.resolve("input").toString());
            ReflectionTestUtils.setField(directories, "stageWorkDirPath", root.resolve("work").toString());
            AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
            tools.setMinimap2(List.of(executable.toString()));

            Map<String, Snapshot> baselines = new LinkedHashMap<>();
            long stageId = 100_000L;
            for (int round = 1; round <= 3; round++) {
                for (String format : FORMATS) {
                    for (String scenario : List.of("mixed", "all-no-match")) {
                        String caseName = "round-" + round + "/" + format + "/" + scenario;
                        Path evidence = Files.createDirectories(root.resolve("cases").resolve(caseName));
                        Map<String, Object> report = new LinkedHashMap<>();
                        report.put("case", caseName);
                        report.put("stageId", ++stageId);
                        report.put("status", "RUNNING");
                        cases.add(report);
                        writeJson(root.resolve("summary.json"), summary);
                        try {
                            Snapshot snapshot = runCase(storage, storageRoot, directories, tools, evidence,
                                    stageId, format, scenario, query,
                                    scenario.equals("mixed") ? mixedReferences : unmatchedReferences, report);
                            Snapshot baseline = baselines.putIfAbsent(scenario, snapshot);
                            if (baseline != null) {
                                assertEquals(baseline, snapshot,
                                        "Metrics and report contents must be stable across formats and repeated runs: " + caseName);
                            }
                            report.put("stableAgainstFirstRun", true);
                            report.put("status", "PASS");
                        } catch (Exception | AssertionError failure) {
                            report.put("status", "FAIL");
                            report.put("error", failure.toString());
                            throw failure;
                        } finally {
                            writeJson(evidence.resolve("case.json"), report);
                            writeJson(root.resolve("summary.json"), summary);
                        }
                    }
                }
            }
            assertEquals(18, cases.size());
            summary.put("completedStageCount", cases.size());
            summary.put("realToolInvocationCount", cases.size() * 3);
            summary.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            summary.put("status", "FAIL");
            summary.put("error", failure.toString());
            throw failure;
        } finally {
            writeJson(root.resolve("summary.json"), summary);
        }
    }

    private static Snapshot runCase(LocalStorageService storage, Path storageRoot, BioStageUtil directories,
            AnalysisPipelineToolsConfig tools, Path evidence, long stageId, String format, String scenario,
            String query, List<String> referenceTexts, Map<String, Object> report) throws Exception {
        String fixturePrefix = "fixtures/" + format + "/" + scenario + "/";
        Path queryFile = wrap(storageRoot.resolve(fixturePrefix + "query/shared.fasta"), fasta("random_query", query), format);
        String queryKey = storageRoot.relativize(queryFile).toString();
        List<ReferenceSequence> references = new ArrayList<>();
        Map<Path, String> originalHashes = new LinkedHashMap<>();
        originalHashes.put(queryFile, hash(queryFile));
        for (int index = 0; index < referenceTexts.size(); index++) {
            Path file = wrap(storageRoot.resolve(fixturePrefix + "references/" + index + "/shared.fasta"),
                    fasta("random_reference", referenceTexts.get(index)), format);
            originalHashes.put(file, hash(file));
            ReferenceSequence reference = new ReferenceSequence();
            reference.setReferenceId((long) index + 1L);
            reference.setAccession("RANDOM_SOFTWARE_FIXTURE_" + (index + 1));
            reference.setPath(storageRoot.relativize(file).toString());
            reference.setSourceDb("RANDOM_TEST_FIXTURE");
            reference.setTaxId(0);
            reference.setOrganismName("Random software fixture (not a biological reference)");
            references.add(reference);
        }

        PipelineSampleInput input = new PipelineSampleInput(queryKey, null);
        input.setSequenceLevel(SEQUENCE_LEVEL_ASSEMBLY);
        PipelineConfigurations configuration = new PipelineConfigurations();
        configuration.setCandidateReferenceSequences(references);
        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildVirusStages(input, configuration);
        assertEquals(1, stages.size(), "Assembly input must build one Comparison stage, no Selection/Mapping");
        BioPipelineStage stage = stages.get(0);
        assertEquals(PIPELINE_STAGE_REFERENCE_COMPARISON, stage.getStageType());
        assertEquals(0, stage.getStageIndex());
        assertNotNull(stage.getStageName());
        ReferenceComparisonStageParameters parameters = JsonUtil.toObject(stage.getParameters(),
                ReferenceComparisonStageParameters.class);
        assertEquals(3, parameters.getCandidateReferences().size());
        assertNull(parameters.getReferenceGenome());
        stage.setPipelineId(stageId);
        stage.setStageId(stageId);
        stage.setVersion(0);
        writeJson(evidence.resolve("built-stage.json"), stage);

        RecordingExecutor executor = new RecordingExecutor(evidence);
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        StageRunResult<ReferenceComparisonStageOutput> execution = executor.execute(stage);
        assertTrue(execution.isSuccess(), execution.getErrorLog());
        assertEquals(3, executor.commands.size(), "One real process per configured candidate");
        assertFalse(Files.exists(directories.stageExecutorInputDir(execution.getStageContext())));
        List<String> expectedStatuses = scenario.equals("mixed")
                ? List.of("MATCHED", "MATCHED", "NO_MATCH") : List.of("NO_MATCH", "NO_MATCH", "NO_MATCH");
        var comparisons = execution.getStageOutput().getComparisons();
        assertEquals(expectedStatuses, comparisons.stream().map(candidate -> candidate.metrics().status()).toList());
        if (scenario.equals("mixed")) {
            assertEquals(100.0d, comparisons.get(0).metrics().sequenceIdentity(), 0.000001d);
            assertEquals(1.0d, comparisons.get(0).metrics().referenceCoverage(), 0.000001d);
            assertTrue(comparisons.get(1).metrics().referenceCoverage() > 0.4d);
            assertTrue(comparisons.get(1).metrics().referenceCoverage() < 0.7d);
            assertTrue(comparisons.get(1).metrics().sequenceIdentity() < 100.0d);
        }
        List<String> pafs = new ArrayList<>();
        List<String> differences = new ArrayList<>();
        for (var candidate : comparisons) {
            pafs.add(Files.readString(candidate.alignmentPafPath()));
            differences.add(Files.readString(candidate.differenceTsvPath()));
            if (candidate.metrics().status().equals("NO_MATCH")) {
                assertEquals(0L, Files.size(candidate.alignmentPafPath()));
                assertEquals(0.0d, candidate.metrics().referenceCoverage());
                assertEquals(1, Files.readAllLines(candidate.differenceTsvPath()).size());
            }
        }
        assertEquals(3, comparisons.stream().map(candidate -> candidate.alignmentPafPath()).distinct().count());
        assertEquals(3, comparisons.stream().map(candidate -> candidate.differenceTsvPath()).distinct().count());
        String tsv = Files.readString(execution.getStageOutput().getComparisonTsvPath());
        assertEquals(4, tsv.lines().count());
        tsv.lines().forEach(line -> assertEquals(15, line.split("\t", -1).length));

        RecordingPipelineService service = new RecordingPipelineService();
        ReferenceComparisonStageDoneHandler handler = new ReferenceComparisonStageDoneHandler();
        ReflectionTestUtils.setField(handler, "pipelineService", service);
        ReflectionTestUtils.setField(handler, "storageService", storage);
        ReflectionTestUtils.setField(handler, "bioStageUtil", directories);
        handler.handleStageDone(execution);
        assertEquals(1, service.updates.size(), service.failureMessages.toString());
        BioPipelineStage update = service.updates.get(0).getUpdateStage();
        assertEquals(PIPELINE_STAGE_STATUS_FINISHED, update.getStatus(), service.failureMessages.toString());
        assertEquals(1, update.getVersion());
        assertEquals(List.of(stageId + ":true"), service.callbacks);
        assertTrue(service.failureMessages.isEmpty());
        assertNull(update.getOutputInline());
        assertTrue(update.getOutputUrl().length() <= 500, "Database output contains only the compact file index");
        ReferenceComparisonResult result = JsonUtil.toObject(update.getOutputUrl(), ReferenceComparisonResult.class);
        int matched = scenario.equals("mixed") ? 2 : 0;
        assertEquals(2, result.getSchemaVersion());
        assertEquals(3, result.getCandidateCount());
        assertEquals(matched, result.getMatchedCandidateCount());
        assertEquals(3 - matched, result.getUnmatchedCandidateCount());
        assertEquals(tsv, Files.readString(storage.getObject(result.getComparisonTsvUrl())));
        JsonNode manifest = JSON.readTree(storage.getObject(result.getComparisonJsonUrl()).toFile());
        assertEquals(3, manifest.path("candidateCount").asInt());
        assertEquals(matched, manifest.path("matchedCandidateCount").asInt());
        assertEquals(3 - matched, manifest.path("unmatchedCandidateCount").asInt());
        assertEquals(3, manifest.path("comparisons").size());
        ObjectNode normalizedManifest = (ObjectNode) manifest.deepCopy();
        for (int index = 0; index < comparisons.size(); index++) {
            JsonNode item = manifest.path("comparisons").get(index);
            assertEquals(pafs.get(index), Files.readString(storage.getObject(item.path("alignmentPafUrl").asText())));
            assertEquals(differences.get(index), Files.readString(storage.getObject(item.path("differenceTsvUrl").asText())));
            // Compare the serialized contract: readTree narrows small integer
            // values to IntNode whereas valueToTree retains Java long fields.
            assertEquals(JSON.readTree(JSON.writeValueAsBytes(comparisons.get(index).metrics())),
                    withoutFileUrls((ObjectNode) item.deepCopy()));
            ObjectNode normalized = (ObjectNode) normalizedManifest.path("comparisons").get(index);
            normalized.remove(List.of("alignmentPafUrl", "differenceTsvUrl"));
        }
        assertFalse(Files.exists(execution.getWorkDir()), "Successful handler cleans work after storing artifacts");
        Map<String, String> retainedHashes = new LinkedHashMap<>();
        for (var entry : originalHashes.entrySet()) {
            assertEquals(entry.getValue(), hash(entry.getKey()), "Input/reference storage bytes must not change");
            retainedHashes.put(storageRoot.relativize(entry.getKey()).toString(), entry.getValue());
        }
        report.put("inputSha256", retainedHashes);
        report.put("stageOutput", JSON.readTree(update.getOutputUrl()));
        report.put("metrics", normalizedManifest);
        report.put("toolInvocations", executor.commands.size());
        report.put("summaryTsvSha256", hash(tsv.getBytes(StandardCharsets.UTF_8)));
        writeJson(evidence.resolve("persisted-stage-update.json"), update);
        return new Snapshot(normalizedManifest, tsv, pafs, differences);
    }

    private static ObjectNode withoutFileUrls(ObjectNode node) {
        node.remove(List.of("alignmentPafUrl", "differenceTsvUrl"));
        return node;
    }

    private static String randomSequence(long seed, int length) {
        Random random = new Random(seed);
        StringBuilder text = new StringBuilder(length);
        for (int index = 0; index < length; index++) {
            text.append("ACGT".charAt(random.nextInt(4)));
        }
        return text.toString();
    }

    private static String mutate(String text) {
        char[] result = text.toCharArray();
        for (int index = 100; index < result.length; index += 211) {
            result[index] = "ACGT".charAt(("ACGT".indexOf(result[index]) + 1) % 4);
        }
        return new String(result);
    }

    private static byte[] fasta(String name, String text) {
        return (">" + name + "\n" + text + "\n").getBytes(StandardCharsets.US_ASCII);
    }

    private static Path wrap(Path plainPath, byte[] data, String format) throws IOException {
        Files.createDirectories(plainPath.getParent());
        if (format.equals("plain")) {
            return Files.write(plainPath, data);
        }
        Path result = plainPath.resolveSibling(plainPath.getFileName() + (format.equals("gzip") ? ".gz" : ".zip"));
        if (format.equals("gzip")) {
            try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(result))) {
                out.write(data);
            }
        } else {
            try (ZipOutputStream out = new ZipOutputStream(Files.newOutputStream(result))) {
                ZipEntry entry = new ZipEntry(plainPath.getFileName().toString());
                entry.setTime(0L);
                out.putNextEntry(entry);
                out.write(data);
                out.closeEntry();
            }
        }
        return result;
    }

    private static String version(Path executable, Path root) throws Exception {
        Path log = root.resolve("minimap2-version.log");
        Process process = new ProcessBuilder(executable.toString(), "--version")
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if (!process.waitFor(10L, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("minimap2 --version timed out");
        }
        assertEquals(0, process.exitValue());
        return Files.readString(log).trim();
    }

    private static String hash(Path path) throws Exception {
        return hash(Files.readAllBytes(path));
    }

    private static String hash(byte[] data) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
    }

    private static void writeJson(Path path, Object value) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
    }

    private record Snapshot(JsonNode metrics, String summaryTsv, List<String> pafs, List<String> differences) { }

    /** Logs the exact command, then invokes the real production subprocess implementation. */
    private static final class RecordingExecutor extends ReferenceComparsionStageExecutor {
        private final Path evidence;
        private final List<Map<String, Object>> commands = new ArrayList<>();

        private RecordingExecutor(Path evidence) {
            this.evidence = evidence;
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            int number = commands.size() + 1;
            Map<String, Object> invocation = new LinkedHashMap<>();
            invocation.put("command", List.copyOf(command));
            invocation.put("workDir", workDir.toString());
            commands.add(invocation);
            try {
                writeJson(evidence.resolve("commands.json"), commands);
                ExecuteResult result = super._execute(command, workDir,
                        evidence.resolve("command-" + number + ".stdout.log"),
                        evidence.resolve("command-" + number + ".stderr.log"));
                invocation.put("exitCode", result.runCode);
                if (result.ex != null) {
                    invocation.put("exception", result.ex.toString());
                }
                writeJson(evidence.resolve("commands.json"), commands);
                return result;
            } catch (IOException error) {
                return new ExecuteResult(-1, error);
            }
        }
    }

    /** No database access: only records the real handler's completion boundary. */
    private static final class RecordingPipelineService extends PipelineService {
        private final List<UpdateStageCommand> updates = new ArrayList<>();
        private final List<String> callbacks = new ArrayList<>();
        private final List<String> failureMessages = new ArrayList<>();

        @Override
        public int updateStageFromVersion(UpdateStageCommand command) {
            updates.add(command);
            return 1;
        }

        @Override
        public void pipelineStageDone(long stageId, boolean success) {
            callbacks.add(stageId + ":" + success);
        }

        @Override
        public void recordStageFailure(StageContext context, String message) {
            failureMessages.add(message);
        }
    }
}
