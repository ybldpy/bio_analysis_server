package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.READ_LEN_TYPE_LONG;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.TaxonomyStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.AssemblyStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.stageResult.AssemblyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.stageResult.ReadInspectStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.QCStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/**
 * Opt-in software integration using externally supplied benchmark fixtures.
 * Real Builder, Orchestrator, ReadInspect, QC, Assembly and local storage;
 * in-memory result publication instead of production DoneHandlers/service/DB.
 * With assembly.flye.integration.includeQc=false, QC is not executed: the test
 * supplies an unfiltered benchmark fixture as the upstream Assembly input. This
 * diagnostic mode must not be interpreted as successful QC/pipeline validation.
 * Downstream execution and species-specific biological validation are out of scope.
 */
@EnabledIfSystemProperty(named = "assembly.flye.integration.manifest", matches = ".+")
class AssemblyFlyePipelineIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long TIMEOUT_SECONDS = 1200L;
    private static final String CONDA = "/usr/conda/condabin/conda";

    @Test
    void threeDistinctInputsThroughBothAssemblyEntryPaths() throws Exception {
        Path manifestPath = Path.of(System.getProperty("assembly.flye.integration.manifest")).toAbsolutePath();
        JsonNode manifest = JSON.readTree(manifestPath.toFile());
        JsonNode cases = manifest.isArray() ? manifest : manifest.get("cases");
        assertNotNull(cases);
        assertEquals(3, cases.size(), "Supply exactly three distinct input fixtures");
        Set<String> names = new HashSet<>();
        Set<String> decodedHashes = new HashSet<>();
        for (JsonNode fixture : cases) {
            String name = fixture.path("name").asText();
            assertTrue(name.matches("[a-zA-Z0-9][a-zA-Z0-9_-]*") && names.add(name));
            Path reads = resolveReads(manifestPath, fixture);
            assertTrue(Files.isRegularFile(reads));
            assertTrue(decodedHashes.add(decodedSha256(reads)), "Recompressed copies are not distinct inputs");
            assertTrue(fixture.has("platform") && fixture.hasNonNull("expectedFlyeMode"));
        }
        Path parent = Path.of(System.getProperty("assembly.flye.integration.reportDir",
                "target/flye-pipeline-integration")).toAbsolutePath();
        Files.createDirectories(parent);
        Path root = Files.createTempDirectory(parent, "run-");
        Map<String, Object> summary = new LinkedHashMap<>();
        List<Map<String, Object>> results = new ArrayList<>();
        boolean includeQc = Boolean.parseBoolean(System.getProperty("assembly.flye.integration.includeQc", "true"));
        summary.put("scope", includeQc
                ? "ReadInspect -> QC -> Assembly, with downstream input planning only; not full pipelines"
                : "Assembly boundary diagnostic: real ReadInspect and Assembly; unfiltered benchmark fixture injected as QcResult; QC NOT executed; downstream input planning only");
        summary.put("qcExecuted", includeQc);
        summary.put("publication", "TEST_PUBLICATION: real local storage and DTOs; no production DoneHandlers or database");
        summary.put("manifest", manifest);
        summary.put("createdAt", Instant.now().toString());
        summary.put("results", results);
        System.out.println("Flye integration report root: " + root);
        writeJson(root.resolve("summary.json"), summary);
        for (boolean virus : new boolean[] { false, true }) {
            for (JsonNode fixture : cases) {
                Map<String, Object> result = runCase(root, manifestPath, fixture, virus, includeQc);
                results.add(result);
                writeJson(root.resolve("summary.json"), summary);
                System.out.println("Flye case " + result.get("case") + ": " + result.get("status"));
            }
        }
        long passed = results.stream().filter(r -> "PASS".equals(r.get("status"))).count();
        summary.put("passed", passed);
        summary.put("failed", results.size() - passed);
        summary.put("completedAt", Instant.now().toString());
        writeJson(root.resolve("summary.json"), summary);
        assertEquals(6, passed, () -> "Some cases failed; all results and logs retained at " + root);
    }

    private Map<String, Object> runCase(Path root, Path manifestPath, JsonNode fixture, boolean virus, boolean includeQc)
            throws Exception {
        String label = (virus ? "virus_" : "bacteria_") + fixture.path("name").asText();
        Path runRoot = Files.createDirectory(root.resolve(label));
        Path reads = resolveReads(manifestPath, fixture);
        String originalHash = AssemblyQualityTestSupport.sha256(reads);
        Map<String, Object> report = new LinkedHashMap<>();
        ProcessRecorder recorder = new ProcessRecorder();
        long started = System.nanoTime();
        report.put("case", label);
        report.put("input", fixture);
        report.put("inputSha256", originalHash);
        report.put("decodedInputSha256", decodedSha256(reads));
        report.put("runRoot", runRoot.toString());
        report.put("processes", recorder.records);
        report.put("status", "RUNNING");
        report.put("qcExecuted", includeQc);
        writeJson(runRoot.resolve("report.json"), report);
        try {
            Path storageRoot = Files.createDirectory(runRoot.resolve("storage"));
            Path inputCopy = storageRoot.resolve(reads.getFileName());
            Files.copy(reads, inputCopy);
            String readObject = inputCopy.getFileName().toString();
            LocalStorageService storage = new LocalStorageService();
            ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
            BioStageUtil directories = new BioStageUtil();
            ReflectionTestUtils.setField(directories, "stageWorkDirPath", runRoot.resolve("work").toString());
            ReflectionTestUtils.setField(directories, "stageInputDirPath", runRoot.resolve("input").toString());
            AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
            tools.setFlye(List.of(CONDA, "run", "-n", "bio-flye", "flye"));
            tools.setFastplong(List.of(CONDA, "run", "-n", "fastplong", "fastplong"));
            tools.setSpades(List.of(CONDA, "run", "-n", "bio-spades", "spades.py"));
            tools.setFastp(List.of(CONDA, "run", "-n", "bio-fastp", "fastp"));

            PipelineSampleInput sample = new PipelineSampleInput();
            sample.setR1(readObject);
            sample.setSequenceLevel(SEQUENCE_LEVEL_READ);
            sample.setSequencePlatform(fixture.path("platform").asInt());
            PipelineConfigurations config = new PipelineConfigurations();
            config.setCandidateReferenceSequences(List.of());
            List<BioPipelineStage> stages = virus
                    ? AnalysisPipelineStagesBuilder.buildVirusStages(sample, config)
                    : AnalysisPipelineStagesBuilder.buildRegularBacteriaPipeline(sample, config);
            for (int i = 0; i < stages.size(); i++) {
                stages.get(i).setStageId((long) i + 1L);
                stages.get(i).setPipelineId(0L);
                stages.get(i).setVersion(0);
            }
            StageOrchestrator orchestrator = new StageOrchestrator();
            TrackedInspect inspect = new TrackedInspect(recorder);
            TrackedQc qc = new TrackedQc(recorder);
            TrackedAssembly assembly = new TrackedAssembly(recorder);
            for (Object executor : List.of(inspect, qc, assembly)) {
                ReflectionTestUtils.setField(executor, "storageService", storage);
                ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
                ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
            }
            ReflectionTestUtils.setField(inspect, "bbmapPath", "/home/jcy/test/bio_test/bbmap");
            ReflectionTestUtils.setField(assembly, "contigsFileName", "contigs.fasta");
            ReflectionTestUtils.setField(assembly, "scaffoldsFileName", "scaffolds.fasta");
            if (fixture.hasNonNull("ontInputMode")) {
                ReflectionTestUtils.setField(assembly, "flyeOntInputMode", fixture.get("ontInputMode").asText());
            }
            if (fixture.hasNonNull("pacbioInputMode")) {
                ReflectionTestUtils.setField(assembly, "flyePacbioInputMode", fixture.get("pacbioInputMode").asText());
            }

            BioPipelineStage inspectStage = find(stages, PIPELINE_STAGE_READ_INSPECT);
            report.put("lastStage", "ReadInspect");
            orchestrator.makePlan(stages, inspectStage.getStageId());
            ReadInspectStageOutput inspected = requireSuccess(inspect.execute(inspectStage));
            report.put("readMeta", inspected.getReadMeta());
            assertEquals(READ_LEN_TYPE_LONG, inspected.getReadMeta().getReadLenType());
            assertEquals(fixture.path("platform").asInt(), inspected.getReadMeta().getSequencingPlatform());
            ReadInspectStageResult inspection = new ReadInspectStageResult();
            inspection.setReadMeta(inspected.getReadMeta());
            inspection.setR1Url(inspected.getR1Path() == null ? readObject
                    : publish(storage, inspectStage, inspected.getR1Path(), readFilename(inspected.getR1Path(), "r1")));
            inspection.setR2Url(inspected.getR2Path() == null ? null
                    : publish(storage, inspectStage, inspected.getR2Path(), readFilename(inspected.getR2Path(), "r2")));
            assertNull(inspection.getR2Url(), "Long-read benchmark inputs should remain single-file reads");
            finish(inspectStage, inspection);

            BioPipelineStage qcStage = find(stages, PIPELINE_STAGE_QC);
            String assemblyR1;
            if (includeQc) {
                report.put("lastStage", "QC");
                orchestrator.makePlan(stages, qcStage.getStageId());
                QCStageOutput cleaned = requireSuccess(qc.execute(qcStage));
                Path cleanedPath = Path.of(cleaned.getR1Path());
                assemblyR1 = publish(storage, qcStage, cleanedPath, readFilename(cleanedPath, "cleaned"));
                assertNull(cleaned.getR2Path());
                QcResult qcResult = new QcResult(assemblyR1, null,
                        publish(storage, qcStage, Path.of(cleaned.getHtmlPath()), "qc.html"),
                        publish(storage, qcStage, Path.of(cleaned.getJsonPath()), "qc.json"));
                report.put("qcReportPath", cleaned.getJsonPath());
                report.put("qcSummary", JSON.readTree(Path.of(cleaned.getJsonPath()).toFile()).get("summary"));
                finish(qcStage, qcResult);
            } else {
                // In-memory upstream fixture only; neither bypass production QC
                // nor claim that this unfiltered input has passed QC.
                assemblyR1 = inspection.getR1Url();
                finish(qcStage, new QcResult(assemblyR1, null, null, null));
                report.put("qcDisposition", "NOT_EXECUTED_TEST_FIXTURE: unfiltered input, not QC-approved");
            }

            BioPipelineStage assemblyStage = find(stages, PIPELINE_STAGE_ASSEMBLY);
            report.put("lastStage", "Assembly");
            orchestrator.makePlan(stages, assemblyStage.getStageId());
            AssemblyStageParameters params = JsonUtil.toObject(assemblyStage.getParameters(), AssemblyStageParameters.class);
            assertEquals(JSON.valueToTree(inspected.getReadMeta()), JSON.valueToTree(params.getReadMeta()));
            assertEquals(virus ? BaseStageParams.ANALYSIS_TARGET_TYPE_VIRUS
                    : BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, params.getAnalysisTargetType());
            AssemblyInputUrls assemblyInput = JsonUtil.toObject(assemblyStage.getInputUrl(), AssemblyInputUrls.class);
            assertEquals(assemblyR1, assemblyInput.getRead1Url());
            assertNull(assemblyInput.getRead2Url());
            report.put("assemblyParameters", params);
            report.put("assemblyInput", assemblyInput);
            AssemblyStageOutput assembled = requireSuccess(assembly.execute(assemblyStage));
            assertNull(assembled.getScaffoldPath());
            Path contigs = Path.of(assembled.getContigPath());
            assertEquals("assembly.fasta", contigs.getFileName().toString());
            assertTrue(Files.size(contigs) > 0L);
            Map<String, String> records = AssemblyQualityTestSupport.readFasta(contigs);
            assertFalse(records.isEmpty());
            report.put("contigs", AssemblyQualityTestSupport.fastaMetrics(contigs));
            report.put("contigsSha256", AssemblyQualityTestSupport.sha256(contigs));
            String contigsObject = publish(storage, assemblyStage, contigs, "contigs.fasta");
            assertEquals(AssemblyQualityTestSupport.sha256(contigs),
                    AssemblyQualityTestSupport.sha256(storageRoot.resolve(contigsObject)));
            finish(assemblyStage, new AssemblyResult(contigsObject, null));

            BioPipelineStage downstream = find(stages,
                    virus ? PIPELINE_STAGE_REFERENCE_SELECTION : PIPELINE_STAGE_TAXONOMY);
            report.put("lastStage", "downstream-input-planning-only");
            orchestrator.makePlan(stages, downstream.getStageId());
            String downstreamContigs = virus
                    ? JsonUtil.toObject(downstream.getInputUrl(), ReferenceSelectionStageInputUrls.class).getContigsUrl()
                    : JsonUtil.toObject(downstream.getInputUrl(), TaxonomyStageInputUrls.class).getContigs();
            assertEquals(contigsObject, downstreamContigs);
            report.put("downstreamInput", JSON.readTree(downstream.getInputUrl()));
            List<Map<String, Object>> flyeCalls = recorder.records.stream()
                    .filter(record -> ((List<?>) record.get("command")).contains("flye")).toList();
            assertEquals(1, flyeCalls.size());
            assertTrue(((List<?>) flyeCalls.get(0).get("command"))
                    .contains("--" + fixture.path("expectedFlyeMode").asText()));
            assertEquals(0, flyeCalls.get(0).get("exitCode"));
            assertFalse(recorder.records.stream().anyMatch(record ->
                    ((List<?>) record.get("command")).contains("spades.py")));
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL");
            report.put("failure", failure.toString());
        } finally {
            try {
                boolean unchanged = originalHash.equals(AssemblyQualityTestSupport.sha256(reads));
                report.put("originalInputUnchanged", unchanged);
                if (!unchanged) {
                    report.put("status", "FAIL");
                    report.putIfAbsent("failure", "Original input changed");
                }
            } catch (Exception hashFailure) {
                report.put("status", "FAIL");
                report.put("inputVerificationFailure", hashFailure.toString());
            }
            report.put("durationSeconds", (System.nanoTime() - started) / 1_000_000_000.0);
            writeJson(runRoot.resolve("report.json"), report);
        }
        return report;
    }

    private static Path resolveReads(Path manifest, JsonNode fixture) {
        return manifest.getParent().resolve(fixture.path("reads").asText()).toAbsolutePath().normalize();
    }

    private static BioPipelineStage find(List<BioPipelineStage> stages, int type) {
        return stages.stream().filter(stage -> stage.getStageType() == type).findFirst().orElseThrow();
    }

    private static <T extends com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput>
            T requireSuccess(StageRunResult<T> result) {
        assertNotNull(result);
        assertTrue(result.isSuccess(), result::getErrorLog);
        assertNotNull(result.getStageOutput());
        return result.getStageOutput();
    }

    private static void finish(BioPipelineStage stage, Object result) throws Exception {
        stage.setOutputUrl(JsonUtil.toJson(result));
        stage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
    }

    private static String publish(LocalStorageService storage, BioPipelineStage stage, Path path, String filename)
            throws IOException {
        String object = "stageOutput/" + stage.getStageId() + "/" + filename;
        try (InputStream input = Files.newInputStream(path)) {
            var result = storage.putObject(object, input);
            assertTrue(result.success(), () -> "Local upload failed: " + result.e());
        }
        return object;
    }

    private static String readFilename(Path source, String stem) throws IOException {
        // Publication copies bytes; choose an extension without pretending to compress them.
        try (InputStream input = Files.newInputStream(source)) {
            boolean gzip = input.read() == 0x1f && input.read() == 0x8b;
            return stem + (gzip ? ".fastq.gz" : ".fastq");
        }
    }

    private static String decodedSha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream file = new BufferedInputStream(Files.newInputStream(path));
                InputStream decoded = path.toString().endsWith(".gz") ? new GZIPInputStream(file) : file) {
            byte[] buffer = new byte[65536];
            int count;
            while ((count = decoded.read(buffer)) != -1) {
                digest.update(buffer, 0, count);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void writeJson(Path path, Object value) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
    }

    /** Only adds timeout/logging around real processes; does not change assembler arguments. */
    private static final class ProcessRecorder {
        final List<Map<String, Object>> records = new ArrayList<>();

        int run(List<String> command, Path work, Path stdout, Path stderr, boolean append)
                throws IOException, InterruptedException {
            Map<String, Object> record = new LinkedHashMap<>();
            records.add(record);
            record.put("command", List.copyOf(command));
            record.put("workDir", work.toString());
            stdout = stdout == null ? work.resolve("process-" + records.size() + ".stdout.log") : stdout;
            stderr = stderr == null ? work.resolve("process-" + records.size() + ".stderr.log") : stderr;
            record.put("stdout", stdout.toString());
            record.put("stderr", stderr.toString());
            long start = System.nanoTime();
            Process process = null;
            Set<ProcessHandle> observed = new LinkedHashSet<>();
            try {
                ProcessBuilder builder = new ProcessBuilder(command).directory(work.toFile());
                builder.redirectOutput(append ? ProcessBuilder.Redirect.appendTo(stdout.toFile())
                        : ProcessBuilder.Redirect.to(stdout.toFile()));
                builder.redirectError(append ? ProcessBuilder.Redirect.appendTo(stderr.toFile())
                        : ProcessBuilder.Redirect.to(stderr.toFile()));
                process = builder.start();
                observed.add(process.toHandle());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
                while (true) {
                    process.descendants().forEach(observed::add);
                    if (process.waitFor(1, TimeUnit.SECONDS)) {
                        record.put("exitCode", process.exitValue());
                        return process.exitValue();
                    }
                    if (System.nanoTime() >= deadline) {
                        record.put("timeout", true);
                        throw new IOException("Tool exceeded " + TIMEOUT_SECONDS + " seconds");
                    }
                }
            } catch (IOException | InterruptedException e) {
                record.put("error", e.toString());
                throw e;
            } finally {
                if (process != null) {
                    process.descendants().forEach(observed::add);
                    observed.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (observed.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < deadline) {
                        try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                    observed.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                    long forcedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (observed.stream().anyMatch(ProcessHandle::isAlive) && System.nanoTime() < forcedDeadline) {
                        try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                    record.put("observedProcessIds", observed.stream().map(ProcessHandle::pid).sorted().toList());
                    record.put("remainingProcessIds", observed.stream().filter(ProcessHandle::isAlive)
                            .map(ProcessHandle::pid).sorted().toList());
                }
                record.put("durationSeconds", (System.nanoTime() - start) / 1_000_000_000.0);
                writeJson(work.resolve("process-" + records.size() + ".json"), record);
                if (observed.stream().anyMatch(ProcessHandle::isAlive)) {
                    throw new IOException("Owned tool process still alive after cleanup; see process report");
                }
            }
        }
    }

    private static final class TrackedInspect extends ReadInspectStageExecutor {
        final ProcessRecorder recorder;
        TrackedInspect(ProcessRecorder recorder) { this.recorder = recorder; }
        @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a)
                throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
    }

    private static final class TrackedQc extends QcStageExecutor {
        final ProcessRecorder recorder;
        TrackedQc(ProcessRecorder recorder) { this.recorder = recorder; }
        @Override protected int runSubProcess(List<String> c, Path w)
                throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
    }

    private static final class TrackedAssembly extends AssemblyExecutor {
        final ProcessRecorder recorder;
        TrackedAssembly(ProcessRecorder recorder) { this.recorder = recorder; }
        @Override protected int runSubProcess(List<String> c, Path w)
                throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
    }
}
