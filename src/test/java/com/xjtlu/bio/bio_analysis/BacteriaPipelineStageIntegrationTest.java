package com.xjtlu.bio.bio_analysis;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_REGULAR_BACTERIA;
import static com.xjtlu.bio.analysisPipeline.Constants.SequenceInput.SEQUENCE_LEVEL_READ;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.*;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.service.TaxonomyClassificationService;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.*;
import com.xjtlu.bio.analysisPipeline.taskrunner.*;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineConfigurations;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder.PipelineSampleInput;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.command.UpdateStageCommand;

/**
 * Opt-in, externally supplied benign bacterial software fixtures. Manifest:
 * {"cases":[{"name":"control_1","r1":"reads_1.fastq.gz","r2":"reads_2.fastq.gz"}, ...]}.
 * Paths are relative to the manifest unless absolute. Supports one to five cases so
 * an individual failed fixture can be rerun without repeating the entire batch.
 * Each input slot accepts FASTQ/FQ, optionally wrapped in gzip (.gz/.gzip) or ZIP (.zip).
 * Optional sequencePlatform uses the production integer enum; all cases enter at reads.
 *
 * Real builder, orchestrator, executors, done handlers and local storage. Only persistence
 * is replaced. No Spring context is started: no datasource, mapper, HTTP application,
 * dispatcher, poller, scheduled reconciliation or RefSeqService initialization exists.
 * Tool properties are read from application-dev.yml without bootstrapping its services.
 * Taxonomy metadata initialization reads its filesystem DB; it does not require SQL.
 * ACTION_REQUIRED is reported, never auto-confirmed. This is not biological validation.
 */
@EnabledIfSystemProperty(named = "bacteria.integration.manifest", matches = ".+")
class BacteriaPipelineStageIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int THREADS = 2;

    @Test
    @Timeout(value = 180, unit = TimeUnit.MINUTES)
    void runCompleteReadLevelBacterialPipelines() throws Exception {
        Path manifestPath = Path.of(System.getProperty("bacteria.integration.manifest")).toAbsolutePath();
        JsonNode manifest = JSON.readTree(manifestPath.toFile());
        JsonNode cases = manifest.isArray() ? manifest : manifest.path("cases");
        assertTrue(cases.isArray() && !cases.isEmpty() && cases.size() <= 5,
                "Supply one to five distinct public bacterial software fixtures");
        Path parent = Path.of(System.getProperty("bacteria.integration.reportDir",
                "target/bacteria-pipeline-integration")).toAbsolutePath();
        Files.createDirectories(parent);
        Path root = Files.createTempDirectory(parent, "run-");
        Map<String, Object> summary = new LinkedHashMap<>();
        List<Map<String, Object>> reports = new ArrayList<>();
        summary.put("scope", "Real read-level bacterial DAG including real done handlers; in-memory persistence only");
        summary.put("databaseAccess", "NONE: no application context, datasource, mappers or scheduler created");
        summary.put("manifest", manifestPath.toString());
        summary.put("threads", THREADS);
        summary.put("startedAt", Instant.now().toString());
        summary.put("cases", reports);
        writeJson(root.resolve("summary.json"), summary);
        System.out.println("Bacterial pipeline integration reports: " + root);
        Set<String> names = new HashSet<>();
        Set<String> inputPairs = new HashSet<>();
        try {
            StandardEnvironment environment = toolEnvironment();
            int index = 0;
            for (JsonNode fixture : cases) {
                String name = fixture.path("name").asText();
                assertTrue(name.matches("[a-zA-Z0-9][a-zA-Z0-9_-]*") && names.add(name),
                        "Fixture names must be safe and unique");
                Path runRoot = Files.createDirectory(root.resolve(name));
                Map<String, Object> report = new LinkedHashMap<>();
                reports.add(report);
                report.put("fixture", fixture);
                report.put("runRoot", runRoot.toString());
                report.put("status", "RUNNING");
                report.put("startedAt", Instant.now().toString());
                long start = System.nanoTime();
                try {
                    runCase(manifestPath, fixture, runRoot, environment, ++index, inputPairs, report);
                } catch (Exception | AssertionError failure) {
                    report.put("status", "FAILED");
                    report.put("error", failure.toString());
                } finally {
                    report.put("durationSeconds", elapsed(start));
                    report.put("finishedAt", Instant.now().toString());
                    writeJson(runRoot.resolve("report.json"), report);
                    writeJson(root.resolve("summary.json"), summary);
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Bacterial integration interrupted; reports retained at " + root);
                }
            }
        } finally {
            summary.put("finishedAt", Instant.now().toString());
            summary.put("completed", reports.stream().filter(r -> "COMPLETE".equals(r.get("status"))).count());
            summary.put("actionRequired", reports.stream().filter(r -> "ACTION_REQUIRED".equals(r.get("status"))).count());
            summary.put("failed", reports.stream().filter(r -> "FAILED".equals(r.get("status"))).count());
            writeJson(root.resolve("summary.json"), summary);
        }
        assertEquals(cases.size(), reports.size(), "All requested fixtures must be attempted");
        assertTrue(reports.stream().noneMatch(r -> "FAILED".equals(r.get("status"))),
                "A bacterial workflow failed; inspect retained reports at " + root);
    }

    private void runCase(Path manifestPath, JsonNode fixture, Path root, StandardEnvironment environment,
            long pipelineId, Set<String> inputPairs, Map<String, Object> report) throws Exception {
        Path storageRoot = Files.createDirectory(root.resolve("storage"));
        Path objects = Files.createDirectory(storageRoot.resolve("fixture"));
        String r1 = copyInput(manifestPath, fixture, "r1", objects, report);
        String r2 = fixture.hasNonNull("r2") && !fixture.path("r2").asText().isBlank()
                ? copyInput(manifestPath, fixture, "r2", objects, report) : null;
        assertTrue(inputPairs.add(report.get("r1Sha256") + ":" + report.get("r2Sha256")),
                "Duplicated input file pairs are not distinct test fixtures");
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", root.resolve("work").toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", root.resolve("input").toString());
        AnalysisPipelineToolsConfig tools = Binder.get(environment)
                .bind("analysis-pipeline.tools", Bindable.of(AnalysisPipelineToolsConfig.class)).get();
        TaxonomyClassificationService taxonomy = new TaxonomyClassificationService();
        injectValues(taxonomy, environment);
        taxonomy.init();
        report.put("taxonomyResources", Map.of("kraken2", taxonomy.getKraken2DB(),
                "fastANI", taxonomy.getFastANIDB(), "metadataLoaded", taxonomy.isServiceOk()));
        assertTrue(taxonomy.isServiceOk(), "Configured FastANI filesystem metadata failed initialization");
        ProcessRecorder processes = new ProcessRecorder(root.resolve("processes"));
        report.put("processes", processes.records);
        report.put("cpuAffinity", processes.cpuAffinity);
        Map<Integer, PipelineStageExecutor> executors = createExecutors(processes);
        for (PipelineStageExecutor executor : executors.values()) {
            injectValues(executor, environment);
            ReflectionTestUtils.setField(executor, "objectMapper", JSON);
            ReflectionTestUtils.setField(executor, "storageService", storage);
            ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
            ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
            // taskDispatcher and refSeqService are intentionally not constructed or injected.
        }
        ReflectionTestUtils.setField(executors.get(PIPELINE_STAGE_TAXONOMY), "taxonomyClassificationService", taxonomy);
        PipelineSampleInput input = new PipelineSampleInput(r1, r2);
        input.setSequenceLevel(SEQUENCE_LEVEL_READ);
        if (fixture.hasNonNull("sequencePlatform")) input.setSequencePlatform(fixture.get("sequencePlatform").asInt());
        List<BioPipelineStage> stages = AnalysisPipelineStagesBuilder.buildRegularBacteriaPipeline(input,
                new PipelineConfigurations());
        InMemoryPipelineService persistence = new InMemoryPipelineService();
        long id = pipelineId * 100;
        for (BioPipelineStage stage : stages) {
            stage.setStageId(++id);
            stage.setPipelineId(pipelineId);
            stage.setVersion(0);
            persistence.stages.put(stage.getStageId(), stage);
        }
        Map<Integer, StageDoneHandler> handlers = new LinkedHashMap<>();
        for (AbstractStageDoneHandler handler : List.of(new ReadInspectStageDoneHandler(), new QcStageDoneHandler(),
                new AssemblyStageDoneHandler(), new TaxonomyStageDoneHandler(), new AMRStageDoneHandler(),
                new VFStageDoneHandler(), new MLSTStageDoneHandler(), new SeroTypeStageDoneHandler(), new FailStageDoneHandler())) {
            injectValues(handler, environment);
            ReflectionTestUtils.setField(handler, "storageService", storage);
            ReflectionTestUtils.setField(handler, "bioStageUtil", directories);
            ReflectionTestUtils.setField(handler, "pipelineService", persistence);
            handlers.put(handler.getType(), handler);
        }
        StageOrchestrator orchestrator = new StageOrchestrator();
        report.put("stages", stages);
        List<Map<String, Object>> trace = new ArrayList<>();
        report.put("trace", trace);
        Deque<BioPipelineStage> ready = new ArrayDeque<>();
        for (BioPipelineStage stage : stages) {
            if (stage.getStageIndex() == 0) ready.addAll(orchestrator.makePlan(stages, stage.getStageId()).getRunStages());
        }
        Set<Long> executed = new HashSet<>();
        while (!ready.isEmpty()) {
            BioPipelineStage stage = ready.removeFirst();
            assertTrue(executed.add(stage.getStageId()), "Stage scheduled twice: " + stage.getStageId());
            Map<String, Object> event = new LinkedHashMap<>();
            trace.add(event);
            event.put("stageId", stage.getStageId());
            event.put("stageType", stage.getStageType());
            event.put("name", stage.getStageName());
            event.put("startedAt", Instant.now().toString());
            stage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);
            stage.setVersion(stage.getVersion() + 1);
            long start = System.nanoTime();
            try {
                StageRunResult result = executors.get(stage.getStageType()).execute(stage);
                assertNotNull(result, "Executor returned no result");
                event.put("executorSuccess", result.isSuccess());
                if (!result.isSuccess()) event.put("error", result.getErrorLog());
                StageDoneHandler handler = result.isSuccess() ? handlers.get(stage.getStageType()) : handlers.get(-1);
                handler.handleStageDone(result);
                if (stage.getStatus() == PIPELINE_STAGE_STATUS_FAIL && !event.containsKey("error")) {
                    event.put("error", "DoneHandler marked stage FAIL; inspect the retained test and process logs");
                }
                assertTrue(Set.of(PIPELINE_STAGE_STATUS_FINISHED, PIPELINE_STAGE_STATUS_FAIL,
                        PIPELINE_STAGE_STATUS_ACTION_REQUIRED, PIPELINE_STAGE_STATUS_NOT_APPLICABLE).contains(stage.getStatus()),
                        "Done handler did not publish a terminal or actionable state");
                if (stage.getStatus() != PIPELINE_STAGE_STATUS_FAIL) {
                    ready.addAll(orchestrator.makeDownstreamPlan(stage, stages, PIPELINE_REGULAR_BACTERIA).getRunStages());
                }
            } catch (Exception | AssertionError failure) {
                event.put("error", failure.toString());
                stage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            } finally {
                event.put("durationSeconds", elapsed(start));
                event.put("status", statusName(stage.getStatus()));
                event.put("outputUrl", stage.getOutputUrl());
                event.put("outputInline", stage.getOutputInline());
                writeJson(root.resolve("report.json"), report);
            }
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Tool execution interrupted");
        }
        boolean failed = stages.stream().anyMatch(s -> s.getStatus() == PIPELINE_STAGE_STATUS_FAIL);
        boolean actionable = stages.stream().anyMatch(s -> s.getStatus() == PIPELINE_STAGE_STATUS_ACTION_REQUIRED);
        if (!failed) {
            for (BioPipelineStage stage : stages) {
                boolean terminal = stage.getStatus() == PIPELINE_STAGE_STATUS_FINISHED
                        || stage.getStatus() == PIPELINE_STAGE_STATUS_NOT_APPLICABLE
                        || stage.getStatus() == PIPELINE_STAGE_STATUS_ACTION_REQUIRED;
                boolean waitingForTaxonomy = actionable && stage.getStageType() == PIPELINE_STAGE_SEROTYPE
                        && stage.getStatus() == PIPELINE_STAGE_STATUS_PENDING;
                assertTrue(terminal || waitingForTaxonomy, "Unexpected stranded stage: " + stage.getStageName());
            }
        }
        report.put("status", failed ? "FAILED" : actionable ? "ACTION_REQUIRED" : "COMPLETE");
        report.put("storageRoot", storageRoot.toString());
        report.put("confirmationPolicy", "No automatic taxonomy confirmation; independent branches still execute");
    }

    private static StandardEnvironment toolEnvironment() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application-dev.yml"));
        Properties properties = Objects.requireNonNull(yaml.getObject());
        Map<String, Object> selected = new LinkedHashMap<>();
        properties.forEach((key, value) -> {
            String name = key.toString();
            if (name.startsWith("analysis-pipeline.") || Set.of("scriptsDir", "seroBADatabase", "pFasterDir").contains(name)) {
                selected.put(name, value);
            }
        });
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().addLast(new MapPropertySource("bacterialToolConfiguration", selected));
        return environment;
    }

    private static void injectValues(Object target, StandardEnvironment environment) {
        for (Class<?> type = target.getClass(); type != null; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                Value value = field.getAnnotation(Value.class);
                if (value != null) ReflectionTestUtils.setField(target, field.getName(),
                        environment.resolveRequiredPlaceholders(value.value()));
            }
        }
    }

    private static String copyInput(Path manifest, JsonNode fixture, String key, Path objects,
            Map<String, Object> report) throws Exception {
        String value = fixture.path(key).asText();
        assertFalse(value.isBlank(), "Missing fixture input: " + key);
        Path source = Path.of(value);
        if (!source.isAbsolute()) source = manifest.getParent().resolve(source);
        source = source.normalize().toAbsolutePath();
        assertTrue(Files.isRegularFile(source), "Missing input file: " + source);
        assertTrue(source.getFileName().toString().matches("(?i).+\\.(fastq|fq)(\\.(gz|gzip|zip))?"),
                "This read-level benchmark requires FASTQ/FQ fixtures, optionally .gz, .gzip or .zip: " + source);
        Path copy = objects.resolve(key + "_" + source.getFileName());
        Files.copy(source, copy);
        String hash = sha256(source);
        assertEquals(hash, sha256(copy), "Fixture copy changed contents");
        report.put(key + "Source", source.toString());
        report.put(key + "Sha256", hash);
        return "fixture/" + copy.getFileName();
    }

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (var input = Files.newInputStream(path)) {
            byte[] buffer = new byte[128 * 1024];
            int count;
            while ((count = input.read(buffer)) >= 0) digest.update(buffer, 0, count);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static double elapsed(long start) { return (System.nanoTime() - start) / 1_000_000_000.0; }
    private static void writeJson(Path path, Object value) throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
    }
    private static String statusName(Integer status) {
        return switch (status) {
            case PIPELINE_STAGE_STATUS_PENDING -> "PENDING";
            case PIPELINE_STAGE_STATUS_QUEUING -> "QUEUING";
            case PIPELINE_STAGE_STATUS_RUNNING -> "RUNNING";
            case PIPELINE_STAGE_STATUS_FAIL -> "FAIL";
            case PIPELINE_STAGE_STATUS_FINISHED -> "FINISHED";
            case PIPELINE_STAGE_STATUS_ACTION_REQUIRED -> "ACTION_REQUIRED";
            case PIPELINE_STAGE_STATUS_NOT_APPLICABLE -> "NOT_APPLICABLE";
            default -> "UNKNOWN";
        };
    }

    private static final class InMemoryPipelineService extends PipelineService {
        final Map<Long, BioPipelineStage> stages = new LinkedHashMap<>();
        @Override public void recordStageFailure(com.xjtlu.bio.analysisPipeline.context.runtime.StageContext context,
                String message) { /* Failure details are retained in the test report; no database writes. */ }
        @Override public BioPipelineStage queryStageById(long id) { return stages.get(id); }
        @Override public void pipelineStageDone(long id, boolean success) { /* Orchestrated synchronously above. */ }
        @Override public int updateStageFromVersion(UpdateStageCommand command) {
            BioPipelineStage target = stages.get(command.getStageId());
            if (target == null || !Objects.equals(target.getVersion(), command.getCurrentVersion())) return 0;
            BioPipelineStage patch = command.getUpdateStage();
            if (patch.getInputUrl() != null) target.setInputUrl(patch.getInputUrl());
            if (patch.getOutputUrl() != null) target.setOutputUrl(patch.getOutputUrl());
            if (patch.getOutputInline() != null) target.setOutputInline(patch.getOutputInline());
            if (patch.getParameters() != null) target.setParameters(patch.getParameters());
            if (patch.getStatus() != null) target.setStatus(patch.getStatus());
            if (patch.getStartTime() != null) target.setStartTime(patch.getStartTime());
            if (patch.getEndTime() != null) target.setEndTime(patch.getEndTime());
            target.setVersion(command.getCurrentVersion() + 1);
            return 1;
        }
    }

    /** Process-only instrumentation: CPU limits, timeout and durable logs outside cleaned work dirs. */
    private static final class ProcessRecorder {
        final Path logs;
        final String cpuAffinity;
        final List<Map<String, Object>> records = new ArrayList<>();
        ProcessRecorder(Path logs) throws IOException {
            this.logs = Files.createDirectories(logs);
            String allowed = Files.readAllLines(Path.of("/proc/self/status")).stream()
                    .filter(line -> line.startsWith("Cpus_allowed_list:")).findFirst().orElseThrow().split(":", 2)[1].strip();
            List<Integer> cpus = new ArrayList<>();
            for (String part : allowed.split(",")) {
                String[] range = part.split("-");
                int first = Integer.parseInt(range[0]);
                int last = range.length == 1 ? first : Integer.parseInt(range[1]);
                for (int cpu = first; cpu <= last && cpus.size() < THREADS; cpu++) cpus.add(cpu);
                if (cpus.size() == THREADS) break;
            }
            cpuAffinity = String.join(",", cpus.stream().map(Object::toString).toList());
            if (cpuAffinity.isEmpty() || !Files.isExecutable(Path.of("/usr/bin/taskset")))
                throw new IOException("Linux taskset and a nonempty allowed CPU set are required");
        }
        int run(List<String> original, Path work, Path stdout, Path stderr, boolean append)
                throws IOException, InterruptedException {
            int index = records.size() + 1;
            Map<String, Object> record = new LinkedHashMap<>();
            records.add(record);
            List<String> command = new ArrayList<>(original);
            // -t has unrelated meanings in some tools, so only alter known assembler flags.
            boolean spades = original.stream().anyMatch(s -> Path.of(s).getFileName().toString().equals("spades.py"));
            boolean flye = original.stream().anyMatch(s -> Path.of(s).getFileName().toString().equals("flye"));
            for (int i = 0; i + 1 < command.size(); i++) {
                if ((spades && command.get(i).equals("-t")) || (flye && command.get(i).equals("--threads")))
                    command.set(i + 1, Integer.toString(THREADS));
            }
            if (spades && !command.contains("-m") && !command.contains("--memory")) {
                // Resource-only cap for this shared development host, recorded below.
                command.addAll(List.of("-m", "6"));
            }
            List<String> bounded = new ArrayList<>(List.of("/usr/bin/taskset", "--cpu-list", cpuAffinity));
            bounded.addAll(command);
            Path durableOut = logs.resolve(index + ".stdout.log");
            Path durableErr = logs.resolve(index + ".stderr.log");
            Path actualOut = stdout == null ? durableOut : stdout;
            Path actualErr = stderr == null ? durableErr : stderr;
            Files.createDirectories(actualOut.toAbsolutePath().getParent());
            Files.createDirectories(actualErr.toAbsolutePath().getParent());
            record.put("productionCommand", original);
            record.put("executedCommand", bounded);
            record.put("workDir", work.toString());
            record.put("stdout", durableOut.toString());
            record.put("stderr", durableErr.toString());
            long start = System.nanoTime();
            Process process = null;
            Set<ProcessHandle> owned = new LinkedHashSet<>();
            try {
                ProcessBuilder builder = new ProcessBuilder(bounded).directory(work.toFile());
                for (String key : List.of("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"))
                    builder.environment().put(key, Integer.toString(THREADS));
                builder.redirectOutput(append ? ProcessBuilder.Redirect.appendTo(actualOut.toFile()) : ProcessBuilder.Redirect.to(actualOut.toFile()));
                builder.redirectError(append ? ProcessBuilder.Redirect.appendTo(actualErr.toFile()) : ProcessBuilder.Redirect.to(actualErr.toFile()));
                process = builder.start();
                owned.add(process.toHandle());
                long timeout = Long.getLong("bacteria.integration.processTimeoutSeconds", 1800L);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);
                while (true) {
                    process.descendants().forEach(owned::add);
                    if (process.waitFor(1, TimeUnit.SECONDS)) {
                        record.put("exitCode", process.exitValue());
                        return process.exitValue();
                    }
                    if (System.nanoTime() >= deadline) {
                        record.put("timeout", true);
                        throw new IOException("Process exceeded " + timeout + " seconds");
                    }
                }
            } catch (IOException | InterruptedException failure) {
                record.put("error", failure.toString());
                throw failure;
            } finally {
                if (process != null) {
                    process.descendants().forEach(owned::add);
                    owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroy);
                    owned.stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
                    long stopDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (owned.stream().anyMatch(ProcessHandle::isAlive)
                            && System.nanoTime() < stopDeadline && !Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    record.put("survivingProcessIds", owned.stream().filter(ProcessHandle::isAlive)
                            .map(ProcessHandle::pid).sorted().toList());
                }
                if (!actualOut.equals(durableOut) && Files.exists(actualOut)) Files.copy(actualOut, durableOut, StandardCopyOption.REPLACE_EXISTING);
                if (!actualErr.equals(durableErr) && Files.exists(actualErr)) Files.copy(actualErr, durableErr, StandardCopyOption.REPLACE_EXISTING);
                record.put("durationSeconds", elapsed(start));
                record.put("observedProcessIds", owned.stream().map(ProcessHandle::pid).sorted().toList());
                writeJson(logs.resolve(index + ".json"), record);
            }
        }
    }

    private static Map<Integer, PipelineStageExecutor> createExecutors(ProcessRecorder recorder) {
        List<PipelineStageExecutor> executors = List.of(
                new ReadInspectStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new QcStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new AssemblyExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new TaxonomyStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new AmrStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new VirulenceFactorStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new MLSTStageExecutor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                }, new SeroTypingStageExectuor() {
                    @Override protected int runSubProcess(List<String> c, Path w) throws IOException, InterruptedException { return recorder.run(c, w, null, null, false); }
                    @Override protected int runSubProcess(List<String> c, Path w, Path o, Path e, boolean a) throws IOException, InterruptedException { return recorder.run(c, w, o, e, a); }
                });
        Map<Integer, PipelineStageExecutor> result = new LinkedHashMap<>();
        executors.forEach(executor -> result.put(executor.id(), executor));
        return result;
    }
}
