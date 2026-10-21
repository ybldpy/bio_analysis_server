package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CovidWastewaterStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.CovidWastewaterStageParameters;
import com.xjtlu.bio.analysisPipeline.stageMetrics.CovidWastewaterResultMetrics;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.CovidWastewaterStageOutput;
import com.xjtlu.bio.configuration.CovidWastewaterConfig;
import com.xjtlu.bio.configuration.CovidWastewaterConfig.Profile;
import com.xjtlu.bio.utils.JsonUtil;

/** All external tool execution and storage are fixtures; no database or biology tools are used. */
class CovidWastewaterStageExecutorTest {

    private static final String FASTQ = "@synthetic-fixture\nACGT\n+\nIIII\n";
    private static final String REPORT = "\tsynthetic-sample\nlineages\tLineage-A Lineage-B\n"
            + "abundances\t0.6 0.2\nresid\t0.1\ncoverage\t95.0\n";
    private static final String PROFILE = "fixture-profile";

    @TempDir
    Path tempDir;

    @Test
    void successProducesValidatedArtifactsRequestManifestAndChecksummedProvenance() throws Exception {
        CovidWastewaterConfig config = config();
        FixtureExecutor executor = new FixtureExecutor(config);
        CovidWastewaterStageParameters parameters = new CovidWastewaterStageParameters();
        parameters.setThreads(3);

        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input("uploads/r2.fastq"), parameters, tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(3, executor.commands.size());
        List<String> variantsCommand = executor.commands.get(1);
        assertEquals("aligned.bam", variantsCommand.get(2));
        assertEquals("resources/reference.fasta", option(variantsCommand, "--ref"));
        assertEquals("variants", option(variantsCommand, "--variants"));
        assertEquals("depths.tsv", option(variantsCommand, "--depths"));
        assertEquals(Constants.StageType.PIPELINE_STAGE_COVID_WASTEWATER, executor.id());
        assertEquals(CovidWastewaterStageInputUrls.class, executor.stageInputType());
        assertEquals(CovidWastewaterStageParameters.class, executor.stageParameterType());
        CovidWastewaterStageOutput output = result.getStageOutput();
        for (Path path : List.of(output.getReportPath(), output.getMetricsPath(), output.getProvenancePath(),
                output.getExecutionLogPath(), output.getBamPath(), output.getBamIndexPath())) {
            assertTrue(Files.isRegularFile(path));
            assertTrue(Files.size(path) > 0L);
            assertEquals(output.getParentPath(), path.getParent());
        }
        assertTrue(output.getParentPath().isAbsolute());
        assertEquals(tempDir.resolve("work"), output.getParentPath().getParent());
        Map<String, Object> request = json(output.getParentPath().resolve("preparation-request.json"));
        assertEquals(1, request.get("schemaVersion"));
        assertEquals(PROFILE, request.get("profileId"));
        assertEquals("profile-v1", request.get("profileVersion"));
        assertEquals("AMPLICON", request.get("libraryType"));
        assertEquals(3, request.get("threads"));
        assertEquals(Constants.SequencingPlatform.ILLUMINA, request.get("sequencingPlatform"));
        assertEquals(executor.loaded.get("uploads/r1.fastq").toString(), request.get("r1"));
        assertEquals(executor.loaded.get("uploads/r2.fastq").toString(), request.get("r2"));
        assertEquals(output.getBamPath().toString(), request.get("bamOutput"));
        Path copiedReference = Path.of((String) request.get("referenceFasta"));
        assertTrue(copiedReference.startsWith(output.getParentPath().resolve("resources")));
        assertNotEquals(profile(config).getReferenceFasta(), copiedReference.toString());
        assertEquals(hash(copiedReference), request.get("referenceSha256"));

        CovidWastewaterResultMetrics metrics = JsonUtil.toObject(
                Files.readString(output.getMetricsPath()), CovidWastewaterResultMetrics.class);
        assertEquals(2, metrics.getLineages().size());
        assertEquals(0.6, metrics.getLineages().get(0).getAbundance());
        assertEquals(0.2, metrics.getLineages().get(1).getAbundance());
        assertEquals(95.0, metrics.getCoveragePercent());
        Map<String, Object> provenance = json(output.getProvenancePath());
        assertEquals(PROFILE, provenance.get("profileId"));
        assertEquals("profile-v1", provenance.get("profileVersion"));
        assertEquals("fixture-data-v1", provenance.get("dataVersion"));
        assertEquals("fixture-tool-v1", provenance.get("configuredFreyjaVersion"));
        assertEquals("server-configuration-not-runtime-verified", provenance.get("toolVersionSource"));
        assertEquals("declared-analysis-profile-not-inferred-from-fastq", provenance.get("libraryTypeSource"));
        assertNotNull(provenance.get("completedAt"));
        assertEquals(hash(output.getReportPath()), map(provenance.get("report")).get("sha256"));
        assertEquals(hash(executor.loaded.get("uploads/r1.fastq")), map(provenance.get("inputR1")).get("sha256"));
        assertNotNull(provenance.get("inputR2"));
        assertEquals(Set.of("reference", "barcodes", "hierarchy", "metadata"),
                map(provenance.get("resources")).keySet());
        String log = Files.readString(output.getExecutionLogPath());
        assertTrue(log.contains("START profile=" + PROFILE));
        assertTrue(log.contains("FINISHED; report validation passed"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "\t"})
    void singleEndInputOmitsR2FromDownloadsManifestAndProvenance(String r2) throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());

        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(r2), null, tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(Set.of("uploads/r1.fastq"), executor.loaded.keySet());
        Map<String, Object> request = json(result.getStageOutput().getParentPath().resolve("preparation-request.json"));
        assertNull(request.get("r2"));
        assertEquals(2, request.get("threads"));
        assertNull(json(result.getStageOutput().getProvenancePath()).get("inputR2"));
    }

    @Test
    void sameBasenamesFromDifferentObjectsAreIsolatedInReadSlotDirectories() throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        CovidWastewaterStageInputUrls input = input("uploads/right/shared.fastq");
        input.setR1Url("uploads/left/shared.fastq");

        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input, null, tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(tempDir.resolve("input/r1/shared.fastq"), executor.loaded.get(input.getR1Url()));
        assertEquals(tempDir.resolve("input/r2/shared.fastq"), executor.loaded.get(input.getR2Url()));
        assertNotEquals(executor.loaded.get(input.getR1Url()), executor.loaded.get(input.getR2Url()));
    }

    @Test
    void eachAttemptUsesAFreshDirectoryAndCannotReuseThePreviousReport() throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        StageRunResult<CovidWastewaterStageOutput> first = executor.run(input(null), null, tempDir);
        assertTrue(first.isSuccess(), first.getErrorLog());
        executor.omitted = Set.of("freyja.demix.tsv");

        StageRunResult<CovidWastewaterStageOutput> second = executor.run(input(null), null, tempDir);

        assertFailure(second, "Freyja report");
        Path firstAttempt = first.getStageOutput().getParentPath();
        Path secondAttempt = executor.workDirs.get(executor.workDirs.size() - 1);
        assertNotEquals(firstAttempt, secondAttempt);
        assertTrue(Files.exists(first.getStageOutput().getReportPath()));
        assertFalse(Files.exists(secondAttempt.resolve("freyja.demix.tsv")));
    }

    @Test
    void snapshotsConfigurationAndResourceCopiesBeforeCallingExternalAdapters() throws Exception {
        CovidWastewaterConfig config = config();
        FixtureExecutor executor = new FixtureExecutor(config);
        Path originalReference = Path.of(profile(config).getReferenceFasta());
        String originalHash = hash(originalReference);
        executor.beforeFirstProcess = () -> {
            profile(config).setVersion("modified-after-start");
            config.setConfiguredFreyjaVersion("modified-tool");
            try { Files.writeString(originalReference, "modified-resource\n"); }
            catch (IOException e) { throw new IllegalStateException(e); }
        };

        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);

        assertTrue(result.isSuccess(), result.getErrorLog());
        Map<String, Object> provenance = json(result.getStageOutput().getProvenancePath());
        assertEquals("profile-v1", provenance.get("profileVersion"));
        assertEquals("fixture-tool-v1", provenance.get("configuredFreyjaVersion"));
        assertEquals(originalHash, map(map(provenance.get("resources")).get("reference")).get("sha256"));
        assertNotEquals(originalHash, hash(originalReference));
    }

    @ParameterizedTest
    @MethodSource("badConfigurations")
    void configurationFailuresStopBeforeDownloadingOrStartingProcesses(Consumer<CovidWastewaterConfig> mutate,
            String expected) throws Exception {
        CovidWastewaterConfig config = config();
        mutate.accept(config);
        FixtureExecutor executor = new FixtureExecutor(config);

        assertRejectedBeforeWork(executor, executor.run(input(null), null, tempDir), expected);
    }

    private static Stream<Arguments> badConfigurations() {
        return Stream.of(
                badConfig(c -> c.setDefaultProfile(null), "default-profile"),
                badConfig(c -> c.setProfiles(Map.of()), "default-profile"),
                badConfig(c -> profile(c).setLibraryType("UNDECLARED"), "AMPLICON"),
                badConfig(c -> profile(c).setVersion(""), "profile version"),
                badConfig(c -> profile(c).setDataVersion(""), "data-version"),
                badConfig(c -> profile(c).setSupportedPlatforms(List.of()), "not supported"),
                badConfig(c -> profile(c).setPreparationCommand(List.of()), "preparation-command"),
                badConfig(c -> c.setFreyjaCommand(List.of()), "freyja-command"),
                badConfig(c -> c.setFreyjaCommand(List.of("bad\0command")), "freyja-command"),
                badConfig(c -> c.setConfiguredFreyjaVersion(null), "configured-freyja-version"),
                badConfig(c -> profile(c).setReferenceFasta("relative.fa"), "absolute server path"),
                badConfig(c -> profile(c).setLineageHierarchy(null), "lineage-hierarchy"));
    }

    private static Arguments badConfig(Consumer<CovidWastewaterConfig> mutate, String expected) {
        return Arguments.of(mutate, expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "empty", "directory"})
    void invalidServerResourcesFailBeforeDownloads(String kind) throws Exception {
        CovidWastewaterConfig config = config();
        Path resource = tempDir.resolve("unusable-resource");
        if ("empty".equals(kind)) { Files.createFile(resource); }
        if ("directory".equals(kind)) { Files.createDirectories(resource); }
        profile(config).setBarcodes(resource.toString());
        FixtureExecutor executor = new FixtureExecutor(config);

        assertRejectedBeforeWork(executor, executor.run(input(null), null, tempDir), "barcodes");
    }

    @Test
    void nullConfigurationAndUnknownProfileFailClosed() throws Exception {
        FixtureExecutor missing = new FixtureExecutor(null);
        assertRejectedBeforeWork(missing, missing.run(input(null), null, tempDir), "configuration");
        FixtureExecutor executor = new FixtureExecutor(config());
        CovidWastewaterStageParameters parameters = new CovidWastewaterStageParameters();
        parameters.setAnalysisProfile("unregistered-profile");
        assertRejectedBeforeWork(executor, executor.run(input(null), parameters, tempDir), "default-profile");
    }

    @Test
    void explicitProfileIsUsedWithoutADefaultAndUnknownPlatformRequiresExplicitSupport() throws Exception {
        CovidWastewaterConfig config = config();
        config.setDefaultProfile(null);
        CovidWastewaterStageParameters parameters = new CovidWastewaterStageParameters();
        parameters.setAnalysisProfile(PROFILE);
        CovidWastewaterStageInputUrls input = input(null);
        input.setSequencingPlatform(null);
        FixtureExecutor unsupported = new FixtureExecutor(config);
        assertRejectedBeforeWork(unsupported, unsupported.run(input, parameters, tempDir), "UNKNOWN (0)");

        profile(config).setSupportedPlatforms(List.of(Constants.SequencingPlatform.UNKNOWN));
        FixtureExecutor supported = new FixtureExecutor(config);
        StageRunResult<CovidWastewaterStageOutput> result = supported.run(input, parameters, tempDir);
        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(0, json(result.getStageOutput().getProvenancePath()).get("sequencingPlatform"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "reads.fasta", "reads.txt"})
    void missingOrNonFastqR1IsRejectedBeforeWork(String r1) throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        CovidWastewaterStageInputUrls input = input(null);
        input.setR1Url(r1);
        assertRejectedBeforeWork(executor, executor.run(input, null, tempDir), "FASTQ");
    }

    @Test
    void nullInputDuplicateMatesAndNonFastqR2AreRejected() throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        assertRejectedBeforeWork(executor, executor.run(null, null, tempDir), "R1");
        assertRejectedBeforeWork(executor, executor.run(input("uploads/r1.fastq"), null, tempDir), "same input object");
        assertRejectedBeforeWork(executor, executor.run(input("uploads/r2.fasta"), null, tempDir), "FASTQ");
    }

    @ParameterizedTest
    @ValueSource(strings = {"threads", "timeout", "profile"})
    void rejectsInvalidRequestParametersBeforeWork(String field) throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        CovidWastewaterStageParameters parameters = new CovidWastewaterStageParameters();
        if ("threads".equals(field)) { parameters.setThreads(0); }
        if ("timeout".equals(field)) { parameters.setTimeoutSeconds(0); }
        if ("profile".equals(field)) { parameters.setAnalysisProfile("../not-a-profile"); }
        assertRejectedBeforeWork(executor, executor.run(input(null), parameters, tempDir), "Wastewater");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", ">not-fastq\nACGT\n", "@id\nACGT\n+\nI\n", "@id\nACGT\nwrong\nIIII\n"})
    void rejectsEmptyOrMalformedDownloadedInputBeforeAnyProcess(String content) throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        executor.fastq = content;
        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);
        assertFailure(result, "input preparation");
        assertTrue(executor.commands.isEmpty());
    }

    @Test
    void storageFailuresBecomeActionableStageFailures() throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        executor.failDownload = true;
        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);
        assertFailure(result, "input preparation");
        assertTrue(executor.commands.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"prepare", "variants", "demix"})
    void nonzeroProcessFailuresStopTheRemainingSteps(String failedStep) throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        executor.failedStep = failedStep;
        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);
        assertFailure(result, "code 9");
        assertTrue(result.getFailReason().contains("fixture stderr detail"));
        assertEquals(List.of("prepare", "variants", "demix").indexOf(failedStep) + 1, executor.commands.size());
    }

    @Test
    void failureRetainsABoundedLogTailForPersistenceAfterWorkDirectoryCleanup() throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        executor.failedStep = "prepare";
        executor.failureLog = "discarded-log-prefix\n" + "x".repeat(20000) + "\nretained-final-detail\n";

        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);

        assertFailure(result, "retained-final-detail");
        assertFalse(result.getFailReason().contains("discarded-log-prefix"));
        assertTrue(result.getFailReason().length() < 9000);
    }

    @ParameterizedTest
    @ValueSource(strings = {"aligned.bam", "aligned.bam.bai", "variants.tsv", "depths.tsv", "freyja.demix.tsv"})
    void zeroExitWithoutRequiredOutputFailsInsteadOfPublishing(String filename) throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        executor.omitted = Set.of(filename);
        StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);
        assertFailure(result, "missing, empty or not a regular file");
        assertNull(result.getStageOutput());
    }

    @Test
    void emptyOutputAndMalformedReportAreNotSuccessfulResults() throws Exception {
        FixtureExecutor empty = new FixtureExecutor(config());
        empty.empty = Set.of("aligned.bam");
        assertFailure(empty.run(input(null), null, tempDir), "missing, empty");
        FixtureExecutor malformed = new FixtureExecutor(config());
        malformed.report = "lineages\t[]\nabundances\t[]\nresid\t-1\ncoverage\t0\n";
        StageRunResult<CovidWastewaterStageOutput> result = malformed.run(input(null), null, tempDir);
        assertFailure(result, "result validation");
        assertTrue(result.getFailReason().contains("unavailable"));
        assertFalse(Files.exists(malformed.workDirs.get(2).resolve("covid-wastewater.metrics.json")));
    }

    @Test
    void interruptedAdapterRestoresTheThreadInterruptFlagAndReturnsFailure() throws Exception {
        FixtureExecutor executor = new FixtureExecutor(config());
        executor.interrupt = true;
        try {
            StageRunResult<CovidWastewaterStageOutput> result = executor.run(input(null), null, tempDir);
            assertFailure(result, "fixture interruption");
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, executor.commands.size());
        } finally {
            Thread.interrupted();
        }
    }

    private CovidWastewaterConfig config() throws IOException {
        CovidWastewaterConfig config = new CovidWastewaterConfig();
        config.setDefaultProfile(PROFILE);
        config.setFreyjaCommand(List.of("fixture-freyja"));
        config.setConfiguredFreyjaVersion("fixture-tool-v1");
        Profile profile = new Profile();
        profile.setVersion("profile-v1");
        profile.setDataVersion("fixture-data-v1");
        profile.setLibraryType("AMPLICON");
        profile.setPreparationCommand(List.of("fixture-prepare"));
        profile.setSupportedPlatforms(List.of(Constants.SequencingPlatform.ILLUMINA));
        Path resources = Files.createDirectories(tempDir.resolve("server-resources"));
        profile.setReferenceFasta(resource(resources, "reference.fixture"));
        profile.setBarcodes(resource(resources, "barcodes.fixture"));
        profile.setLineageHierarchy(resource(resources, "hierarchy.fixture"));
        profile.setLineageMetadata(resource(resources, "metadata.fixture"));
        config.setProfiles(Map.of(PROFILE, profile));
        return config;
    }

    private static String resource(Path directory, String name) throws IOException {
        return Files.writeString(directory.resolve(name), "synthetic-resource " + name + "\n").toString();
    }

    private static Profile profile(CovidWastewaterConfig config) { return config.getProfiles().get(PROFILE); }

    private static CovidWastewaterStageInputUrls input(String r2) {
        CovidWastewaterStageInputUrls input = new CovidWastewaterStageInputUrls();
        input.setR1Url("uploads/r1.fastq");
        input.setR2Url(r2);
        input.setSequencingPlatform(Constants.SequencingPlatform.ILLUMINA);
        return input;
    }

    private static void assertFailure(StageRunResult<?> result, String message) {
        assertFalse(result.isSuccess());
        assertNotNull(result.getFailReason());
        assertTrue(result.getFailReason().contains(message), result.getFailReason());
    }

    private static void assertRejectedBeforeWork(FixtureExecutor executor, StageRunResult<?> result, String message) {
        assertFailure(result, message);
        assertTrue(executor.loaded.isEmpty());
        assertTrue(executor.commands.isEmpty());
    }

    private static Map<String, Object> json(Path path) throws IOException {
        return map(JsonUtil.toObject(Files.readString(path), Map.class));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) { return (Map<String, Object>) value; }

    private static String hash(Path path) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    private static String option(List<String> command, String option) {
        assertTrue(command.contains(option), command.toString());
        return command.get(command.indexOf(option) + 1);
    }

    private static final class FixtureExecutor extends CovidWastewaterStageExecutor {
        private final Map<String, Path> loaded = new LinkedHashMap<>();
        private final List<List<String>> commands = new ArrayList<>();
        private final List<Path> workDirs = new ArrayList<>();
        private Set<String> omitted = Set.of();
        private Set<String> empty = Set.of();
        private String fastq = FASTQ;
        private String report = REPORT;
        private String failedStep;
        private String failureLog = "fixture stderr detail\n";
        private boolean failDownload;
        private boolean interrupt;
        private Runnable beforeFirstProcess;

        private FixtureExecutor(CovidWastewaterConfig config) {
            ReflectionTestUtils.setField(this, "covidWastewaterConfig", config);
        }

        private StageRunResult<CovidWastewaterStageOutput> run(CovidWastewaterStageInputUrls input,
                CovidWastewaterStageParameters parameters, Path root) throws IOException {
            StageExecutionInput execution = new StageExecutionInput();
            execution.input = input;
            execution.stageParameters = parameters;
            execution.stageContext = new StageContext(1L, 0, id(), 1);
            execution.inputDir = Files.createDirectories(root.resolve("input"));
            execution.workDir = Files.createDirectories(root.resolve("work"));
            return _execute(execution);
        }

        @Override
        protected void loadInput(Map<String, Path> paths) throws LoadFailException {
            if (failDownload) { throw new LoadFailException(new IOException("fixture storage unavailable")); }
            try {
                for (Map.Entry<String, Path> entry : paths.entrySet()) {
                    loaded.put(entry.getKey(), entry.getValue());
                    Files.writeString(entry.getValue(), fastq, StandardCharsets.UTF_8);
                }
            } catch (IOException e) { throw new LoadFailException(e); }
        }

        @Override
        protected void runProcess(List<String> command, Path workDir, Path log, long deadline)
                throws IOException, InterruptedException {
            commands.add(List.copyOf(command));
            workDirs.add(workDir);
            if (commands.size() == 1 && beforeFirstProcess != null) { beforeFirstProcess.run(); }
            if (interrupt) { throw new InterruptedException("fixture interruption"); }
            String step = "fixture-prepare".equals(command.get(0)) ? "prepare" : command.get(1);
            if (step.equals(failedStep)) {
                Files.writeString(log, failureLog, StandardOpenOption.APPEND);
                throw new IOException("fixture command exited with code 9");
            }
            switch (step) {
                case "prepare" -> {
                    Map<String, Object> request = json(Path.of(option(command, "--request-json")));
                    artifact(Path.of((String) request.get("bamOutput")), "fixture BAM marker\n");
                    artifact(Path.of((String) request.get("bamIndexOutput")), "fixture index marker\n");
                }
                case "variants" -> {
                    // Freyja accepts an output prefix and the external adapter adds .tsv.
                    artifact(workDir.resolve(option(command, "--variants") + ".tsv"), "fixture statistics\n");
                    artifact(workDir.resolve(option(command, "--depths")), "fixture depths\n");
                }
                case "demix" -> artifact(workDir.resolve(option(command, "--output")), report);
                default -> throw new AssertionError("Unexpected fixture command: " + command);
            }
        }

        private void artifact(Path path, String content) throws IOException {
            String name = path.getFileName().toString();
            if (!omitted.contains(name)) { Files.writeString(path, empty.contains(name) ? "" : content); }
        }
    }
}
