package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.CovidWastewaterStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.CovidWastewaterStageParameters;
import com.xjtlu.bio.analysisPipeline.stageMetrics.CovidWastewaterResultMetrics;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.CovidWastewaterStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FreyjaReportParser;
import com.xjtlu.bio.configuration.CovidWastewaterConfig;
import com.xjtlu.bio.configuration.CovidWastewaterConfig.Profile;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

/**
 * One public stage; preprocessing is a server-approved, versioned external adapter.
 * A Freyja installation alone does NOT provide the FASTQ-to-prepared-BAM adapter.
 * No ordinary-virus selection/assembly/consensus workflow is invoked here.
 */
@Component
public class CovidWastewaterStageExecutor extends AbstractPipelineStageExector<
        CovidWastewaterStageOutput, CovidWastewaterStageInputUrls, CovidWastewaterStageParameters> {

    @Resource
    private CovidWastewaterConfig covidWastewaterConfig;

    @Override
    public int id() {
        return Constants.StageType.PIPELINE_STAGE_COVID_WASTEWATER;
    }

    @Override
    protected Class<CovidWastewaterStageInputUrls> stageInputType() {
        return CovidWastewaterStageInputUrls.class;
    }

    @Override
    protected Class<CovidWastewaterStageParameters> stageParameterType() {
        return CovidWastewaterStageParameters.class;
    }

    @Override
    protected StageRunResult<CovidWastewaterStageOutput> _execute(StageExecutionInput execution) {
        String phase = "configuration";
        Path log = null;
        try {
            CovidWastewaterStageParameters parameters = execution.stageParameters == null
                    ? new CovidWastewaterStageParameters() : execution.stageParameters;
            parameters.validate();
            validateInput(execution.input);

            // Copy the server configuration before any external work. Request JSON cannot
            // supply a command, a server filesystem path, or an unregistered profile.
            CovidWastewaterConfig config = JsonUtil.toObject(
                    JsonUtil.toJson(covidWastewaterConfig), CovidWastewaterConfig.class);
            if (config == null) {
                throw new IllegalArgumentException("Wastewater server configuration is missing");
            }
            String profileId = hasText(parameters.getAnalysisProfile())
                    ? parameters.getAnalysisProfile() : config.getDefaultProfile();
            Profile profile = requireProfile(config, profileId, execution.input.getSequencingPlatform());
            List<String> freyja = requireCommand(config.getFreyjaCommand(), "freyja-command");
            List<String> prepare = requireCommand(profile.getPreparationCommand(), "preparation-command");
            requireText(config.getConfiguredFreyjaVersion(), "configured-freyja-version");

            Map<String, Path> sourceResources = new LinkedHashMap<>();
            sourceResources.put("reference", requireResource(profile.getReferenceFasta(), "reference-fasta"));
            sourceResources.put("barcodes", requireResource(profile.getBarcodes(), "barcodes"));
            sourceResources.put("hierarchy", requireResource(profile.getLineageHierarchy(), "lineage-hierarchy"));
            sourceResources.put("metadata", requireResource(profile.getLineageMetadata(), "lineage-metadata"));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(parameters.getTimeoutSeconds());
            Path workDir = execution.workDir.toAbsolutePath().normalize();
            Files.createDirectories(workDir);
            Path attempt = Files.createTempDirectory(workDir, "wastewater-");
            log = attempt.resolve("execution.log");
            appendLog(log, "START profile=" + profileId + " version=" + profile.getVersion());

            phase = "input preparation";
            checkExecutionBudget(deadline);
            Path r1 = downloadFastq(execution.input.getR1Url(), execution.inputDir.resolve("r1"));
            checkExecutionBudget(deadline);
            Path r2 = hasText(execution.input.getR2Url())
                    ? downloadFastq(execution.input.getR2Url(), execution.inputDir.resolve("r2")) : null;
            Path resourceDir = Files.createDirectories(attempt.resolve("resources"));
            Map<String, Path> resources = new LinkedHashMap<>();
            Map<String, Object> resourceProvenance = new LinkedHashMap<>();
            for (Map.Entry<String, Path> entry : sourceResources.entrySet()) {
                checkExecutionBudget(deadline);
                String filename = "reference".equals(entry.getKey()) ? "reference.fasta"
                        : entry.getKey() + "-" + entry.getValue().getFileName();
                Path copy = resourceDir.resolve(filename);
                Files.copy(entry.getValue(), copy);
                resources.put(entry.getKey(), copy);
                resourceProvenance.put(entry.getKey(), fingerprint(copy));
            }

            Path bam = attempt.resolve("aligned.bam");
            Path bamIndex = attempt.resolve("aligned.bam.bai");
            Map<String, Object> request = new LinkedHashMap<>();
            request.put("schemaVersion", 1);
            request.put("profileId", profileId);
            request.put("profileVersion", profile.getVersion());
            request.put("libraryType", profile.getLibraryType());
            request.put("r1", r1.toAbsolutePath().toString());
            request.put("r2", r2 == null ? null : r2.toAbsolutePath().toString());
            request.put("sequencingPlatform", platform(execution.input.getSequencingPlatform()));
            request.put("referenceFasta", resources.get("reference").toAbsolutePath().toString());
            request.put("referenceSha256", sha256(resources.get("reference")));
            request.put("bamOutput", bam.toAbsolutePath().toString());
            request.put("bamIndexOutput", bamIndex.toAbsolutePath().toString());
            request.put("threads", parameters.getThreads());
            Path requestFile = attempt.resolve("preparation-request.json");
            Files.writeString(requestFile, JsonUtil.toJson(request), StandardCharsets.UTF_8);

            phase = "prepared BAM";
            runStep(phase, command(prepare, "--request-json", requestFile.toAbsolutePath().toString()),
                    attempt, log, deadline);
            requireArtifact(bam, "prepared BAM");
            requireArtifact(bamIndex, "prepared BAM index");

            phase = "Freyja variant statistics";
            Path variants = attempt.resolve("variants.tsv");
            Path depths = attempt.resolve("depths.tsv");
            // Freyja builds an internal shell command: only fixed, shell-safe relative
            // names belong here. iVar receives an output PREFIX and appends .tsv.
            runStep(phase, command(freyja, "variants", "aligned.bam",
                    "--ref", "resources/reference.fasta",
                    "--variants", "variants", "--depths", "depths.tsv"),
                    attempt, log, deadline);
            requireArtifact(variants, "variant statistics");
            requireArtifact(depths, "depth statistics");

            phase = "Freyja mixture analysis";
            Path report = attempt.resolve("freyja.demix.tsv");
            runStep(phase, command(freyja, "demix", variants.toString(), depths.toString(),
                    "--output", report.toString(),
                    "--barcodes", resources.get("barcodes").toString(),
                    "--lineageyml", resources.get("hierarchy").toString(),
                    "--meta", resources.get("metadata").toString(),
                    "--max-solver-threads", Integer.toString(parameters.getThreads())),
                    attempt, log, deadline);
            requireArtifact(report, "Freyja report");

            phase = "result validation";
            checkExecutionBudget(deadline);
            CovidWastewaterResultMetrics metrics = FreyjaReportParser.parse(report);
            Path metricsPath = attempt.resolve("covid-wastewater.metrics.json");
            Files.writeString(metricsPath, JsonUtil.toJson(metrics), StandardCharsets.UTF_8);
            Map<String, Object> provenance = new LinkedHashMap<>();
            provenance.put("schemaVersion", 1);
            provenance.put("completedAt", Instant.now().toString());
            provenance.put("profileId", profileId);
            provenance.put("profileVersion", profile.getVersion());
            provenance.put("libraryType", profile.getLibraryType());
            provenance.put("libraryTypeSource", "declared-analysis-profile-not-inferred-from-fastq");
            provenance.put("configuredFreyjaVersion", config.getConfiguredFreyjaVersion());
            provenance.put("toolVersionSource", "server-configuration-not-runtime-verified");
            provenance.put("dataVersion", profile.getDataVersion());
            provenance.put("parameters", parameters);
            provenance.put("sequencingPlatform", platform(execution.input.getSequencingPlatform()));
            provenance.put("inputR1", fingerprint(r1));
            provenance.put("inputR2", r2 == null ? null : fingerprint(r2));
            provenance.put("resources", resourceProvenance);
            provenance.put("report", fingerprint(report));
            Path provenancePath = attempt.resolve("provenance.json");
            Files.writeString(provenancePath, JsonUtil.toJson(provenance), StandardCharsets.UTF_8);
            checkExecutionBudget(deadline);
            appendLog(log, "FINISHED; report validation passed");

            CovidWastewaterStageOutput output = new CovidWastewaterStageOutput();
            output.setReportPath(report);
            output.setMetricsPath(metricsPath);
            output.setProvenancePath(provenancePath);
            output.setExecutionLogPath(log);
            output.setBamPath(bam);
            output.setBamIndexPath(bamIndex);
            return OK(output, execution);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            String message = "Wastewater stage failed at [" + phase + "]: "
                    + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            // The ordinary failure handler removes workDir. Persist a bounded tail
            // with failMessage so the diagnostic survives that cleanup.
            if (log != null && !Thread.currentThread().isInterrupted()) {
                try {
                    String tail = logTail(log);
                    if (!tail.isBlank()) { message += "\nExecution log tail:\n" + tail; }
                } catch (IOException ignored) { /* retain the original cause */ }
            }
            if (log != null && !Thread.currentThread().isInterrupted()) {
                try { appendLog(log, message); } catch (IOException ignored) { /* retain the original cause */ }
            }
            logger.error(message, e);
            return runFail(execution.stageContext, execution.workDir, message, e);
        }
    }

    private Profile requireProfile(CovidWastewaterConfig config, String profileId, Integer declaredPlatform) {
        if (!hasText(profileId) || config.getProfiles() == null || !config.getProfiles().containsKey(profileId)) {
            throw new IllegalArgumentException("新冠污水扩增子分析方案未配置：请配置 "
                    + "analysis-pipeline.covid-wastewater.default-profile/profiles；不能自动猜测前处理方案");
        }
        Profile profile = config.getProfiles().get(profileId);
        if (profile == null || !"AMPLICON".equals(profile.getLibraryType())) {
            throw new IllegalArgumentException("Wastewater profile must explicitly declare library-type AMPLICON");
        }
        requireText(profile.getVersion(), "profile version");
        requireText(profile.getDataVersion(), "data-version");
        if (profile.getSupportedPlatforms() == null
                || !profile.getSupportedPlatforms().contains(platform(declaredPlatform))) {
            throw new IllegalArgumentException("Sequencing platform " + platform(declaredPlatform)
                    + " is not supported by wastewater profile " + profileId
                    + "; UNKNOWN (0) must be explicitly supported by the configured profile");
        }
        return profile;
    }

    private void validateInput(CovidWastewaterStageInputUrls input) {
        if (input == null || !hasText(input.getR1Url())) {
            throw new IllegalArgumentException("请上传新冠扩增子测序 FASTQ；R1 不能为空，双端数据分别放入 R1、R2");
        }
        validateFastqName(input.getR1Url());
        if (hasText(input.getR2Url())) {
            validateFastqName(input.getR2Url());
            if (input.getR1Url().equals(input.getR2Url())) {
                throw new IllegalArgumentException("R1 and R2 must not refer to the same input object");
            }
        }
    }

    private static void validateFastqName(String name) {
        String uncompressed = Constants.CompressedFormat.removeCompressedSuffix(name).toLowerCase(Locale.ROOT);
        if (!uncompressed.endsWith(".fastq") && !uncompressed.endsWith(".fq")) {
            throw new IllegalArgumentException("Wastewater input must be amplicon FASTQ (.fastq/.fq, optionally compressed)");
        }
    }

    private Path downloadFastq(String key, Path directory) throws Exception {
        Files.createDirectories(directory);
        Path local = directory.resolve(Path.of(key).getFileName().toString()).toAbsolutePath();
        loadInput(Map.of(key, local));
        requireArtifact(local, "input FASTQ");
        Path unpacked = uncompressIfCompressedFormat(local);
        requireArtifact(unpacked, "uncompressed FASTQ");
        // A cheap file-type check, not a claim of whole-file validation or library detection.
        try (BufferedReader reader = Files.newBufferedReader(unpacked, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            String sequence = reader.readLine();
            String separator = reader.readLine();
            String quality = reader.readLine();
            if (header == null || !header.startsWith("@") || sequence == null || sequence.isEmpty()
                    || separator == null || !separator.startsWith("+")
                    || quality == null || sequence.length() != quality.length()) {
                throw new IOException("Input is empty or does not start with a valid FASTQ record: " + key);
            }
        }
        return unpacked;
    }

    private void runStep(String phase, List<String> command, Path workDir, Path log, long deadline) throws Exception {
        appendLog(log, "START " + phase);
        logger.info("Wastewater internal step: {}", phase);
        runProcess(command, workDir, log, deadline);
        appendLog(log, "DONE " + phase);
    }

    /** Process-only adapter. No shell interpolation; stdout/stderr go directly to a log file. */
    protected void runProcess(List<String> command, Path workDir, Path log, long deadlineNanos)
            throws IOException, InterruptedException {
        checkExecutionBudget(deadlineNanos);
        Process process = new ProcessBuilder(command)
                .directory(workDir.toFile())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                .start();
        boolean completed = false;
        Set<ProcessHandle> children = new LinkedHashSet<>();
        try {
            process.getOutputStream().close();
            while (!completed) {
                process.descendants().forEach(children::add);
                checkExecutionBudget(deadlineNanos);
                long remaining = deadlineNanos - System.nanoTime();
                completed = process.waitFor(Math.max(1L,
                        Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(100))), TimeUnit.NANOSECONDS);
            }
            if (process.exitValue() != 0) {
                throw new IOException("Wastewater command exited with code " + process.exitValue());
            }
        } finally {
            process.descendants().forEach(children::add);
            terminateProcessTree(process, children);
        }
    }

    private static void terminateProcessTree(Process process, Set<ProcessHandle> children) {
        boolean interrupted = Thread.interrupted();
        try {
            for (ProcessHandle child : children) {
                if (child.isAlive()) { child.destroyForcibly(); }
            }
            if (process.isAlive()) { process.destroyForcibly(); }
            // Bounded cleanup before the superclass deletes input files. Adapters
            // must remain foreground processes, not daemonize or detach children.
            process.waitFor(2, TimeUnit.SECONDS);
            CompletableFuture<?>[] exits = children.stream().filter(ProcessHandle::isAlive)
                    .map(ProcessHandle::onExit).toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(exits).get(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            interrupted = true;
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException ignored) {
            // The termination signals were already sent; cleanup must stay bounded.
        } finally {
            if (interrupted) { Thread.currentThread().interrupt(); }
        }
    }

    private static void checkExecutionBudget(long deadlineNanos) throws IOException, InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Wastewater execution interrupted");
        }
        if (deadlineNanos - System.nanoTime() <= 0) {
            throw new IOException("Wastewater execution timed out");
        }
    }

    private static String logTail(Path log) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(log, StandardOpenOption.READ)) {
            long size = channel.size();
            channel.position(Math.max(0, size - 8192));
            ByteBuffer buffer = ByteBuffer.allocate((int) Math.min(size, 8192));
            while (buffer.hasRemaining() && channel.read(buffer) != -1) { /* bounded read */ }
            buffer.flip();
            return StandardCharsets.UTF_8.decode(buffer).toString();
        }
    }

    private static List<String> command(List<String> prefix, String... arguments) {
        List<String> result = new ArrayList<>(prefix);
        result.addAll(List.of(arguments));
        return result;
    }

    private static List<String> requireCommand(List<String> command, String name) {
        if (command == null || command.isEmpty()
                || command.stream().anyMatch(s -> !hasText(s) || s.indexOf('\0') >= 0)) {
            throw new IllegalArgumentException("Wastewater " + name + " is not configured as an argument list");
        }
        return List.copyOf(command);
    }

    private static Path requireResource(String value, String name) throws IOException {
        requireText(value, name);
        Path path = Path.of(value);
        if (!path.isAbsolute()) {
            throw new IllegalArgumentException("Wastewater " + name + " must be an absolute server path");
        }
        if (!Files.isRegularFile(path) || !Files.isReadable(path) || Files.size(path) == 0) {
            throw new IOException("Wastewater " + name + " is missing, empty or unreadable: " + path);
        }
        return path;
    }

    private static void requireArtifact(Path path, String name) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) == 0) {
            throw new IOException("Required " + name + " output is missing, empty or not a regular file: " + path);
        }
    }

    private static void requireText(String value, String name) {
        if (!hasText(value)) { throw new IllegalArgumentException("Wastewater " + name + " is not configured"); }
    }

    private static boolean hasText(String value) { return value != null && !value.isBlank(); }
    private static int platform(Integer value) { return value == null ? Constants.SequencingPlatform.UNKNOWN : value; }

    private static void appendLog(Path log, String message) throws IOException {
        Files.writeString(log, Instant.now() + " " + message + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static Map<String, Object> fingerprint(Path file) throws Exception {
        return Map.of("fileName", file.getFileName().toString(), "sizeBytes", Files.size(file), "sha256", sha256(file));
    }

    private static String sha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(file)) {
            byte[] buffer = new byte[65536];
            for (int n; (n = in.read(buffer)) != -1;) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Wastewater fingerprint interrupted");
                }
                digest.update(buffer, 0, n);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
