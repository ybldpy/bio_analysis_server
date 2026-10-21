package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.taskrunner.AssemblyQualityTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.AssemblyInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AssemblyStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/**
 * Explicit opt-in, isolated real-tool tests; no Spring/server/database.
 * -Dassembly.spades=/path/to/spades.py -Dassembly.fixture=/path/to/test_dataset
 * -Dassembly.quast=/path/to/quast.py [-Dassembly.python=python3]
 * Reports and process logs are retained in fresh target/assembly-quality directories.
 * The bundled 1kb fixture is an installation smoke benchmark, not a clinical validation set.
 */
@Timeout(value = 15, unit = TimeUnit.MINUTES)
class AssemblyQualityIntegrationTest {

    @Test
    @EnabledIfSystemProperty(named = "assembly.spades", matches = ".+")
    void bundledBenchmarkMatchesTruthStandaloneAndRepeatedExecution() throws Exception {
        Path root = reportDirectory("bundled-");
        Path fixture = Path.of(requiredProperty("assembly.fixture")).toAbsolutePath();
        Path r1 = fixture.resolve("ecoli_1K_1.fq.gz");
        Path r2 = fixture.resolve("ecoli_1K_2.fq.gz");
        Path reference = fixture.resolve("reference_1K.fa.gz");
        String spades = requiredProperty("assembly.spades");
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("scope", "SPAdes bundled 1kb smoke fixture; not a representative accuracy estimate");
        report.put("fixtureSource", "https://ablab.github.io/spades/installation.html#verifying-your-installation");
        report.put("createdAt", Instant.now().toString());
        report.put("fixtureSha256", Map.of("r1", sha256(r1), "r2", sha256(r2), "reference", sha256(reference)));
        report.put("spadesVersion", toolVersion(List.of(spades, "--version"), root, "spades-version"));
        report.put("referenceMetrics", fastaMetrics(reference));
        report.put("limitations", List.of("A 1kb fixture cannot validate complex repeats or large structural errors.",
                "Repeated and standalone execution check wrapper consistency, not independent algorithm accuracy.",
                "No thresholds here are production acceptance rules for arbitrary samples."));
        writeJson(root.resolve("manifest.json"), report);
        assertEquals(1000L, fastaMetrics(reference).get("totalBases"), "Unexpected bundled fixture");

        Path storageRoot = Files.createDirectories(root.resolve("storage"));
        Files.copy(r1, storageRoot.resolve(r1.getFileName()));
        Files.copy(r2, storageRoot.resolve(r2.getFileName()));
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", root.resolve("work").toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", root.resolve("input").toString());
        AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
        tools.setSpades(List.of(spades));
        TimedAssemblyExecutor executor = new TimedAssemblyExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        ReflectionTestUtils.setField(executor, "contigsFileName", "contigs.fasta");
        ReflectionTestUtils.setField(executor, "scaffoldsFileName", "scaffolds.fasta");

        Map<String, Path> assemblies = new LinkedHashMap<>();
        for (int run = 1; run <= 3; run++) {
            boolean paired = run != 3;
            StageRunResult<AssemblyStageOutput> result = executor.execute(stage(run,
                    r1.getFileName().toString(), paired ? r2.getFileName().toString() : null));
            assertTrue(result.isSuccess(), () -> "Assembly failed; logs retained in " + root + ": " + result.getErrorLog());
            String label = paired ? "paired" + run : "single";
            assemblies.put(label + "_contigs", Path.of(result.getStageOutput().getContigPath()));
            assertNotNull(result.getStageOutput().getScaffoldPath(), "Bundled fixture should generate scaffolds");
            assemblies.put(label + "_scaffolds", Path.of(result.getStageOutput().getScaffoldPath()));
        }

        // Deliberately constructed independently of the executor's command builder.
        Path standalone = Files.createDirectories(root.resolve("standalone"));
        runProcess(List.of(spades, "-t", "3", "-1", r1.toString(), "-2", r2.toString(),
                "--phred-offset", "33", "-o", standalone.toString()), root, "standalone");
        assemblies.put("standalone_contigs", standalone.resolve("contigs.fasta"));
        assemblies.put("standalone_scaffolds", standalone.resolve("scaffolds.fasta"));

        Map<String, Object> metrics = new LinkedHashMap<>();
        for (Map.Entry<String, Path> entry : assemblies.entrySet()) {
            metrics.put(entry.getKey(), fastaMetrics(entry.getValue()));
        }
        report.put("artifacts", metrics);
        if (System.getProperty("assembly.quast") != null) {
            report.put("quast", evaluate(root, reference, assemblies));
        } else {
            report.put("quast", "NOT_RUN: assembly.quast not supplied; exact-reference comparison still runs");
        }
        writeJson(root.resolve("report.json"), report);
        for (Map.Entry<String, Path> entry : assemblies.entrySet()) {
            assertEquals(canonicalSequences(reference), canonicalSequences(entry.getValue()),
                    entry.getKey() + " does not exactly recover the 1kb fixture; inspect " + root);
        }
        assertEquals(sha256(r1), sha256(storageRoot.resolve(r1.getFileName())));
        assertEquals(sha256(r2), sha256(storageRoot.resolve(r2.getFileName())));
        if (report.get("quast") instanceof Map<?, ?>) {
            Map<String, Map<String, String>> quast = readQuastReport(root.resolve("quast/report.tsv"));
            for (String label : assemblies.keySet()) {
                assertEquals(100.0, number(quast, label, "Genome fraction (%)"), 0.001);
                assertEquals(0.0, number(quast, label, "# misassemblies"));
                assertEquals(0.0, number(quast, label, "# mismatches per 100 kbp"));
                assertEquals(0.0, number(quast, label, "# indels per 100 kbp"));
            }
        }
        report.put("status", "PASS");
        writeJson(root.resolve("report.json"), report);
        System.out.println("Assembly bundled benchmark report: " + root.resolve("report.json"));
    }

    @Test
    @EnabledIfSystemProperty(named = "assembly.quast", matches = ".+")
    void independentEvaluatorRecognizesKnownSoftwareErrorControls() throws Exception {
        Path root = reportDirectory("controls-");
        // Seeded random text, not a biological reference or a designed functional sequence.
        Random random = new Random(20261016L);
        String a = randomDna(random, 8000), b = randomDna(random, 8000);
        Path reference = fasta(root, "truth", Map.of("textA", a, "textB", b));
        Map<String, Path> controls = new LinkedHashMap<>();
        controls.put("perfect", fasta(root, "perfect", Map.of("a", a, "b", b)));
        controls.put("missing", fasta(root, "missing", Map.of("a", a.substring(0, 4000), "b", b)));
        char alternative = a.charAt(4000) == 'A' ? 'C' : 'A';
        controls.put("mismatch", fasta(root, "mismatch", Map.of("a",
                a.substring(0, 4000) + alternative + a.substring(4001), "b", b)));
        controls.put("indel", fasta(root, "indel", Map.of("a", a.substring(0, 4000) + a.substring(4001), "b", b)));
        controls.put("wrong_join", fasta(root, "wrong_join", Map.of("joined", a + b)));
        controls.put("duplicate", fasta(root, "duplicate", Map.of("a", a, "a_copy", a, "b", b)));
        controls.put("fragmented", fasta(root, "fragmented", Map.of("a1", a.substring(0, 4000),
                "a2", a.substring(4000), "b1", b.substring(0, 4000), "b2", b.substring(4000))));
        controls.put("gapped", fasta(root, "gapped", Map.of("a",
                a.substring(0, 3500) + "N".repeat(1000) + a.substring(4500), "b", b)));
        Map<String, Map<String, String>> quast = evaluate(root, reference, controls);
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("scope", "Evaluator/parser sensitivity controls on seeded random text, not assembler performance");
        report.put("randomSeed", 20261016L);
        report.put("truthSha256", sha256(reference));
        report.put("metrics", quast);
        report.put("limitations", "Known edits test the measurement layer; no biological accuracy claim.");
        writeJson(root.resolve("report.json"), report);

        assertEquals(100.0, number(quast, "perfect", "Genome fraction (%)"), 0.001);
        assertEquals(0.0, number(quast, "perfect", "# misassemblies"));
        assertEquals(0.0, number(quast, "perfect", "# mismatches per 100 kbp"));
        assertEquals(0.0, number(quast, "perfect", "# indels per 100 kbp"));
        assertEquals(75.0, number(quast, "missing", "Genome fraction (%)"), 0.01);
        assertTrue(number(quast, "mismatch", "# mismatches per 100 kbp") > 0);
        assertTrue(number(quast, "indel", "# indels per 100 kbp") > 0);
        assertTrue(number(quast, "wrong_join", "# misassemblies") > 0);
        assertTrue(number(quast, "duplicate", "Duplication ratio") > 1.0);
        assertEquals(100.0, number(quast, "fragmented", "Genome fraction (%)"), 0.001);
        assertTrue(number(quast, "fragmented", "N50") < number(quast, "perfect", "N50"));
        assertTrue(number(quast, "gapped", "# N's per 100 kbp") > 0);
        report.put("status", "PASS");
        writeJson(root.resolve("report.json"), report);
        System.out.println("Assembly evaluator control report: " + root.resolve("report.json"));
    }

    private static Map<String, Map<String, String>> evaluate(Path root, Path reference,
            Map<String, Path> assemblies) throws Exception {
        List<String> prefix = List.of(System.getProperty("assembly.python", "python3"), requiredProperty("assembly.quast"));
        String version = toolVersion(List.of(prefix.get(0), prefix.get(1), "--version"), root, "quast-version");
        List<String> command = new ArrayList<>(prefix);
        assemblies.values().forEach(path -> command.add(path.toString()));
        command.addAll(List.of("-r", reference.toString(), "-o", root.resolve("quast").toString(),
                "--labels", String.join(",", assemblies.keySet()), "--threads", "1", "--no-plots", "--no-html", "--no-icarus"));
        runProcess(command, root, "quast");
        Map<String, Map<String, String>> report = readQuastReport(root.resolve("quast/report.tsv"));
        assertEquals(assemblies.keySet(), report.keySet(), "Missing assembly columns in QUAST output");
        writeJson(root.resolve("quast-provenance.json"), Map.of("version", version,
                "executableSha256", sha256(Path.of(prefix.get(1))), "command", command));
        return report;
    }

    private static double number(Map<String, Map<String, String>> report, String label, String metric) {
        assertNotNull(report.get(label), "Missing QUAST assembly: " + label);
        String value = report.get(label).get(metric);
        assertNotNull(value, "Missing QUAST metric: " + metric);
        assertFalse(value.isBlank() || value.equals("-"), "Unavailable metric must not be treated as zero: " + metric);
        return Double.parseDouble(value);
    }

    private static BioPipelineStage stage(long id, String r1, String r2) throws Exception {
        AssemblyInputUrls input = new AssemblyInputUrls();
        input.setRead1Url(r1);
        input.setRead2Url(r2);
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(0L);
        stage.setStageId(id);
        stage.setVersion(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_ASSEMBLY);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(input));
        stage.setParameters(JsonUtil.toJson(new BaseStageParams()));
        return stage;
    }

    private static Path reportDirectory(String prefix) throws IOException {
        Path parent = Files.createDirectories(Path.of("target", "assembly-quality").toAbsolutePath());
        Path root = Files.createTempDirectory(parent, prefix);
        System.out.println("Assembly quality artifacts: " + root);
        return root;
    }

    private static String requiredProperty(String name) {
        String value = System.getProperty(name);
        assertNotNull(value, "Missing test property: " + name);
        assertFalse(value.isBlank(), "Empty test property: " + name);
        return value;
    }

    private static String toolVersion(List<String> command, Path root, String label) throws Exception {
        runProcess(command, root, label);
        return Files.readString(root.resolve(label + ".log")).trim();
    }

    private static void runProcess(List<String> command, Path directory, String label) throws IOException, InterruptedException {
        Files.createDirectories(directory);
        writeJson(directory.resolve(label + ".command.json"), command);
        Path log = directory.resolve(label + ".log");
        Process process = new ProcessBuilder(command).directory(directory.toFile())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        Map<Long, ProcessHandle> descendants = new LinkedHashMap<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(180);
        try {
            do {
                process.descendants().forEach(child -> descendants.put(child.pid(), child));
                if (process.waitFor(200, TimeUnit.MILLISECONDS)) break;
            } while (System.nanoTime() < deadline);
            if (process.isAlive()) {
                throw new IOException("Test-only 180s process timeout; see " + log);
            }
            if (process.exitValue() != 0) {
                throw new IOException("Tool exit=" + process.exitValue() + "; see " + log);
            }
        } finally {
            // Retain known child handles even if the launcher has already exited.
            process.descendants().forEach(child -> descendants.put(child.pid(), child));
            descendants.values().stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
            if (process.isAlive()) {
                process.destroyForcibly();
            }
            process.waitFor(5, TimeUnit.SECONDS);
            long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (descendants.values().stream().anyMatch(ProcessHandle::isAlive)
                    && System.nanoTime() < cleanupDeadline) Thread.sleep(20);
            if (process.isAlive() || descendants.values().stream().anyMatch(ProcessHandle::isAlive)) {
                throw new IOException("Test subprocess cleanup incomplete; see " + log);
            }
        }
    }

    private static Path fasta(Path root, String name, Map<String, String> records) throws IOException {
        StringBuilder content = new StringBuilder();
        records.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry ->
                content.append('>').append(entry.getKey()).append('\n').append(entry.getValue()).append('\n'));
        Path path = root.resolve(name + ".fasta");
        Files.writeString(path, content);
        return path;
    }

    private static String randomDna(Random random, int length) {
        StringBuilder text = new StringBuilder(length);
        for (int i = 0; i < length; i++) text.append("ACGT".charAt(random.nextInt(4)));
        return text.toString();
    }

    private static final class TimedAssemblyExecutor extends AssemblyExecutor {
        @Override
        protected int runSubProcess(List<String> command, Path workDir) throws IOException, InterruptedException {
            runProcess(command, workDir, "tool");
            return 0;
        }
    }
}
