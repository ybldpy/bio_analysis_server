package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.taskrunner.AssemblyQualityTestSupport.sha256;
import static com.xjtlu.bio.analysisPipeline.taskrunner.AssemblyQualityTestSupport.writeJson;
import static com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.compare;
import static com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.readCalledAlleles;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.VarientCallInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.Accuracy;
import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.Allele;
import com.xjtlu.bio.analysisPipeline.taskrunner.VariantAccuracyTestSupport.FilterPolicy;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VariantStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.FaiBuilder;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/**
 * Real BAM -> production executor -> VCF tests on controlled, independently recorded truth.
 * Opt in with -Dvariant.bcftools=/path/to/bcftools -Dvariant.samtools=/path/to/samtools.
 * Each invocation retains inputs, commands, tool logs and reports in target/variant-accuracy.
 * Input alignments are known: mapper accuracy, biological representativeness and complex
 * variant equivalence are outside this small benchmark's scope.
 */
@EnabledIfSystemProperty(named = "variant.bcftools", matches = ".+")
@Timeout(value = 3, unit = TimeUnit.MINUTES)
class VariantAccuracyIntegrationTest {

    private static Path runRoot;
    private static String bcftools;
    private static String samtools;
    private static final Map<String, Object> suite = new LinkedHashMap<>();
    private static final Map<String, Object> cases = new LinkedHashMap<>();

    @BeforeAll
    static void setUp() throws Exception {
        bcftools = requiredProperty("variant.bcftools");
        samtools = requiredProperty("variant.samtools");
        Path parent = Files.createDirectories(Path.of("target", "variant-accuracy").toAbsolutePath());
        runRoot = Files.createTempDirectory(parent, "run-");
        suite.put("createdAt", Instant.now().toString());
        suite.put("scope", "Controlled single-sample haploid caller integration; known input alignments");
        suite.put("bcftoolsVersion", version(bcftools, "bcftools-version"));
        suite.put("samtoolsVersion", version(samtools, "samtools-version"));
        suite.put("comparison", "Exact normalized CHROM/POS/REF/ALT for actual GT alleles");
        suite.put("evaluationRegion", "Entire declared reference; no coverage-derived exclusion of truth");
        suite.put("undefinedRatios", "null means not applicable; negative controls do not receive artificial 100% recall");
        suite.put("falsePositiveRate", "NOT_ESTIMATED: no true-negative denominator; falseDiscoveryRate is reported separately");
        suite.put("limitations", List.of(
                "Small seeded alignment fixtures are not population-wide or clinical accuracy estimates.",
                "Mapping is not rerun: this isolates the existing BAM-to-VCF stage.",
                "Low-support truth remains in the recall denominator; expected misses are reported, not hidden.",
                "A same-tool standalone comparison verifies wrapper consistency, not independent algorithm accuracy.",
                "Complex haplotype equivalence, structural variants and mixed/diploid samples are not evaluated.",
                "The artificial case mix must not be pooled into a real-sample error-rate estimate."));
        suite.put("cases", cases);
        writeJson(runRoot.resolve("report.json"), suite);
        System.out.println("Variant accuracy reports: " + runRoot);
    }

    static Stream<String> benchmarkCases() {
        return VariantAccuracyFixture.CASES.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("benchmarkCases")
    void callsAndFiltersAgainstIndependentTruth(String caseName) throws Exception {
        Path root = Files.createDirectory(runRoot.resolve(caseName));
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("case", caseName);
        report.put("evaluationRegion", "Entire declared reference, including uncovered truth positions");
        try {
            VariantAccuracyFixture.Fixture fixture = VariantAccuracyFixture.create(
                    Files.createDirectory(root.resolve("fixture")), caseName);
            report.put("fixture", fixture.manifest());
            Map<String, String> originalHashes = Map.of("reference", sha256(fixture.reference()),
                    "sam", sha256(fixture.sam()), "truthVcf", sha256(fixture.truthVcf()));
            report.put("inputSha256", originalHashes);
            runProcess(List.of(samtools, "faidx", fixture.reference().toString()), root, "reference-index");
            Path normalizedTruthVcf = root.resolve("truth.normalized.vcf");
            normalize(fixture.truthVcf(), fixture.reference(), normalizedTruthVcf, root, "truth-normalize");
            Set<Allele> truth = readCalledAlleles(normalizedTruthVcf, FilterPolicy.PASS_ONLY);
            assertEquals(Set.copyOf(fixture.truth()), truth,
                    "Fixture events must already be unambiguous, left-normalized alleles");
            report.put("truthAlleles", ordered(truth));

            Path storageRoot = Files.createDirectory(root.resolve("storage"));
            Files.copy(fixture.reference(), storageRoot.resolve("reference.fasta"));
            Path bam = storageRoot.resolve("reads.bam");
            runProcess(List.of(samtools, "sort", "-@", "1", "-O", "BAM", "-o", bam.toString(),
                    fixture.sam().toString()), root, "bam-sort");
            runProcess(List.of(samtools, "index", bam.toString()), root, "bam-index");
            runProcess(List.of(samtools, "quickcheck", "-v", bam.toString()), root, "bam-quickcheck");
            String bamHash = sha256(bam);

            VarientCallParameters parameters = parameters();
            report.put("stageParameters", JsonUtil.toObject(JsonUtil.toJson(parameters), Map.class));
            RecordingExecutor executor = executor(root, storageRoot);
            VariantStageOutput output = execute(executor, parameters, 1L);
            Path normalizedRawVcf = root.resolve("raw.normalized.vcf");
            normalize(Path.of(output.getRawVcfGz()), fixture.reference(), normalizedRawVcf, root, "raw-normalize");
            Set<Allele> raw = readCalledAlleles(normalizedRawVcf, FilterPolicy.ALL);
            Set<Allele> accepted = readCalledAlleles(Path.of(output.getVcfGz()), FilterPolicy.PASS_ONLY);
            Accuracy acceptedAccuracy = compare(truth, accepted);
            report.put("rawCalledAlleles", ordered(raw));
            report.put("acceptedAlleles", ordered(accepted));
            report.put("rawAccuracy", compare(truth, raw));
            report.put("acceptedAccuracy", acceptedAccuracy);
            report.put("missedTruthAlleles", ordered(truth.stream().filter(a -> !accepted.contains(a)).toList()));
            report.put("falseCalledAlleles", ordered(accepted.stream().filter(a -> !truth.contains(a)).toList()));
            report.put("acceptedAccuracyByType", accuracyByType(truth, accepted));
            report.put("productionMetrics", output.getMetrics());
            report.put("rawVcf", output.getRawVcfGz());
            report.put("filteredVcf", output.getVcfGz());
            report.put("expectedCounts", Map.of("truePositives", fixture.expectedTp(),
                    "falsePositives", fixture.expectedFp(), "falseNegatives", fixture.expectedFn()));
            writeJson(root.resolve("report.json"), report);

            assertAll("Known truth for " + caseName + "; inspect " + root,
                    () -> assertEquals(fixture.expectedTp(), acceptedAccuracy.truePositives(), "TP"),
                    () -> assertEquals(fixture.expectedFp(), acceptedAccuracy.falsePositives(), "FP"),
                    () -> assertEquals(fixture.expectedFn(), acceptedAccuracy.falseNegatives(), "FN"));
            if (caseName.equals("low_depth")) {
                assertEquals(truth, raw, "Known low-depth SNP should be called before the acceptance filter");
                assertTrue(accepted.isEmpty(), "Low-depth truth must stay FN after failing the default filter");
            }
            assertEquals(accepted.size(), output.getMetrics().getExplicitPassCount(),
                    "Metrics must agree with actual accepted ALT genotypes in this haploid fixture");

            if (caseName.equals("high_support")) {
                VariantStageOutput repeated = execute(executor, parameters, 2L);
                assertEquals(accepted, readCalledAlleles(Path.of(repeated.getVcfGz()), FilterPolicy.PASS_ONLY));
                assertEquals(vcfRows(Path.of(output.getVcfGz())), vcfRows(Path.of(repeated.getVcfGz())),
                        "Repeated execution must preserve VCF data rows");
                report.put("repeatVcfDataRowsIdentical", true);
                Path standalone = standalone(root, fixture.reference(), bam);
                Set<Allele> standaloneAccepted = readCalledAlleles(standalone, FilterPolicy.PASS_ONLY);
                report.put("standaloneAcceptedAccuracy", compare(truth, standaloneAccepted));
                assertEquals(truth, standaloneAccepted);
                assertEquals(vcfRows(Path.of(output.getVcfGz())), vcfRows(standalone),
                        "Independent same-tool command must agree with the production executor");
                report.put("standaloneVcfDataRowsIdentical", true);
            }
            assertEquals(originalHashes.get("reference"), sha256(fixture.reference()));
            assertEquals(originalHashes.get("sam"), sha256(fixture.sam()));
            assertEquals(originalHashes.get("truthVcf"), sha256(fixture.truthVcf()));
            assertEquals(bamHash, sha256(bam), "Production staging must not alter source BAM");
            report.put("status", "PASS");
        } catch (Exception | AssertionError failure) {
            report.put("status", "FAIL");
            report.put("failure", failure.toString());
            throw failure;
        } finally {
            writeJson(root.resolve("report.json"), report);
            cases.put(caseName, Map.of("status", report.getOrDefault("status", "INCOMPLETE"),
                    "report", root.resolve("report.json").toString(),
                    "acceptedAccuracy", report.getOrDefault("acceptedAccuracy", Map.of())));
        }
    }

    @AfterAll
    static void report() throws IOException {
        if (runRoot != null) {
            suite.put("status", cases.size() == VariantAccuracyFixture.CASES.size()
                    && cases.values().stream().allMatch(c -> "PASS".equals(((Map<?, ?>) c).get("status")))
                    ? "PASS" : "FAIL_OR_INCOMPLETE");
            writeJson(runRoot.resolve("report.json"), suite);
            System.out.println("Variant accuracy summary: " + runRoot.resolve("report.json"));
        }
    }

    private static Map<String, Accuracy> accuracyByType(Set<Allele> truth, Set<Allele> calls) {
        Map<String, Accuracy> result = new LinkedHashMap<>();
        for (String type : List.of("SNP", "INSERTION", "DELETION")) {
            result.put(type, compare(truth.stream().filter(a -> type.equals(type(a))).toList(),
                    calls.stream().filter(a -> type.equals(type(a))).toList()));
        }
        return result;
    }

    private static String type(Allele allele) {
        return allele.ref().length() < allele.alt().length() ? "INSERTION"
                : allele.ref().length() > allele.alt().length() ? "DELETION" : "SNP";
    }

    private static List<Allele> ordered(java.util.Collection<Allele> values) {
        return values.stream().sorted(Comparator.comparing(Allele::chrom).thenComparingLong(Allele::pos)
                .thenComparing(Allele::ref).thenComparing(Allele::alt)).toList();
    }

    private static VarientCallParameters parameters() {
        VarientCallParameters parameters = new VarientCallParameters();
        parameters.setThreads(1);
        ReferenceSequence sequence = new ReferenceSequence();
        sequence.setAccession("TEXT_REFERENCE");
        sequence.setPath("reference.fasta");
        parameters.setReferenceGenome(ReferenceGenome.fromSingleSequence(sequence));
        return parameters;
    }

    private static RecordingExecutor executor(Path root, Path storageRoot) {
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        BioStageUtil directories = new BioStageUtil();
        ReflectionTestUtils.setField(directories, "stageWorkDirPath", root.resolve("work").toString());
        ReflectionTestUtils.setField(directories, "stageInputDirPath", root.resolve("input").toString());
        AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
        tools.setBcftools(List.of(bcftools));
        tools.setSamtools(List.of(samtools));
        RecordingExecutor executor = new RecordingExecutor();
        ReflectionTestUtils.setField(executor, "storageService", storage);
        ReflectionTestUtils.setField(executor, "bioStageUtil", directories);
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        ReflectionTestUtils.setField(executor, "faiBuilder", new FaiBuilder(tools));
        return executor;
    }

    private static VariantStageOutput execute(RecordingExecutor executor, VarientCallParameters parameters,
            long stageId) throws Exception {
        VarientCallInputUrls inputs = new VarientCallInputUrls();
        inputs.setBamUrl("reads.bam");
        inputs.setBamIndexUrl("reads.bam.bai");
        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(0L);
        stage.setStageId(stageId);
        stage.setVersion(0);
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_VARIANT_CALL);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);
        stage.setInputUrl(JsonUtil.toJson(inputs));
        stage.setParameters(JsonUtil.toJson(parameters));
        StageRunResult<VariantStageOutput> result = executor.execute(stage);
        assertNotNull(result);
        assertTrue(result.isSuccess(), result.getErrorLog());
        VariantStageOutput output = result.getStageOutput();
        assertNotNull(output);
        for (String path : List.of(output.getRawVcfGz(), output.getRawVcfTbi(), output.getVcfGz(),
                output.getVcfTbi(), output.getMetricsJson())) {
            assertTrue(Files.isRegularFile(Path.of(path)) && Files.size(Path.of(path)) > 0, path);
        }
        return output;
    }

    private static Path standalone(Path root, Path reference, Path bam) throws Exception {
        Path work = Files.createDirectory(root.resolve("standalone"));
        Path bcf = work.resolve("raw.bcf"), raw = work.resolve("raw.vcf.gz");
        Path normalized = work.resolve("normalized.vcf"), filtered = work.resolve("filtered.vcf.gz");
        // Deliberately written independently of the production expression/command builder.
        runProcess(List.of(bcftools, "mpileup", "-f", reference.toString(), "-q", "20", "-Q", "20",
                "-a", "DP,AD", "--threads", "1", "-Ou", "-o", bcf.toString(), bam.toString()), work, "pileup");
        runProcess(List.of(bcftools, "call", "-m", "--ploidy", "1", "--threads", "1", "-Oz",
                "-o", raw.toString(), bcf.toString()), work, "call");
        normalize(raw, reference, normalized, work, "normalize");
        runProcess(List.of(bcftools, "filter", "-m", "x", "-s", "LowConfidence", "-e",
                "GT!=\"alt\" || QUAL<20.0 || FORMAT/DP[0]<10 || FORMAT/AD[0:GT]<5 || FORMAT/AD[0:GT]/FORMAT/DP[0]<0.75",
                "--threads", "1", "-Oz", "-o", filtered.toString(), normalized.toString()), work, "filter");
        return filtered;
    }

    private static void normalize(Path source, Path reference, Path destination, Path root, String label)
            throws IOException, InterruptedException {
        runProcess(List.of(bcftools, "norm", "-f", reference.toString(), "-Ov", "-o",
                destination.toString(), source.toString()), root, label);
    }

    private static List<String> vcfRows(Path path) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(Files.newInputStream(path)), StandardCharsets.UTF_8))) {
            return reader.lines().filter(line -> !line.startsWith("#")).toList();
        }
    }

    private static String requiredProperty(String key) {
        String value = System.getProperty(key);
        assertNotNull(value, "Missing opt-in test property: " + key);
        assertFalse(value.isBlank(), "Empty test property: " + key);
        return value;
    }

    private static String version(String tool, String label) throws IOException, InterruptedException {
        runProcess(List.of(tool, "--version"), runRoot, label);
        return Files.readString(runRoot.resolve(label + ".log")).trim();
    }

    private static void runProcess(List<String> command, Path directory, String label)
            throws IOException, InterruptedException {
        writeJson(directory.resolve(label + ".command.json"), command);
        Path log = directory.resolve(label + ".log");
        Process process = new ProcessBuilder(command).directory(directory.toFile())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        Map<Long, ProcessHandle> descendants = new LinkedHashMap<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        try {
            do {
                process.descendants().forEach(child -> descendants.put(child.pid(), child));
                if (process.waitFor(100, TimeUnit.MILLISECONDS)) break;
            } while (System.nanoTime() < deadline);
            if (process.isAlive()) throw new IOException("Test process timeout; see " + log);
            if (process.exitValue() != 0) throw new IOException("Tool exit " + process.exitValue() + "; see " + log);
        } finally {
            process.descendants().forEach(child -> descendants.put(child.pid(), child));
            descendants.values().stream().filter(ProcessHandle::isAlive).forEach(ProcessHandle::destroyForcibly);
            if (process.isAlive()) process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
            long cleanupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (descendants.values().stream().anyMatch(ProcessHandle::isAlive)
                    && System.nanoTime() < cleanupDeadline) Thread.sleep(20);
            if (process.isAlive() || descendants.values().stream().anyMatch(ProcessHandle::isAlive)) {
                throw new IOException("Test process cleanup incomplete; see " + log);
            }
        }
    }

    private static final class RecordingExecutor extends VarientExecutor {
        private int commandNumber;

        @Override
        protected int runSubProcess(List<String> command, Path workDir) throws IOException, InterruptedException {
            runProcess(new ArrayList<>(command), workDir, String.format("tool-%02d", ++commandNumber));
            return 0;
        }
    }
}
