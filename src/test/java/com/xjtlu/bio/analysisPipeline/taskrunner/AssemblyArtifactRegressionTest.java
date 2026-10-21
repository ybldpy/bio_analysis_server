package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Compares two recorded assembly outputs; never invokes an assembler or other external tool.
 * Enable with -Dassembly.regression.manifest=/path/to/manifest.json. Relative artifact paths
 * are resolved against the manifest directory. This is consistency evidence, not a fresh
 * assembly run or validation against an independently known correct sequence.
 */
@EnabledIfSystemProperty(named = "assembly.regression.manifest", matches = ".+")
class AssemblyArtifactRegressionTest {

    private static final Set<String> UNKNOWN_VALUES = Set.of("unknown", "unspecified", "null", "n/a", "-");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void recordedArtifactsRemainEquivalentForIdenticalInputsAndConfiguration() throws Exception {
        Path manifestPath = Path.of(System.getProperty("assembly.regression.manifest"))
                .toAbsolutePath().normalize();
        requireFile(manifestPath, "manifest");
        JsonNode manifest = JSON.readTree(manifestPath.toFile());
        if (manifest == null || !manifest.isObject()) {
            throw new IllegalArgumentException("Regression manifest must be a JSON object");
        }
        ArtifactSet baseline = artifactSet(manifest, "baseline", manifestPath.getParent());
        ArtifactSet current = artifactSet(manifest, "current", manifestPath.getParent());

        Path reportRoot = Files.createDirectories(Path.of("target", "assembly-quality"));
        Path reportDirectory = Files.createTempDirectory(reportRoot, "recorded-");
        Path reportPath = reportDirectory.resolve("comparison.json");
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("verificationType", "historical-artifact-consistency");
        report.put("assemblerExecuted", false);
        report.put("newAssemblyRun", false);
        report.put("independentSequenceTruthValidated", false);
        report.put("limitation", "Matching recorded outputs does not establish biological correctness "
                + "or guarantee that a newly executed assembly will match.");
        report.put("createdAt", Instant.now().toString());
        report.put("manifest", manifestPath.toString());
        report.put("baseline", describe(baseline));
        report.put("current", describe(current));

        String baselineR1 = decompressedSha256(baseline.r1());
        String baselineR2 = decompressedSha256(baseline.r2());
        String currentR1 = decompressedSha256(current.r1());
        String currentR2 = decompressedSha256(current.r2());
        report.put("inputContentSha256", Map.of(
                "baseline", Map.of("r1", baselineR1, "r2", baselineR2),
                "current", Map.of("r1", currentR1, "r2", currentR2)));

        Map<String, Boolean> prerequisites = new LinkedHashMap<>();
        prerequisites.put("toolVersionEqual", baseline.toolVersion().equals(current.toolVersion()));
        prerequisites.put("configurationEqual", baseline.configurationFingerprint()
                .equals(current.configurationFingerprint()));
        prerequisites.put("decompressedR1Equal", baselineR1.equals(currentR1));
        prerequisites.put("decompressedR2Equal", baselineR2.equals(currentR2));
        report.put("prerequisites", prerequisites);
        report.put("status", "PREREQUISITES_PENDING");
        AssemblyQualityTestSupport.writeJson(reportPath, report);
        System.out.println("Historical assembly comparison report: " + reportPath.toAbsolutePath());

        boolean comparable = prerequisites.values().stream().allMatch(Boolean::booleanValue);
        if (!comparable) {
            report.put("status", "NOT_COMPARABLE");
            AssemblyQualityTestSupport.writeJson(reportPath, report);
        }
        // Do not compare assembly results until fixed inputs and configuration are verified.
        assertAll("Recorded runs must use the same input contents and recorded configuration",
                prerequisites.entrySet().stream().map(check ->
                        (org.junit.jupiter.api.function.Executable) () -> assertTrue(check.getValue(),
                                check.getKey() + "; see " + reportPath)));

        Map<String, Object> contigs = compareFasta(baseline.contigs(), current.contigs());
        Map<String, Object> scaffolds = compareFasta(baseline.scaffolds(), current.scaffolds());
        report.put("contigs", contigs);
        report.put("scaffolds", scaffolds);
        boolean equivalent = Boolean.TRUE.equals(contigs.get("canonicalSequencesEqual"))
                && Boolean.TRUE.equals(contigs.get("metricsEqual"))
                && Boolean.TRUE.equals(scaffolds.get("canonicalSequencesEqual"))
                && Boolean.TRUE.equals(scaffolds.get("metricsEqual"));
        report.put("status", equivalent ? "RECORDED_OUTPUTS_EQUIVALENT" : "RECORDED_OUTPUTS_DIFFER");
        AssemblyQualityTestSupport.writeJson(reportPath, report);

        assertAll("Historical assembly output consistency (not independent truth validation)",
                () -> assertTrue(Boolean.TRUE.equals(contigs.get("canonicalSequencesEqual")),
                        "Contig sequence multisets differ; see " + reportPath),
                () -> assertTrue(Boolean.TRUE.equals(contigs.get("metricsEqual")),
                        "Contig metrics differ; see " + reportPath),
                () -> assertTrue(Boolean.TRUE.equals(scaffolds.get("canonicalSequencesEqual")),
                        "Scaffold sequence multisets differ; see " + reportPath),
                () -> assertTrue(Boolean.TRUE.equals(scaffolds.get("metricsEqual")),
                        "Scaffold metrics differ; see " + reportPath));
    }

    private static ArtifactSet artifactSet(JsonNode root, String label, Path manifestDirectory)
            throws IOException {
        JsonNode node = root.get(label);
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("Missing manifest object: " + label);
        }
        String toolVersion = requiredText(node, "toolVersion", label);
        String configuration = requiredText(node, "configurationFingerprint", label);
        return new ArtifactSet(
                artifactPath(node, "contigs", label, manifestDirectory),
                artifactPath(node, "scaffolds", label, manifestDirectory),
                artifactPath(node, "r1", label, manifestDirectory),
                artifactPath(node, "r2", label, manifestDirectory),
                toolVersion, configuration, node.get("source"));
    }

    private static String requiredText(JsonNode node, String field, String label) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException("Missing nonblank string: " + label + "." + field);
        }
        String text = value.textValue().trim();
        if (UNKNOWN_VALUES.contains(text.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Unverified placeholder is not accepted: " + label + "." + field);
        }
        return text;
    }

    private static Path artifactPath(JsonNode node, String field, String label, Path manifestDirectory)
            throws IOException {
        Path path = Path.of(requiredText(node, field, label));
        if (!path.isAbsolute()) {
            path = manifestDirectory.resolve(path);
        }
        path = path.toAbsolutePath().normalize();
        requireFile(path, label + "." + field);
        return path;
    }

    private static void requireFile(Path path, String label) throws IOException {
        if (!Files.isRegularFile(path) || !Files.isReadable(path) || Files.size(path) == 0) {
            throw new IOException("Expected readable nonempty file for " + label + ": " + path);
        }
    }

    private static Map<String, Object> describe(ArtifactSet artifacts) {
        Map<String, Object> description = new LinkedHashMap<>();
        description.put("contigs", artifacts.contigs().toString());
        description.put("scaffolds", artifacts.scaffolds().toString());
        description.put("r1", artifacts.r1().toString());
        description.put("r2", artifacts.r2().toString());
        description.put("toolVersion", artifacts.toolVersion());
        description.put("configurationFingerprint", artifacts.configurationFingerprint());
        if (artifacts.source() != null) {
            description.put("source", artifacts.source());
        }
        return description;
    }

    private static Map<String, Object> compareFasta(Path baseline, Path current) throws IOException {
        List<String> baselineSequences = AssemblyQualityTestSupport.canonicalSequences(baseline);
        List<String> currentSequences = AssemblyQualityTestSupport.canonicalSequences(current);
        Map<String, Object> baselineMetrics = AssemblyQualityTestSupport.fastaMetrics(baseline);
        Map<String, Object> currentMetrics = AssemblyQualityTestSupport.fastaMetrics(current);
        Map<String, Object> comparison = new LinkedHashMap<>();
        comparison.put("comparisonRule", "Ignore record names, order and strand; preserve repeated sequences");
        comparison.put("canonicalSequencesEqual", baselineSequences.equals(currentSequences));
        comparison.put("metricsEqual", baselineMetrics.equals(currentMetrics));
        comparison.put("baselineMetrics", baselineMetrics);
        comparison.put("currentMetrics", currentMetrics);
        comparison.put("baselineFileSha256", AssemblyQualityTestSupport.sha256(baseline));
        comparison.put("currentFileSha256", AssemblyQualityTestSupport.sha256(current));
        return comparison;
    }

    /** Compares FASTQ contents, including headers and quality strings, not gzip container bytes. */
    private static String decompressedSha256(Path path) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is required by the Java platform", impossible);
        }
        long bytesRead = 0;
        try (BufferedInputStream input = new BufferedInputStream(Files.newInputStream(path))) {
            input.mark(2);
            int first = input.read();
            int second = input.read();
            input.reset();
            try (InputStream decoded = first == 0x1f && second == 0x8b ? new GZIPInputStream(input) : input) {
                byte[] buffer = new byte[8192];
                int count;
                while ((count = decoded.read(buffer)) != -1) {
                    digest.update(buffer, 0, count);
                    bytesRead += count;
                }
            }
        }
        if (bytesRead == 0) {
            throw new IOException("FASTQ has empty decoded contents: " + path);
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private record ArtifactSet(Path contigs, Path scaffolds, Path r1, Path r2,
            String toolVersion, String configurationFingerprint, JsonNode source) {
    }
}
