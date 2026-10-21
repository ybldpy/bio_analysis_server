package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.QcParameters;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;

/** Compare old implicit tool defaults with the explicit Java defaults on format-only fixtures. */
@EnabledIfSystemProperty(named = "qc.defaults.integration", matches = "true")
class QcFilterDefaultsIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Set<String> FILTER_OPTIONS = Set.of(
            "--qualified_quality_phred", "--unqualified_percent_limit", "--average_qual", "--mean_qual",
            "--length_required", "--length_limit", "--n_base_limit", "--n_percent_limit");

    static Stream<Arguments> modes() {
        return Stream.of(Arguments.of(false, false), Arguments.of(false, true), Arguments.of(true, false));
    }

    @ParameterizedTest(name = "longReads={0}, paired={1}")
    @MethodSource("modes")
    void explicitDefaultsPreserveFilteredReadsAndCounts(boolean longReads, boolean paired) throws Exception {
        String tool = System.getProperty(longReads ? "qc.fastplong" : "qc.fastp");
        assertNotNull(tool, "Supply executable paths with -Dqc.fastp and -Dqc.fastplong");
        assertTrue(Files.isExecutable(Path.of(tool)));
        Path parent = Files.createDirectories(Path.of("target/qc-defaults-validation"));
        Path root = Files.createTempDirectory(parent, longReads ? "long-" : paired ? "paired-" : "single-")
                .toAbsolutePath();
        Path r1 = root.resolve("fixture_R1.fastq");
        Path r2 = paired ? root.resolve("fixture_R2.fastq") : null;
        Files.writeString(r1, fixture(1));
        if (paired) {
            Files.writeString(r2, fixture(2));
        }
        QcStageExecutor executor = new QcStageExecutor();
        AnalysisPipelineToolsConfig tools = new AnalysisPipelineToolsConfig();
        // Limit test resources identically in both runs; all filtering defaults stay untouched.
        tools.setFastp(List.of(tool, "--thread", "1"));
        tools.setFastplong(List.of(tool, "--thread", "1"));
        ReflectionTestUtils.setField(executor, "analysisPipelineToolsConfig", tools);
        for (boolean explicit : new boolean[] {false, true}) {
            Path dir = Files.createDirectory(root.resolve(explicit ? "explicit" : "implicit"));
            List<String> built = ReflectionTestUtils.invokeMethod(executor, "buildQcRunCmd", longReads ? 1 : 0,
                    r1, r2, dir.resolve("r1.fastq"), paired ? dir.resolve("r2.fastq") : null,
                    dir.resolve("report.json"), dir.resolve("report.html"), new QcParameters());
            assertNotNull(built);
            List<String> command = new ArrayList<>();
            for (int i = 0; i < built.size(); i++) {
                if (!explicit && FILTER_OPTIONS.contains(built.get(i))) {
                    i++; // The previous executor omitted these option/value pairs.
                } else {
                    command.add(built.get(i));
                }
            }
            JSON.writerWithDefaultPrettyPrinter().writeValue(dir.resolve("command.json").toFile(), command);
            Process process = new ProcessBuilder(command).directory(dir.toFile())
                    .redirectOutput(dir.resolve("stdout.log").toFile())
                    .redirectError(dir.resolve("stderr.log").toFile()).start();
            try {
                assertTrue(process.waitFor(120, TimeUnit.SECONDS), "Tool timed out: " + dir);
                assertEquals(0, process.exitValue(), () -> "Inspect logs at " + dir);
            } finally {
                if (process.isAlive()) {
                    process.destroyForcibly();
                    process.waitFor(5, TimeUnit.SECONDS);
                }
            }
        }
        Path implicit = root.resolve("implicit");
        Path explicit = root.resolve("explicit");
        assertEquals(-1L, Files.mismatch(implicit.resolve("r1.fastq"), explicit.resolve("r1.fastq")));
        if (paired) {
            assertEquals(-1L, Files.mismatch(implicit.resolve("r2.fastq"), explicit.resolve("r2.fastq")));
        }
        JsonNode before = JSON.readTree(implicit.resolve("report.json").toFile());
        JsonNode after = JSON.readTree(explicit.resolve("report.json").toFile());
        assertEquals(before.get("filtering_result"), after.get("filtering_result"));
        assertEquals(before.path("summary").get("before_filtering"), after.path("summary").get("before_filtering"));
        assertEquals(before.path("summary").get("after_filtering"), after.path("summary").get("after_filtering"));
        int retained = after.path("summary").path("after_filtering").path("total_reads").asInt();
        int input = after.path("summary").path("before_filtering").path("total_reads").asInt();
        assertTrue(retained > 0 && retained < input, "Fixture must exercise retention and rejection");
        System.out.println("QC implicit/explicit defaults identical: " + root + " reads=" + input + " -> " + retained);
    }

    private static String fixture(int mate) {
        StringBuilder fastq = new StringBuilder();
        Random random = new Random(121L + mate);
        int[] lengths = {14, 15, 19, 20, 100, 100, 100, 100, 100, 700, 700, 700};
        for (int i = 0; i < lengths.length; i++) {
            StringBuilder bases = new StringBuilder();
            for (int j = 0; j < lengths[i]; j++) {
                bases.append("ACGT".charAt(random.nextInt(4)));
            }
            if (i == 7 || i == 8) {
                int nCount = i == 7 ? 5 : 11;
                bases.replace(0, nCount, "N".repeat(nCount));
            }
            char quality = i == 4 ? '!' : i == 5 ? '+' : i == 6 ? '/' : 'I';
            fastq.append("@format_fixture_").append(i).append('/').append(mate).append('\n')
                    .append(bases).append("\n+\n").append(String.valueOf(quality).repeat(lengths[i])).append('\n');
        }
        return fastq.toString();
    }
}
