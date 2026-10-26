package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_QUEUING;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.analysisPipeline.BioStageUtil;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.LocalStorageService;
import com.xjtlu.bio.utils.JsonUtil;

/** Synthetic sequence fixtures and real parsing/storage; only process execution is replaced. */
class ReferenceComparisonCandidatesExecutorTest {

    private static final String QUERY = ">query\nACGTACGT\n";
    private static final String REFERENCE = ">actual_header\nACGTACGT\n";
    private static final String FULL_PAF =
            "query\t8\t0\t8\t+\tactual_header\t8\t0\t8\t8\t8\t60\tcs:Z::8\n";
    private static final String PARTIAL_PAF =
            "query\t8\t0\t4\t+\tactual_header\t8\t0\t4\t3\t4\t60\tcs:Z::3*ta\n";

    @TempDir
    Path tempDir;

    private Path storageRoot;
    private Path inputDir;
    private Path workDir;
    private String queryKey;
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
        queryKey = writeObject("samples/shared.fasta", QUERY);
    }

    @Test
    void comparesEveryCandidateWithoutGroupingRankingOrOverwritingSameBasenamesAndHeaders() throws Exception {
        ReferenceSequence first = reference(101L, "CUSTOM_first", "first/shared.fasta", REFERENCE);
        ReferenceSequence second = reference(102L, "CUSTOM_second", "second/shared.fasta", REFERENCE);
        ReferenceSequence third = reference(103L, "CUSTOM_third", "third/shared.fasta",
                ">actual_header\nACGTACGTACGT\n");
        first.setTaxId(11);
        second.setTaxId(11);
        third.setTaxId(22);
        for (ReferenceSequence reference : List.of(first, second, third)) {
            reference.setSegment("segment 1");
            reference.setSegmentOrdinal(1);
            reference.setSourceDb("CUSTOM");
            reference.setOrganismName("Synthetic fixture");
        }
        List<ReferenceSequence> references = List.of(first, second, third);
        List<byte[]> originalReferences = new ArrayList<>();
        for (ReferenceSequence reference : references) {
            originalReferences.add(Files.readAllBytes(storageRoot.resolve(reference.getPath())));
        }
        byte[] originalQuery = Files.readAllBytes(storageRoot.resolve(queryKey));
        executor.responses.addAll(List.of(Response.ok(PARTIAL_PAF), Response.ok(FULL_PAF), Response.ok("")));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(references));

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(3, executor.invocations.size());
        assertEquals(3, result.getStageOutput().getComparisons().size());
        assertEquals(List.of("CUSTOM_first", "CUSTOM_second", "CUSTOM_third"),
                result.getStageOutput().getComparisons().stream()
                        .map(comparison -> comparison.metrics().referenceAccession()).toList());
        for (int index = 0; index < references.size(); index++) {
            String directoryName = "candidate_" + (index + 1);
            Invocation invocation = executor.invocations.get(index);
            var comparison = result.getStageOutput().getComparisons().get(index);
            assertEquals(inputDir.resolve(directoryName + "/reference_shared.fasta"), invocation.referencePath());
            assertEquals(inputDir.resolve("query_shared.fasta"), invocation.queryPath());
            assertEquals(workDir.resolve(directoryName), invocation.directory());
            assertEquals(QUERY, invocation.queryContents());
            assertEquals(Files.readString(storageRoot.resolve(references.get(index).getPath())),
                    invocation.referenceContents());
            assertEquals(List.of("mock-runtime", "mock-minimap2", "-x", "asm5", "--cs=long",
                    "--secondary=no", invocation.referencePath().toString(), invocation.queryPath().toString(),
                    "-o", workDir.resolve(directoryName + "/alignment.paf").toString()), invocation.command());
            assertEquals(workDir.resolve(directoryName + "/alignment.paf"), comparison.alignmentPafPath());
            assertEquals(workDir.resolve(directoryName + "/reference_difference.tsv"), comparison.differenceTsvPath());
            assertTrue(Files.isRegularFile(comparison.alignmentPafPath()));
            assertTrue(Files.readString(comparison.differenceTsvPath()).startsWith("reference_name\tquery_name\t"));
            assertEquals(references.get(index).getReferenceId(), comparison.metrics().referenceId());
            assertEquals(references.get(index).getTaxId(), comparison.metrics().taxId());
            assertEquals("CUSTOM", comparison.metrics().sourceDb());
            assertEquals("Synthetic fixture", comparison.metrics().organismName());
            assertEquals("segment 1", comparison.metrics().segment());
            assertEquals("SEGMENT_1", comparison.metrics().segmentKey());
            assertArrayEquals(originalReferences.get(index),
                    Files.readAllBytes(storageRoot.resolve(references.get(index).getPath())));
        }
        var partial = result.getStageOutput().getComparisons().get(0).metrics();
        assertEquals("MATCHED", partial.status());
        assertEquals(75.0d, partial.sequenceIdentity());
        assertEquals(0.5d, partial.referenceCoverage());
        assertEquals(4L, partial.alignedReferenceBases());
        assertEquals(8L, partial.referenceBases());
        assertEquals(3L, partial.matchingBases());
        assertEquals(4L, partial.alignmentBlockBases());
        assertEquals(1, partial.alignmentCount());
        var full = result.getStageOutput().getComparisons().get(1).metrics();
        assertEquals("MATCHED", full.status());
        assertEquals(100.0d, full.sequenceIdentity());
        assertEquals(1.0d, full.referenceCoverage());
        var noMatch = result.getStageOutput().getComparisons().get(2).metrics();
        assertEquals("NO_MATCH", String.valueOf(noMatch.status()));
        assertEquals(12L, noMatch.referenceBases());
        assertEquals(0L, noMatch.alignedReferenceBases());
        assertEquals(0L, noMatch.matchingBases());
        assertEquals(0L, noMatch.alignmentBlockBases());
        assertEquals(0, noMatch.alignmentCount());
        assertEquals(0.0d, noMatch.sequenceIdentity());
        assertEquals(0.0d, noMatch.referenceCoverage());
        assertEquals(PARTIAL_PAF, Files.readString(result.getStageOutput().getComparisons().get(0).alignmentPafPath()));
        assertEquals(FULL_PAF, Files.readString(result.getStageOutput().getComparisons().get(1).alignmentPafPath()));
        assertEquals("", Files.readString(result.getStageOutput().getComparisons().get(2).alignmentPafPath()));
        assertTrue(Files.readString(result.getStageOutput().getComparisons().get(0).differenceTsvPath())
                .contains("actual_header\tquery\t+\t4\tSNP\tT\tA\t1\n"));
        assertEquals(1, Files.readAllLines(result.getStageOutput().getComparisons().get(1).differenceTsvPath()).size());
        Path summaryPath = result.getStageOutput().getComparisonTsvPath();
        assertEquals(workDir.resolve("reference_comparison.tsv"), summaryPath);
        List<String> summary = Files.readAllLines(summaryPath);
        assertEquals(4, summary.size());
        assertEquals("reference_id\treference_accession\tsource_db\ttax_id\torganism_name\tsegment\tsegment_key"
                + "\tstatus\tsequence_identity_percent\treference_coverage\taligned_reference_bases"
                + "\treference_bases\tmatching_bases\talignment_block_bases\talignment_count", summary.get(0));
        for (int index = 0; index < references.size(); index++) {
            String[] fields = summary.get(index + 1).split("\t", -1);
            var metrics = result.getStageOutput().getComparisons().get(index).metrics();
            assertEquals(15, fields.length);
            assertEquals(String.valueOf(metrics.referenceId()), fields[0]);
            assertEquals(references.get(index).getAccession(), fields[1], "Summary must preserve candidate order");
            assertEquals(metrics.sourceDb(), fields[2]);
            assertEquals(String.valueOf(metrics.taxId()), fields[3]);
            assertEquals(metrics.organismName(), fields[4]);
            assertEquals(metrics.segment(), fields[5]);
            assertEquals(metrics.segmentKey(), fields[6]);
            assertEquals(metrics.status(), fields[7]);
            assertEquals(metrics.sequenceIdentity(), Double.parseDouble(fields[8]));
            assertEquals(metrics.referenceCoverage(), Double.parseDouble(fields[9]));
            assertEquals(metrics.alignedReferenceBases(), Long.parseLong(fields[10]));
            assertEquals(metrics.referenceBases(), Long.parseLong(fields[11]));
            assertEquals(metrics.matchingBases(), Long.parseLong(fields[12]));
            assertEquals(metrics.alignmentBlockBases(), Long.parseLong(fields[13]));
            assertEquals(metrics.alignmentCount(), Integer.parseInt(fields[14]));
        }
        assertArrayEquals(originalQuery, Files.readAllBytes(storageRoot.resolve(queryKey)));
        assertFalse(Files.exists(inputDir));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void emptyPafForOneOrAllCandidatesIsSuccessfulNoMatch(int candidateCount) throws Exception {
        List<ReferenceSequence> references = new ArrayList<>();
        for (int index = 0; index < candidateCount; index++) {
            references.add(reference(null, "CUSTOM_" + index, "references/" + index + ".fasta", REFERENCE));
            executor.responses.add(Response.ok(""));
        }

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(references));

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(candidateCount, executor.invocations.size());
        assertEquals(candidateCount, result.getStageOutput().getComparisons().size());
        for (var comparison : result.getStageOutput().getComparisons()) {
            var metrics = comparison.metrics();
            assertEquals("NO_MATCH", String.valueOf(metrics.status()));
            assertEquals(0.0d, metrics.sequenceIdentity());
            assertEquals(0.0d, metrics.referenceCoverage());
            assertEquals(0L, metrics.alignedReferenceBases());
            assertEquals(8L, metrics.referenceBases());
            assertEquals(0L, metrics.matchingBases());
            assertEquals(0L, metrics.alignmentBlockBases());
            assertEquals(0, metrics.alignmentCount());
            assertEquals(0L, Files.size(comparison.alignmentPafPath()));
            assertEquals(1, Files.readAllLines(comparison.differenceTsvPath()).size());
        }
        assertEquals(candidateCount + 1, Files.readAllLines(result.getStageOutput().getComparisonTsvPath()).size());
    }

    @Test
    void metricsUseAllReferenceRecordsAndMergedIntervalsForMultipleAlignments() throws Exception {
        ReferenceSequence reference = reference(101L, "CUSTOM_metadata", "references/multiple.fasta",
                ">actual_header description\nACGTACGT\n>other_header\nACGT\n");
        executor.responses.add(Response.ok(
                "query\t8\t0\t4\t+\tactual_header\t8\t0\t4\t4\t4\t60\tcs:Z::4\n"
                        + "query\t8\t4\t8\t+\tactual_header\t8\t2\t6\t4\t4\t60\tcs:Z::4\n"
                        + "query\t8\t0\t4\t+\tother_header\t4\t0\t4\t4\t4\t60\tcs:Z::4\n"));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(reference)));

        assertTrue(result.isSuccess(), result.getErrorLog());
        var metrics = result.getStageOutput().getComparisons().get(0).metrics();
        assertEquals("CUSTOM_metadata", metrics.referenceAccession());
        assertEquals(12L, metrics.referenceBases());
        assertEquals(10L, metrics.alignedReferenceBases());
        assertEquals(10.0d / 12.0d, metrics.referenceCoverage(), 1.0e-12);
        assertEquals(100.0d, metrics.sequenceIdentity());
        assertEquals(12L, metrics.matchingBases());
        assertEquals(12L, metrics.alignmentBlockBases());
        assertEquals(3, metrics.alignmentCount());
    }

    @Test
    void validLongCsProducesSuccessfulMetricsAndHeaderOnlyDifferenceReport() throws Exception {
        ReferenceSequence reference = reference(101L, "CUSTOM_long_cs", "references/long_cs.fasta", REFERENCE);
        String paf = FULL_PAF.replace("cs:Z::8", "cs:Z:=ACGTACGT");
        executor.responses.add(Response.ok(paf));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(reference)));

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(1, executor.invocations.size());
        var comparison = result.getStageOutput().getComparisons().get(0);
        assertEquals("MATCHED", comparison.metrics().status());
        assertEquals(100.0d, comparison.metrics().sequenceIdentity());
        assertEquals(1.0d, comparison.metrics().referenceCoverage());
        assertEquals(8L, comparison.metrics().matchingBases());
        assertEquals(1, comparison.metrics().alignmentCount());
        assertEquals(paf, Files.readString(comparison.alignmentPafPath()));
        assertEquals(List.of("reference_name\tquery_name\tstrand\treference_position\ttype\tref\tquery\tlength"),
                Files.readAllLines(comparison.differenceTsvPath()));
    }

    @Test
    void manyCsOperationsDoNotOverflowTheValidationStack() throws Exception {
        String bases = "A".repeat(16_384);
        queryKey = writeObject("samples/long.fasta", ">query\n" + bases + "\n");
        ReferenceSequence reference = reference(101L, "CUSTOM_many_tokens", "references/long.fasta",
                ">actual_header\n" + bases + "\n");
        String paf = "query\t16384\t0\t16384\t+\tactual_header\t16384\t0\t16384\t16384\t16384\t60\tcs:Z:"
                + ":1".repeat(16_384) + "\n";
        executor.responses.add(Response.ok(paf));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(reference)));

        assertTrue(result.isSuccess(), result.getErrorLog());
        var comparison = result.getStageOutput().getComparisons().get(0);
        assertEquals(16_384L, comparison.metrics().alignedReferenceBases());
        assertEquals(1.0d, comparison.metrics().referenceCoverage());
        assertEquals(1, Files.readAllLines(comparison.differenceTsvPath()).size());
    }

    @Test
    void summaryEscapesMetadataSeparatorsAndKeepsNullMetadataAsEmptyFields() throws Exception {
        ReferenceSequence first = reference(101L, "CUSTOM_special", "references/special.fasta", REFERENCE);
        first.setSourceDb("CUSTOM\tDB");
        first.setOrganismName("Synthetic\nfixture\rname");
        first.setSegment("segment\tlabel\nvalue");
        ReferenceSequence second = reference(null, "CUSTOM_nulls", "references/nulls.fasta", REFERENCE);
        executor.responses.addAll(List.of(Response.ok(FULL_PAF), Response.ok("")));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(first, second)));

        assertTrue(result.isSuccess(), result.getErrorLog());
        List<String> rows = Files.readAllLines(result.getStageOutput().getComparisonTsvPath());
        assertEquals(3, rows.size(), "Metadata newlines must not create extra TSV rows");
        for (String row : rows) {
            assertEquals(15, row.split("\t", -1).length, "Metadata tabs must not create extra TSV columns");
        }
        String[] special = rows.get(1).split("\t", -1);
        assertEquals("CUSTOM DB", special[2]);
        assertEquals("Synthetic fixture name", special[4]);
        assertEquals("segment label value", special[5]);
        var originalMetrics = result.getStageOutput().getComparisons().get(0).metrics();
        assertEquals(first.getSourceDb(), originalMetrics.sourceDb());
        assertEquals(first.getOrganismName(), originalMetrics.organismName());
        assertEquals(first.getSegment(), originalMetrics.segment());
        String[] nulls = rows.get(2).split("\t", -1);
        assertEquals("CUSTOM_nulls", nulls[1]);
        assertEquals("NO_MATCH", nulls[7]);
        for (int column : new int[] {0, 2, 3, 4, 5, 6}) {
            assertEquals("", nulls[column], "Null metadata must remain an empty TSV field");
        }
        var nullMetrics = result.getStageOutput().getComparisons().get(1).metrics();
        assertNull(nullMetrics.referenceId());
        assertNull(nullMetrics.sourceDb());
        assertNull(nullMetrics.taxId());
        assertNull(nullMetrics.organismName());
        assertNull(nullMetrics.segment());
        assertNull(nullMetrics.segmentKey());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void historicalReferenceGenomeParametersRemainSupportedWhenCandidatesAreAbsent(int candidateCount) throws Exception {
        List<ReferenceSequence> references = new ArrayList<>();
        for (int index = 0; index < candidateCount; index++) {
            ReferenceSequence reference = reference(101L + index, "historical_" + index,
                    "references/historical_" + index + ".fasta", REFERENCE);
            reference.setSegmentOrdinal(index + 1);
            references.add(reference);
            executor.responses.add(Response.ok(FULL_PAF));
        }
        ReferenceComparisonStageParameters parameters = new ReferenceComparisonStageParameters();
        parameters.setReferenceGenome(ReferenceGenome.fromSelectedSequences(references));
        assertNull(parameters.getCandidateReferences());

        BioPipelineStage stage = stage(parameters);
        stage.setParameters("{\"referenceGenome\":" + JsonUtil.toJson(parameters.getReferenceGenome()) + "}");
        StageRunResult<ReferenceComparisonStageOutput> result = executor.execute(stage);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(candidateCount, executor.invocations.size());
        assertEquals(candidateCount, result.getStageOutput().getComparisons().size());
        assertEquals(references.stream().map(ReferenceSequence::getAccession).toList(),
                result.getStageOutput().getComparisons().stream()
                        .map(comparison -> comparison.metrics().referenceAccession()).toList());
    }

    @Test
    void explicitCandidatesTakePrecedenceOverHistoricalReferenceGenome() throws Exception {
        ReferenceSequence reference = reference(101L, "explicit", "references/explicit.fasta", REFERENCE);
        ReferenceSequence historical = reference(102L, "historical", "references/historical.fasta", REFERENCE);
        ReferenceComparisonStageParameters parameters = parameters(List.of(reference));
        parameters.setReferenceGenome(ReferenceGenome.fromSingleSequence(historical));
        executor.responses.add(Response.ok(FULL_PAF));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters);

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(1, executor.invocations.size());
        assertEquals("explicit", result.getStageOutput().getComparisons().get(0).metrics().referenceAccession());
        assertTrue(executor.invocations.get(0).referencePath().toString().endsWith("reference_explicit.fasta"));
    }

    @Test
    void distinctReferenceIdsSharingAccessionAndSourceObjectKeepIndependentOutputsAndMetrics() throws Exception {
        ReferenceSequence first = reference(101L, "CUSTOM_shared", "references/shared.fasta", REFERENCE);
        ReferenceSequence second = reference(102L, "CUSTOM_shared", "references/shared.fasta", REFERENCE);
        byte[] originalReference = Files.readAllBytes(storageRoot.resolve(first.getPath()));
        executor.responses.addAll(List.of(Response.ok(FULL_PAF), Response.ok(PARTIAL_PAF)));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(first, second)));

        assertTrue(result.isSuccess(), result.getErrorLog());
        assertEquals(2, executor.invocations.size());
        assertEquals(2, result.getStageOutput().getComparisons().size());
        List<String> summary = Files.readAllLines(result.getStageOutput().getComparisonTsvPath());
        assertEquals(3, summary.size());
        for (int index = 0; index < 2; index++) {
            String directory = "candidate_" + (index + 1);
            var comparison = result.getStageOutput().getComparisons().get(index);
            assertEquals(Long.valueOf(101L + index), comparison.metrics().referenceId());
            assertEquals("CUSTOM_shared", comparison.metrics().referenceAccession());
            assertEquals("MATCHED", comparison.metrics().status());
            assertEquals(inputDir.resolve(directory + "/reference_shared.fasta"),
                    executor.invocations.get(index).referencePath());
            assertEquals(REFERENCE, executor.invocations.get(index).referenceContents());
            assertEquals(workDir.resolve(directory + "/alignment.paf"), comparison.alignmentPafPath());
            assertEquals(workDir.resolve(directory + "/reference_difference.tsv"), comparison.differenceTsvPath());
            assertEquals(index == 0 ? FULL_PAF : PARTIAL_PAF, Files.readString(comparison.alignmentPafPath()));
            assertEquals(index + 1, Files.readAllLines(comparison.differenceTsvPath()).size());
            String[] fields = summary.get(index + 1).split("\t", -1);
            assertEquals(Long.toString(101L + index), fields[0]);
            assertEquals("CUSTOM_shared", fields[1]);
            assertEquals(comparison.metrics().sequenceIdentity(), Double.parseDouble(fields[8]));
            assertEquals(comparison.metrics().referenceCoverage(), Double.parseDouble(fields[9]));
        }
        assertEquals(100.0d, result.getStageOutput().getComparisons().get(0).metrics().sequenceIdentity());
        assertEquals(75.0d, result.getStageOutput().getComparisons().get(1).metrics().sequenceIdentity());
        assertEquals(1.0d, result.getStageOutput().getComparisons().get(0).metrics().referenceCoverage());
        assertEquals(0.5d, result.getStageOutput().getComparisons().get(1).metrics().referenceCoverage());
        assertArrayEquals(originalReference, Files.readAllBytes(storageRoot.resolve(first.getPath())));
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-parameters", "missing-candidates", "empty-candidates", "empty-with-fallback",
            "null-candidate", "null-accession", "blank-accession", "null-path", "blank-path", "duplicate-id"})
    void invalidCandidatesFailBeforeStartingAnyCommand(String scenario) throws Exception {
        ReferenceSequence first = reference(101L, "first", "references/first.fasta", REFERENCE);
        ReferenceSequence second = reference(102L, "second", "references/second.fasta", REFERENCE);
        ReferenceComparisonStageParameters parameters = parameters(List.of(first, second));
        switch (scenario) {
            case "missing-parameters" -> parameters = null;
            case "missing-candidates" -> parameters = new ReferenceComparisonStageParameters();
            case "empty-candidates" -> parameters.setCandidateReferences(List.of());
            case "empty-with-fallback" -> {
                parameters.setCandidateReferences(List.of());
                parameters.setReferenceGenome(ReferenceGenome.fromSingleSequence(first));
            }
            case "null-candidate" -> parameters.setCandidateReferences(Arrays.asList(first, null));
            case "null-accession" -> second.setAccession(null);
            case "blank-accession" -> second.setAccession(" \t ");
            case "null-path" -> second.setPath(null);
            case "blank-path" -> second.setPath(" \t ");
            case "duplicate-id" -> second.setReferenceId(first.getReferenceId());
            default -> throw new AssertionError("Unknown validation scenario: " + scenario);
        }

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters);

        assertFalse(result.isSuccess(), scenario);
        assertNotNull(result.getErrorLog());
        assertTrue(executor.invocations.isEmpty(), "Invalid candidate list must not start a comparison: " + scenario);
        assertFalse(Files.exists(inputDir));
    }

    @ParameterizedTest
    @ValueSource(strings = {"nonzero-exit", "nonzero-empty-paf", "process-exception", "missing-paf", "short-row", "unknown-target",
            "wrong-target-length", "invalid-interval", "invalid-counts", "invalid-number", "missing-cs", "empty-cs"})
    void failedCommandsAndMissingOrMalformedPafFailTheStage(String scenario) throws Exception {
        ReferenceSequence reference = reference(101L, "CUSTOM_reference", "references/source.fasta", REFERENCE);
        Response response = switch (scenario) {
            case "nonzero-exit" -> new Response(7, null, FULL_PAF);
            case "nonzero-empty-paf" -> new Response(7, null, "");
            case "process-exception" -> new Response(0, new IOException("synthetic process failure"), FULL_PAF);
            case "missing-paf" -> Response.ok(null);
            case "short-row" -> Response.ok("query\t8\t0\t8\n");
            case "unknown-target" -> Response.ok(FULL_PAF.replace("actual_header", "CUSTOM_reference"));
            case "wrong-target-length" -> Response.ok(FULL_PAF.replace("actual_header\t8", "actual_header\t9"));
            case "invalid-interval" -> Response.ok("query\t8\t0\t8\t+\tactual_header\t8\t0\t9\t8\t8\t60\n");
            case "invalid-counts" -> Response.ok("query\t8\t0\t8\t+\tactual_header\t8\t0\t8\t9\t8\t60\n");
            case "invalid-number" -> Response.ok("query\t8\t0\t8\t+\tactual_header\t8\t0\t8\tNaN\t8\t60\n");
            case "missing-cs" -> Response.ok(FULL_PAF.replace("\tcs:Z::8", ""));
            case "empty-cs" -> Response.ok(FULL_PAF.replace("cs:Z::8", "cs:Z:"));
            default -> throw new AssertionError("Unknown output failure scenario: " + scenario);
        };
        executor.responses.add(response);

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(reference)));

        assertFalse(result.isSuccess(), scenario);
        assertNotNull(result.getErrorLog());
        assertEquals(1, executor.invocations.size());
        assertNull(result.getStageOutput(), "Failed comparisons must not publish successful output");
        assertFalse(Files.exists(inputDir));
    }

    @ParameterizedTest
    @ValueSource(strings = {"garbage", "*", "*a", ":", "=", "+", "-", "~gt2ag", "=ACGT*", ":8?"})
    void malformedTruncatedOrUnsupportedCsCannotProduceSuccessfulComparison(String cs) throws Exception {
        ReferenceSequence reference = reference(101L, "CUSTOM_invalid_cs", "references/invalid_cs.fasta", REFERENCE);
        executor.responses.add(Response.ok(FULL_PAF.replace("cs:Z::8", "cs:Z:" + cs)));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(reference)));

        assertFalse(result.isSuccess(), "Invalid cs must not become a successful empty difference report: " + cs);
        assertNotNull(result.getErrorLog());
        assertEquals(1, executor.invocations.size());
        assertNull(result.getStageOutput());
        assertFalse(Files.exists(inputDir));
    }

    @ParameterizedTest
    @ValueSource(strings = {"nonzero-exit", "process-exception", "missing-paf", "malformed-paf"})
    void failureOfALaterCandidateDoesNotReturnPartialSuccess(String scenario) throws Exception {
        ReferenceSequence first = reference(101L, "first", "first/shared.fasta", REFERENCE);
        ReferenceSequence second = reference(102L, "second", "second/shared.fasta", REFERENCE);
        Response failure = switch (scenario) {
            case "nonzero-exit" -> new Response(7, null, "");
            case "process-exception" -> new Response(0, new IOException("synthetic later process failure"), FULL_PAF);
            case "missing-paf" -> Response.ok(null);
            case "malformed-paf" -> Response.ok("query\t8\t0\t8\n");
            default -> throw new AssertionError("Unknown later failure scenario: " + scenario);
        };
        executor.responses.addAll(List.of(Response.ok(FULL_PAF), failure));

        StageRunResult<ReferenceComparisonStageOutput> result = run(parameters(List.of(first, second)));

        assertFalse(result.isSuccess());
        assertEquals(2, executor.invocations.size());
        assertEquals(FULL_PAF, Files.readString(workDir.resolve("candidate_1/alignment.paf")));
        assertNull(result.getStageOutput());
    }

    private ReferenceComparisonStageParameters parameters(List<ReferenceSequence> references) {
        ReferenceComparisonStageParameters parameters = new ReferenceComparisonStageParameters();
        parameters.setCandidateReferences(references);
        return parameters;
    }

    private StageRunResult<ReferenceComparisonStageOutput> run(ReferenceComparisonStageParameters parameters)
            throws Exception {
        return executor.execute(stage(parameters));
    }

    private BioPipelineStage stage(ReferenceComparisonStageParameters parameters) throws Exception {
        BioPipelineStage stage = new BioPipelineStage();
        stage.setStageId(42L);
        stage.setPipelineId(1L);
        stage.setStageType(PIPELINE_STAGE_REFERENCE_COMPARISON);
        stage.setStageIndex(0);
        stage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        stage.setVersion(3);
        stage.setInputUrl(JsonUtil.toJson(new ReferenceComparisonStageInputUrls(queryKey)));
        stage.setParameters(parameters == null ? "null" : JsonUtil.toJson(parameters));
        return stage;
    }

    private ReferenceSequence reference(Long id, String accession, String path, String fasta) throws IOException {
        ReferenceSequence reference = new ReferenceSequence();
        reference.setReferenceId(id);
        reference.setAccession(accession);
        reference.setPath(writeObject(path, fasta));
        return reference;
    }

    private String writeObject(String key, String contents) throws IOException {
        Path path = storageRoot.resolve(key);
        Files.createDirectories(path.getParent());
        Files.writeString(path, contents);
        return key;
    }

    private record Response(int exitCode, Exception exception, String paf) {
        static Response ok(String paf) {
            return new Response(0, null, paf);
        }
    }

    private record Invocation(List<String> command, Path directory, Path referencePath, Path queryPath,
            String referenceContents, String queryContents) {
    }

    private static final class StubExecutor extends ReferenceComparsionStageExecutor {
        private final List<Response> responses = new ArrayList<>();
        private final List<Invocation> invocations = new ArrayList<>();

        @Override
        protected ExecuteResult _execute(List<String> command, Path directory) {
            int referenceIndex = command.indexOf("--secondary=no") + 1;
            Path referencePath = Path.of(command.get(referenceIndex));
            Path queryPath = Path.of(command.get(referenceIndex + 1));
            try {
                invocations.add(new Invocation(List.copyOf(command), directory, referencePath, queryPath,
                        Files.readString(referencePath), Files.readString(queryPath)));
                assertTrue(Files.isDirectory(directory), "Candidate working directory must exist at execution time");
                assertTrue(invocations.size() <= responses.size(), "Unexpected extra process invocation");
                Response response = responses.get(invocations.size() - 1);
                if (response.paf() != null) {
                    Files.writeString(Path.of(command.get(command.indexOf("-o") + 1)), response.paf());
                }
                return new ExecuteResult(response.exitCode(), response.exception());
            } catch (IOException e) {
                throw new AssertionError("Cannot prepare isolated candidate comparison fixture", e);
            }
        }
    }
}
