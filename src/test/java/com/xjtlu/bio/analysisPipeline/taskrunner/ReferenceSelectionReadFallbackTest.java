package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.xjtlu.bio.analysisPipeline.Constants.SequenceInput;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore.EvidenceSource;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSelectionOutcome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics.Status;
import com.xjtlu.bio.analysisPipeline.taskrunner.AbstractPipelineStageExector.LoadFailException;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.utils.JsonUtil;

/** Runs the executor orchestration with stubbed storage and commands, and real PAF parsing. */
class ReferenceSelectionReadFallbackTest {

    @TempDir
    Path tempDir;

    @Test
    void assemblyOnlyKeepsPartialSelectionWithoutLoadingReads() throws Exception {
        Fixture fixture = fixture(false, false, reference("A", 1), reference("B", 2));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("A"), selected(output));
        assertEquals(ReferenceSelectionOutcome.PARTIAL, output.getOutcome());
        assertEquals(List.of("SEGMENT_2"), output.getUnmatchedSegmentKeys());
        assertEquals(Status.NO_READS, output.getReadFallbackMetrics().status());
        assertTrue(fixture.executor.readCommands.isEmpty());
        assertFalse(fixture.executor.loaded.contains("qc/r1.fq"));
        assertEquals(EvidenceSource.CONTIGS, score(output, "A").getEvidenceSource());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void onlyUnmatchedGroupsUseOptionalSingleOrPairedReads(boolean paired) throws Exception {
        Fixture fixture = fixture(true, paired, reference("A", 1), reference("B", 2), reference("C", 2));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));
        fixture.executor.readPafs.put("A", paf("A", 0, 1000, 1000, 60));
        fixture.executor.readPafs.put("B", paf("B", 0, 600, 540, 60));
        fixture.executor.readPafs.put("C", paf("C", 0, 600, 600, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("A", "C"), selected(output));
        assertFalse(output.isPartial());
        assertEquals(ReferenceSelectionOutcome.MATCHED, output.getOutcome());
        assertEquals(EvidenceSource.CONTIGS, score(output, "A").getEvidenceSource());
        assertEquals(0.8d, score(output, "A").getReferenceCoverage());
        assertEquals(EvidenceSource.READS, score(output, "C").getEvidenceSource());
        assertEquals(Status.COMPLETED, output.getReadFallbackMetrics().status());
        assertEquals(2, output.getReadFallbackMetrics().evaluatedCandidates());
        assertEquals(1, output.getReadFallbackMetrics().acceptedGroups());
        assertEquals(2, fixture.executor.readCommands.size());
        for (List<String> command : fixture.executor.readCommands) {
            assertEquals("sr", command.get(command.indexOf("-x") + 1));
            int outputIndex = command.indexOf("-o");
            assertEquals(paired ? 5 : 4, command.size() - outputIndex);
            assertTrue(command.get(outputIndex + 3).endsWith("reads_r1_r1.fq"));
            if (paired) {
                assertTrue(command.get(outputIndex + 4).endsWith("reads_r2_r2.fq"));
            }
        }
        assertEquals(paired, fixture.executor.loaded.contains("qc/r2.fq"));
        assertTrue(Files.readString(output.getComparisonReportPath()).contains("evidence_source\trejection_reason"));
    }

    @Test
    void fallbackCanRunWhenNoContigCandidateMatched() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2));
        fixture.executor.readPafs.put("B", paf("B", 0, 700, 630, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("B"), selected(output));
        assertEquals(List.of("SEGMENT_1"), output.getUnmatchedSegmentKeys());
        assertEquals("NO_ALIGNMENT_PASSING_MAPPING_QUALITY", score(output, "A").getRejectionReason());
    }

    @Test
    void highLocalDepthDoesNotPassCoverageBreadthGate() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));
        fixture.executor.readPafs.put("B", paf("B", 0, 100, 100, 60).repeat(100));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("A"), selected(output));
        assertEquals(0.1d, score(output, "B").getReferenceCoverage());
        assertEquals(100, score(output, "B").getAlignmentCount());
        assertEquals("REFERENCE_COVERAGE_BELOW_THRESHOLD", score(output, "B").getRejectionReason());
        assertEquals(List.of("SEGMENT_2"), output.getUnmatchedSegmentKeys());
    }

    @Test
    void selectsQualifiedCandidateEvenIfRejectedCandidateHasHigherScore() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 1));
        fixture.executor.readPafs.put("A", paf("A", 0, 1000, 790, 60));
        fixture.executor.readPafs.put("B", paf("B", 0, 600, 570, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertTrue(score(output, "A").getScore() > score(output, "B").getScore());
        assertEquals(List.of("B"), selected(output));
        assertEquals("SEQUENCE_IDENTITY_BELOW_THRESHOLD", score(output, "A").getRejectionReason());
    }

    @Test
    void appliesConfiguredThresholdsAndIncludesTheirSnapshot() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1));
        fixture.parameters.setMinimumReadReferenceCoverage(0.75d);
        fixture.parameters.setMinimumReadSequenceIdentity(90.0d);
        fixture.parameters.setMinimumReadMappingQuality(30);
        fixture.executor.readPafs.put("A", paf("A", 0, 750, 675, 30));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("A"), selected(output));
        assertEquals(0.75d, output.getReadFallbackMetrics().minimumReferenceCoverage());
        assertEquals(90.0d, output.getReadFallbackMetrics().minimumSequenceIdentity());
        assertEquals(30, output.getReadFallbackMetrics().minimumMappingQuality());
    }

    @ParameterizedTest
    @ValueSource(ints = {19, 255})
    void lowOrUnknownMappingQualityDoesNotSupplyCoverage(int mapq) throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));
        fixture.executor.readPafs.put("B", paf("B", 0, 1000, 1000, mapq));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("A"), selected(output));
        assertEquals(0.0d, score(output, "B").getReferenceCoverage());
        assertEquals("NO_ALIGNMENT_PASSING_MAPPING_QUALITY", score(output, "B").getRejectionReason());
    }

    @Test
    void allRejectedReturnsSuccessfulNoMatchWithDiagnosticsInsteadOfInventingAReference() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1));
        fixture.executor.readPafs.put("A", paf("A", 0, 100, 100, 60));

        StageRunResult<ReferenceSelectionStageOutput> result = fixture.run();

        assertTrue(result.isSuccess());
        assertNull(result.getFailReason());
        assertNull(result.getE());
        ReferenceSelectionStageOutput output = result.getStageOutput();
        assertEquals(ReferenceSelectionOutcome.NO_MATCH, output.getOutcome());
        assertNull(output.getSelectedReference());
        assertFalse(output.isPartial());
        assertEquals(List.of("SEGMENT_1"), output.getUnmatchedSegmentKeys());
        assertEquals(Status.COMPLETED, output.getReadFallbackMetrics().status());
        assertEquals(0, output.getReadFallbackMetrics().acceptedGroups());
        assertEquals("REFERENCE_COVERAGE_BELOW_THRESHOLD", score(output, "A").getRejectionReason());
        assertTrue(Files.readString(tempDir.resolve("work/reference_selection.tsv"))
                .contains("REFERENCE_COVERAGE_BELOW_THRESHOLD"));
        assertTrue(Files.readString(tempDir.resolve("work/reference_selection_read_fallback.json"))
                .contains("\"acceptedGroups\":0"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void emptyContigAlignmentsReturnNoMatchForWholeAndSegmentedReferences(boolean segmented) throws Exception {
        Fixture fixture = fixture(false, false, reference("A", segmented ? 1 : null));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(ReferenceSelectionOutcome.NO_MATCH, output.getOutcome());
        assertNull(output.getSelectedReference());
        assertFalse(output.isPartial());
        assertEquals(Status.NO_READS, output.getReadFallbackMetrics().status());
        assertEquals(1, output.getCandidateScores().size());
        assertEquals(0L, score(output, "A").getAlignedReferenceBases());
        assertTrue(Files.isRegularFile(output.getComparisonReportPath()));
    }

    @Test
    void emptyReadAndContigAlignmentsReturnNoMatchNotToolFailure() throws Exception {
        ReferenceSelectionStageOutput output = success(fixture(true, false, reference("A", null)));

        assertEquals(ReferenceSelectionOutcome.NO_MATCH, output.getOutcome());
        assertEquals(Status.COMPLETED, output.getReadFallbackMetrics().status());
        assertEquals("NO_ALIGNMENT_PASSING_MAPPING_QUALITY", score(output, "A").getRejectionReason());
    }

    @Test
    void disabledFallbackWithNoContigMatchPreservesDisabledStatus() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", null));
        fixture.parameters.setReadFallbackEnabled(false);

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(ReferenceSelectionOutcome.NO_MATCH, output.getOutcome());
        assertEquals(Status.DISABLED, output.getReadFallbackMetrics().status());
        assertTrue(fixture.executor.readCommands.isEmpty());
    }

    @Test
    void contigToolFailureRemainsExecutionFailure() throws Exception {
        Fixture fixture = fixture(false, false, reference("A", null));
        fixture.executor.failContigCommand = true;

        StageRunResult<ReferenceSelectionStageOutput> result = fixture.run();

        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
        assertTrue(result.getFailReason().contains("exitCode=9"));
    }

    @Test
    void malformedAlignmentRemainsExecutionFailure() throws Exception {
        Fixture fixture = fixture(false, false, reference("A", null));
        fixture.executor.contigPafs.put("A", "not a PAF record\n");

        StageRunResult<ReferenceSelectionStageOutput> result = fixture.run();

        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
        assertTrue(result.getFailReason().contains("Failed to parse minimap2 PAF"));
    }

    @Test
    void missingReferenceInputStillRaisesLoadFailure() throws Exception {
        Fixture fixture = fixture(false, false, reference("A", null));
        fixture.executor.objects.remove("references/A.fa");

        assertThrows(LoadFailException.class, fixture::run);
    }

    @Test
    void emptyCandidateConfigurationIsNotABusinessNoMatch() throws Exception {
        StageRunResult<ReferenceSelectionStageOutput> result = fixture(false, false).run();

        assertFalse(result.isSuccess());
        assertNull(result.getStageOutput());
    }

    @Test
    void disabledFallbackDoesNotLoadReadFilesOrRequireMetadata() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2));
        fixture.parameters.setReadFallbackEnabled(false);
        fixture.parameters.setReadMeta(null);
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(Status.DISABLED, output.getReadFallbackMetrics().status());
        assertTrue(fixture.executor.readCommands.isEmpty());
        assertFalse(fixture.executor.loaded.contains("qc/r1.fq"));
    }

    @Test
    void allGroupsAlreadyMatchedDoNotLoadReadFiles() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(Status.NOT_NEEDED, output.getReadFallbackMetrics().status());
        assertTrue(fixture.executor.readCommands.isEmpty());
        assertFalse(fixture.executor.loaded.contains("qc/r1.fq"));
    }

    @Test
    void missingSegmentMetadataIsNotInventedDuringFallback() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2), reference("UNKNOWN", null));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));
        fixture.executor.readPafs.put("B", paf("B", 0, 800, 800, 60));
        fixture.executor.readPafs.put("UNKNOWN", paf("UNKNOWN", 0, 1000, 1000, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(List.of("A", "B"), selected(output));
        assertEquals(1, fixture.executor.readCommands.size());
        assertEquals(EvidenceSource.CONTIGS, score(output, "UNKNOWN").getEvidenceSource());
    }

    @Test
    void unsegmentedCandidateAlsoSupportsFallback() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", null));
        fixture.executor.readPafs.put("A", paf("A", 0, 800, 800, 60));

        assertEquals(List.of("A"), selected(success(fixture)));
    }

    @Test
    void unknownLongReadTechnologyIsReportedAndNotAssumedToBeNanopore() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2));
        fixture.parameters.getReadMeta().setReadLenType(SequenceInput.READ_LEN_TYPE_LONG);
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(Status.UNSUPPORTED_READ_TYPE, output.getReadFallbackMetrics().status());
        assertEquals(List.of("SEGMENT_2"), output.getUnmatchedSegmentKeys());
        assertTrue(fixture.executor.readCommands.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"map-ont", "map-pb", "map-hifi"})
    void acceptsExplicitLongReadPreset(String preset) throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1));
        fixture.parameters.getReadMeta().setReadLenType(SequenceInput.READ_LEN_TYPE_LONG);
        fixture.parameters.setReadFallbackPreset(preset);
        fixture.executor.readPafs.put("A", paf("A", 0, 800, 800, 60));

        ReferenceSelectionStageOutput output = success(fixture);

        assertEquals(preset, output.getReadFallbackMetrics().preset());
        assertEquals(List.of("A"), selected(output));
    }

    @Test
    void readAlignmentFailureIsNotSilentlyReportedAsNoMatch() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1), reference("B", 2));
        fixture.executor.contigPafs.put("A", paf("A", 0, 800, 800, 60));
        fixture.executor.failReadCommand = true;

        StageRunResult<ReferenceSelectionStageOutput> result = fixture.run();

        assertFalse(result.isSuccess());
        assertTrue(result.getErrorLog().contains("Read alignment failed for B, exitCode=9"));
    }

    @Test
    void validatesReadInputAndThresholdContractsBeforeLoadingFiles() throws Exception {
        Fixture fixture = fixture(true, false, reference("A", 1));
        fixture.input.setR2Url("qc/r2.fq");
        fixture.input.setR1Url(null);
        assertTrue(ReferenceSelectionStageExecutor.validateInput(fixture.input, fixture.parameters).contains("requires R1"));
        fixture.input.setR1Url("qc/r2.fq");
        assertTrue(ReferenceSelectionStageExecutor.validateInput(fixture.input, fixture.parameters).contains("distinct URLs"));
        fixture.input.setR1Url("qc/r1.fq");
        fixture.parameters.setMinimumReadReferenceCoverage(Double.NaN);
        assertTrue(ReferenceSelectionStageExecutor.validateInput(fixture.input, fixture.parameters).contains("fraction"));
        fixture.parameters.setMinimumReadReferenceCoverage(0.5d);
        fixture.parameters.setMinimumReadSequenceIdentity(101.0d);
        assertTrue(ReferenceSelectionStageExecutor.validateInput(fixture.input, fixture.parameters).contains("percentage"));
        fixture.parameters.setMinimumReadSequenceIdentity(80.0d);
        fixture.parameters.setMinimumReadMappingQuality(255);
        assertTrue(ReferenceSelectionStageExecutor.validateInput(fixture.input, fixture.parameters).contains("254"));
        fixture.parameters.setMinimumReadMappingQuality(20);
        fixture.parameters.setReadMeta(null);
        assertTrue(ReferenceSelectionStageExecutor.validateInput(fixture.input, fixture.parameters).contains("metadata"));
        assertTrue(fixture.executor.loaded.isEmpty());
    }

    @Test
    void legacyContigsOnlyJsonStillHasNoReadRequirement() throws Exception {
        Fixture fixture = fixture(false, false, reference("A", 1));
        ReferenceSelectionStageInputUrls legacy = JsonUtil.toObject(
                "{\"contigsUrl\":\"query.fa\"}", ReferenceSelectionStageInputUrls.class);

        assertNull(ReferenceSelectionStageExecutor.validateInput(legacy, fixture.parameters));
        assertNull(legacy.getR1Url());
        assertNull(legacy.getR2Url());
    }

    private Fixture fixture(boolean reads, boolean paired, ReferenceSequence... references) {
        StubExecutor executor = new StubExecutor();
        ReferenceSelectionStageInputUrls input = new ReferenceSelectionStageInputUrls("query.fa");
        ReferenceSelectionStageParameters parameters = new ReferenceSelectionStageParameters(List.of(references));
        executor.objects.put("query.fa", ">query\n" + "A".repeat(1000) + "\n");
        for (ReferenceSequence reference : references) {
            executor.objects.put(reference.getPath(), ">" + reference.getAccession() + "\n" + "A".repeat(1000) + "\n");
        }
        if (reads) {
            input.setR1Url("qc/r1.fq");
            executor.objects.put("qc/r1.fq", "@read/1\nAAAA\n+\nIIII\n");
            parameters.setReadMeta(new SequenceMeta());
            if (paired) {
                input.setR2Url("qc/r2.fq");
                executor.objects.put("qc/r2.fq", "@read/2\nTTTT\n+\nIIII\n");
            }
        }
        return new Fixture(executor, input, parameters);
    }

    private ReferenceSelectionStageOutput success(Fixture fixture) throws Exception {
        StageRunResult<ReferenceSelectionStageOutput> result = fixture.run();
        assertTrue(result.isSuccess(), result.getErrorLog());
        return result.getStageOutput();
    }

    private static List<String> selected(ReferenceSelectionStageOutput output) {
        return output.getSelectedReference().getSequences().stream().map(ReferenceSequence::getAccession).toList();
    }

    private static ReferenceCandidateScore score(ReferenceSelectionStageOutput output, String accession) {
        return output.getCandidateScores().stream().filter(s -> accession.equals(s.getReferenceAccession())).findFirst().orElseThrow();
    }

    private static ReferenceSequence reference(String accession, Integer segment) {
        ReferenceSequence reference = new ReferenceSequence();
        reference.setAccession(accession);
        reference.setTaxId(1234);
        reference.setPath("references/" + accession + ".fa");
        if (segment != null) {
            reference.setSegment(segment.toString());
            reference.setSegmentOrdinal(segment);
        }
        return reference;
    }

    private static String paf(String accession, int start, int end, int matches, int mapq) {
        int length = end - start;
        return "query\t" + length + "\t0\t" + length + "\t+\t" + accession + "\t1000\t"
                + start + "\t" + end + "\t" + matches + "\t" + length + "\t" + mapq + "\ttp:A:P\n";
    }

    private class Fixture {
        final StubExecutor executor;
        final ReferenceSelectionStageInputUrls input;
        final ReferenceSelectionStageParameters parameters;

        Fixture(StubExecutor executor, ReferenceSelectionStageInputUrls input, ReferenceSelectionStageParameters parameters) {
            this.executor = executor;
            this.input = input;
            this.parameters = parameters;
        }

        StageRunResult<ReferenceSelectionStageOutput> run() throws Exception {
            return executor.run(input, parameters, tempDir);
        }
    }

    private static class StubExecutor extends ReferenceSelectionStageExecutor {
        final Map<String, String> objects = new HashMap<>();
        final Map<String, String> contigPafs = new HashMap<>();
        final Map<String, String> readPafs = new HashMap<>();
        final Set<String> loaded = new HashSet<>();
        final List<List<String>> readCommands = new ArrayList<>();
        boolean failReadCommand;
        boolean failContigCommand;

        StubExecutor() {
            analysisPipelineToolsConfig = new AnalysisPipelineToolsConfig();
            analysisPipelineToolsConfig.setMinimap2(List.of("mock-minimap2"));
        }

        StageRunResult<ReferenceSelectionStageOutput> run(ReferenceSelectionStageInputUrls input,
                ReferenceSelectionStageParameters parameters, Path root) throws Exception {
            StageExecutionInput execution = new StageExecutionInput();
            execution.input = input;
            execution.stageParameters = parameters;
            execution.stageContext = new StageContext(1L, 0, 44, 1);
            execution.inputDir = Files.createDirectories(root.resolve("input"));
            execution.workDir = Files.createDirectories(root.resolve("work"));
            return _execute(execution);
        }

        @Override
        protected void loadInput(Map<String, Path> paths) throws LoadFailException {
            try {
                for (Map.Entry<String, Path> entry : paths.entrySet()) {
                    String contents = objects.get(entry.getKey());
                    if (contents == null) {
                        throw new IOException("Unexpected input: " + entry.getKey());
                    }
                    Files.createDirectories(entry.getValue().getParent());
                    Files.writeString(entry.getValue(), contents);
                    loaded.add(entry.getKey());
                }
            } catch (IOException e) {
                throw new LoadFailException(e);
            }
        }

        @Override
        protected ExecuteResult _execute(List<String> command, Path workDir) {
            boolean reads = !"asm20".equals(command.get(command.indexOf("-x") + 1));
            if (!reads && failContigCommand) {
                return new ExecuteResult(9, null);
            }
            if (reads) {
                readCommands.add(List.copyOf(command));
                if (failReadCommand) {
                    return new ExecuteResult(9, null);
                }
            }
            try {
                int outputIndex = command.indexOf("-o");
                Path reference = Path.of(command.get(outputIndex + 2));
                String accession = Files.readAllLines(reference).get(0).substring(1);
                Files.writeString(Path.of(command.get(outputIndex + 1)),
                        (reads ? readPafs : contigPafs).getOrDefault(accession, ""));
                return new ExecuteResult(0, null);
            } catch (IOException e) {
                return new ExecuteResult(1, e);
            }
        }
    }
}
