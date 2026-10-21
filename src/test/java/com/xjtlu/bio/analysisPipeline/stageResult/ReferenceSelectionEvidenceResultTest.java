package com.xjtlu.bio.analysisPipeline.stageResult;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore.EvidenceSource;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.ReferenceSelectionStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics.Status;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

class ReferenceSelectionEvidenceResultTest {

    @TempDir
    Path tempDir;

    @Test
    void legacyResultDefaultsScoresToContigsWithoutInventingFallbackMetrics() throws Exception {
        ReferenceSelectionStageResult result = JsonUtil.toObject("""
                {
                  "candidateScores": [{
                    "referenceAccession": "TEST_REFERENCE_1",
                    "segmentKey": "SEGMENT_1",
                    "averageNucleotideIdentity": 98.0,
                    "alignmentFraction": 0.75,
                    "score": 73.5
                  }]
                }
                """, ReferenceSelectionStageResult.class);

        assertEquals(1, result.getCandidateScores().size());
        ReferenceCandidateScore score = result.getCandidateScores().get(0);
        assertEquals(EvidenceSource.CONTIGS, score.getEvidenceSource());
        assertNull(score.getRejectionReason());
        assertEquals(98.0d, score.getSequenceIdentity());
        assertEquals(0.75d, score.getReferenceCoverage());
        assertNull(result.getReadFallbackMetrics());
        assertTrue(result.getUnmatchedSegmentKeys().isEmpty());
    }

    @Test
    void acceptedAndRejectedReadsEvidenceRoundTripWithoutLosingReasonsOrMeasurements()
            throws Exception {
        ReferenceCandidateScore accepted = readsScore("TEST_REFERENCE_2", 2, 0.9d, null);
        ReferenceCandidateScore rejected = readsScore(
                "TEST_REFERENCE_3", 3, 0.1d, "Reference coverage is below the configured minimum");

        for (ReferenceCandidateScore original : List.of(accepted, rejected)) {
            ReferenceCandidateScore restored = JsonUtil.toObject(
                    JsonUtil.toJson(original), ReferenceCandidateScore.class);

            assertEquals(EvidenceSource.READS, restored.getEvidenceSource());
            assertEquals(original.getReferenceAccession(), restored.getReferenceAccession());
            assertEquals(original.getSegmentKey(), restored.getSegmentKey());
            assertEquals(original.getRejectionReason(), restored.getRejectionReason());
            assertEquals(original.getReferenceCoverage(), restored.getReferenceCoverage());
            assertEquals(original.getSequenceIdentity(), restored.getSequenceIdentity());
            assertEquals(original.getAlignedReferenceBases(), restored.getAlignedReferenceBases());
            assertEquals(original.getReferenceBases(), restored.getReferenceBases());
            assertEquals(original.getAlignmentCount(), restored.getAlignmentCount());
            assertEquals(original.getScore(), restored.getScore());
        }
    }

    @Test
    void doneHandlerPreservesEvidenceAppliedPolicyAndUnmatchedSegmentsInStoredResult()
            throws Exception {
        ReferenceSequence selected = referenceSequence("TEST_REFERENCE_2", 2);
        ReferenceCandidateScore accepted = readsScore(selected.getAccession(), 2, 0.9d, null);
        ReferenceCandidateScore rejected = readsScore(
                "TEST_REFERENCE_3", 3, 0.1d, "Reference coverage is below the configured minimum");
        ReferenceCandidateScore contigsScore = new ReferenceCandidateScore(
                selected.getAccession(), selected.getSegmentKey(), 0.0d, 0.0d,
                0L, 100L, 0L, 0L, 0, 0.0d);
        ReferenceSelectionReadFallbackMetrics metrics = new ReferenceSelectionReadFallbackMetrics(
                Status.COMPLETED, "TEST_UNVALIDATED_DEFAULTS", 0.8d, 95.0d, 25, 2, 1);
        ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                ReferenceGenome.fromSingleSequence(selected),
                List.of(contigsScore, accepted, rejected),
                tempDir.resolve("reference_selection.tsv"), List.of("SEGMENT_3"));
        output.setReadFallbackMetrics(metrics);

        ReferenceSelectionStageResult restored = storedResult(output);

        assertEquals(metrics, restored.getReadFallbackMetrics());
        assertEquals(Status.COMPLETED, restored.getReadFallbackMetrics().status());
        assertEquals("TEST_UNVALIDATED_DEFAULTS", restored.getReadFallbackMetrics().preset());
        assertEquals(0.8d, restored.getReadFallbackMetrics().minimumReferenceCoverage());
        assertEquals(95.0d, restored.getReadFallbackMetrics().minimumSequenceIdentity());
        assertEquals(25, restored.getReadFallbackMetrics().minimumMappingQuality());
        assertEquals(2, restored.getReadFallbackMetrics().evaluatedCandidates());
        assertEquals(1, restored.getReadFallbackMetrics().acceptedGroups());
        assertEquals(List.of("SEGMENT_3"), restored.getUnmatchedSegmentKeys());
        assertTrue(restored.isPartial());
        assertEquals(1, restored.getSelectedReference().getSequences().size());
        assertEquals(selected.getAccession(),
                restored.getSelectedReference().getSequences().get(0).getAccession());
        assertEquals(3, restored.getCandidateScores().size());
        assertEquals(EvidenceSource.CONTIGS, restored.getCandidateScores().get(0).getEvidenceSource());
        assertEquals(EvidenceSource.READS, restored.getCandidateScores().get(1).getEvidenceSource());
        assertNull(restored.getCandidateScores().get(1).getRejectionReason());
        assertEquals(EvidenceSource.READS, restored.getCandidateScores().get(2).getEvidenceSource());
        assertEquals(rejected.getRejectionReason(),
                restored.getCandidateScores().get(2).getRejectionReason());
    }

    @Test
    void doneHandlerPreservesReasonsWhyReadsFallbackWasNotPerformed() throws Exception {
        ReferenceSequence selected = referenceSequence("TEST_REFERENCE_1", 1);
        for (Status status : List.of(
                Status.NO_READS, Status.DISABLED, Status.NOT_NEEDED, Status.UNSUPPORTED_READ_TYPE)) {
            ReferenceSelectionReadFallbackMetrics metrics = new ReferenceSelectionReadFallbackMetrics(
                    status, "TEST_CONFIGURED_POLICY", 0.6d, 92.0d, 20, 0, 0);
            ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                    ReferenceGenome.fromSingleSequence(selected), List.of(),
                    tempDir.resolve("reference_selection.tsv"));
            output.setReadFallbackMetrics(metrics);

            ReferenceSelectionStageResult restored = storedResult(output);

            assertEquals(metrics, restored.getReadFallbackMetrics());
            assertEquals(status, restored.getReadFallbackMetrics().status());
            assertTrue(restored.getUnmatchedSegmentKeys().isEmpty());
        }
    }

    private ReferenceSelectionStageResult storedResult(ReferenceSelectionStageOutput output)
            throws Exception {
        StageRunResult<ReferenceSelectionStageOutput> runResult = StageRunResult.OK(
                output, new StageContext(42L, 0, PIPELINE_STAGE_REFERENCE_SELECTION, 7), tempDir);
        Pair<Map<String, String>, ReferenceSelectionStageResult> prepared =
                new ExposedHandler().prepare(runResult);
        assertTrue(prepared.getLeft().isEmpty());
        assertNotNull(prepared.getRight());
        return JsonUtil.toObject(JsonUtil.toJson(prepared.getRight()), ReferenceSelectionStageResult.class);
    }

    private static ReferenceSequence referenceSequence(String accession, int ordinal) {
        ReferenceSequence sequence = new ReferenceSequence();
        sequence.setAccession(accession);
        sequence.setTaxId(123);
        sequence.setSegmentOrdinal(ordinal);
        sequence.setPath("refseq/test/" + accession + ".fna");
        return sequence;
    }

    private static ReferenceCandidateScore readsScore(
            String accession, int ordinal, double coverage, String rejectionReason) {
        ReferenceCandidateScore score = new ReferenceCandidateScore(
                accession, "SEGMENT_" + ordinal, 98.0d, coverage,
                Math.round(100L * coverage), 100L, 98L, 100L, 2, 98.0d * coverage);
        score.setEvidenceSource(EvidenceSource.READS);
        score.setRejectionReason(rejectionReason);
        return score;
    }

    private static final class ExposedHandler extends ReferenceSelectionStageDoneHandler {
        Pair<Map<String, String>, ReferenceSelectionStageResult> prepare(
                StageRunResult<ReferenceSelectionStageOutput> runResult) {
            return buildUploadConfigAndOutputUrlMap(runResult);
        }
    }
}
