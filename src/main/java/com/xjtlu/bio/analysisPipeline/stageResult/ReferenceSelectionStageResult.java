package com.xjtlu.bio.analysisPipeline.stageResult;

import java.util.ArrayList;
import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSelectionOutcome;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics;

public class ReferenceSelectionStageResult implements StageResult {
    private ReferenceGenome selectedReference;
    private List<ReferenceCandidateScore> candidateScores;
    private List<String> unmatchedSegmentKeys = new ArrayList<>();
    private ReferenceSelectionReadFallbackMetrics readFallbackMetrics;
    // Null is retained for legacy results; absent output is not an explicit NO_MATCH.
    private ReferenceSelectionOutcome outcome;

    public ReferenceSelectionStageResult() {
    }

    public ReferenceSelectionStageResult(ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores) {
        this(selectedReference, candidateScores, List.of());
    }

    public ReferenceSelectionStageResult(ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores,
            List<String> unmatchedSegmentKeys) {
        this.selectedReference = selectedReference;
        this.candidateScores = candidateScores;
        setUnmatchedSegmentKeys(unmatchedSegmentKeys);
    }

    public ReferenceGenome getSelectedReference() {
        return selectedReference;
    }

    public void setSelectedReference(ReferenceGenome selectedReference) {
        this.selectedReference = selectedReference;
    }

    public List<ReferenceCandidateScore> getCandidateScores() {
        return candidateScores;
    }

    public void setCandidateScores(List<ReferenceCandidateScore> candidateScores) {
        this.candidateScores = candidateScores;
    }

    public List<String> getUnmatchedSegmentKeys() {
        return unmatchedSegmentKeys;
    }

    public void setUnmatchedSegmentKeys(List<String> unmatchedSegmentKeys) {
        this.unmatchedSegmentKeys = unmatchedSegmentKeys == null
                ? new ArrayList<>()
                : new ArrayList<>(unmatchedSegmentKeys);
    }

    public boolean isPartial() {
        return selectedReference != null && !selectedReference.getSequences().isEmpty()
                && !unmatchedSegmentKeys.isEmpty();
    }

    public ReferenceSelectionOutcome getOutcome() {
        return outcome;
    }

    public void setOutcome(ReferenceSelectionOutcome outcome) {
        this.outcome = outcome;
    }

    public ReferenceSelectionReadFallbackMetrics getReadFallbackMetrics() {
        return readFallbackMetrics;
    }

    public void setReadFallbackMetrics(ReferenceSelectionReadFallbackMetrics readFallbackMetrics) {
        this.readFallbackMetrics = readFallbackMetrics;
    }
}
