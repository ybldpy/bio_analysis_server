package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics;

public class ReferenceSelectionStageOutput implements StageOutput {
    private ReferenceGenome selectedReference;
    private List<ReferenceCandidateScore> candidateScores;
    private Path comparisonReportPath;
    private List<String> unmatchedSegmentKeys = new ArrayList<>();
    private ReferenceSelectionReadFallbackMetrics readFallbackMetrics;

    public ReferenceSelectionStageOutput() {
    }

    public ReferenceSelectionStageOutput(ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores,
            Path comparisonReportPath) {
        this(selectedReference, candidateScores, comparisonReportPath, List.of());
    }

    public ReferenceSelectionStageOutput(ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores,
            Path comparisonReportPath,
            List<String> unmatchedSegmentKeys) {
        this.selectedReference = selectedReference;
        this.candidateScores = candidateScores;
        this.comparisonReportPath = comparisonReportPath;
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

    public Path getComparisonReportPath() {
        return comparisonReportPath;
    }

    public void setComparisonReportPath(Path comparisonReportPath) {
        this.comparisonReportPath = comparisonReportPath;
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
        return !unmatchedSegmentKeys.isEmpty();
    }

    public ReferenceSelectionReadFallbackMetrics getReadFallbackMetrics() {
        return readFallbackMetrics;
    }

    public void setReadFallbackMetrics(ReferenceSelectionReadFallbackMetrics readFallbackMetrics) {
        this.readFallbackMetrics = readFallbackMetrics;
    }

    @Override
    public Path getParentPath() {
        return comparisonReportPath == null ? null : comparisonReportPath.getParent();
    }
}
