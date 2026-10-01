package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;
import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;

public class ReferenceSelectionStageOutput implements StageOutput {
    private ReferenceGenome selectedReference;
    private List<ReferenceCandidateScore> candidateScores;
    private Path comparisonReportPath;

    public ReferenceSelectionStageOutput() {
    }

    public ReferenceSelectionStageOutput(ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores,
            Path comparisonReportPath) {
        this.selectedReference = selectedReference;
        this.candidateScores = candidateScores;
        this.comparisonReportPath = comparisonReportPath;
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

    @Override
    public Path getParentPath() {
        return comparisonReportPath == null ? null : comparisonReportPath.getParent();
    }
}
