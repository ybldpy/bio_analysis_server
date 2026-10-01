package com.xjtlu.bio.analysisPipeline.stageResult;

import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;

public class ReferenceSelectionStageResult implements StageResult {
    private ReferenceGenome selectedReference;
    private List<ReferenceCandidateScore> candidateScores;

    public ReferenceSelectionStageResult() {
    }

    public ReferenceSelectionStageResult(ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores) {
        this.selectedReference = selectedReference;
        this.candidateScores = candidateScores;
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
}
