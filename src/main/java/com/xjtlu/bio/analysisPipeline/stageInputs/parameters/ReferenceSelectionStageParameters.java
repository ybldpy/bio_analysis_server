package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;

public class ReferenceSelectionStageParameters extends BaseStageParams {
    private List<ReferenceSequence> candidateReferences;
    private int threads = 2;

    public ReferenceSelectionStageParameters() {
    }

    public ReferenceSelectionStageParameters(List<ReferenceSequence> candidateReferences) {
        this.candidateReferences = candidateReferences;
    }

    public List<ReferenceSequence> getCandidateReferences() {
        return candidateReferences;
    }

    public void setCandidateReferences(List<ReferenceSequence> candidateReferences) {
        this.candidateReferences = candidateReferences;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }
}
