package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;

public class ReferenceComparisonStageParameters extends BaseStageParams {

    // Null preserves historical stages that only carry referenceGenome.
    // An explicitly empty candidate list is invalid, not a request for fallback.
    private List<ReferenceSequence> candidateReferences;

    public List<ReferenceSequence> getCandidateReferences() {
        return candidateReferences;
    }

    public void setCandidateReferences(List<ReferenceSequence> candidateReferences) {
        this.candidateReferences = candidateReferences;
    }
}
