package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import java.util.List;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;

public class ReferenceSelectionStageParameters extends BaseStageParams {
    private List<ReferenceSequence> candidateReferences;
    private int threads = 2;
    private boolean readFallbackEnabled = true;
    // Provisional engineering defaults; validate these for the intended datasets.
    // Coverage is a fraction [0, 1]; identity is a percentage [0, 100].
    private double minimumReadReferenceCoverage = 0.5d;
    private double minimumReadSequenceIdentity = 80.0d;
    private int minimumReadMappingQuality = 20;
    // Short reads default to sr. Long reads require an explicit platform preset.
    private String readFallbackPreset;

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

    public boolean isReadFallbackEnabled() {
        return readFallbackEnabled;
    }

    public void setReadFallbackEnabled(boolean readFallbackEnabled) {
        this.readFallbackEnabled = readFallbackEnabled;
    }

    public double getMinimumReadReferenceCoverage() {
        return minimumReadReferenceCoverage;
    }

    public void setMinimumReadReferenceCoverage(double minimumReadReferenceCoverage) {
        this.minimumReadReferenceCoverage = minimumReadReferenceCoverage;
    }

    public double getMinimumReadSequenceIdentity() {
        return minimumReadSequenceIdentity;
    }

    public void setMinimumReadSequenceIdentity(double minimumReadSequenceIdentity) {
        this.minimumReadSequenceIdentity = minimumReadSequenceIdentity;
    }

    public int getMinimumReadMappingQuality() {
        return minimumReadMappingQuality;
    }

    public void setMinimumReadMappingQuality(int minimumReadMappingQuality) {
        this.minimumReadMappingQuality = minimumReadMappingQuality;
    }

    public String getReadFallbackPreset() {
        return readFallbackPreset;
    }

    public void setReadFallbackPreset(String readFallbackPreset) {
        this.readFallbackPreset = readFallbackPreset;
    }
}
