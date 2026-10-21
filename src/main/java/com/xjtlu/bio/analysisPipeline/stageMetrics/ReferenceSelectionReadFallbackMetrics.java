package com.xjtlu.bio.analysisPipeline.stageMetrics;

/** Records whether optional reads were examined and the applied acceptance policy. */
public record ReferenceSelectionReadFallbackMetrics(
        Status status,
        String preset,
        double minimumReferenceCoverage,
        double minimumSequenceIdentity,
        int minimumMappingQuality,
        int evaluatedCandidates,
        int acceptedGroups) {

    public enum Status {
        NO_READS, DISABLED, NOT_NEEDED, COMPLETED, UNSUPPORTED_READ_TYPE
    }
}
