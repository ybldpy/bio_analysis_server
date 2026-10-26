package com.xjtlu.bio.analysisPipeline.stageMetrics;

/**
 * One candidate's comparison, independent of reference selection or ranking.
 * Identity is a percentage (0–100); coverage is a fraction (0–1) of the
 * candidate file's reference bases, with overlapping target intervals merged.
 * MATCHED means an alignment exists, not that a quality threshold was passed.
 */
public record ReferenceComparisonMetrics(
        Long referenceId,
        String referenceAccession,
        String sourceDb,
        Integer taxId,
        String organismName,
        String segment,
        String segmentKey,
        String status,
        double sequenceIdentity,
        double referenceCoverage,
        long alignedReferenceBases,
        long referenceBases,
        long matchingBases,
        long alignmentBlockBases,
        int alignmentCount) {

    public static final String MATCHED = "MATCHED";
    public static final String NO_MATCH = "NO_MATCH";
}
