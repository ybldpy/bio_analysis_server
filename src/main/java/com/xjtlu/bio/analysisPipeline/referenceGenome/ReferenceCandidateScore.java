package com.xjtlu.bio.analysisPipeline.referenceGenome;

import com.fasterxml.jackson.annotation.JsonAlias;

public class ReferenceCandidateScore {
    private String referenceAccession;
    private String segmentKey;

    // Percentage in [0, 100], calculated from PAF matching/alignment bases.
    @JsonAlias("averageNucleotideIdentity")
    private double sequenceIdentity;

    // Fraction in [0, 1], calculated from the union of aligned reference intervals.
    @JsonAlias("alignmentFraction")
    private double referenceCoverage;

    private long alignedReferenceBases;
    private long referenceBases;
    private long matchingBases;
    private long alignmentBlockBases;
    private int alignmentCount;
    private double score;

    public ReferenceCandidateScore() {
    }

    public ReferenceCandidateScore(
            String referenceAccession,
            String segmentKey,
            double sequenceIdentity,
            double referenceCoverage,
            long alignedReferenceBases,
            long referenceBases,
            long matchingBases,
            long alignmentBlockBases,
            int alignmentCount,
            double score) {
        this.referenceAccession = referenceAccession;
        this.segmentKey = segmentKey;
        this.sequenceIdentity = sequenceIdentity;
        this.referenceCoverage = referenceCoverage;
        this.alignedReferenceBases = alignedReferenceBases;
        this.referenceBases = referenceBases;
        this.matchingBases = matchingBases;
        this.alignmentBlockBases = alignmentBlockBases;
        this.alignmentCount = alignmentCount;
        this.score = score;
    }

    public String getReferenceAccession() {
        return referenceAccession;
    }

    public void setReferenceAccession(String referenceAccession) {
        this.referenceAccession = referenceAccession;
    }

    public String getSegmentKey() {
        return segmentKey;
    }

    public void setSegmentKey(String segmentKey) {
        this.segmentKey = segmentKey;
    }

    public double getSequenceIdentity() {
        return sequenceIdentity;
    }

    public void setSequenceIdentity(double sequenceIdentity) {
        this.sequenceIdentity = sequenceIdentity;
    }

    public double getReferenceCoverage() {
        return referenceCoverage;
    }

    public void setReferenceCoverage(double referenceCoverage) {
        this.referenceCoverage = referenceCoverage;
    }

    public long getAlignedReferenceBases() {
        return alignedReferenceBases;
    }

    public void setAlignedReferenceBases(long alignedReferenceBases) {
        this.alignedReferenceBases = alignedReferenceBases;
    }

    public long getReferenceBases() {
        return referenceBases;
    }

    public void setReferenceBases(long referenceBases) {
        this.referenceBases = referenceBases;
    }

    public long getMatchingBases() {
        return matchingBases;
    }

    public void setMatchingBases(long matchingBases) {
        this.matchingBases = matchingBases;
    }

    public long getAlignmentBlockBases() {
        return alignmentBlockBases;
    }

    public void setAlignmentBlockBases(long alignmentBlockBases) {
        this.alignmentBlockBases = alignmentBlockBases;
    }

    public int getAlignmentCount() {
        return alignmentCount;
    }

    public void setAlignmentCount(int alignmentCount) {
        this.alignmentCount = alignmentCount;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}
