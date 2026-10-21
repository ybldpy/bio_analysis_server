package com.xjtlu.bio.analysisPipeline.stageMetrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Reference-level alignment and depth metrics produced by the mapping stage.
 *
 * <p>The read counts are read-segment counts reported by {@code samtools
 * idxstats}; paired-end mates are therefore counted independently.</p>
 */
public class MappingResultMetrics {

    private long totalReferenceBases;
    private long coveredBases;
    private long zeroDepthBases;
    private double coverageFractionAt1x;
    private long depthSum;
    private double meanDepth;
    private long maxDepth;
    private long mappedReadSegments;
    private long unmappedReadSegments;
    private long totalReadSegments;
    private double mappingFraction;
    private List<ReferenceMetrics> references = new ArrayList<>();

    public MappingResultMetrics() {
    }

    public MappingResultMetrics(
            long totalReferenceBases,
            long coveredBases,
            long depthSum,
            long maxDepth,
            long mappedReadSegments,
            long unmappedReadSegments,
            List<ReferenceMetrics> references) {
        this.totalReferenceBases = totalReferenceBases;
        this.coveredBases = coveredBases;
        this.zeroDepthBases = totalReferenceBases - coveredBases;
        this.coverageFractionAt1x = fraction(coveredBases, totalReferenceBases);
        this.depthSum = depthSum;
        this.meanDepth = fraction(depthSum, totalReferenceBases);
        this.maxDepth = maxDepth;
        this.mappedReadSegments = mappedReadSegments;
        this.unmappedReadSegments = unmappedReadSegments;
        this.totalReadSegments = Math.addExact(mappedReadSegments, unmappedReadSegments);
        this.mappingFraction = fraction(mappedReadSegments, totalReadSegments);
        this.references = references == null ? new ArrayList<>() : new ArrayList<>(references);
    }

    private static double fraction(long numerator, long denominator) {
        return denominator == 0L ? 0.0d : (double) numerator / denominator;
    }

    public long getTotalReferenceBases() {
        return totalReferenceBases;
    }

    public void setTotalReferenceBases(long totalReferenceBases) {
        this.totalReferenceBases = totalReferenceBases;
    }

    public long getCoveredBases() {
        return coveredBases;
    }

    public void setCoveredBases(long coveredBases) {
        this.coveredBases = coveredBases;
    }

    public long getZeroDepthBases() {
        return zeroDepthBases;
    }

    public void setZeroDepthBases(long zeroDepthBases) {
        this.zeroDepthBases = zeroDepthBases;
    }

    public double getCoverageFractionAt1x() {
        return coverageFractionAt1x;
    }

    public void setCoverageFractionAt1x(double coverageFractionAt1x) {
        this.coverageFractionAt1x = coverageFractionAt1x;
    }

    public long getDepthSum() {
        return depthSum;
    }

    public void setDepthSum(long depthSum) {
        this.depthSum = depthSum;
    }

    public double getMeanDepth() {
        return meanDepth;
    }

    public void setMeanDepth(double meanDepth) {
        this.meanDepth = meanDepth;
    }

    public long getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(long maxDepth) {
        this.maxDepth = maxDepth;
    }

    public long getMappedReadSegments() {
        return mappedReadSegments;
    }

    public void setMappedReadSegments(long mappedReadSegments) {
        this.mappedReadSegments = mappedReadSegments;
    }

    public long getUnmappedReadSegments() {
        return unmappedReadSegments;
    }

    public void setUnmappedReadSegments(long unmappedReadSegments) {
        this.unmappedReadSegments = unmappedReadSegments;
    }

    public long getTotalReadSegments() {
        return totalReadSegments;
    }

    public void setTotalReadSegments(long totalReadSegments) {
        this.totalReadSegments = totalReadSegments;
    }

    /**
     * Fraction of all idxstats read segments that are mapped. Paired mates are
     * counted independently.
     */
    public double getMappingFraction() {
        return mappingFraction;
    }

    public void setMappingFraction(double mappingFraction) {
        this.mappingFraction = mappingFraction;
    }

    public List<ReferenceMetrics> getReferences() {
        return references;
    }

    public void setReferences(List<ReferenceMetrics> references) {
        this.references = references;
    }

    public static class ReferenceMetrics {

        private String referenceName;
        private long referenceLength;
        private long coveredBases;
        private long zeroDepthBases;
        private double coverageFractionAt1x;
        private long depthSum;
        private double meanDepth;
        private long maxDepth;
        private long mappedReadSegments;
        private long unmappedReadSegments;

        public ReferenceMetrics() {
        }

        public ReferenceMetrics(
                String referenceName,
                long referenceLength,
                long coveredBases,
                long depthSum,
                long maxDepth,
                long mappedReadSegments,
                long unmappedReadSegments) {
            this.referenceName = referenceName;
            this.referenceLength = referenceLength;
            this.coveredBases = coveredBases;
            this.zeroDepthBases = referenceLength - coveredBases;
            this.coverageFractionAt1x = fraction(coveredBases, referenceLength);
            this.depthSum = depthSum;
            this.meanDepth = fraction(depthSum, referenceLength);
            this.maxDepth = maxDepth;
            this.mappedReadSegments = mappedReadSegments;
            this.unmappedReadSegments = unmappedReadSegments;
        }

        public String getReferenceName() {
            return referenceName;
        }

        public void setReferenceName(String referenceName) {
            this.referenceName = referenceName;
        }

        public long getReferenceLength() {
            return referenceLength;
        }

        public void setReferenceLength(long referenceLength) {
            this.referenceLength = referenceLength;
        }

        public long getCoveredBases() {
            return coveredBases;
        }

        public void setCoveredBases(long coveredBases) {
            this.coveredBases = coveredBases;
        }

        public long getZeroDepthBases() {
            return zeroDepthBases;
        }

        public void setZeroDepthBases(long zeroDepthBases) {
            this.zeroDepthBases = zeroDepthBases;
        }

        public double getCoverageFractionAt1x() {
            return coverageFractionAt1x;
        }

        public void setCoverageFractionAt1x(double coverageFractionAt1x) {
            this.coverageFractionAt1x = coverageFractionAt1x;
        }

        public long getDepthSum() {
            return depthSum;
        }

        public void setDepthSum(long depthSum) {
            this.depthSum = depthSum;
        }

        public double getMeanDepth() {
            return meanDepth;
        }

        public void setMeanDepth(double meanDepth) {
            this.meanDepth = meanDepth;
        }

        public long getMaxDepth() {
            return maxDepth;
        }

        public void setMaxDepth(long maxDepth) {
            this.maxDepth = maxDepth;
        }

        public long getMappedReadSegments() {
            return mappedReadSegments;
        }

        public void setMappedReadSegments(long mappedReadSegments) {
            this.mappedReadSegments = mappedReadSegments;
        }

        public long getUnmappedReadSegments() {
            return unmappedReadSegments;
        }

        public void setUnmappedReadSegments(long unmappedReadSegments) {
            this.unmappedReadSegments = unmappedReadSegments;
        }
    }
}
