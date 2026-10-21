package com.xjtlu.bio.analysisPipeline.stageMetrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Base-level coverage metrics used to audit consensus masking.
 */
public class ConsensusResultMetrics {

    private int minimumDepth;
    private long totalBases;
    private long unmaskedBases;
    private long maskedBases;
    private double unmaskedFraction;
    private double maskedFraction;
    private List<SequenceMetrics> sequences = new ArrayList<>();

    public ConsensusResultMetrics() {
    }

    public ConsensusResultMetrics(
            int minimumDepth,
            long totalBases,
            long unmaskedBases,
            long maskedBases,
            List<SequenceMetrics> sequences) {
        this.minimumDepth = minimumDepth;
        this.totalBases = totalBases;
        this.unmaskedBases = unmaskedBases;
        this.maskedBases = maskedBases;
        this.unmaskedFraction = fraction(unmaskedBases, totalBases);
        this.maskedFraction = fraction(maskedBases, totalBases);
        this.sequences = sequences == null ? new ArrayList<>() : new ArrayList<>(sequences);
    }

    private static double fraction(long numerator, long denominator) {
        return denominator == 0L ? 0.0d : (double) numerator / denominator;
    }

    public int getMinimumDepth() {
        return minimumDepth;
    }

    public void setMinimumDepth(int minimumDepth) {
        this.minimumDepth = minimumDepth;
    }

    public long getTotalBases() {
        return totalBases;
    }

    public void setTotalBases(long totalBases) {
        this.totalBases = totalBases;
    }

    public long getUnmaskedBases() {
        return unmaskedBases;
    }

    public void setUnmaskedBases(long unmaskedBases) {
        this.unmaskedBases = unmaskedBases;
    }

    public long getMaskedBases() {
        return maskedBases;
    }

    public void setMaskedBases(long maskedBases) {
        this.maskedBases = maskedBases;
    }

    public double getUnmaskedFraction() {
        return unmaskedFraction;
    }

    public void setUnmaskedFraction(double unmaskedFraction) {
        this.unmaskedFraction = unmaskedFraction;
    }

    public double getMaskedFraction() {
        return maskedFraction;
    }

    public void setMaskedFraction(double maskedFraction) {
        this.maskedFraction = maskedFraction;
    }

    public List<SequenceMetrics> getSequences() {
        return sequences;
    }

    public void setSequences(List<SequenceMetrics> sequences) {
        this.sequences = sequences;
    }

    public static class SequenceMetrics {

        private String sequenceName;
        private long totalBases;
        private long unmaskedBases;
        private long maskedBases;
        private double unmaskedFraction;
        private double maskedFraction;

        public SequenceMetrics() {
        }

        public SequenceMetrics(
                String sequenceName,
                long totalBases,
                long unmaskedBases,
                long maskedBases) {
            this.sequenceName = sequenceName;
            this.totalBases = totalBases;
            this.unmaskedBases = unmaskedBases;
            this.maskedBases = maskedBases;
            this.unmaskedFraction = fraction(unmaskedBases, totalBases);
            this.maskedFraction = fraction(maskedBases, totalBases);
        }

        public String getSequenceName() {
            return sequenceName;
        }

        public void setSequenceName(String sequenceName) {
            this.sequenceName = sequenceName;
        }

        public long getTotalBases() {
            return totalBases;
        }

        public void setTotalBases(long totalBases) {
            this.totalBases = totalBases;
        }

        public long getUnmaskedBases() {
            return unmaskedBases;
        }

        public void setUnmaskedBases(long unmaskedBases) {
            this.unmaskedBases = unmaskedBases;
        }

        public long getMaskedBases() {
            return maskedBases;
        }

        public void setMaskedBases(long maskedBases) {
            this.maskedBases = maskedBases;
        }

        public double getUnmaskedFraction() {
            return unmaskedFraction;
        }

        public void setUnmaskedFraction(double unmaskedFraction) {
            this.unmaskedFraction = unmaskedFraction;
        }

        public double getMaskedFraction() {
            return maskedFraction;
        }

        public void setMaskedFraction(double maskedFraction) {
            this.maskedFraction = maskedFraction;
        }
    }
}
