package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

/**
 * Runtime configuration for haploid variant calling and the VCF acceptance
 * filter used by downstream consensus generation.
 *
 * <p>The defaults provide a reproducible baseline profile. They are serialized
 * with the stage so that every run records the exact policy it used. A
 * validated pipeline profile can override them for a particular sequencing
 * protocol or analysis target.</p>
 */
public class VarientCallParameters extends EvidenceQualityStageParameters {

    public static final int DEFAULT_THREADS = 2;
    public static final double DEFAULT_MINIMUM_VARIANT_QUALITY = 20.0d;
    public static final int DEFAULT_MINIMUM_VARIANT_DEPTH = 10;
    public static final int DEFAULT_MINIMUM_ALTERNATE_DEPTH = 5;
    public static final double DEFAULT_MINIMUM_ALTERNATE_ALLELE_FRACTION = 0.75d;

    private int threads = DEFAULT_THREADS;
    private double minimumVariantQuality = DEFAULT_MINIMUM_VARIANT_QUALITY;
    private int minimumVariantDepth = DEFAULT_MINIMUM_VARIANT_DEPTH;
    private int minimumAlternateDepth = DEFAULT_MINIMUM_ALTERNATE_DEPTH;
    private double minimumAlternateAlleleFraction =
            DEFAULT_MINIMUM_ALTERNATE_ALLELE_FRACTION;

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public double getMinimumVariantQuality() {
        return minimumVariantQuality;
    }

    public void setMinimumVariantQuality(double minimumVariantQuality) {
        this.minimumVariantQuality = minimumVariantQuality;
    }

    public int getMinimumVariantDepth() {
        return minimumVariantDepth;
    }

    public void setMinimumVariantDepth(int minimumVariantDepth) {
        this.minimumVariantDepth = minimumVariantDepth;
    }

    public int getMinimumAlternateDepth() {
        return minimumAlternateDepth;
    }

    public void setMinimumAlternateDepth(int minimumAlternateDepth) {
        this.minimumAlternateDepth = minimumAlternateDepth;
    }

    public double getMinimumAlternateAlleleFraction() {
        return minimumAlternateAlleleFraction;
    }

    public void setMinimumAlternateAlleleFraction(
            double minimumAlternateAlleleFraction) {
        this.minimumAlternateAlleleFraction = minimumAlternateAlleleFraction;
    }
}
