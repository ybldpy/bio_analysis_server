package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import java.util.Objects;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;

/** Builds and validates the bcftools expression used to soft-filter calls. */
public final class VariantFilterExpressionBuilder {

    public static final String LOW_CONFIDENCE_FILTER_NAME = "LowConfidence";

    private VariantFilterExpressionBuilder() {
    }

    /**
     * Returns an exclusion expression: records for which the expression is
     * true are tagged {@value #LOW_CONFIDENCE_FILTER_NAME}.
     *
     * <p>The pipeline currently produces a single-sample, haploid VCF. The
     * {@code FORMAT/AD[0:GT]} expression therefore addresses the depth of the
     * allele selected by the first sample's genotype without assuming that it
     * is always the first ALT in a multiallelic record.</p>
     */
    public static String buildExclusionExpression(
            VarientCallParameters parameters) {
        validate(parameters);
        return String.join(
                " || ",
                "GT!=\"alt\"",
                "QUAL<" + Double.toString(parameters.getMinimumVariantQuality()),
                "FORMAT/DP[0]<" + parameters.getMinimumVariantDepth(),
                "FORMAT/AD[0:GT]<" + parameters.getMinimumAlternateDepth(),
                "FORMAT/AD[0:GT]/FORMAT/DP[0]<"
                        + Double.toString(
                                parameters.getMinimumAlternateAlleleFraction()));
    }

    public static void validate(VarientCallParameters parameters) {
        Objects.requireNonNull(parameters, "variant call parameters must not be null");
        requireAtLeast(parameters.getThreads(), 1, "threads");
        requireAtLeast(parameters.getMinimumMappingQuality(), 0,
                "minimumMappingQuality");
        requireAtLeast(parameters.getMinimumBaseQuality(), 0,
                "minimumBaseQuality");
        requireFiniteAtLeast(parameters.getMinimumVariantQuality(), 0.0d,
                "minimumVariantQuality");
        requireAtLeast(parameters.getMinimumVariantDepth(), 1,
                "minimumVariantDepth");
        requireAtLeast(parameters.getMinimumAlternateDepth(), 0,
                "minimumAlternateDepth");

        double alleleFraction = parameters.getMinimumAlternateAlleleFraction();
        if (!Double.isFinite(alleleFraction)
                || alleleFraction < 0.0d
                || alleleFraction > 1.0d) {
            throw new IllegalArgumentException(
                    "minimumAlternateAlleleFraction must be between 0 and 1");
        }
    }

    private static void requireAtLeast(int value, int minimum, String name) {
        if (value < minimum) {
            throw new IllegalArgumentException(
                    name + " must be at least " + minimum);
        }
    }

    private static void requireFiniteAtLeast(
            double value, double minimum, String name) {
        if (!Double.isFinite(value) || value < minimum) {
            throw new IllegalArgumentException(
                    name + " must be finite and at least " + minimum);
        }
    }
}
