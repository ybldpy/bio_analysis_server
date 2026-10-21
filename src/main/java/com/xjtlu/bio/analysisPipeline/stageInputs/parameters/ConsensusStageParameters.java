package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

public class ConsensusStageParameters extends BaseStageParams {

    public static final int DEFAULT_MINIMUM_CONSENSUS_DEPTH = 10;
    public static final boolean DEFAULT_APPLY_ONLY_PASSING_VARIANTS = true;

    /**
     * Positions below this read depth are masked with N in the consensus.
     */
    private int minimumConsensusDepth = DEFAULT_MINIMUM_CONSENSUS_DEPTH;

    /**
     * When enabled, only accepted VCF records may alter the consensus. PASS
     * records are accepted, as are unfiltered records ({@code .}) for backward
     * compatibility with results produced before the filtering chain existed.
     * Rejected records remain available in the filtered VCF for audit, but do
     * not change the reference base.
     */
    private boolean applyOnlyPassingVariants =
            DEFAULT_APPLY_ONLY_PASSING_VARIANTS;

    public int getMinimumConsensusDepth() {
        return minimumConsensusDepth;
    }

    public void setMinimumConsensusDepth(int minimumConsensusDepth) {
        this.minimumConsensusDepth = minimumConsensusDepth;
    }

    public boolean isApplyOnlyPassingVariants() {
        return applyOnlyPassingVariants;
    }

    public void setApplyOnlyPassingVariants(boolean applyOnlyPassingVariants) {
        this.applyOnlyPassingVariants = applyOnlyPassingVariants;
    }
}
