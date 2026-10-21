package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

/**
 * Shared quality thresholds for read evidence used by coverage depth and
 * variant calling.
 *
 * <p>Each stage receives its own serialized snapshot of these values. The
 * common base class keeps their meaning and defaults consistent.</p>
 */
public class EvidenceQualityStageParameters extends BaseStageParams {

    public static final int DEFAULT_MINIMUM_MAPPING_QUALITY = 20;
    public static final int DEFAULT_MINIMUM_BASE_QUALITY = 20;

    private int minimumMappingQuality = DEFAULT_MINIMUM_MAPPING_QUALITY;
    private int minimumBaseQuality = DEFAULT_MINIMUM_BASE_QUALITY;

    public int getMinimumMappingQuality() {
        return minimumMappingQuality;
    }

    public void setMinimumMappingQuality(int minimumMappingQuality) {
        this.minimumMappingQuality = minimumMappingQuality;
    }

    public int getMinimumBaseQuality() {
        return minimumBaseQuality;
    }

    public void setMinimumBaseQuality(int minimumBaseQuality) {
        this.minimumBaseQuality = minimumBaseQuality;
    }
}
