package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

/**
 * Mapping-stage parameters.
 *
 * <p>The inherited evidence-quality thresholds are applied when generating
 * the complete coverage-depth table. They do not remove alignments from the
 * persisted BAM.</p>
 */
public class MappingParameters extends EvidenceQualityStageParameters {
}
