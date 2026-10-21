package com.xjtlu.bio.analysisPipeline.taskrunner.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.VarientCallParameters;
import com.xjtlu.bio.utils.JsonUtil;

class VariantFilterExpressionBuilderTest {

    @Test
    void buildsDefaultSingleSampleHaploidFilter() {
        VarientCallParameters parameters = new VarientCallParameters();

        assertEquals(
                "GT!=\"alt\" || QUAL<20.0 || FORMAT/DP[0]<10"
                        + " || FORMAT/AD[0:GT]<5"
                        + " || FORMAT/AD[0:GT]/FORMAT/DP[0]<0.75",
                VariantFilterExpressionBuilder.buildExclusionExpression(parameters));
    }

    @Test
    void usesRuntimeOverridesAndPersistsThemInStageJson() throws Exception {
        VarientCallParameters parameters = new VarientCallParameters();
        parameters.setThreads(6);
        parameters.setMinimumMappingQuality(15);
        parameters.setMinimumBaseQuality(18);
        parameters.setMinimumVariantQuality(35.5d);
        parameters.setMinimumVariantDepth(25);
        parameters.setMinimumAlternateDepth(12);
        parameters.setMinimumAlternateAlleleFraction(0.9d);

        String json = JsonUtil.toJson(parameters);
        VarientCallParameters restored = JsonUtil.toObject(
                json, VarientCallParameters.class);

        assertEquals(6, restored.getThreads());
        assertEquals(15, restored.getMinimumMappingQuality());
        assertEquals(18, restored.getMinimumBaseQuality());
        assertEquals(35.5d, restored.getMinimumVariantQuality());
        assertEquals(25, restored.getMinimumVariantDepth());
        assertEquals(12, restored.getMinimumAlternateDepth());
        assertEquals(0.9d, restored.getMinimumAlternateAlleleFraction());
        assertEquals(
                "GT!=\"alt\" || QUAL<35.5 || FORMAT/DP[0]<25"
                        + " || FORMAT/AD[0:GT]<12"
                        + " || FORMAT/AD[0:GT]/FORMAT/DP[0]<0.9",
                VariantFilterExpressionBuilder.buildExclusionExpression(restored));
    }

    @Test
    void rejectsInvalidRuntimeConfiguration() {
        VarientCallParameters parameters = new VarientCallParameters();
        parameters.setMinimumAlternateAlleleFraction(1.01d);

        assertThrows(
                IllegalArgumentException.class,
                () -> VariantFilterExpressionBuilder.buildExclusionExpression(
                        parameters));

        parameters.setMinimumAlternateAlleleFraction(0.75d);
        parameters.setThreads(0);
        assertThrows(
                IllegalArgumentException.class,
                () -> VariantFilterExpressionBuilder.buildExclusionExpression(
                        parameters));

        parameters.setThreads(2);
        parameters.setMinimumVariantDepth(0);
        assertThrows(
                IllegalArgumentException.class,
                () -> VariantFilterExpressionBuilder.buildExclusionExpression(
                        parameters));
    }
}
