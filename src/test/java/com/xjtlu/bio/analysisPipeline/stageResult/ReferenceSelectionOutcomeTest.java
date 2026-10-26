package com.xjtlu.bio.analysisPipeline.stageResult;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSelectionOutcome;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.utils.JsonUtil;

class ReferenceSelectionOutcomeTest {

    @ParameterizedTest
    @EnumSource(ReferenceSelectionOutcome.class)
    void businessOutcomeSurvivesJsonRoundTrip(ReferenceSelectionOutcome outcome) throws Exception {
        ReferenceSelectionStageResult result = new ReferenceSelectionStageResult();
        result.setOutcome(outcome);

        ReferenceSelectionStageResult restored = JsonUtil.toObject(
                JsonUtil.toJson(result), ReferenceSelectionStageResult.class);

        assertEquals(outcome, restored.getOutcome());
    }

    @Test
    void absentBusinessOutcomeDoesNotTurnMissingOrLegacyOutputIntoNoMatch() throws Exception {
        ReferenceSelectionStageResult result = JsonUtil.toObject(
                "{\"selectedReference\":null,\"candidateScores\":[]}", ReferenceSelectionStageResult.class);

        assertNull(result.getOutcome());
    }

    @Test
    void allUnmatchedSegmentsAreNotAPartialSuccess() {
        ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                null, List.of(), Path.of("report.tsv"), List.of("SEGMENT_1", "SEGMENT_2"));
        output.setOutcome(ReferenceSelectionOutcome.NO_MATCH);
        ReferenceSelectionStageResult result = new ReferenceSelectionStageResult(
                null, List.of(), output.getUnmatchedSegmentKeys());
        result.setOutcome(output.getOutcome());

        assertFalse(output.isPartial());
        assertFalse(result.isPartial());
        assertEquals(2, result.getUnmatchedSegmentKeys().size());
    }
}
