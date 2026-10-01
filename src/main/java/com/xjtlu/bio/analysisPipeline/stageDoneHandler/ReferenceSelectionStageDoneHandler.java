package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;

@Component
public class ReferenceSelectionStageDoneHandler
        extends AbstractStageDoneHandler<ReferenceSelectionStageOutput> {

    @Override
    public int getType() {
        return PIPELINE_STAGE_REFERENCE_SELECTION;
    }

    @Override
    protected Pair<Map<String, String>, ReferenceSelectionStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ReferenceSelectionStageOutput> stageRunResult) {

        ReferenceSelectionStageOutput stageOutput = stageRunResult.getStageOutput();
        ReferenceSelectionStageResult result = new ReferenceSelectionStageResult(
                stageOutput.getSelectedReference(),
                stageOutput.getCandidateScores());

        return Pair.of(Map.of(), result);
    }
}
