package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceComparisonResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;

@Component
public class ReferenceComparisonStageDoneHandler extends AbstractStageDoneHandler<ReferenceComparisonStageOutput>{

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
    }

    @Override
    protected Pair<Map<String, String>, ReferenceComparisonResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ReferenceComparisonStageOutput> stageRunResult) {

        ReferenceComparisonStageOutput referenceComparisonStageOutput = stageRunResult.getStageOutput();
        Path pafLocalPath = referenceComparisonStageOutput.getAlignmentPafPath();
        Path differenceTsvPath =referenceComparisonStageOutput.getDifferenceTsvPath();


        String pafUrl = this.createStoreObjectName(stageRunResult.getStageContext(), "alignment.paf");
        String differenceTsvUrl = this.createStoreObjectName(stageRunResult.getStageContext(), "reference_difference.tsv");

        ReferenceComparisonResult referenceComparisonResult = new ReferenceComparisonResult(pafUrl, differenceTsvUrl);

        return Pair.of(
            Map.of(pafLocalPath.toString(), pafUrl, differenceTsvPath.toString(), differenceTsvUrl),
            referenceComparisonResult
        );

    }



}
