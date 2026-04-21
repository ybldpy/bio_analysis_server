package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_DEPTH_COVERAGE;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.stageResult.CoverageDepthStageResult;

import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.CoverageDepthStageOutput;


@Component
public class CoverageDepthStageDoneHandler extends AbstractStageDoneHandler<CoverageDepthStageOutput> implements StageDoneHandler<CoverageDepthStageOutput>{

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_DEPTH_COVERAGE;
    }

    @Override
    protected Pair<Map<String, String>, CoverageDepthStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<CoverageDepthStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        String depthTableObjectName = this.createStoreObjectName(stageRunResult.getStageContext(), "depth.tsv");
        String summaryObjectName = this.createStoreObjectName(stageRunResult.getStageContext(), "summary.json");


        CoverageDepthStageOutput coverageDepthStageOutput = stageRunResult.getStageOutput();
        return Pair.of(
            Map.of(coverageDepthStageOutput.getDetphTable().toString(), depthTableObjectName, coverageDepthStageOutput.getSummary().toString(), summaryObjectName),
            new CoverageDepthStageResult(depthTableObjectName, summaryObjectName)
        );
        
    }



}
