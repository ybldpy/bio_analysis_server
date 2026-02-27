package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.xjtlu.bio.analysisPipeline.stageResult.SeroTypeResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;

public class SeroTypeStageDoneHandler extends AbstractStageDoneHandler<SeroTypingStageOutput> implements StageDoneHandler<SeroTypingStageOutput>{


    private static final String serotypeResultName = "serotypeResult";

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_SEROTYPE;
    }

    @Override
    protected int serializedOutputType() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'serializedOutputType'");
    }

    @Override
    protected Pair<Map<String, String>, SeroTypeResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<SeroTypingStageOutput> stageRunResult) {
        // TODO Auto-generated method stub

        BioPipelineStage stage = stageRunResult.getStage();
        Path resultPath = stageRunResult.getStageOutput().getResultPath();
        
        String resultFileName = resultPath.getFileName().toString();
        String url = this.createStoreObjectName(stage, serotypeResultName + resultFileName.substring(resultFileName.lastIndexOf(".")));

        return Pair.of(
            Map.of(resultPath.toString(), url), 
            new SeroTypeResult(url)
        );

        
    }



}
