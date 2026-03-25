package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MLST;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;

import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.MLSTStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MLSTStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

public class MLSTStageDoneHandler extends AbstractStageDoneHandler<MLSTStageOutput> implements StageDoneHandler<MLSTStageOutput>{


    //TODO


    @Value("${analysis-pipeline.stage.mlst.tsvFileName}")
    private String mlstOutputFileName;

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_MLST;
    }

    @Override
    protected int serializedOutputType() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'serializedOutputType'");
    }

    @Override
    protected Pair<Map<String, String>, MLSTStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<MLSTStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        StageContext stage = stageRunResult.getStage();
        MLSTStageOutput mlstStageOutput = stageRunResult.getStageOutput();

        String url = this.createStoreObjectName(stage, mlstOutputFileName);
        return Pair.of(
            Map.of(mlstStageOutput.getMlstPath().toString(), url),
            new MLSTStageResult(url)
        );

    }

}
