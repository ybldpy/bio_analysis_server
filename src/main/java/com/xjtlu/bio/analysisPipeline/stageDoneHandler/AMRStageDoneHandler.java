package com.xjtlu.bio.analysisPipeline.stageDoneHandler;



import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_AMR;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.xjtlu.bio.analysisPipeline.stageResult.AMRStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.AmrStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

public class AMRStageDoneHandler extends AbstractStageDoneHandler<AmrStageOutput> implements StageDoneHandler<AmrStageOutput>{



    private static String AMR_OUTPUT_NAME = "amr_out.tsv";
    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_AMR;
    }

    @Override
    protected int serializedOutputType() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'serializedOutputType'");
    }

    @Override
    protected Pair<Map<String, String>, AMRStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<AmrStageOutput> stageRunResult) {
        // TODO Auto-generated method stub

        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        AmrStageOutput amrStageOutput = stageRunResult.getStageOutput();


        String amrResultFileUrl = this.createStoreObjectName(bioPipelineStage, AMR_OUTPUT_NAME);
        Map<String,String> uploadSpec = Map.of(amrStageOutput.getAmrResultPath().toString(), amrResultFileUrl);

        return Pair.of(
            uploadSpec, 
            new AMRStageResult(amrResultFileUrl)
        );

    }



}
