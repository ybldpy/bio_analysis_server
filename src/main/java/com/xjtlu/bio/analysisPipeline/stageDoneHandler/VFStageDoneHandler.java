package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.VFResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VirulenceFactorStageOutput;

@Component
public class VFStageDoneHandler extends AbstractStageDoneHandler<VirulenceFactorStageOutput>{

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_VIRULENCE;
    }

    @Override
    protected Pair<Map<String, String>, VFResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<VirulenceFactorStageOutput> stageRunResult) {
        // TODO Auto-generated method stub

        VirulenceFactorStageOutput virulenceFactorStageOutput = stageRunResult.getStageOutput();
        Path tsvResultLocalPath = virulenceFactorStageOutput.getVfResult();

        String outputName = "amr.tsv";
        String tsvUrl = createStoreObjectName(stageRunResult.getStageContext(), outputName);

        return Pair.of(
            Map.of(tsvResultLocalPath.toString(), tsvUrl),
            new VFResult(tsvUrl)
        );
        
    }




}
