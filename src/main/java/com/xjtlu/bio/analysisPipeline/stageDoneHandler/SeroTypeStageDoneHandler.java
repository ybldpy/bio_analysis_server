package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SEROTYPE;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.SeroTypeResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SeroTypingStageOutput;


@Component
public class SeroTypeStageDoneHandler extends AbstractStageDoneHandler<SeroTypingStageOutput> implements StageDoneHandler<SeroTypingStageOutput>{


    private static final String serotypeResultName = "serotypeResult";

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_SEROTYPE;
    }

    @Override
    protected int serializedOutputType() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'serializedOutputType'");
    }

    @Override
    protected Pair<Map<String, String>, SeroTypeResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<SeroTypingStageOutput> stageRunResult) {
        StageContext stage = stageRunResult.getStageContext();
        SeroTypingStageOutput output = stageRunResult.getStageOutput();
        Path resultPath = output.getResultPath();
        Path kaptiveKPath = output.getKaptiveKResultPath();
        Path kaptiveOPath = output.getKaptiveOResultPath();
        if ((kaptiveKPath == null) != (kaptiveOPath == null)) {
            throw new IllegalArgumentException("Kaptive K and O reports must be supplied together");
        }
        
        String resultFileName = resultPath.getFileName().toString();
        String url = this.createStoreObjectName(stage, serotypeResultName + resultFileName.substring(resultFileName.lastIndexOf(".")));

        Map<String, String> uploads = new LinkedHashMap<>();
        uploads.put(resultPath.toString(), url);
        SeroTypeResult result = new SeroTypeResult(url);
        if (kaptiveKPath != null) {
            String kaptiveKUrl = createStoreObjectName(stage, "kaptive_k.tsv");
            String kaptiveOUrl = createStoreObjectName(stage, "kaptive_o.tsv");
            uploads.put(kaptiveKPath.toString(), kaptiveKUrl);
            uploads.put(kaptiveOPath.toString(), kaptiveOUrl);
            result.setKaptiveKResultUrl(kaptiveKUrl);
            result.setKaptiveOResultUrl(kaptiveOUrl);
        }
        return Pair.of(uploads, result);

        
    }



}
