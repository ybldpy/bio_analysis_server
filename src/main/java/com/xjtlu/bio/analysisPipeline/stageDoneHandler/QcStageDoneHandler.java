package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import com.xjtlu.bio.analysisPipeline.stageResult.QcResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.QCStageOutput;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_QC;


@Component
public class QcStageDoneHandler extends AbstractStageDoneHandler<QCStageOutput> implements StageDoneHandler<QCStageOutput>{

    @Value("${analysis-pipeline.stage.qc.r1FileName}")
    private String r1OutputName;
    @Value("${analysis-pipeline.stage.qc.r1FileGzName}")
    private String r1GzOutputName;

    @Value("${analysis-pipeline.stage.qc.r2FileName}")
    private String r2OutputName;
    @Value("${analysis-pipeline.stage.qc.r2FileGzName}")
    private String r2GzOutputName;


    @Value("${analysis-pipeline.stage.qc.HTMLFileName}")
    private String htmlFileName;

    @Value("${analysis-pipeline.stage.qc.JSONFileName}")
    private String jsonFileName;


    @Override
    public int getType() {
        return PIPELINE_STAGE_QC;
    }


    @Override
    protected Pair<Map<String, String>, QcResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<QCStageOutput> stageRunResult) {

        String r1Path = stageRunResult.getStageOutput().getR1Path();
        String r2Path = stageRunResult.getStageOutput().getR2Path();


        String r1Url = this.createStoreObjectName(stageRunResult.getStage(), readObjectName("trimmed_r1", r1Path));
        String r2Url = r2Path == null ? null
                : this.createStoreObjectName(stageRunResult.getStage(), readObjectName("trimmed_r2", r2Path));
        String jsonUrl = this.createStoreObjectName(stageRunResult.getStage(), jsonFileName);
        String htmlUrl = this.createStoreObjectName(stageRunResult.getStage(), htmlFileName);

        HashMap<String,String> uploadConfig = new HashMap<>();
        HashMap<String,Object> outputUrlMap = new HashMap<>();
        uploadConfig.put(stageRunResult.getStageOutput().getR1Path(), r1Url);
        if(r2Url!=null){
            uploadConfig.put(stageRunResult.getStageOutput().getR2Path(), r2Url);
        }
        uploadConfig.put(stageRunResult.getStageOutput().getJsonPath(), jsonUrl);

        uploadConfig.put(stageRunResult.getStageOutput().getHtmlPath(), htmlUrl);

        return Pair.of(
            uploadConfig,
            new QcResult(r1Url, r2Url, htmlUrl, jsonUrl)
        );
    }

    private String readObjectName(String prefix, String path) {
        String fileName = substractFileNameFromPath(path);
        int extensionEnd = fileName.endsWith(".gz") ? fileName.length() - 3 : fileName.length();
        int extensionStart = fileName.lastIndexOf('.', extensionEnd - 1);
        if (extensionStart < 0) {
            throw new IllegalArgumentException("QC output filename has no extension: " + fileName);
        }
        return prefix + fileName.substring(extensionStart);
    }

}
