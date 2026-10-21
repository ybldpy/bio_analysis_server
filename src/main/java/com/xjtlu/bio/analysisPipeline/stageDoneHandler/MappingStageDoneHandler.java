package com.xjtlu.bio.analysisPipeline.stageDoneHandler;


import com.xjtlu.bio.analysisPipeline.stageResult.MappingResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MappingStageOutput;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_MAPPING;

@Component
public class MappingStageDoneHandler extends AbstractStageDoneHandler<MappingStageOutput> implements StageDoneHandler<MappingStageOutput>{


    @Value("${analysis-pipeline.stage.mapping.bamFileName}")
    private String bamFileName;

    @Value("${analysis-pipeline.stage.mapping.bamIndexFileName}")
    private String bamIndexFileName;

    private static final String COVERAGE_DEPTH_FILE_NAME = "coverage_depth.tsv";
    private static final String METRICS_FILE_NAME = MappingStageOutput.METRICS_JSON;

    @Override
    public int getType() {
        return PIPELINE_STAGE_MAPPING;
    }


    // @Override
    // protected Map<String, String> createOutputUrlMap(StageRunResult<MappingStageOutput> stageOutput) {
    //     // TODO Auto-generated method stub
    //     return Map.of(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY, this.createStoreObjectName(stageOutput.getStage(), BAM_NAME), PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY, this.createStoreObjectName(stageOutput.getStage(), BAM_INDEX_NAME));
    // }

    // @Override
    // protected boolean batchUploadObjectsFromLocal(StageRunResult<MappingStageOutput> stageRunResult) {
    //     // TODO Auto-generated method stub
    //     return batchUploadObjectsFromLocal(Map.of(stageRunResult.getStageOutput().getBamPath(), this.createStoreObjectName(stageRunResult.getStage(), BAM_NAME), stageRunResult.getStageOutput().getBamIndexPath(), this.createStoreObjectName(stageRunResult.getStage(), BAM_INDEX_NAME)));
    // }


    @Override
    protected Pair<Map<String, String>, MappingResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<MappingStageOutput> stageRunResult) {

        String bamUrl = this.createStoreObjectName(stageRunResult.getStage(),bamFileName);
        String bamIndexUrl = this.createStoreObjectName(stageRunResult.getStage(), bamIndexFileName);
        String coverageDepthUrl = this.createStoreObjectName(stageRunResult.getStage(), COVERAGE_DEPTH_FILE_NAME);
        MappingStageOutput stageOutput = stageRunResult.getStageOutput();
        String metricsUrl = stageOutput.getMetricsJsonPath() == null
                ? null
                : this.createStoreObjectName(stageRunResult.getStage(), METRICS_FILE_NAME);

        Map<String, String> uploadConfig = new LinkedHashMap<>();
        uploadConfig.put(stageOutput.getBamPath().toAbsolutePath().toString(), bamUrl);
        uploadConfig.put(stageOutput.getBamIndexPath().toAbsolutePath().toString(), bamIndexUrl);
        uploadConfig.put(
                stageOutput.getCoverageDepthPath().toAbsolutePath().toString(),
                coverageDepthUrl);
        if (stageOutput.getMetricsJsonPath() != null) {
            uploadConfig.put(
                    stageOutput.getMetricsJsonPath().toAbsolutePath().toString(),
                    metricsUrl);
        }

        return Pair.of(
            uploadConfig,
            new MappingResult(
                bamUrl,
                bamIndexUrl,
                coverageDepthUrl,
                metricsUrl)
        );
    }
}
