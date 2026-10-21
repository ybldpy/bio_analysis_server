package com.xjtlu.bio.analysisPipeline.stageDoneHandler;


import com.xjtlu.bio.analysisPipeline.stageResult.VarientCallStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.VariantStageOutput;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_VARIANT_CALL;

import java.util.LinkedHashMap;
import java.util.Map;



@Component
public class VarientStageDoneHandler extends AbstractStageDoneHandler<VariantStageOutput> implements StageDoneHandler<VariantStageOutput>{


    // private static final String VCFGZ_NAME = "vcfGz";
    // private static final String VCFTBI_NAME = "vcfTbi";

    @Value("${analysis-pipeline.stage.varient.vcfFileName}")
    private String vcfFileName;
    @Value("${analysis-pipeline.stage.varient.vcfIndexFileName}")
    private String vcfFileIndexFileName;
    @Value("${analysis-pipeline.stage.varient.rawVcfFileName:variants.raw.vcf.gz}")
    private String rawVcfFileName;
    @Value("${analysis-pipeline.stage.varient.rawVcfIndexFileName:variants.raw.vcf.gz.tbi}")
    private String rawVcfFileIndexFileName;
    @Value("${analysis-pipeline.stage.varient.metricsFileName:${analysis-pipeline.stage.varient.qcFileName:variant.metrics.json}}")
    private String metricsFileName;

    @Override
    public int getType() {
        return PIPELINE_STAGE_VARIANT_CALL;
    }

    @Override
    protected Pair<Map<String, String>, VarientCallStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<VariantStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        VariantStageOutput variantStageOutput = stageRunResult.getStageOutput();
        String vcfGzUrl = this.createStoreObjectName(stageRunResult.getStage(), vcfFileName);
        String vcfTbiUrl = this.createStoreObjectName(stageRunResult.getStage(), vcfFileIndexFileName);
        Map<String, String> uploadConfig = new LinkedHashMap<>();
        uploadConfig.put(variantStageOutput.getVcfGz(), vcfGzUrl);
        uploadConfig.put(variantStageOutput.getVcfTbi(), vcfTbiUrl);

        String rawVcfGzUrl = null;
        String rawVcfTbiUrl = null;
        if (variantStageOutput.getRawVcfGz() != null
                && !variantStageOutput.getRawVcfGz().isBlank()
                && variantStageOutput.getRawVcfTbi() != null
                && !variantStageOutput.getRawVcfTbi().isBlank()) {
            rawVcfGzUrl = this.createStoreObjectName(
                    stageRunResult.getStage(), rawVcfFileName);
            rawVcfTbiUrl = this.createStoreObjectName(
                    stageRunResult.getStage(), rawVcfFileIndexFileName);
            uploadConfig.put(variantStageOutput.getRawVcfGz(), rawVcfGzUrl);
            uploadConfig.put(variantStageOutput.getRawVcfTbi(), rawVcfTbiUrl);
        }

        String metricsUrl = null;
        if (variantStageOutput.getMetricsJson() != null
                && !variantStageOutput.getMetricsJson().isBlank()) {
            metricsUrl = this.createStoreObjectName(
                    stageRunResult.getStage(), metricsFileName);
            uploadConfig.put(variantStageOutput.getMetricsJson(), metricsUrl);
        }

        return Pair.of(
            uploadConfig,
            new VarientCallStageResult(
                    rawVcfGzUrl,
                    rawVcfTbiUrl,
                    vcfGzUrl,
                    vcfTbiUrl,
                    metricsUrl)
        );
    }

    @Override
    protected int serializedOutputType() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'serializedOutputType'");
    }


    // @Override
    // public void handleStageDone(StageRunResult<VariantStageOutput> stageRunResult) {
    //     VariantStageOutput variantStageOutput = (VariantStageOutput) stageRunResult.getStageOutput();
    //     BioPipelineStage bioPipelineStage = stageRunResult.getStage();

    //     String vcfGzObjctName = createStoreObjectName(bioPipelineStage, "vcf.gz");


    //     String vcfTbiObjectName = createStoreObjectName(bioPipelineStage, "vcf.gz.tbi");

    //     boolean uploadSuccess = this.batchUploadObjectsFromLocal(Map.of(
    //             variantStageOutput.getVcfGz(),
    //             vcfGzObjctName,
    //             variantStageOutput.getVcfTbi(),
    //             vcfTbiObjectName
    //     ));

    //     Path resultDirPath = Path.of(variantStageOutput.getVcfGz()).getParent();

    //     if (!uploadSuccess) {
    //         this.handleUnsuccessUpload(bioPipelineStage, resultDirPath.toString());
    //         return;
    //     }
    //     this.deleteStageResultDir(resultDirPath.toString());

    //     HashMap<String,String> outputUrlMap = new HashMap<>();
    //     outputUrlMap.put(PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ, vcfGzObjctName);
    //     outputUrlMap.put(PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI, vcfTbiObjectName);

    //     String outputUrl = null;

    //     try {
    //         outputUrl = JsonUtil.toJson(outputUrlMap);
    //     } catch (JsonProcessingException e) {
    //         // TODO Auto-generated catch block
    //         logger.error("{} parsing json exception", bioPipelineStage.getStageId(), e);
    //         this.handleFail(bioPipelineStage, resultDirPath.toString());
    //         return;
    //     }


    //     int updateRes = this.updateStageFinish(bioPipelineStage, outputUrl);

    //     if (updateRes != 1) {
    //         return;
    //     }

    //     BioPipelineStageExample consensusStageExample = new BioPipelineStageExample();
    //     consensusStageExample.createCriteria()
    //             .andPipelineIdEqualTo(bioPipelineStage.getPipelineId())
    //             .andStageTypeEqualTo(PIPELINE_STAGE_CONSENSUS);

    //     List<BioPipelineStage> consensusStageList = pipelineService.getStagesFromExample(consensusStageExample);

    //     if (consensusStageList == null || consensusStageList.isEmpty()) {
    //         return;
    //     }

    //     BioPipelineStage consensusStage = consensusStageList.get(0);
    //     Map<String,String> inputMap = Map.of(PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ, vcfGzObjctName, PIPELINE_STAGE_CONSENSUS_INPUT_VCFGZ_TBI, vcfTbiObjectName);
    //     String serializedInputMap = null;
    //     try {
    //         serializedInputMap = JsonUtil.toJson(inputMap);
    //     } catch (JsonProcessingException e) {
    //         logger.error("{} happens exception when serialzing inputMap", bioPipelineStage, e);
    //         return;
    //     }

    //     BioPipelineStage updateConsensusStage = new BioPipelineStage();

    //     int curVersion = consensusStage.getVersion();
    //     consensusStage.setInputUrl(serializedInputMap);
    //     consensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);


    //     updateConsensusStage.setInputUrl(serializedInputMap);
    //     updateConsensusStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);

    //     updateConsensusStage.setVersion(curVersion+1);
    //     consensusStage.setVersion(curVersion+1);

    //     int res = this.updateStageFromVersion(updateConsensusStage, consensusStage.getStageId(),
    //             curVersion);
    //     if (res == 1) {
    //         pipelineService.addStageTask(consensusStage);
    //     }
    // }
}
