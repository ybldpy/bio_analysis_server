package com.xjtlu.bio.stageDoneHandler;


import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.StageRunResult;
import com.xjtlu.bio.taskrunner.stageOutput.VariantStageOutput;


import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;


import java.util.Map;



@Component
public class VarientStageDoneHandler extends AbstractStageDoneHandler<VariantStageOutput> implements StageDoneHandler<VariantStageOutput>{


    private static final String VCFGZ_NAME = "vcfGz";
    private static final String VCFTBI_NAME = "vcfTbi";

    @Override
    public int getType() {
        return PipelineService.PIPELINE_STAGE_VARIANT_CALL;
    }

    @Override
    protected Pair<Map<String, String>, Map<String, Object>> buildUploadConfigAndOutputUrlMap(
            StageRunResult<VariantStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        VariantStageOutput variantStageOutput = stageRunResult.getStageOutput();
        String vcfGzUrl = this.createStoreObjectName(stageRunResult.getStage(), VCFGZ_NAME);
        String vcfTbiUrl = this.createStoreObjectName(stageRunResult.getStage(), VCFTBI_NAME);

        return Pair.of(
            Map.of(variantStageOutput.getVcfGz(), vcfGzUrl, variantStageOutput.getVcfTbi(), vcfTbiUrl),
            Map.of(PipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ, vcfGzUrl, pipelineService.PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI, vcfTbiUrl)
        );
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
