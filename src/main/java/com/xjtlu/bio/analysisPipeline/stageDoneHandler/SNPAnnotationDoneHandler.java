package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_SNP_ANNOTATION;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.stageResult.SNPAnnotationResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.SNPAnnotationStageOutput;


@Component
public class SNPAnnotationDoneHandler extends AbstractStageDoneHandler<SNPAnnotationStageOutput> implements StageDoneHandler<SNPAnnotationStageOutput>{

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_SNP_ANNOTATION;
    }

    @Override
    protected Pair<Map<String, String>, SNPAnnotationResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<SNPAnnotationStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        SNPAnnotationStageOutput snpAnnotationStageOutput = stageRunResult.getStageOutput();

        String annotatedVcf = this.createStoreObjectName(stageRunResult.getStageContext(), "annotated.vcf");
        return Pair.of(
            Map.of(snpAnnotationStageOutput.getAnnotatedFilePath().toString(), annotatedVcf),
            new SNPAnnotationResult(annotatedVcf)
        );
    }

}
