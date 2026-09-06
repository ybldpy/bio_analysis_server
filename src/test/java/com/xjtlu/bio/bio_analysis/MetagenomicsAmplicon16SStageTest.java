package com.xjtlu.bio.bio_analysis;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_METAGENOMICS_AMPLICON16S;

import java.nio.file.Files;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MetagenomicsAnalysisStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.Amplicon16SAnalysisStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.Amplicon16SAnalysisStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest",
        "analysis-pipeline.stage.baseWorkDir=/home/jcy/bioTest/workDir/metagenomicsAmplicon16STest",
        "analysis-pipeline.stage.baseInputDir=/home/jcy/bioTest/inputDir/metagenomicsAmplicon16STest"
})
@ActiveProfiles("dev")
public class MetagenomicsAmplicon16SStageTest {

    @Resource
    Amplicon16SAnalysisStageExecutor amplicon16SAnalysisStageExecutor;

    @Test
    public void doTest() throws JsonProcessingException {

        BioPipelineStage bioPipelineStage = new BioPipelineStage();
        bioPipelineStage.setPipelineId(0L);
        bioPipelineStage.setStageId(0L);

        BaseStageParams baseStageParams = new BaseStageParams();
        bioPipelineStage.setParameters(JsonUtil.toJson(baseStageParams));

        MetagenomicsAnalysisStageInputUrls inputUrls = new MetagenomicsAnalysisStageInputUrls();
        inputUrls.setR1Url("sinput/metagenomics16STest/16sTest_1.fastq");
        inputUrls.setR2Url("sinput/metagenomics16STest/16sTest_2.fastq");
        bioPipelineStage.setInputUrl(JsonUtil.toJson(inputUrls));

        bioPipelineStage.setVersion(0);
        bioPipelineStage.setStageType(PIPELINE_STAGE_METAGENOMICS_AMPLICON16S);

        StageRunResult<Amplicon16SAnalysisStageOutput> stageRunResult = amplicon16SAnalysisStageExecutor
                .execute(bioPipelineStage);

        Assertions.assertNotNull(stageRunResult);
        Assertions.assertNotNull(stageRunResult.getStageOutput());

        Amplicon16SAnalysisStageOutput output = stageRunResult.getStageOutput();

        Assertions.assertTrue(Files.exists(output.getAsvTablePath()));
        Assertions.assertTrue(Files.exists(output.getRepresentativeSequencesPath()));
        Assertions.assertTrue(Files.exists(output.getTaxonomyPath()));
        Assertions.assertTrue(Files.exists(output.getRelativeAbundancePath()));
        Assertions.assertTrue(Files.exists(output.getGenusAbundancePath()));
        Assertions.assertTrue(Files.exists(output.getAlphaDiversityPath()));
        Assertions.assertNull(output.getBetaDiversityPath());
        Assertions.assertNull(output.getSummaryPath());
    }

}
