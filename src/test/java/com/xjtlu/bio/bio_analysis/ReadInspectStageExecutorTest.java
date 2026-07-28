package com.xjtlu.bio.bio_analysis;

import java.nio.file.Files;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.ReadInspectStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.analysisPipeline.workflow.AnalysisPipelineStagesBuilder;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.annotation.Resource;


@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest",
        "analysis-pipeline.stage.baseWorkDir=/home/jcy/bioTest/workDir",
        "analysis-pipeline.stage.baseInputDir=/home/jcy/bioTest/inputDir"
})
@ActiveProfiles("dev")
class ReadInspectStageExecutorBlackBoxTest {

    @Resource
    private ReadInspectStageExecutor readInspectStageExecutor;

    static Stream<Arguments> readInspectCases() {
        return Stream.of(
                Arguments.of(
                        "interleaved fastq should be split",
                        "read-inspect/interleaved.fastq.gz",
                        null,
                        false,
                        null,
                        true,
                        true,
                        Constants.SequenceInput.READ_LEN_TYPE_SHORT
                ),
                Arguments.of(
                        "single-end fastq should keep original",
                        "read-inspect/single.fastq.gz",
                        null,
                        true,
                        "read-inspect/single.fastq.gz",
                        false,
                        false,
                        Constants.SequenceInput.READ_LEN_TYPE_SHORT
                ),
                Arguments.of(
                        "paired separate fastq should keep original",
                        "read-inspect/paired_R1.fastq.gz",
                        "read-inspect/paired_R2.fastq.gz",
                        true,
                        "read-inspect/paired_R1.fastq.gz",
                        false,
                        false,
                        Constants.SequenceInput.READ_LEN_TYPE_SHORT
                ),
                Arguments.of(
                        "long-read fastq should be long read and keep original",
                        "read-inspect/long.fastq.gz",
                        null,
                        true,
                        "read-inspect/long.fastq.gz",
                        false,
                        false,
                        Constants.SequenceInput.READ_LEN_TYPE_LONG
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("readInspectCases")
    void testReadInspectBlackBox(
            String caseName,
            String read1Url,
            String read2Url,
            boolean expectedUseOriginal,
            String expectedOriginalSequenceUrl,
            boolean expectedR1PathPresent,
            boolean expectedR2PathPresent,
            int expectedReadLenType
    ) throws Exception {

        BioPipelineStage stage = buildStage(read1Url, read2Url);

        StageRunResult<ReadInspectStageOutput> result =
                readInspectStageExecutor.execute(stage);

        assertNotNull(result);
        assertTrue(result.isSuccess(), caseName + " should succeed");

        ReadInspectStageOutput output = result.getStageOutput();
        assertNotNull(output);

        assertEquals(
                expectedUseOriginal,
                output.isUseOriginalSequence(),
                caseName + " useOriginalSequence mismatch"
        );

        assertEquals(
                expectedOriginalSequenceUrl,
                output.getOriginalSequenceUrl(),
                caseName + " originalSequenceUrl mismatch"
        );

        if (expectedR1PathPresent) {
            assertNotNull(output.getR1Path(), caseName + " r1Path should exist");
            assertTrue(Files.exists(output.getR1Path()), caseName + " r1Path file should exist");
        } else {
            assertNull(output.getR1Path(), caseName + " r1Path should be null");
        }

        if (expectedR2PathPresent) {
            assertNotNull(output.getR2Path(), caseName + " r2Path should exist");
            assertTrue(Files.exists(output.getR2Path()), caseName + " r2Path file should exist");
        } else {
            assertNull(output.getR2Path(), caseName + " r2Path should be null");
        }

        assertEquals(
                expectedReadLenType,
                output.getReadLenType(),
                caseName + " readLenType mismatch"
        );

        assertEquals(
                Constants.SequenceInput.QUALITY_ENCODING_33,
                output.getQualityEncoding(),
                caseName + " qualityEncoding mismatch"
        );
    }

    private BioPipelineStage buildStage(String read1Url, String read2Url) throws Exception {
        ReadInspectStageInputUrls inputUrls = new ReadInspectStageInputUrls();
        inputUrls.setRead1Url(read1Url);
        inputUrls.setRead2Url(read2Url);

        BaseStageParams params = new BaseStageParams();
        //AnalysisPipelineStagesBuilder.initializeParameters(params, Constants.SequenceInput.SEQUENCE_LEVEL_READ, BaseStageParams.ANALYSIS_TARGET_TYPE_BACTERIA, false, null);
        
        BioPipelineStage stage = new BioPipelineStage();

        // 这些字段按你项目里 BioPipelineStage 的实际必填项补。
        stage.setStageType(Constants.StageType.PIPELINE_STAGE_READ_INSPECT);
        stage.setStageIndex(0);
        stage.setStatus(Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING);

        stage.setInputUrl(JsonUtil.toJson(inputUrls));
        stage.setParameters(JsonUtil.toJson(params));
        

        return stage;
    }
}
