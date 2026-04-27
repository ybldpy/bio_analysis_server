package com.xjtlu.bio.bio_analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.xjtlu.bio.analysisPipeline.context.ReadMeta;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReadInspectStageInputUrls;
import com.xjtlu.bio.analysisPipeline.taskrunner.ReadInspectStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReadInspectStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest"
})
@ActiveProfiles("dev")
class BioAnalysisApplicationTests {

    @Resource
    public ReadInspectStageExecutor readInspectStageExecutor;

    @Test
    void contextLoads() {
    }

    @Test
    void testDuplicateInsert() {
        System.out.println("Testing duplicate insert:");
        // sampleService.testInsertDuplicate("1", 1);
    }

    @Test
    public void testReadInspectExecutor() throws Exception {

        // 1. 准备输入
        ReadInspectStageInputUrls readInspectStageInputUrls = new ReadInspectStageInputUrls();

        // TODO: 换成你真实的测试 FASTQ 文件路径
        // 单端：
        // readInspectStageInputUrls.setRead1Url("/home/jcy/bio_app_data/test/read/sample.fastq.gz");

        // interleaved：
        readInspectStageInputUrls.setRead1Url("sinput/SRR38251028.fastq.gz");

        BioPipelineStage bioPipelineStage = new BioPipelineStage();
        bioPipelineStage.setStageId(0L);
        bioPipelineStage.setInputUrl(JsonUtil.toJson(readInspectStageInputUrls));
        bioPipelineStage.setVersion(0);
        bioPipelineStage.setStageType(0);

        StageRunResult<ReadInspectStageOutput> stageRunResult = null;

        try {
            // 2. 执行
            stageRunResult = readInspectStageExecutor.execute(bioPipelineStage);

            // 3. 判断执行是否成功
            assertNotNull(stageRunResult);
            assertTrue(stageRunResult.isSuccess(), "Read inspect stage should succeed");

            ReadInspectStageOutput output = stageRunResult.getStageOutput();
            assertNotNull(output, "ReadInspectStageOutput should not be null");

            // 6. 判断输出路径
            if (output.getR1Path() != null && output.getR2Path() != null) {
                // interleaved 被拆成 paired-end
                assertTrue(Files.exists(output.getR1Path()), "R1 output file should exist");
                assertTrue(Files.exists(output.getR2Path()), "R2 output file should exist");

                System.out.println("Interleaved FASTQ detected.");
                System.out.println("R1 path: " + output.getR1Path());
                System.out.println("R2 path: " + output.getR2Path());

            } else if (output.getR1Path() != null) {
                // single-end
                assertTrue(Files.exists(output.getR1Path()), "Single read file should exist");

                System.out.println("Single-end FASTQ detected.");
                System.out.println("Read path: " + output.getR1Path());

            } else {
                // 如果你的设计里 null/null 表示不产生新文件，也可以允许
                System.out.println("No new read output file generated.");
            }


            System.out.println("Length type " + stageRunResult.getStageOutput().getReadLenType());

        } finally {
            // 7. 清理临时目录
            if (stageRunResult != null
                    && stageRunResult.getStageOutput() != null
                    && stageRunResult.getStageOutput().getParentPath() != null) {

                // FileUtils.deleteDirectory(
                //         stageRunResult.getStageOutput().getParentPath().toFile());
            }
        }
    }

}
