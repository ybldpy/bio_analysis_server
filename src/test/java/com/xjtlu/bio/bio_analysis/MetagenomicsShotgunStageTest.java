package com.xjtlu.bio.bio_analysis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.FileSystemUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.MetagenomicsAnalysisStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;
import com.xjtlu.bio.analysisPipeline.taskrunner.MetagenomicShotgunStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MetagenomicsShotgunAnalysisStageOutput;
import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_METAGENOMICS_SHORTGUN;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;

import jakarta.annotation.Resource;

@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest",
        "analysis-pipeline.stage.baseWorkDir=/home/jcy/bioTest/workDir/metagenomicsShotgunTest",
        "analysis-pipeline.stage.baseInputDir=/home/jcy/bioTest/inputDir/metagenomicsShotgunTest"
})
@ActiveProfiles("dev")
public class MetagenomicsShotgunStageTest {

    @Resource
    MetagenomicShotgunStageExecutor metagenomicShotgunStageExecutor;

    @Resource
    AnalysisPipelineToolsConfig analysisPipelineToolsConfig;

    @Test
    public void testCheckM2() throws IOException, InterruptedException {

        Path workDir = Path.of(
                "/home/jcy/bioTest/workDir/metagenomicsShotgunTest/0");

        Path binsDir = workDir.resolve("binning/bins");

        Path checkM2OutputDir = workDir.resolve("checkm2-test");

        Path checkM2DatabasePath = Path.of(
                "/home/jcy/bioData/checkm2_database/uniref100.KO.1.dmnd");

        Assertions.assertTrue(
                Files.isDirectory(binsDir),
                "MetaBAT2 bins directory does not exist: " + binsDir);

        if (Files.exists(checkM2OutputDir)) {
            FileSystemUtils.deleteRecursively(checkM2OutputDir);
        }

        List<String> cmd = new ArrayList<>();
        cmd.addAll(analysisPipelineToolsConfig.getCheckm2());

        cmd.addAll(List.of(
                "predict",
                "--input", binsDir.toString(),
                "--output-dir", checkM2OutputDir.toString(),
                "--threads", "4",
                "--database_path", checkM2DatabasePath.toString(),
                "-x", "fa"));

        System.out.println("CheckM2 command: " + String.join(" ", cmd));

        ProcessBuilder processBuilder = new ProcessBuilder(cmd);

        processBuilder.directory(workDir.toFile());
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(ProcessBuilder.Redirect.INHERIT);

        Process process = processBuilder.start();

        int exitCode = process.waitFor();

        Assertions.assertEquals(
                0,
                exitCode,
                "CheckM2 execution failed, exit code: " + exitCode);

        Path binQualityPath = checkM2OutputDir.resolve("quality_report.tsv");

        Assertions.assertTrue(
                Files.exists(binQualityPath),
                "CheckM2 quality report was not generated: " + binQualityPath);
    }

    @Test
    public void doTest() throws JsonProcessingException {

        BioPipelineStage bioPipelineStage = new BioPipelineStage();
        bioPipelineStage.setPipelineId(0L);
        bioPipelineStage.setStageId(0L);

        BaseStageParams baseStageParams = new BaseStageParams();

        bioPipelineStage.setParameters(
                JsonUtil.toJson(baseStageParams));

        MetagenomicsAnalysisStageInputUrls inputUrls = new MetagenomicsAnalysisStageInputUrls();

        inputUrls.setR1Url(
                "sinput/metagenomicsShotgunTest/sample_0_200k_R1.fastq.gz");

        inputUrls.setR2Url(
                "sinput/metagenomicsShotgunTest/sample_0_200k_R2.fastq.gz");

        bioPipelineStage.setInputUrl(
                JsonUtil.toJson(inputUrls));

        bioPipelineStage.setVersion(0);
        bioPipelineStage.setStageType(PIPELINE_STAGE_METAGENOMICS_SHORTGUN);

        StageRunResult<MetagenomicsShotgunAnalysisStageOutput> stageRunResult = metagenomicShotgunStageExecutor
                .execute(bioPipelineStage);

        Assertions.assertNotNull(stageRunResult);
        Assertions.assertNotNull(stageRunResult.getStageOutput());

        MetagenomicsShotgunAnalysisStageOutput output = stageRunResult.getStageOutput();

        Assertions.assertTrue(
                Files.exists(output.getKraken2ReportPath()));

        Assertions.assertTrue(
                Files.exists(output.getSpeciesAbundancePath()));

        Assertions.assertTrue(
                Files.exists(output.getAlphaDiversityPath()));

        Assertions.assertTrue(
                Files.exists(output.getContigsPath()));

        Assertions.assertTrue(
                Files.exists(output.getAssemblySummaryPath()));

        Assertions.assertTrue(
                Files.exists(output.getPredictedGenesPath()));

        Assertions.assertTrue(
                Files.exists(output.getPredictedProteinsPath()));

        Assertions.assertTrue(
                Files.exists(output.getFunctionalAnnotationPath()));

        Assertions.assertTrue(
                Files.isDirectory(output.getBinsDir()));

        Assertions.assertTrue(
                Files.exists(output.getBinQualityPath()));

    }

}
