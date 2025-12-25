package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;
import com.xjtlu.bio.taskrunner.stageOutput.QCStageOutput;

@Component
public class QcStageExecutor extends AbstractPipelineStageExector {

    private String qcCmd;

    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrlsJson = bioPipelineStage.getInputUrl();
        Map<String, String> inputUrls = null;

        try {
            inputUrls = objectMapper.readValue(inputUrlsJson, Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            return StageRunResult.fail("解析输入参数错误", bioPipelineStage);
        }

        String inputUrl1 = inputUrls.get(PipelineService.PIPELINE_STAGE_INPUT_READ1_KEY);
        String input1FileName = inputUrl1.substring(inputUrl1.lastIndexOf("/") + 1);
        String inputUrl2 = inputUrls.size() > 1 ? inputUrls.get(pipelineService.PIPELINE_STAGE_INPUT_READ2_KEY) : null;
        String input2FileName = inputUrl2 == null ? null : inputUrl2.substring(inputUrl2.lastIndexOf("/") + 1);

        Path outputDir = Paths
                .get(String.format("%s/%d/output/qc", stageResultTmpBasePath, bioPipelineStage.getStageId()));

        try {
            Files.createDirectories(outputDir);
        } catch (IOException e) {
            return StageRunResult.fail("IO错误\n" + e.getMessage(), bioPipelineStage);
        }

        Path trimmedR1Path = outputDir.resolve(appendSuffixBeforeExtensions(input1FileName, "_trimmed"));
        Path trimmedR2Path = inputUrl2 == null ? null
                : outputDir.resolve(appendSuffixBeforeExtensions(input2FileName, "_trimmed"));

        Path outputQcJson = outputDir.resolve("qc_json.json");
        Path outputQcHtml = outputDir.resolve("qc_html.html");

        GetObjectResult objectResult = storageService.getObject(inputUrl1,
                String.format("%s/%d/input/%s", stageResultTmpBasePath, bioPipelineStage.getStageId(), input1FileName));
        if (objectResult.e() != null) {
            return StageRunResult.fail(objectResult.e().getMessage(), bioPipelineStage);
        }

        File inputFile1 = objectResult.objectFile();
        File inputFile2 = null;
        if (StringUtils.isNotBlank(inputUrl2)) {
            GetObjectResult r2ObjectGetResult = storageService.getObject(inputUrl2, String.format("%s/%d/input/%s",
                    stageResultTmpBasePath, bioPipelineStage.getStageId(), input2FileName));
            if (null != r2ObjectGetResult.e()) {
                inputFile1.delete();
                return StageRunResult.fail(objectResult.e().getMessage(), bioPipelineStage);
            }
            inputFile2 = objectResult.objectFile();
        }

        List<String> cmd = new ArrayList<>();
        cmd.add(qcCmd);
        if (inputUrl2 != null) {
            // 双端
            cmd.addAll(List.of(
                    "-i", inputFile1.getAbsolutePath(),
                    "-I", inputFile2.getAbsolutePath(),
                    "-o", trimmedR1Path.toString(),
                    "-O", trimmedR2Path.toString()));
        } else {
            // 单端
            cmd.addAll(List.of(
                    "-i", inputFile1.getAbsolutePath(),
                    "-o", trimmedR1Path.toString()));
        }
        cmd.addAll(List.of(
                "--json", outputQcJson.toString(),
                "--html", outputQcHtml.toString(),
                "--thread", String.valueOf(Math.max(2, Runtime.getRuntime().availableProcessors() / 4))));

        int runResult = -1;
        try {
            runResult = runSubProcess(cmd, outputDir);
        } catch (IOException | InterruptedException e) {
            return StageRunResult.fail("QC 子进程异常: " + e.getMessage(), bioPipelineStage);
        }

        if (runResult != 0) {
            return StageRunResult.fail("QC 退出码=" + runResult, bioPipelineStage);
        }

        if (!Files.exists(trimmedR1Path) || (inputUrl2 != null && !Files.exists(trimmedR2Path))
                || !Files.exists(outputQcJson) || !Files.exists(outputQcHtml)) {
            try {
                Files.delete(trimmedR1Path);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                Files.delete(trimmedR2Path);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                Files.delete(outputQcJson);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                Files.delete(outputQcHtml);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            inputFile1.delete();
            inputFile2.delete();
            return StageRunResult.fail("qc工具未产出结果", bioPipelineStage);
        }

        return StageRunResult.OK(
                new QCStageOutput(trimmedR1Path.toAbsolutePath().toString(), trimmedR2Path.toAbsolutePath().toString(),
                        outputQcJson.toAbsolutePath().toString(),
                        outputQcHtml.toAbsolutePath().toString()),
                bioPipelineStage);
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_QC;
    }

}
