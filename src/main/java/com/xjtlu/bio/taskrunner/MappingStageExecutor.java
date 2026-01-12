package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.xjtlu.bio.configuration.AnalysisPipelineToolsConfig;
import jakarta.annotation.Resource;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.parameters.RefSeqConfig;
import com.xjtlu.bio.taskrunner.stageOutput.MappingStageOutput;
import com.xjtlu.bio.service.StorageService.GetObjectResult;

@Component
public class MappingStageExecutor extends AbstractPipelineStageExector implements PipelineStageExecutor {





    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_MAPPING;
    }

    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrls = bioPipelineStage.getInputUrl();
        Map<String, String> inputUrlJson = null;
        Map<String, Object> params = null;
        try {
            inputUrlJson = objectMapper.readValue(inputUrls, Map.class);
            params = objectMapper.readValue(bioPipelineStage.getParameters(), Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return StageRunResult.fail(PARSE_JSON_ERROR, bioPipelineStage, null);
        }

        RefSeqConfig refSeqConfig = this.getRefSeqConfigFromParams(params);

        if (refSeqConfig == null) {
            return StageRunResult.fail("未能加载参考基因", bioPipelineStage, null);
        }

        File refSeq = refSeqConfig.getRefseqId() >= 0 ? this.refSeqService.getRefseq(refSeqConfig.getRefseqId())
                : this.refSeqService.getRefseq((String) refSeqConfig.getRefseqObjectName());

        if (refSeq == null) {
            return StageRunResult.fail("参考基因组加载失败", bioPipelineStage, null);
        }

        String inputR1Url = inputUrlJson.get(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R1);
        String inputR2Url = inputUrlJson.get(PipelineService.PIPELINE_STAGE_MAPPING_INPUT_R2);

        Path inputTmpPath = this.stageInputPath(bioPipelineStage);
        File inputTmpDir = inputTmpPath.toFile();

        if (!inputTmpDir.exists()) {
            try {
                FileUtils.createParentDirectories(inputTmpDir);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return StageRunResult.fail("创建临时目录出错", bioPipelineStage, null);
            }
        }

        Path workDir = this.workDirPath(bioPipelineStage);
        try {
            FileUtils.createParentDirectories(workDir.toFile());
        } catch (IOException e) {
            return this.runFail(bioPipelineStage, "创建临时目录出错", e);
        }

        Path r1TmpPath = inputTmpPath.resolve(inputR1Url.substring(inputR1Url.lastIndexOf("/") + 1));
        Path r2TmpPath = inputR2Url == null ? null
                : inputTmpPath.resolve(inputR2Url.substring(inputR2Url.lastIndexOf("/") + 1));

        Map<String, Path> loadMap = new HashMap<>();
        loadMap.put(inputR1Url, r1TmpPath);
        if (r2TmpPath != null) {
            loadMap.put(inputR2Url, r2TmpPath);
        }
        Map<String, GetObjectResult> r1AndR2GetResult = this.loadInput(loadMap);

        String errorLoadingInput = this.findFailedLoadingObject(r1AndR2GetResult);
        if (errorLoadingInput != null) {
            this.deleteTmpFiles(List.of(inputTmpDir));
            return this.runFail(bioPipelineStage, errorLoadingInput + "加载失败",
                    r1AndR2GetResult.get(errorLoadingInput).e());
        }

        // Path samTmp = workDir.resolve("aln.sam");
        // Path bamTmp = workDir.resolve("aln.bam"); // view 输出的 BAM（未排序）

        MappingStageOutput mappingStageOutput = bioStageUtil.mappingOutput(bioPipelineStage, workDir);
        Path bamSortedTmp =  Path.of(mappingStageOutput.getBamPath());
        Path bamIndexTmp = Path.of(mappingStageOutput.getBamIndexPath()); // index 结果

        String pipelineCmd = buildMappingPipelineCmd(refSeq, r1TmpPath, r2TmpPath, bamSortedTmp);

        List<String> cmd = new ArrayList<>();
        cmd.add("sh");
        cmd.add("-c");
        cmd.add(pipelineCmd);


        ExecuteResult executeResult = execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "运行mapping tool失败", executeResult.ex, inputTmpPath, workDir);
        }

        List<StageOutputValidationResult> errorOutput = validateOutputFiles(bamSortedTmp);
        if (!errorOutput.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, inputTmpPath, workDir);
        }

        cmd.clear();
        cmd.addAll(analysisPipelineToolsConfig.getSamtools());
        cmd.add("index");
        cmd.add(bamSortedTmp.toString());
        cmd.add("-o");
        cmd.add(bamIndexTmp.toString());

        executeResult = execute(cmd, workDir);
        if(!executeResult.success()){
            return this.runFail(bioPipelineStage, "生成bam索引失败", executeResult.ex, inputTmpPath, workDir);
        }

        errorOutput = validateOutputFiles(bamIndexTmp);
        if (!errorOutput.isEmpty()) {
            return this.runFail(bioPipelineStage, createStageOutputValidationErrorMessge(errorOutput), null, inputTmpPath, workDir);
        }

        StageRunResult stageRunResult = StageRunResult
                .OK(new MappingStageOutput(bamSortedTmp.toString(), bamIndexTmp.toString()), bioPipelineStage);

        return stageRunResult;
    }


    private String buildMappingPipelineCmd(File refSeq, Path r1, Path r2, Path bamSortedOut) {
        StringBuilder sb = new StringBuilder(256);
        // mapping tool（输出到 stdout）
        sb.append(quote(String.join(" ", this.analysisPipelineToolsConfig.getMinimap2()))).append(" -ax sr ")
          .append(quote(refSeq.getAbsolutePath())).append(' ')
          .append(quote(r1.toString())).append(' ');
        if (r2 != null) {
            sb.append(quote(r2.toString())).append(' ');
        }
        // view: 从 stdin 读 SAM，输出 BAM 到 stdout
        sb.append("| ").append(quote(String.join(" ", this.analysisPipelineToolsConfig.getSamtools()))).append(" view -bS - ");
        // sort: 从 stdin 读 BAM，输出最终排序 bam
        sb.append("| ").append(quote(String.join(" ", this.analysisPipelineToolsConfig.getSamtools()))).append(" sort -o ").append(quote(bamSortedOut.toString())).append(" -");
        return sb.toString();
    }

    private static String quote(String s) {
        if (s == null) return "''";
        return "'" + s.replace("'", "'\"'\"'") + "'";
    }

}
