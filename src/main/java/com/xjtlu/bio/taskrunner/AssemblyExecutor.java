package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.taskrunner.stageOutput.AssemblyStageOutput;

@Component
public class AssemblyExecutor extends AbstractPipelineStageExector {

    protected String spadesTool;

    @Override
    public StageRunResult execute(BioPipelineStage bioPipelineStage) {
        // TODO Auto-generated method stub
        String inputUrl = bioPipelineStage.getInputUrl();
        Map<String, String> inputUrlMap = null;
        try {
            inputUrlMap = this.objectMapper.readValue(inputUrl, Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return parseError(bioPipelineStage);
        }

        String r1 = inputUrlMap.get(PipelineService.PIPELINE_STAGE_QC_INPUT_R1);
        String r2 = inputUrlMap.get(PipelineService.PIPELINE_STAGE_QC_INPUT_R2);

        Path tempInputDir = Paths
                .get(String.format("%s/%d", this.stageInputTmpBasePath, bioPipelineStage.getStageId()));
        Path r1Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r1.substring(r1.lastIndexOf("/") + 1), ""));
        Path r2Path = null;
        if (r2 != null) {
            r2Path = tempInputDir.resolve(appendSuffixBeforeExtensions(r2.substring(r2.lastIndexOf("/") + 1), ""));
        }

        File[] readFiles = moveSampleReadFilesToTmpPath(inputUrl, r1Path, inputUrl, r2Path);
        if (readFiles[0] == null || (r2Path != null && readFiles[1] == null)) {
            if (readFiles[0] != null) {
                readFiles[0].delete();
            }
            if (readFiles[1] != null) {
                readFiles[1].delete();
            }
            return this.runFail(bioPipelineStage, "读取样本文件出错");
        }

        String resultDirFormat = "%s/%d";

        Path workDir = Paths
                .get(String.format(resultDirFormat, this.stageResultTmpBasePath, bioPipelineStage.getStageId()));
        try {
            Files.createDirectories(workDir, null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            this.deleteTmpFiles(List.of(readFiles[0], readFiles[1]));
            return this.runException(bioPipelineStage, e);
        }

        List<String> cmd = new ArrayList<>();
        cmd.add(this.spadesTool);
        cmd.add("-t");
        cmd.add(String.valueOf(2));
        if (r2Path != null) { // 双端
            cmd.add("-1");
            cmd.add(r1Path.toString());
            cmd.add("-2");
            cmd.add(r2Path.toString());
        } else { // 单端
            cmd.add("-s");
            cmd.add(r1Path.toString());
        }
        cmd.add("-o");
        cmd.add(workDir.toString());

        try {
            int code = this.runSubProcess(cmd, workDir);
            if (code != 0) {
                // 失败清理输入副本
                this.deleteTmpFiles(List.of(readFiles[0], readFiles[1]));
                if (readFiles.length > 1 && readFiles[1] != null)
                    readFiles[1].delete();
                return this.runFail(bioPipelineStage, "SPAdes 运行失败，exitCode=" + code);
            }
        } catch (Exception e) {
            this.deleteTmpFiles(List.of(readFiles[0], readFiles[1]));
            return this.runException(bioPipelineStage, e);
        }

        Path contigs = workDir.resolve("contigs.fasta");
        Path scaffolds = workDir.resolve("scaffolds.fasta");


        Map<String,String> outputMap = new HashMap<>();        
        
        
        try{
            boolean exists = requireNonEmpty(contigs);
            if(!exists){
                this.deleteTmpFiles(List.of(readFiles[0],readFiles[1], workDir.toFile()));
                return this.runFail(bioPipelineStage, "组装输出为空");
            }
            outputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY, contigs.toString());
        }catch(IOException e){
            return this.runException(bioPipelineStage, e);
        }




        boolean hasScaffold = true;
        try{
            if(this.requireNonEmpty(scaffolds)){
                outputMap.put(PipelineService.PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY, scaffolds.toString());
            }else {
                hasScaffold = false;
            }
        }catch(IOException e){
            //if error happen here, just ingore. The callback will know it and handle
        }

        return StageRunResult.OK(new AssemblyStageOutput(contigs.toAbsolutePath().toString(), hasScaffold? scaffolds.toAbsolutePath().toString():null), bioPipelineStage);
        
    }

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_ASSEMBLY;
    }

}
