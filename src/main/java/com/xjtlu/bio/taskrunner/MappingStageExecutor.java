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

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.ParameterUtil;

@Component
public class MappingStageExecutor extends AbstractPipelineStageExector{

    private String mappingTools;
    private String samTools;

    @Override
    public int id() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_MAPPING;
    }


    private boolean runSamTool(List<String> cmd, Path workDir){
    
        try {
            int runCode = this.runSubProcess(cmd, workDir);
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return false;
        }
        return true;

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
            return StageRunResult.fail(PARSE_JSON_ERROR, bioPipelineStage);
        }

        String refSeqAccession = ParameterUtil.getStrFromMap("refSeq", params);
        File refSeq = refSeqService.getRefSeqByAccession(refSeqAccession);
        if (refSeq == null) {
            return StageRunResult.fail("未找到参考基因组", bioPipelineStage);
        }

        String inputR1Url = inputUrlJson.get("r1");
        String inputR2Url = inputUrlJson.get("r2");


        String tempFormat = "%s/%d/input/%s";
        Path inputTmpPath = Paths.get(String.format(tempFormat, this.stageInputTmpBasePath, bioPipelineStage.getStageId()));
        File inputTmpDir = inputTmpPath.toFile();
        if (!inputTmpDir.exists()) {
            try {
                Files.createDirectories(inputTmpPath);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return StageRunResult.fail("创建临时目录出错", bioPipelineStage);
            }
        }



        

        
        Path r1TmpPath = inputTmpPath.resolve(inputR1Url.substring(inputR1Url.lastIndexOf("/") + 1));
        Path r2TmpPath = inputR2Url == null ? null: inputTmpPath.resolve(inputR2Url.substring(inputR2Url.lastIndexOf("/") + 1));
        File[] readFiles = this.moveSampleReadFilesToTmpPath(inputR1Url, r1TmpPath, inputR2Url, r2TmpPath);

        if (readFiles[0] == null || (r2TmpPath != null && readFiles[1] == null)) {
            return StageRunResult.fail("读取样本文件错误", bioPipelineStage);
        }

        Path workDir = Paths.get(String.format("%s/%d/output/mapping", this.stageResultTmpBasePath, bioPipelineStage.getStageId()));
        Path samTmp = workDir.resolve("aln.sam");
        Path bamTmp = workDir.resolve("aln.bam"); // view 输出的 BAM（未排序）
        Path bamSortedTmp = workDir.resolve("aln.sorted.bam"); // sort 的结果
        Path bamIndexTmp = workDir.resolve("aln.sorted.bam.bai"); // index 结果

        try {
            Files.createDirectories(workDir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            readFiles[0].delete();
            File f2 = readFiles[1];
            if (f2 != null) {
                f2.delete();
            }
            e.printStackTrace();
            return StageRunResult.fail("创建运行目录失败", bioPipelineStage);
        }

        List<String> cmd = new ArrayList<>();
        cmd.add(this.mappingTools);
        cmd.add("-ax");
        cmd.add("sr");
        cmd.add(refSeq.getAbsolutePath());
        cmd.add(r1TmpPath.toString());

        //sam输出文件目录
        cmd.add("-o");
        cmd.add(samTmp.toString());

        if (r2TmpPath != null) {
            cmd.add(r2TmpPath.toString());
        }

        List<File> toDeleteFile = new ArrayList<>();
        toDeleteFile.add(readFiles[0]);
        if (readFiles[1] != null) {
            toDeleteFile.add(readFiles[1]);
        }

        boolean runError = false;
        try {
            int runCode = runSubProcess(cmd, workDir);
            if (runCode != 0) {
                runError = true;
            }
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            runError = true;
        }

        if (runError) {
            this.deleteTmpFiles(toDeleteFile);
            return StageRunResult.fail("运行错误", bioPipelineStage);
        }


        cmd.clear();
        cmd.add(this.samTools);
        cmd.add("view");
        cmd.add("-bS");
        cmd.add(samTmp.toString());
        cmd.add("-o");
        cmd.add(bamTmp.toString());

        toDeleteFile.add(new File(bamTmp.toString()));
        boolean samRunResult = this.runSamTool(cmd, workDir);
        String samToolError = "samtool运行错误";
        if (!samRunResult) {
            this.deleteTmpFiles(toDeleteFile);
            return StageRunResult.fail(samToolError, bioPipelineStage);
        }

        cmd.clear();
        cmd.add(this.samTools);
        cmd.add("sort");
        cmd.add("-o");
        cmd.add(bamSortedTmp.toString());
        cmd.add(bamTmp.toString());

        samRunResult = this.runSamTool(cmd, workDir);
        //this.deleteTmpFiles(toDeleteFile);
        toDeleteFile.add(new File(bamSortedTmp.toString()));
        if (!samRunResult) {
            this.deleteTmpFiles(toDeleteFile);
            
            return StageRunResult.fail(samToolError, bioPipelineStage);
        }

        cmd.clear();
        cmd.add(this.samTools);
        cmd.add("index");
        cmd.add(bamSortedTmp.toString());
        cmd.add("-o");
        cmd.add(bamIndexTmp.toString());

        samRunResult = this.runSamTool(cmd, workDir);
        toDeleteFile.add(new File(bamIndexTmp.toString()));
        if (!samRunResult) {
            this.deleteTmpFiles(toDeleteFile);
            return StageRunResult.fail(samToolError, bioPipelineStage);
        }


        HashMap<String,String> outputMap = new HashMap<>();

        outputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY, bamSortedTmp.toString());
        outputMap.put(PipelineService.PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY, bamIndexTmp.toString());

        StageRunResult stageRunResult = StageRunResult.OK(outputMap, bioPipelineStage);

        return stageRunResult;
    }

    

}
