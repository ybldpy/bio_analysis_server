package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.service.RefSeqService;
import com.xjtlu.bio.service.StorageService;
import com.xjtlu.bio.service.StorageService.GetObjectResult;

import jakarta.annotation.Resource;

public abstract class AbstractPipelineStageExector implements PipelineStageExecutor {

    @Resource
    protected PipelineService pipelineService;
    @Resource
    protected ObjectMapper objectMapper;
    @Resource
    protected RefSeqService refSeqService;

    @Resource
    protected StorageService storageService;

    protected static final String PARSE_JSON_ERROR = "解析参数错误";

    protected String stageResultTmpBasePath;
    protected String stageInputTmpBasePath;



    protected boolean requireNonEmpty(Path p) throws IOException {
       
        if (!Files.exists(p)||Files.size(p) <= 0) {
            return false;
        }
        return true;
        
    }

    protected StageRunResult runException(BioPipelineStage bioPipelineStage, Exception e) {
        return runFail(bioPipelineStage, "异常\n" + e.getMessage());
    }

    protected StageRunResult runFail(BioPipelineStage bioPipelineStage, String msg) {
        return StageRunResult.fail(msg, bioPipelineStage);
    }


    

    protected static String appendSuffixBeforeExtensions(String fileName, String suffix) {
        if (fileName == null || fileName.isEmpty())
            return fileName;

        // 如果已经有相同后缀就不重复添加
        String stem = fileName;
        String ext = "";

        // 先处理双扩展 .fastq.gz / .fq.gz
        if (fileName.endsWith(".fastq.gz")) {
            stem = fileName.substring(0, fileName.length() - ".fastq.gz".length());
            ext = ".fastq.gz";
        } else if (fileName.endsWith(".fq.gz")) {
            stem = fileName.substring(0, fileName.length() - ".fq.gz".length());
            ext = ".fq.gz";
        } else if (fileName.endsWith(".fastq")) {
            stem = fileName.substring(0, fileName.length() - ".fastq".length());
            ext = ".fastq";
        } else if (fileName.endsWith(".fq")) {
            stem = fileName.substring(0, fileName.length() - ".fq".length());
            ext = ".fq";
        } else if (fileName.endsWith(".gz")) {
            // 泛化：如果只是 .gz，再往前找一次点
            String withoutGz = fileName.substring(0, fileName.length() - 3);
            int lastDot = withoutGz.lastIndexOf('.');
            if (lastDot >= 0) {
                stem = withoutGz.substring(0, lastDot);
                ext = withoutGz.substring(lastDot) + ".gz"; // .xxx + .gz
            } else {
                stem = withoutGz;
                ext = ".gz";
            }
        } else {
            // 普通扩展或无扩展
            int lastDot = fileName.lastIndexOf('.');
            if (lastDot >= 0) {
                stem = fileName.substring(0, lastDot);
                ext = fileName.substring(lastDot);
            } else {
                stem = fileName;
                ext = "";
            }
        }

        if (stem.endsWith(suffix))
            return stem + ext; // 已有后缀则直接返回
        return stem + suffix + ext;
    }

    public abstract int id();

    protected StageRunResult parseError(BioPipelineStage bioPipelineStage) {
        return StageRunResult.fail(PARSE_JSON_ERROR, bioPipelineStage);
    }

    protected File[] moveSampleReadFilesToTmpPath(String r1Url, Path r1TmpPath, String r2Url, Path r2TmpPath) {

        GetObjectResult r1Result = storageService.getObject(r1Url, r1TmpPath.toString());
        GetObjectResult r2Result = null;
        if (r2Url != null) {
            r2Result = storageService.getObject(r2Url, r2TmpPath.toString());
        }
        return new File[] { r1Result.objectFile(), r2Url != null ? r2Result.objectFile() : null };

    }

    protected static int runSubProcess(List<String> cmd, Path workDir) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(workDir.toFile());

        // 不需要日志：直接丢弃 stdout/stderr
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process p = pb.start();
        int code = p.waitFor(); // 无超时：一直等到结束
        return code;

    }

    protected void deleteTmpFiles(List<File> tmpFiles) {
        for (File f : tmpFiles) {
            if (f != null && f.exists()) {
                f.delete();
            }
        }
    }

    
}
