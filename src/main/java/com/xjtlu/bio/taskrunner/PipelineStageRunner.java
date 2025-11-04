package com.xjtlu.bio.taskrunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.MinioService;
import com.xjtlu.bio.service.PipelineService;

import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;

@Component
public class PipelineStageRunner implements Runnable {

    @Resource
    private PipelineService pipelineService;
    @Resource
    private MinioService minioService;

    private static final int taskBufferCapacity = 200;
    private BlockingQueue<BioPipelineStage> stageBuffer;

    private ObjectMapper objectMapper;

    private String stageResultTmpBasePath;

    @Value("${analysisPipeline.stage.qc.cmd}")
    private String qcCmd;

    @Override
    public void run() {
        // TODO Auto-generated method stub
        while (true) {
            // todo
            try {
                BioPipelineStage bioPipelineStage = stageBuffer.take();
                runStage(bioPipelineStage);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public PipelineStageRunner() {
        stageBuffer = new LinkedBlockingQueue<>(taskBufferCapacity);
    }

    @PostConstruct
    public void init() {
        new Thread(this).start();
        new Thread(this).start();
    }

    private void streamClose(InputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private static String appendSuffixBeforeExtensions(String fileName, String suffix) {
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

    private static int runSubProcess(List<String> cmd, Path workDir) throws IOException, InterruptedException {

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.directory(workDir.toFile());

        // 不需要日志：直接丢弃 stdout/stderr
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        pb.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process p = pb.start();
        int code = p.waitFor(); // 无超时：一直等到结束
        return code;

    }


    // private void deleteTmpFiles(List<File> tmpFiles){

    //     for(File f: tmpFiles){
    //         if (f.exists()) {
    //             f.delete();
    //         }
    //     }



    // }

    

    private StageRunResult runQc(BioPipelineStage bioPipelineStage) {

        String inputUrlsJson = bioPipelineStage.getInputUrl();
        String outputUrlsJson = bioPipelineStage.getOutputUrl();
        Map<String, String> inputUrls = null;
        Map<String, String> outputUrlsMap = null;

        ArrayList<File> toDeleteTmpFile = new ArrayList<>(); 

        try {
            inputUrls = objectMapper.readValue(inputUrlsJson, Map.class);
            outputUrlsMap = objectMapper.readValue(outputUrlsJson, Map.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            return StageRunResult.fail("解析输入参数错误", bioPipelineStage);
        }

        String inputUrl1 = inputUrls.get("r1");
        String input1FileName = inputUrl1.substring(inputUrl1.lastIndexOf("/") + 1);
        String inputUrl2 = inputUrls.size() > 1 ? inputUrls.get("r2") : null;
        String input2FileName = inputUrl2 == null ? null : inputUrl2.substring(inputUrl2.lastIndexOf("/") + 1);

        Path outputDir = Paths.get(stageResultTmpBasePath, bioPipelineStage.getStageId(), "output", "qc");

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

        InputStream input1Stream = null;
        InputStream input2Stream = null;
        try {
            input1Stream = minioService.getObjectStream(inputUrl1);
            if (StringUtils.isNotBlank(inputUrl2)) {
                input2Stream = minioService.getObjectStream(inputUrl2);
            }
        } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                | IllegalArgumentException | IOException e) {
            // TODO Auto-generated catch block
            this.streamClose(input1Stream);
            this.streamClose(input2Stream);
            return StageRunResult.fail("加载文件失败",bioPipelineStage);
        }

        File inputFile1 = new File(
                String.format("%s/%d/input/%s", stageResultTmpBasePath, bioPipelineStage.getStageId(), input1FileName));
        File inputFile2 = input2Stream == null ? null
                : new File(String.format("%s/%d/input/%s", stageResultTmpBasePath, bioPipelineStage.getStageId(),
                        input2FileName));


        
        try {
            FileUtils.copyInputStreamToFile(input1Stream, inputFile1);
            if (inputFile2 != null) {
                FileUtils.copyInputStreamToFile(input2Stream, inputFile2);
            }
        } catch (IOException ie) {

            try {
                FileUtils.delete(inputFile1);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (inputFile2 != null) {
                try {
                    FileUtils.delete(inputFile2);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            return StageRunResult.fail("文件IO错误");
        } finally {
            this.streamClose(input1Stream);
            this.streamClose(input2Stream);
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
                "--thread", String.valueOf(Math.max(2, Runtime.getRuntime().availableProcessors() / 2))));

        int runResult = -1;
        try {
            runResult = runSubProcess(cmd, outputDir);
        } catch (IOException | InterruptedException e) {
            return StageRunResult.fail("QC 子进程异常: " + e.getMessage(),bioPipelineStage);
        }

        if (runResult != 0) {
            return StageRunResult.fail("QC 退出码=" + runResult,bioPipelineStage);
        }

        if(!Files.exists(trimmedR1Path) || (inputUrl2!=null && !Files.exists(trimmedR2Path)) || !Files.exists(outputQcJson)||!Files.exists(outputQcHtml)){
            Files.delete(trimmedR1Path);
            Files.delete(trimmedR2Path);
            Files.delete(outputQcJson);
            Files.delete(outputQcHtml);
            inputFile1.delete();
            inputFile2.delete();
            return StageRunResult.fail("qc工具未产出结果",bioPipelineStage);
        }

        

        
        Map<String,String> outputPathMap = createQCOutputMap();
        return StageRunResult.OK(outputPathMap,bioPipelineStage);

    }

    private Map<String,String> createQCOutputMap(){
        //todo
        return null;
    }


    private void notifyPipelineService(StageRunResult stageRunResult){
        this.pipelineService.pipelineStageDone(stageRunResult);
    }

    private void runStage(BioPipelineStage bPipelineStage) {
        StageRunResult stageRunResult = null;
        if (bPipelineStage.getStageType() == PipelineService.PIPELINE_STAGE_QC) {
            stageRunResult = this.runQc(bPipelineStage);
        }


        this.notifyPipelineService(stageRunResult);
    }

    public boolean addTask(BioPipelineStage bioPipelineStage) {
        return stageBuffer.offer(bioPipelineStage);
    }
}
