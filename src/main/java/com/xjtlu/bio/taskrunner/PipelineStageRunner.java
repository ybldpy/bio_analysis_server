package com.xjtlu.bio.taskrunner;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.springframework.scheduling.annotation.Async;
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
public class PipelineStageRunner implements Runnable{

    @Resource
    private PipelineService pipelineService;
    @Resource
    private MinioService minioService;


    private static final int taskBufferCapacity = 200;
    private BlockingQueue<BioPipelineStage> buffer;


    private static final int writeBufferCapacity = 50;
    private LinkedList<BioPipelineStage> writeBuffer;

    private ObjectMapper objectMapper;

    private String stageResultTmpBasePath;



    @Override
    public void run() {
        // TODO Auto-generated method stub


        while (true) {
            //todo
        }
        
    }

    
    private ReentrantLock reentrantLock;


    public PipelineStageRunner(){
        buffer = new LinkedBlockingQueue<>(taskBufferCapacity);
        writeBuffer = new LinkedList<>();
        reentrantLock = new ReentrantLock();
    }

    @PostConstruct
    public void init(){
        new Thread(this).start();
        new Thread(this).start();
    }


    

    private StageRunResult runQc(BioPipelineStage bioPipelineStage){
        String inputUrlsJson = bioPipelineStage.getInputUrl();
        if (inputUrlsJson == null || inputUrlsJson.isBlank()) {
            return StageRunResult.fail("未找到输入参数");
        }

        List<String> inputUrls = null;

        try {
            inputUrls = objectMapper.readValue(inputUrlsJson, List.class);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            return StageRunResult.fail("解析输入参数错误");    
        }


        String inputUrl1 = inputUrls.get(0);
        String input1FileName = inputUrl1.substring(inputUrl1.lastIndexOf("/")+1);
        String inputUrl2 = inputUrls.size() > 1?inputUrls.get(1):null;

        InputStream input1Stream = null;
        try {
            input1Stream = minioService.getObjectStream(inputUrl1);
        } catch (InvalidKeyException | ErrorResponseException | InsufficientDataException | InternalException
                | InvalidResponseException | NoSuchAlgorithmException | ServerException | XmlParserException
                | IllegalArgumentException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return StageRunResult.fail("加载文件失败");
        }


        try{
            File file = new File(String.format("%s/%d/", stageResultTmpBasePath, bioPipelineStage.getStageId(), ""));
            FileUtils.copyInputStreamToFile(input1Stream, file);
        }catch(IOException ie){

        }

        

        



    }


    private void runStage(BioPipelineStage bPipelineStage){
        if (bPipelineStage.getStageType() == PipelineService.PIPELINE_STAGE_QC) {
            this.runQc(bPipelineStage);
        }
    }


    private void writeBufferToStorage(){
        //todo
    }

    public void addTask(BioPipelineStage bioPipelineStage){
        reentrantLock.lock();
        if (buffer.size()==taskBufferCapacity) {
            if (writeBuffer.size() == writeBufferCapacity) {
                 this.writeBufferToStorage();       
            }else {
                writeBuffer.add(bioPipelineStage);
            }
        }else {
            buffer.add(bioPipelineStage);
        }
        reentrantLock.unlock();


    }
}
