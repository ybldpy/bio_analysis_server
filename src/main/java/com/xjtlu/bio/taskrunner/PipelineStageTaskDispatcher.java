package com.xjtlu.bio.taskrunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.springframework.stereotype.Component;

import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;



import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;

@Component
public class PipelineStageTaskDispatcher implements Runnable {

    private static final int taskBufferCapacity = 200;
    private BlockingQueue<BioPipelineStage> stageBuffer;

    @Resource
    private PipelineService pipelineService;
    @Resource
    private Map<Integer, PipelineStageExecutor> stageExecutorMap;

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
    public PipelineStageTaskDispatcher() {
        stageBuffer = new LinkedBlockingQueue<>(taskBufferCapacity);
    }

    @PostConstruct
    public void init() {


        new Thread(this).start();
        new Thread(this).start();
    }
    private void notifyPipelineService(StageRunResult stageRunResult) {
        this.pipelineService.pipelineStageDone(stageRunResult);
    }

    private void runStage(BioPipelineStage bPipelineStage) {
        PipelineStageExecutor executor = stageExecutorMap.get(bPipelineStage.getStageType());
        StageRunResult stageRunResult = executor.execute(bPipelineStage);
        notifyPipelineService(stageRunResult);
    }

    public boolean addTask(BioPipelineStage bioPipelineStage) {
        return stageBuffer.offer(bioPipelineStage);
    }
}
