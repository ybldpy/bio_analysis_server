package com.xjtlu.bio.taskrunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.catalina.Pipeline;
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



    private int concurrentNum;

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
                int updateRes = this.pipelineService.updateStageFromOldToNew(bioPipelineStage.getStageId(), PipelineService.PIPELINE_STAGE_STATUS_QUEUING, PipelineService.PIPELINE_STAGE_STATUS_RUNNING);
                if(updateRes!=1){
                    continue;
                }
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
        for(int i = 0;i<this.concurrentNum;i++){
            new Thread(this).start();
        }
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
