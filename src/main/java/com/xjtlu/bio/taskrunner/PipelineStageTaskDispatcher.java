package com.xjtlu.bio.taskrunner;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger logger = LoggerFactory.getLogger(PipelineStageTaskDispatcher.class);


    private int concurrentNum;

    @Resource
    private PipelineService pipelineService;
    @Resource
    private Map<Integer, PipelineStageExecutor> stageExecutorMap;

    @Override
    public void run() {
        // TODO Auto-generated method stub
        logger.debug("Worker Thread "+Thread.currentThread().getName()+" start to run");
        while (true) {
            // todo
            try {

                BioPipelineStage bioPipelineStage = stageBuffer.take();
                logger.debug("take {} to run", bioPipelineStage);
                int updateRes = this.pipelineService.startStageExecute(bioPipelineStage);
                if(updateRes!=1){
                    logger.debug("stage "+bioPipelineStage.toString()+" unable to update status to running and cannot run");
                    continue;
                }
                logger.info("stage "+bioPipelineStage.toString()+" start running");
                runStage(bioPipelineStage);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.debug("Worker Thread interruption", e);
            }
        }

    }
    public PipelineStageTaskDispatcher() {
        stageBuffer = new LinkedBlockingQueue<>(taskBufferCapacity);
    }

    @PostConstruct
    public void init() {
        for(int i = 0;i<Math.max(4, this.concurrentNum);i++){
            new Thread(this).start();
        }
        logger.debug("create "+Math.max(4, this.concurrentNum)+" worker threads");
    }
    private void notifyPipelineService(StageRunResult stageRunResult) {
        this.pipelineService.pipelineStageDone(stageRunResult);
    }

    private void runStage(BioPipelineStage bPipelineStage) {
        PipelineStageExecutor executor = stageExecutorMap.get(bPipelineStage.getStageType());
        StageRunResult stageRunResult = null;
        try {
            stageRunResult = executor.execute(bPipelineStage);
        }catch (Exception e){
            logger.error("{} 运行时异常", bPipelineStage, e);
            stageRunResult = StageRunResult.fail("运行时异常", bPipelineStage, e);
        }
        notifyPipelineService(stageRunResult);
    }


    

    public boolean addTask(BioPipelineStage bioPipelineStage) {
        boolean pushRes = stageBuffer.offer(bioPipelineStage);
        if(pushRes){
            logger.debug("push {} to queue", bioPipelineStage);
        }else {
            logger.debug("maxsize reach: unable to push {} to queue", bioPipelineStage);
        }
        return pushRes;
    }
}
