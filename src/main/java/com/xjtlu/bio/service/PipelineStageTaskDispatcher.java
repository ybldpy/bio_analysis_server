package com.xjtlu.bio.service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.StageDoneHandler;
import com.xjtlu.bio.analysisPipeline.taskrunner.PipelineStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.entity.BioPipelineStage;

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

    // private Set<Long> stageInQueueIdSet = ConcurrentHashMap.newKeySet();

    // private Set<Long> stageInRunningIdSet = ConcurrentHashMap.newKeySet();

    private Set<Long> inStageIdSet = ConcurrentHashMap.newKeySet();

    @Resource
    private Map<Integer, StageDoneHandler> stageDoneHandlerMap;

    @Override
    public void run() {
        // TODO Auto-generated method stub
        logger.debug("Worker Thread " + Thread.currentThread().getName() + " start to run");
        while (true) {
            // todo
            
            long runStageId = -1;
            try {

                BioPipelineStage bioPipelineStage = stageBuffer.take();
                runStageId = bioPipelineStage.getStageId();
                logger.debug("take {} to run", bioPipelineStage);
                int updateRes = this.pipelineService.startStageExecute(bioPipelineStage);
                if (updateRes != 1) {
                    logger.debug("stage " + bioPipelineStage.toString()
                            + " unable to update status to running and cannot run");
                    inStageIdSet.remove(bioPipelineStage.getStageId());
                    continue;
                }
                logger.info("stage " + bioPipelineStage.toString() + " start running");
                runStage(bioPipelineStage);

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                logger.debug("Worker Thread interruption", e);
            }catch(Exception e){
                logger.error("Worker exception", e);
            }finally{
                inStageIdSet.remove(runStageId);
            }
        }


    }

    public PipelineStageTaskDispatcher() {
        stageBuffer = new LinkedBlockingQueue<>(taskBufferCapacity);
    }

    @PostConstruct
    public void init() {
        for (int i = 0; i < Math.max(4, this.concurrentNum); i++) {
            new Thread(this).start();
        }
        logger.debug("create " + Math.max(4, this.concurrentNum) + " worker threads");
    }

    private void releaseStageRunTaskResource(StageContext stageContext) {

    }

    public void stageComplete(StageRunResult stageRunResult) {
        releaseStageRunTaskResource(stageRunResult.getStageContext());
        // this.notifyPipelineService(stageRunResult);
    }

    // private void notifyPipelineService(StageRunResult stageRunResult) {
    //     this.pipelineService.pipelineStageDone(stageRunResult);
    // }

    // public boolean isStageInWaittingQueue(long stageId) {
    //     return this.stageInQueueIdSet.contains(stageId);
    // }

    // public boolean isStageInRunning(long stageId) {
    //     return this.stageInRunningIdSet.contains(stageId);
    // }

    public boolean isStageIn(long stageId) {
        return this.inStageIdSet.contains(stageId);
    }

    private void runStage(BioPipelineStage bPipelineStage) {

        StageRunResult stageRunResult = null;

        PipelineStageExecutor executor = stageExecutorMap.get(bPipelineStage.getStageType());

        stageRunResult = executor.execute(bPipelineStage);

        if (stageRunResult.isSuccess()) {
            this.stageDoneHandlerMap.get(stageRunResult.getStageContext().getStageType())
                    .handleStageDone(stageRunResult);
        } else {
            this.stageDoneHandlerMap.get(-1).handleStageDone(stageRunResult);
        }

    }

    public boolean addTask(BioPipelineStage bioPipelineStage) {

        if (!this.inStageIdSet.add(bioPipelineStage.getStageId())) {
            return true; // 已经在队列里：幂等成功
        }
        boolean pushRes = stageBuffer.offer(bioPipelineStage);
        if (pushRes) {
            logger.debug("push {} to queue", bioPipelineStage);
        } else {
            inStageIdSet.remove(bioPipelineStage.getStageId());
            logger.debug("maxsize reach: unable to push {} to queue", bioPipelineStage);
        }

        return pushRes;
    }
}
