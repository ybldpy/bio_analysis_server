package com.xjtlu.bio.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.Constants.StageStatus;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.MissingUpstreamException;
import com.xjtlu.bio.analysisPipeline.workflow.StageOrchestrator.OrchestratePlan;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioAnalysisPipelineExample;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;

import jakarta.annotation.Resource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component
public class PipelineStagePoller {

    private static final Logger logger = LoggerFactory.getLogger(PipelineStagePoller.class);

    @Resource
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    @Resource
    private PipelineService pipelineService;

    @Resource
    private StageOrchestrator stageOrchestrator;

    @Scheduled(fixedDelay = 60000, initialDelay = 60000)
    public void dispatchQueuingStages() {
        BioPipelineStageExample queuingStageQuery = new BioPipelineStageExample();
        queuingStageQuery.createCriteria().andStatusEqualTo(StageStatus.PIPELINE_STAGE_STATUS_QUEUING);
        List<BioPipelineStage> queueingStages = pipelineService.queryStages(queuingStageQuery).stream()
                .filter((stage) -> {
                    return !pipelineStageTaskDispatcher.isStageIn(stage.getStageId());
                }).toList();
        for (BioPipelineStage stage : queueingStages) {
            pipelineStageTaskDispatcher.addTask(stage);
        }
    }

    // 2 mins per check
    @Scheduled(fixedDelay = 12000, initialDelay = 12000)
    public void dispatchPendingStages() {

        BioAnalysisPipelineExample runningPipelineQuery = new BioAnalysisPipelineExample();
        runningPipelineQuery.createCriteria().andStatusEqualTo(PipelineService.PIPELINE_STATUS_RUNNING);

        Result<List<BioAnalysisPipeline>> runningPipelinesQueryResult = this.pipelineService
                .queryPipelines(runningPipelineQuery);

        if (runningPipelinesQueryResult.getStatus() != Result.SUCCESS) {
            return;
        }

        BioPipelineStageExample stagesQuery = new BioPipelineStageExample();
        stagesQuery.createCriteria().andPipelineIdIn(
                runningPipelinesQueryResult.getData().stream().map((stage) -> stage.getPipelineId()).toList());

        Map<Long, BioAnalysisPipeline> pipelineMap = new HashMap<>();
        for (BioAnalysisPipeline pipeline : runningPipelinesQueryResult.getData()) {
            pipelineMap.put(pipeline.getPipelineId(), pipeline);
        }

        List<BioPipelineStage> stages = this.pipelineService.queryStages(stagesQuery);

        Map<Long, List<BioPipelineStage>> pipelineStagesMap = new HashMap<>();

        for (BioPipelineStage stage : stages) {
            if (!pipelineStagesMap.containsKey(stage.getPipelineId())) {
                pipelineStagesMap.put(stage.getPipelineId(), new ArrayList<>());
            }
            pipelineStagesMap.get(stage.getPipelineId()).add(stage);
        }

        for (Map.Entry<Long, List<BioPipelineStage>> pipelineAndStage : pipelineStagesMap.entrySet()) {
            Long pipelineId = pipelineAndStage.getKey();
            List<BioPipelineStage> currentPipelineStages = pipelineAndStage.getValue();

            for (BioPipelineStage stage : currentPipelineStages) {
                if (stage.getStatus() == StageStatus.PIPELINE_STAGE_STATUS_PENDING) {
                    Long stageId = stage.getStageId();
                    logger.info("发现待处理任务 [PipelineId: {}, StageId: {}]，正在评估调度策略...", pipelineId, stageId);

                    try {
                        OrchestratePlan plan = stageOrchestrator.makePlan(stages, stageId);
                        int res = pipelineService.updateStageFromVersion(plan.getUpdateStageCommands().get(0));

                        if (res == 1) {
                            logger.info("任务状态抢占成功，已成功提交至执行任务池 [PipelineId: {}, StageId: {}]", pipelineId, stageId);
                            pipelineStageTaskDispatcher.addTask(plan.getRunStages().get(0));
                        } else {
                            // res != 1 通常意味着乐观锁版本冲突，或者状态被 UI/其他地方改掉了
                            logger.warn("任务状态抢占失败（可能已被更新或状态冲突），跳过本次调度 [PipelineId: {}, StageId: {}, 返回值: {}]", pipelineId,
                                    stageId, res);
                        }
                    } catch (JsonProcessingException e) {
                        // 1. 致命缺陷：序列化解析出错，必须把异常栈 e 传进 log.error，否则永远不知道报错细节！
                        logger.error("生成任务调度计划失败：JSON 解析异常 [PipelineId: {}, StageId: {}]", pipelineId, stageId, e);
                    } catch (MissingUpstreamException e) {
                        // 2. 预期情况（前置 DAG 节点还没跑完）：用 DEBUG 级别！
                        // 这样既防止了每几秒轮询时在控制台疯狂刷屏，又能在排查“任务为什么卡住不向下走了”的时候，开 DEBUG 查到原因
                        logger.debug("任务等待中：上游依赖阶段尚未完成，暂不触发调度 [PipelineId: {}, StageId: {}]", pipelineId, stageId);
                    }
                }
            }
        }
    }

}
