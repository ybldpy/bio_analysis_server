package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.response.VirusTargetOption;

@Service
public class PipelineTargetService {

    private static final Logger logger = LoggerFactory.getLogger(PipelineTargetService.class);

    private final BioPipelineTargetMapper targetMapper;

    public PipelineTargetService(BioPipelineTargetMapper targetMapper) {
        this.targetMapper = targetMapper;
    }

    /** Read on every request so configuration edits do not require an application restart. */
    public Result<List<VirusTargetOption>> listVirusTargets() {
        try {
            List<VirusTargetOption> options = targetMapper
                    .selectEnabledVirusTargets(PIPELINE_VIRUS, PIPELINE_VIRUS_COVID)
                    .stream()
                    .map(target -> new VirusTargetOption(
                            target.getLabel(), target.getTaxId(), target.getPipelineType()))
                    .toList();
            return new Result<>(Result.SUCCESS, options, null);
        } catch (RuntimeException e) {
            logger.error("Failed to load virus analysis target options", e);
            return new Result<>(Result.INTERNAL_FAIL, null, "读取病毒分析目标配置失败，请稍后重试");
        }
    }
}
