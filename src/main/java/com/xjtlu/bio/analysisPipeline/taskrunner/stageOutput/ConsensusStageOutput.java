package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;

public class ConsensusStageOutput implements StageOutput{

    public static final String CONSENSUS = "consensus.fa";

    private String consensusFa;
    private String maskBed;
    private String metricsJson;
    private ConsensusResultMetrics metrics;

    public String getConsensusFa() {
        return consensusFa;
    }

    public void setConsensusFa(String consensusFa) {
        this.consensusFa = consensusFa;
    }

    public String getMaskBed() {
        return maskBed;
    }

    public void setMaskBed(String maskBed) {
        this.maskBed = maskBed;
    }

    public String getMetricsJson() {
        return metricsJson;
    }

    public void setMetricsJson(String metricsJson) {
        this.metricsJson = metricsJson;
    }

    public ConsensusResultMetrics getMetrics() {
        return metrics;
    }

    public void setMetrics(ConsensusResultMetrics metrics) {
        this.metrics = metrics;
    }

    public ConsensusStageOutput(
            String consensusFa,
            String maskBed,
            String metricsJson,
            ConsensusResultMetrics metrics) {
        this.consensusFa = consensusFa;
        this.maskBed = maskBed;
        this.metricsJson = metricsJson;
        this.metrics = metrics;
    }

    public ConsensusStageOutput(String consensusFa) {
        this.consensusFa = consensusFa;
    }

    @Override
    public Path getParentPath() {
        return Path.of(consensusFa).getParent();
    }
    


}
