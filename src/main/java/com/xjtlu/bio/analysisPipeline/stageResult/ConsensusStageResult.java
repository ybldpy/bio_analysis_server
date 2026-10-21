package com.xjtlu.bio.analysisPipeline.stageResult;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ConsensusResultMetrics;

public class ConsensusStageResult implements StageResult{

    private String consensusUrl;
    private String maskBedUrl;
    @JsonAlias("qcUrl")
    private String metricsUrl;
    // Accept legacy inline metrics on read; new results only serialize metricsUrl.
    @JsonAlias("qcMetrics")
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private ConsensusResultMetrics metrics;

    public ConsensusStageResult() {
    }

    public String getConsensusUrl() {
        return consensusUrl;
    }

    public ConsensusStageResult(
            String consensusUrl,
            String maskBedUrl,
            String metricsUrl) {
        this.consensusUrl = consensusUrl;
        this.maskBedUrl = maskBedUrl;
        this.metricsUrl = metricsUrl;
    }

    public ConsensusStageResult(
            String consensusUrl,
            String maskBedUrl,
            String metricsUrl,
            ConsensusResultMetrics metrics) {
        this(consensusUrl, maskBedUrl, metricsUrl);
        this.metrics = metrics;
    }

    public ConsensusStageResult(String consensusUrl) {
        this.consensusUrl = consensusUrl;
    }

    public void setConsensusUrl(String consensusUrl) {
        this.consensusUrl = consensusUrl;
    }

    public String getMaskBedUrl() {
        return maskBedUrl;
    }

    public void setMaskBedUrl(String maskBedUrl) {
        this.maskBedUrl = maskBedUrl;
    }

    public String getMetricsUrl() {
        return metricsUrl;
    }

    public void setMetricsUrl(String metricsUrl) {
        this.metricsUrl = metricsUrl;
    }

    public ConsensusResultMetrics getMetrics() {
        return metrics;
    }

    public void setMetrics(ConsensusResultMetrics metrics) {
        this.metrics = metrics;
    }

    
}
