package com.xjtlu.bio.analysisPipeline.stageResult;

public class ConsensusStageResult implements StageResult{

    private String consensusUrl;

    public ConsensusStageResult() {
    }

    public String getConsensusUrl() {
        return consensusUrl;
    }

    public ConsensusStageResult(String consensusUrl) {
        this.consensusUrl = consensusUrl;
    }

    public void setConsensusUrl(String consensusUrl) {
        this.consensusUrl = consensusUrl;
    }

    
}
