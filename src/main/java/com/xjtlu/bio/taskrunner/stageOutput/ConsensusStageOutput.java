package com.xjtlu.bio.taskrunner.stageOutput;

public class ConsensusStageOutput implements StageOutput{

    private String consensusFa;

    public String getConsensusFa() {
        return consensusFa;
    }

    public void setConsensusFa(String consensusFa) {
        this.consensusFa = consensusFa;
    }

    public ConsensusStageOutput(String consensusFa) {
        this.consensusFa = consensusFa;
    }
    


}
