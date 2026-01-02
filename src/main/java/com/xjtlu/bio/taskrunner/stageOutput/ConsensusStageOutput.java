package com.xjtlu.bio.taskrunner.stageOutput;

public class ConsensusStageOutput implements StageOutput{

    public static final String CONSENSUS = "consensus.fa";

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
