package com.xjtlu.bio.taskrunner.stageOutput;

import java.nio.file.Path;

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

    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return Path.of(consensusFa).getParent();
    }
    


}
