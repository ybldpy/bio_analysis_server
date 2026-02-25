package com.xjtlu.bio.analysisPipeline.stageResult;

public class MLSTStageResult implements StageResult{

    private String MLST;

    public String getMLST() {
        return MLST;
    }

    public void setMLST(String mLST) {
        MLST = mLST;
    }

    public MLSTStageResult(String mLST) {
        MLST = mLST;
    }
    public MLSTStageResult(){
        
    }
    

}
