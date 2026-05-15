package com.xjtlu.bio.analysisPipeline.context.runtime;

public class StageContext {

    private long runStageId;
    private int version;
    private int stageType;
    private long pipelineId;

    



    public StageContext(){
        
    }
    public StageContext(long runStageId, int version, int stageType, int pipelineId) {
        this.runStageId = runStageId;
        this.version = version;
        this.stageType = stageType;
        this.pipelineId = pipelineId;
    }

    
    public long getPipelineId() {
        return pipelineId;
    }
    public void setPipelineId(long pipelineId) {
        this.pipelineId = pipelineId;
    }
    public long getRunStageId() {
        return runStageId;
    }
    public void setRunStageId(long runStageId) {
        this.runStageId = runStageId;
    }
    public int getVersion() {
        return version;
    }
    public void setVersion(int version) {
        this.version = version;
    }
    public int getStageType() {
        return stageType;
    }
    public void setStageType(int stageType) {
        this.stageType = stageType;
    }

    

}
