package com.xjtlu.bio.common;

import java.util.Map;

import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.taskrunner.stageOutput.StageOutput;

public class StageRunResult {

    private boolean success;
    private String failReason;

    private Map<String,String> outputPath;

    private StageOutput stageOutput;

    public StageOutput getStageOutput() {
        return stageOutput;
    }

    public void setStageOutput(StageOutput stageOutput) {
        this.stageOutput = stageOutput;
    }
    private BioPipelineStage stage;



    public StageRunResult(boolean success, String failReason,Map<String,String> outputPath, BioPipelineStage stage) {
        this.success = success;
        this.failReason = failReason;
        this.outputPath = outputPath;
        this.stage = stage;
    }

    public Map<String,String> getOutputPath(){
        return this.outputPath;
    }


    public void setOutputPath(Map<String, String> outputPath) {
        this.outputPath = outputPath;
    }

    public BioPipelineStage getStage() {
        return stage;
    }

    public void setStage(BioPipelineStage stage) {
        this.stage = stage;
    }

    public static StageRunResult OK(Map<String,String> outputPath, BioPipelineStage stage){
        return new StageRunResult(true, null,outputPath, stage);
    }
    public static StageRunResult fail(String failReason, BioPipelineStage stage){
        return new StageRunResult(false, failReason, null, stage);
    }

    
    public boolean isSuccess() {
        return success;
    }
    public void setSuccess(boolean success) {
        this.success = success;
    }
    public String getFailReason() {
        return failReason;
    }
    public void setFailReason(String failReason) {
        this.failReason = failReason;
    }

    

}
