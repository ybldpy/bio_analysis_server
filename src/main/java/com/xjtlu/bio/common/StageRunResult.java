package com.xjtlu.bio.common;

import java.util.Map;

public class StageRunResult {

    private boolean success;
    private String failReason;

    private Map<String,String> outputPath;



    public StageRunResult(boolean success, String failReason,Map<String,String> outputPath) {
        this.success = success;
        this.failReason = failReason;
        this.outputPath = outputPath;
    }

    public Map<String,String> getOutputPath(){
        return this.outputPath;
    }


    public static StageRunResult OK(Map<String,String> outputPath){
        return new StageRunResult(true, null,outputPath);
    }
    public static StageRunResult fail(String failReason){
        return new StageRunResult(false, failReason, null);
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
