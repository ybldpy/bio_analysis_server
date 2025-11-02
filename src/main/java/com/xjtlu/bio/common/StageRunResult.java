package com.xjtlu.bio.common;

public class StageRunResult {

    private boolean success;
    private String failReason;



    public StageRunResult(boolean success, String failReason) {
        this.success = success;
        this.failReason = failReason;
    }


    public static StageRunResult OK(){
        return new StageRunResult(true, null);
    }
    public static StageRunResult fail(String failReason){
        return new StageRunResult(false, failReason);
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
