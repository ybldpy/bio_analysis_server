package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

public class StageRunResult<T extends StageOutput> {

    private boolean success;
    private String failReason;

    
    private Map<String,String> outputPath;

    private T stageOutput;

    private Exception e;

    

    

    public Exception getE() {
        return e;
    }

    public void setE(Exception e) {
        this.e = e;
    }

    public T getStageOutput() {
        return stageOutput;
    }

    public void setStageOutput(T stageOutput) {
        this.stageOutput = stageOutput;
    }
    private BioPipelineStage stage;



    public StageRunResult(boolean success, String failReason,Map<String,String> outputPath, BioPipelineStage stage, Exception e) {
        this.success = success;
        this.failReason = failReason;
        this.outputPath = outputPath;
        this.stage = stage;
        this.e = e;
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
        return new StageRunResult(true, null,outputPath, stage,null);
    }

    public static <T extends StageOutput> StageRunResult<T> OK(T stageOutput, BioPipelineStage stage){
        StageRunResult<T> stageRunResult = new StageRunResult<>(true, null, null, stage, null);
        stageRunResult.setStageOutput(stageOutput);
        return stageRunResult;
    }
    public static <T extends StageOutput> StageRunResult<T> fail(String failReason, BioPipelineStage stage,Exception e){
        return new StageRunResult<>(false, failReason, null, stage, e);
    }


    public String getErrorLog(){

                StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        if (e != null) {
            e.printStackTrace(pw);
        }

        String stackTrace = sw.toString();
        // 防止异常栈过长，DB/前端不好处理
        int maxLen = 6000;
        if (stackTrace.length() > maxLen) {
            stackTrace = stackTrace.substring(0, maxLen) + "\n...truncated";
        }

        String errorLog = String.format(
                "PipelineId=%d, StageId=%d, StageIndex=%d, StageName=%s, StageType=%d\n" +
                        "Message=%s\n" +
                        "Exception=%s\n" +
                        "StackTrace:\n%s",
                stage.getPipelineId(),
                stage.getStageId(),
                stage.getStageIndex(),
                stage.getStageName(),
                stage.getStageType(),
                this.failReason,
                e == null ? "N/A" : e.getClass().getName(),
                stackTrace);


        return errorLog;


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
