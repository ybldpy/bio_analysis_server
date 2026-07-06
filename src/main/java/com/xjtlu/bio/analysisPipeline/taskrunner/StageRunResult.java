package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Map;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.StageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;

public class StageRunResult<T extends StageOutput> {

    private boolean success;
    private String failReason;

    
    private Map<String,String> outputPath;

    private T stageOutput;

    private Exception e;

    private Path workDir;

    

    

    

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
    //private BioPipelineStage stage;

    private StageContext stageContext;



    public StageRunResult(boolean success, Path workDir, String failReason,Map<String,String> outputPath, StageContext stageContext,Exception e) {
        this.success = success;
        this.failReason = failReason;
        this.outputPath = outputPath;
        this.e = e;
        this.stageContext = stageContext;
        this.workDir = workDir;
    }

    

    public StageContext getStageContext() {
        return stageContext;
    }

    public void setStageContext(StageContext stageContext) {
        this.stageContext = stageContext;
    }

    public Map<String,String> getOutputPath(){
        return this.outputPath;
    }


    public void setOutputPath(Map<String, String> outputPath) {
        this.outputPath = outputPath;
    }

    public StageContext getStage() {
        return stageContext;
    }

    

    // public static StageRunResult OK(Map<String,String> outputPath, StageContext stageContext){
    //     return new StageRunResult(true, null, outputPath, stageContext, null);
    // }

    public static <T extends StageOutput> StageRunResult<T> OK(T stageOutput, StageContext stageContext, Path workDir){
        StageRunResult<T> stageRunResult = new StageRunResult<>(true, workDir,null, null, stageContext, null);
        stageRunResult.setStageOutput(stageOutput);
        return stageRunResult;
    }
    public static <T extends StageOutput> StageRunResult<T> fail(String failReason, Path workDir, StageContext stage,Exception e){
        return new StageRunResult<>(false, workDir, failReason, null, stage, e);
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
                "PipelineId=%d, StageId=%d, StageName=%s, StageType=%d\n" +
                        "Message=%s\n" +
                        "Exception=%s\n" +
                        "StackTrace:\n%s",
                stageContext.getPipelineId(),
                stageContext.getRunStageId(),
                Constants.StageType.STAGE_NAME_MAP.get(stageContext.getStageType()),
                stageContext.getStageType(),
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

    public Path getWorkDir() {
        return workDir;
    }

    public void setWorkDir(Path workDir) {
        this.workDir = workDir;
    }

    

}
