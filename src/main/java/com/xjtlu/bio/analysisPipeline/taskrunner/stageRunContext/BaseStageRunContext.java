package com.xjtlu.bio.analysisPipeline.taskrunner.stageRunContext;

import java.nio.file.Path;

import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.StageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.BaseStageParams;

public class BaseStageRunContext<Input extends StageInputUrls, StageParameters extends BaseStageParams> {

    private StageContext stageContext;
    private Input inputUrls;
    private StageParameters stageParameters;
    private Path workDir;
    private Path inputDir;
    public StageContext getStageContext() {
        return stageContext;
    }
    public void setStageContext(StageContext stageContext) {
        this.stageContext = stageContext;
    }
    public Input getInputUrls() {
        return inputUrls;
    }
    public void setInputUrls(Input inputUrls) {
        this.inputUrls = inputUrls;
    }
    public StageParameters getStageParameters() {
        return stageParameters;
    }
    public void setStageParameters(StageParameters stageParameters) {
        this.stageParameters = stageParameters;
    }
    public BaseStageRunContext() {
    }
    public Path getWorkDir() {
        return workDir;
    }
    public void setWorkDir(Path workDir) {
        this.workDir = workDir;
    }
    public Path getInputDir() {
        return inputDir;
    }
    public void setInputDir(Path inputDir) {
        this.inputDir = inputDir;
    }

    


}
