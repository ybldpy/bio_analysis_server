package com.xjtlu.bio.taskrunner.stageOutput;

public class QCStageOutput implements StageOutput{


    private String r1Path;
    public String getR1Path() {
        return r1Path;
    }
    public QCStageOutput(String r1Path, String r2Path, String jsonPath, String htmlPath) {
        this.r1Path = r1Path;
        this.r2Path = r2Path;
        this.jsonPath = jsonPath;
        this.htmlPath = htmlPath;
    }
    public void setR1Path(String r1Path) {
        this.r1Path = r1Path;
    }
    public String getR2Path() {
        return r2Path;
    }
    public void setR2Path(String r2Path) {
        this.r2Path = r2Path;
    }
    public String getJsonPath() {
        return jsonPath;
    }
    public void setJsonPath(String jsonPath) {
        this.jsonPath = jsonPath;
    }
    public String getHtmlPath() {
        return htmlPath;
    }
    public void setHtmlPath(String htmlPath) {
        this.htmlPath = htmlPath;
    }
    private String r2Path;
    
    private String jsonPath;
    private String htmlPath;

}
