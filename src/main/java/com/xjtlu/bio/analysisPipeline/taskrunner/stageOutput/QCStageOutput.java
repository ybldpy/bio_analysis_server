package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class QCStageOutput implements StageOutput {


    public static final String R1 = "trimmed_r1.fastq.gz";
    public static final String R2 = "trimmed_r2.fastq.gz";
    public static final String JSON = "qc.json";
    public static final String HTML = "qc.html";

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


    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return Path.of(r1Path).getParent();
    }


}
