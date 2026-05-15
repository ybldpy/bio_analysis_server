package com.xjtlu.bio.analysisPipeline.stageResult;

public class ReferenceComparisonResult implements StageResult{

    private String pafPath;
    private String differenceTsv;
    public String getPafPath() {
        return pafPath;
    }
    public void setPafPath(String pafPath) {
        this.pafPath = pafPath;
    }
    public ReferenceComparisonResult(String pafPath, String differenceTsv) {
        this.pafPath = pafPath;
        this.differenceTsv = differenceTsv;
    }
    public ReferenceComparisonResult() {
    }
    public String getDifferenceTsv() {
        return differenceTsv;
    }
    public void setDifferenceTsv(String differenceTsv) {
        this.differenceTsv = differenceTsv;
    }

    

}
