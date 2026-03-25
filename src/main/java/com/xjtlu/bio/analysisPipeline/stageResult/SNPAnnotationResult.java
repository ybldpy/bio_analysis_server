package com.xjtlu.bio.analysisPipeline.stageResult;

public class SNPAnnotationResult implements StageResult{

    private String annotatedVCF;

    public String getAnnotatedVCF() {
        return annotatedVCF;
    }

    public SNPAnnotationResult() {
    }

    public SNPAnnotationResult(String annotatedVCF) {
        this.annotatedVCF = annotatedVCF;
    }

    public void setAnnotatedVCF(String annotatedVCF) {
        this.annotatedVCF = annotatedVCF;
    }
    

}
