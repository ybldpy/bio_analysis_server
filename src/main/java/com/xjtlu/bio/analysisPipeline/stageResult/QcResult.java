package com.xjtlu.bio.analysisPipeline.stageResult;

public class QcResult implements StageResult{

    private String cleanedR1;
    private String cleanedR2;

    private String reportHTML;
    private String reportJSON;
    public String getCleanedR1() {
        return cleanedR1;
    }
    public QcResult(String cleanedR1, String cleanedR2, String reportHTML, String reportJSON) {
        this.cleanedR1 = cleanedR1;
        this.cleanedR2 = cleanedR2;
        this.reportHTML = reportHTML;
        this.reportJSON = reportJSON;
    }
    public void setCleanedR1(String cleanedR1) {
        this.cleanedR1 = cleanedR1;
    }
    public String getCleanedR2() {
        return cleanedR2;
    }
    public void setCleanedR2(String cleanedR2) {
        this.cleanedR2 = cleanedR2;
    }
    public String getReportHTML() {
        return reportHTML;
    }
    public void setReportHTML(String reportHTML) {
        this.reportHTML = reportHTML;
    }
    public String getReportJSON() {
        return reportJSON;
    }
    public void setReportJSON(String reportJSON) {
        this.reportJSON = reportJSON;
    }



}
