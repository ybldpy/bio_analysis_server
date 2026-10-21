package com.xjtlu.bio.analysisPipeline.stageResult;

/** Artifact references only. The complete metrics belong in the metrics JSON file. */
public class CovidWastewaterStageResult implements StageResult {

    private String reportUrl;
    private String metricsUrl;
    private String provenanceUrl;
    private String executionLogUrl;
    private String bamUrl;
    private String bamIndexUrl;

    public CovidWastewaterStageResult() {
    }

    public String getReportUrl() {
        return reportUrl;
    }

    public void setReportUrl(String reportUrl) {
        this.reportUrl = reportUrl;
    }

    public String getMetricsUrl() {
        return metricsUrl;
    }

    public void setMetricsUrl(String metricsUrl) {
        this.metricsUrl = metricsUrl;
    }

    public String getProvenanceUrl() {
        return provenanceUrl;
    }

    public void setProvenanceUrl(String provenanceUrl) {
        this.provenanceUrl = provenanceUrl;
    }

    public String getExecutionLogUrl() {
        return executionLogUrl;
    }

    public void setExecutionLogUrl(String executionLogUrl) {
        this.executionLogUrl = executionLogUrl;
    }

    public String getBamUrl() {
        return bamUrl;
    }

    public void setBamUrl(String bamUrl) {
        this.bamUrl = bamUrl;
    }

    public String getBamIndexUrl() {
        return bamIndexUrl;
    }

    public void setBamIndexUrl(String bamIndexUrl) {
        this.bamIndexUrl = bamIndexUrl;
    }
}
