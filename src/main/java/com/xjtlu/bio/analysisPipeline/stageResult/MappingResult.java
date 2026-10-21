package com.xjtlu.bio.analysisPipeline.stageResult;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;

public class MappingResult implements StageResult{

    private String bamUrl;
    private String bamIndexUrl;
    private String coverageDepthUrl;
    @JsonAlias("qcUrl")
    private String metricsUrl;
    // Accept legacy inline metrics on read; new results only serialize metricsUrl.
    @JsonAlias("qcMetrics")
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private MappingResultMetrics metrics;




    public String getBamUrl() {
        return bamUrl;
    }
    public MappingResult(String bamUrl, String bamIndexUrl, String coverageDepthUrl) {
        this.bamUrl = bamUrl;
        this.bamIndexUrl = bamIndexUrl;
        this.coverageDepthUrl = coverageDepthUrl;
    }
    public MappingResult(
            String bamUrl,
            String bamIndexUrl,
            String coverageDepthUrl,
            String metricsUrl) {
        this(bamUrl, bamIndexUrl, coverageDepthUrl);
        this.metricsUrl = metricsUrl;
    }
    public MappingResult(
            String bamUrl,
            String bamIndexUrl,
            String coverageDepthUrl,
            String metricsUrl,
            MappingResultMetrics metrics) {
        this(bamUrl, bamIndexUrl, coverageDepthUrl, metricsUrl);
        this.metrics = metrics;
    }
    public MappingResult() {
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
    public String getCoverageDepthUrl() {
        return coverageDepthUrl;
    }
    public void setCoverageDepthUrl(String coverageDepthUrl) {
        this.coverageDepthUrl = coverageDepthUrl;
    }
    public String getMetricsUrl() {
        return metricsUrl;
    }
    public void setMetricsUrl(String metricsUrl) {
        this.metricsUrl = metricsUrl;
    }
    public MappingResultMetrics getMetrics() {
        return metrics;
    }
    public void setMetrics(MappingResultMetrics metrics) {
        this.metrics = metrics;
    }

}
