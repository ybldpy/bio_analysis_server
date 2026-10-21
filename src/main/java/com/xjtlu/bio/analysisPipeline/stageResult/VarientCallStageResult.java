package com.xjtlu.bio.analysisPipeline.stageResult;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;

public class VarientCallStageResult implements StageResult{

    private String rawVcfGzUrl;
    private String rawVcfTbiUrl;
    private String vcfGzUrl;
    private String vcfTbiUrl;
    @JsonAlias("qcUrl")
    private String metricsUrl;
    // Accept legacy inline metrics on read; new results only serialize metricsUrl.
    @JsonAlias("qcMetrics")
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private VariantCallResultMetrics metrics;


    public VarientCallStageResult(){

    }

    


    public VarientCallStageResult(String vcfGzUrl, String vcfTbiUrl) {
        this(null, null, vcfGzUrl, vcfTbiUrl, null);
    }

    public VarientCallStageResult(String vcfGzUrl, String vcfTbiUrl, String metricsUrl) {
        this(null, null, vcfGzUrl, vcfTbiUrl, metricsUrl);
    }

    public VarientCallStageResult(
            String vcfGzUrl,
            String vcfTbiUrl,
            String metricsUrl,
            VariantCallResultMetrics metrics) {
        this(null, null, vcfGzUrl, vcfTbiUrl, metricsUrl, metrics);
    }

    public VarientCallStageResult(
            String rawVcfGzUrl,
            String rawVcfTbiUrl,
            String vcfGzUrl,
            String vcfTbiUrl,
            String metricsUrl) {
        this.rawVcfGzUrl = rawVcfGzUrl;
        this.rawVcfTbiUrl = rawVcfTbiUrl;
        this.vcfGzUrl = vcfGzUrl;
        this.vcfTbiUrl = vcfTbiUrl;
        this.metricsUrl = metricsUrl;
    }

    public VarientCallStageResult(
            String rawVcfGzUrl,
            String rawVcfTbiUrl,
            String vcfGzUrl,
            String vcfTbiUrl,
            String metricsUrl,
            VariantCallResultMetrics metrics) {
        this(rawVcfGzUrl, rawVcfTbiUrl, vcfGzUrl, vcfTbiUrl, metricsUrl);
        this.metrics = metrics;
    }

    public String getRawVcfGzUrl() {
        return rawVcfGzUrl;
    }
    public void setRawVcfGzUrl(String rawVcfGzUrl) {
        this.rawVcfGzUrl = rawVcfGzUrl;
    }
    public String getRawVcfTbiUrl() {
        return rawVcfTbiUrl;
    }
    public void setRawVcfTbiUrl(String rawVcfTbiUrl) {
        this.rawVcfTbiUrl = rawVcfTbiUrl;
    }
    public String getVcfGzUrl() {
        return vcfGzUrl;
    }
    public void setVcfGzUrl(String vcfGzUrl) {
        this.vcfGzUrl = vcfGzUrl;
    }
    public String getVcfTbiUrl() {
        return vcfTbiUrl;
    }
    public void setVcfTbiUrl(String vcfTbiUrl) {
        this.vcfTbiUrl = vcfTbiUrl;
    }
    public String getMetricsUrl() {
        return metricsUrl;
    }
    public void setMetricsUrl(String metricsUrl) {
        this.metricsUrl = metricsUrl;
    }
    public VariantCallResultMetrics getMetrics() {
        return metrics;
    }
    public void setMetrics(VariantCallResultMetrics metrics) {
        this.metrics = metrics;
    }



    

}
