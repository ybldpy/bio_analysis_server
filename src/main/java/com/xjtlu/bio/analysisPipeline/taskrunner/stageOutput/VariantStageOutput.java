package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

import com.xjtlu.bio.analysisPipeline.stageMetrics.VariantCallResultMetrics;

public class VariantStageOutput implements StageOutput{

    public static final String RAW_VCF_GZ = "variants.raw.vcf.gz";
    public static final String RAW_VCF_TBI = "variants.raw.vcf.gz.tbi";
    public static final String VCF_GZ = "variants.vcf.gz";
    public static final String VCF_TBI = "variants.vcf.gz.tbi";
    public static final String METRICS_JSON = "variant.metrics.json";


    public VariantStageOutput(String vcfGz, String vcfTbi) {
        this(null, null, vcfGz, vcfTbi, null, null);
    }

    public VariantStageOutput(
            String vcfGz,
            String vcfTbi,
            String metricsJson,
            VariantCallResultMetrics metrics) {
        this(null, null, vcfGz, vcfTbi, metricsJson, metrics);
    }

    public VariantStageOutput(
            String rawVcfGz,
            String rawVcfTbi,
            String vcfGz,
            String vcfTbi,
            String metricsJson,
            VariantCallResultMetrics metrics) {
        this.rawVcfGz = rawVcfGz;
        this.rawVcfTbi = rawVcfTbi;
        this.vcfGz = vcfGz;
        this.vcfTbi = vcfTbi;
        this.metricsJson = metricsJson;
        this.metrics = metrics;
    }
    public String getRawVcfGz() {
        return rawVcfGz;
    }
    public void setRawVcfGz(String rawVcfGz) {
        this.rawVcfGz = rawVcfGz;
    }
    public String getRawVcfTbi() {
        return rawVcfTbi;
    }
    public void setRawVcfTbi(String rawVcfTbi) {
        this.rawVcfTbi = rawVcfTbi;
    }
    public String getVcfGz() {
        return vcfGz;
    }
    public void setVcfGz(String vcfGz) {
        this.vcfGz = vcfGz;
    }
    public String getVcfTbi() {
        return vcfTbi;
    }
    public void setVcfTbi(String vcfTbi) {
        this.vcfTbi = vcfTbi;
    }
    public String getMetricsJson() {
        return metricsJson;
    }
    public void setMetricsJson(String metricsJson) {
        this.metricsJson = metricsJson;
    }
    public VariantCallResultMetrics getMetrics() {
        return metrics;
    }
    public void setMetrics(VariantCallResultMetrics metrics) {
        this.metrics = metrics;
    }
    private String rawVcfGz;
    private String rawVcfTbi;
    private String vcfGz;
    private String vcfTbi;
    private String metricsJson;
    private VariantCallResultMetrics metrics;

    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return Path.of(vcfGz).getParent();
    }
    

}
