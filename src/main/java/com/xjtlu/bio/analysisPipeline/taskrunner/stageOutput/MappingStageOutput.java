package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

import com.xjtlu.bio.analysisPipeline.stageMetrics.MappingResultMetrics;

public class MappingStageOutput implements StageOutput{


    public static final String BAM = "aln.sorted.bam";
    public static final String BAM_INDEX = "aln.sorted.bam.bai";

    public static final String COVERAGE_DEPTH = "depth.tsv";
    public static final String METRICS_JSON = "mapping.metrics.json";

    private Path bamPath;
    private Path bamIndexPath;
    private Path coverageDepthPath;
    private Path metricsJsonPath;
    private MappingResultMetrics metrics;


    public Path getCoverageDepthPath() {
        return coverageDepthPath;
    }
    public void setCoverageDepthPath(Path coverageDepthPath) {
        this.coverageDepthPath = coverageDepthPath;
    }
    public Path getBamPath() {
        return bamPath;
    }
    public void setBamPath(Path bamPath) {
        this.bamPath = bamPath;
    }
    public MappingStageOutput(Path bamPath, Path bamIndexPath, Path coverageDepthPath) {
        this.bamPath = bamPath;
        this.bamIndexPath = bamIndexPath;
        this.coverageDepthPath = coverageDepthPath;
    }
    public MappingStageOutput(
            Path bamPath,
            Path bamIndexPath,
            Path coverageDepthPath,
            Path metricsJsonPath,
            MappingResultMetrics metrics) {
        this(bamPath, bamIndexPath, coverageDepthPath);
        this.metricsJsonPath = metricsJsonPath;
        this.metrics = metrics;
    }
    public Path getBamIndexPath() {
        return bamIndexPath;
    }
    public void setBamIndexPath(Path bamIndexPath) {
        this.bamIndexPath = bamIndexPath;
    }
    public Path getMetricsJsonPath() {
        return metricsJsonPath;
    }
    public void setMetricsJsonPath(Path metricsJsonPath) {
        this.metricsJsonPath = metricsJsonPath;
    }
    public MappingResultMetrics getMetrics() {
        return metrics;
    }
    public void setMetrics(MappingResultMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return bamIndexPath.toAbsolutePath().getParent();
    }
    

}
