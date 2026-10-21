package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class CovidWastewaterStageOutput implements StageOutput {

    private Path reportPath;
    private Path metricsPath;
    private Path provenancePath;
    private Path executionLogPath;
    private Path bamPath;
    private Path bamIndexPath;

    public CovidWastewaterStageOutput() {
    }

    public Path getReportPath() {
        return reportPath;
    }

    public void setReportPath(Path reportPath) {
        this.reportPath = reportPath;
    }

    public Path getMetricsPath() {
        return metricsPath;
    }

    public void setMetricsPath(Path metricsPath) {
        this.metricsPath = metricsPath;
    }

    public Path getProvenancePath() {
        return provenancePath;
    }

    public void setProvenancePath(Path provenancePath) {
        this.provenancePath = provenancePath;
    }

    public Path getExecutionLogPath() {
        return executionLogPath;
    }

    public void setExecutionLogPath(Path executionLogPath) {
        this.executionLogPath = executionLogPath;
    }

    public Path getBamPath() {
        return bamPath;
    }

    public void setBamPath(Path bamPath) {
        this.bamPath = bamPath;
    }

    public Path getBamIndexPath() {
        return bamIndexPath;
    }

    public void setBamIndexPath(Path bamIndexPath) {
        this.bamIndexPath = bamIndexPath;
    }

    @Override
    public Path getParentPath() {
        return reportPath == null ? null : reportPath.getParent();
    }
}
