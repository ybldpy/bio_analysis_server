package com.xjtlu.bio.analysisPipeline.stageMetrics;

import java.util.ArrayList;
import java.util.List;

/** Report-only values; abundances are fractions and coverage is a percentage. */
public class CovidWastewaterResultMetrics {

    private List<LineageAbundance> lineages = new ArrayList<>();
    private Double coveragePercent;
    private Double residual;
    private String summarizedRaw;

    public CovidWastewaterResultMetrics() {
    }

    public List<LineageAbundance> getLineages() {
        return lineages;
    }

    public void setLineages(List<LineageAbundance> lineages) {
        this.lineages = lineages == null ? new ArrayList<>() : new ArrayList<>(lineages);
    }

    public Double getCoveragePercent() {
        return coveragePercent;
    }

    public void setCoveragePercent(Double coveragePercent) {
        this.coveragePercent = coveragePercent;
    }

    public Double getResidual() {
        return residual;
    }

    public void setResidual(Double residual) {
        this.residual = residual;
    }

    public String getSummarizedRaw() {
        return summarizedRaw;
    }

    public void setSummarizedRaw(String summarizedRaw) {
        this.summarizedRaw = summarizedRaw;
    }

    public static class LineageAbundance {

        private String lineage;
        private double abundance;

        public LineageAbundance() {
        }

        public LineageAbundance(String lineage, double abundance) {
            this.lineage = lineage;
            this.abundance = abundance;
        }

        public String getLineage() {
            return lineage;
        }

        public void setLineage(String lineage) {
            this.lineage = lineage;
        }

        public double getAbundance() {
            return abundance;
        }

        public void setAbundance(double abundance) {
            this.abundance = abundance;
        }
    }
}
