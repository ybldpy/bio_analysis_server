package com.xjtlu.bio.analysisPipeline.stageMetrics;

/** Common count fields shared by overall and per-reference variant metrics. */
public class VariantRecordCounts {

    private long totalVcfRecords;
    private long variantRecords;
    private long snpCount;
    private long indelCount;
    private long otherCount;
    private long explicitPassCount;
    private long filteredCount;
    private long unfilteredCount;

    public VariantRecordCounts() {
    }

    public VariantRecordCounts(
            long totalVcfRecords,
            long variantRecords,
            long snpCount,
            long indelCount,
            long otherCount,
            long explicitPassCount,
            long filteredCount,
            long unfilteredCount) {
        this.totalVcfRecords = totalVcfRecords;
        this.variantRecords = variantRecords;
        this.snpCount = snpCount;
        this.indelCount = indelCount;
        this.otherCount = otherCount;
        this.explicitPassCount = explicitPassCount;
        this.filteredCount = filteredCount;
        this.unfilteredCount = unfilteredCount;
    }

    public long getTotalVcfRecords() {
        return totalVcfRecords;
    }

    public void setTotalVcfRecords(long totalVcfRecords) {
        this.totalVcfRecords = totalVcfRecords;
    }

    public long getVariantRecords() {
        return variantRecords;
    }

    public void setVariantRecords(long variantRecords) {
        this.variantRecords = variantRecords;
    }

    public long getSnpCount() {
        return snpCount;
    }

    public void setSnpCount(long snpCount) {
        this.snpCount = snpCount;
    }

    public long getIndelCount() {
        return indelCount;
    }

    public void setIndelCount(long indelCount) {
        this.indelCount = indelCount;
    }

    public long getOtherCount() {
        return otherCount;
    }

    public void setOtherCount(long otherCount) {
        this.otherCount = otherCount;
    }

    public long getExplicitPassCount() {
        return explicitPassCount;
    }

    public void setExplicitPassCount(long explicitPassCount) {
        this.explicitPassCount = explicitPassCount;
    }

    public long getFilteredCount() {
        return filteredCount;
    }

    public void setFilteredCount(long filteredCount) {
        this.filteredCount = filteredCount;
    }

    public long getUnfilteredCount() {
        return unfilteredCount;
    }

    public void setUnfilteredCount(long unfilteredCount) {
        this.unfilteredCount = unfilteredCount;
    }
}
