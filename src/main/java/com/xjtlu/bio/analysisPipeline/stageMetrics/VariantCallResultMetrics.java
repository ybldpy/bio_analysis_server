package com.xjtlu.bio.analysisPipeline.stageMetrics;

import java.util.ArrayList;
import java.util.List;

/**
 * Record-level metrics calculated from a VCF.
 *
 * <p>{@code totalVcfRecords} includes reference records such as {@code ALT=.},
 * while {@code variantRecords} includes only records with at least one real
 * alternate allele. SNP, indel and other counts form a mutually exclusive
 * partition of {@code variantRecords}. Filter counts also apply only to
 * variant records.</p>
 */
public class VariantCallResultMetrics extends VariantRecordCounts {

    private List<ReferenceMetrics> references = new ArrayList<>();

    public VariantCallResultMetrics() {
    }

    public VariantCallResultMetrics(
            long totalVcfRecords,
            long variantRecords,
            long snpCount,
            long indelCount,
            long otherCount,
            long explicitPassCount,
            long filteredCount,
            long unfilteredCount,
            List<ReferenceMetrics> references) {
        super(
                totalVcfRecords,
                variantRecords,
                snpCount,
                indelCount,
                otherCount,
                explicitPassCount,
                filteredCount,
                unfilteredCount);
        this.references = references == null
                ? new ArrayList<>()
                : new ArrayList<>(references);
    }

    public List<ReferenceMetrics> getReferences() {
        return references;
    }

    public void setReferences(List<ReferenceMetrics> references) {
        this.references = references;
    }

    public static class ReferenceMetrics extends VariantRecordCounts {

        private String referenceName;
        private Long referenceLength;

        public ReferenceMetrics() {
        }

        public ReferenceMetrics(
                String referenceName,
                Long referenceLength,
                long totalVcfRecords,
                long variantRecords,
                long snpCount,
                long indelCount,
                long otherCount,
                long explicitPassCount,
                long filteredCount,
                long unfilteredCount) {
            super(
                    totalVcfRecords,
                    variantRecords,
                    snpCount,
                    indelCount,
                    otherCount,
                    explicitPassCount,
                    filteredCount,
                    unfilteredCount);
            this.referenceName = referenceName;
            this.referenceLength = referenceLength;
        }

        public String getReferenceName() {
            return referenceName;
        }

        public void setReferenceName(String referenceName) {
            this.referenceName = referenceName;
        }

        public Long getReferenceLength() {
            return referenceLength;
        }

        public void setReferenceLength(Long referenceLength) {
            this.referenceLength = referenceLength;
        }
    }

}
