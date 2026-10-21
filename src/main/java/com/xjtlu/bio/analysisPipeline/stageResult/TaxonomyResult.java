package com.xjtlu.bio.analysisPipeline.stageResult;

import java.util.Date;
import java.util.List;

import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics;

public class TaxonomyResult implements StageResult {

    /**
     * Audit information for a human choice made from the stored candidates.
     * Original classification evidence remains available after confirmation.
     */
    public static class ManualConfirmation {

        private int speciesTaxId;
        private Date confirmedAt;
        private String confirmedBy;
        private String reason;
        private int originalStatus;
        private String originalEvidenceResource;

        public ManualConfirmation() {
        }

        public int getSpeciesTaxId() {
            return speciesTaxId;
        }

        public void setSpeciesTaxId(int speciesTaxId) {
            this.speciesTaxId = speciesTaxId;
        }

        public Date getConfirmedAt() {
            return confirmedAt;
        }

        public void setConfirmedAt(Date confirmedAt) {
            this.confirmedAt = confirmedAt;
        }

        public String getConfirmedBy() {
            return confirmedBy;
        }

        public void setConfirmedBy(String confirmedBy) {
            this.confirmedBy = confirmedBy;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }

        public int getOriginalStatus() {
            return originalStatus;
        }

        public void setOriginalStatus(int originalStatus) {
            this.originalStatus = originalStatus;
        }

        public String getOriginalEvidenceResource() {
            return originalEvidenceResource;
        }

        public void setOriginalEvidenceResource(String originalEvidenceResource) {
            this.originalEvidenceResource = originalEvidenceResource;
        }
    }

    public static class Taxon {

        private int taxid;
        private String name;
        private String rank;
        private double score;

        public Taxon() {
        }

        public int getTaxid() {
            return taxid;
        }

        public void setTaxid(int taxid) {
            this.taxid = taxid;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getRank() {
            return rank;
        }

        public void setRank(String rank) {
            this.rank = rank;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "Taxon{" +
                    "taxid=" + taxid +
                    ", name='" + name + '\'' +
                    ", rank='" + rank + '\'' +
                    ", score=" + score +
                    '}';
        }
    }

    private int status; // CONFIDENT / AMBIGUOUS / NO_HIT
    private Taxon best;
    private List<Taxon> candidates;
    private String evidenceResource;
    private ManualConfirmation manualConfirmation;
    private String mainBinFastaUrl;
    private String contigAssignmentReportUrl;
    private TaxonomyBinningMetrics binningMetrics;

    public TaxonomyResult() {
    }

    

    public Taxon getBest() {
        return best;
    }

    public void setBest(Taxon best) {
        this.best = best;
    }

    public List<Taxon> getCandidates() {
        return candidates;
    }

    public void setCandidates(List<Taxon> candidates) {
        this.candidates = candidates;
    }

    public String getEvidenceResource() {
        return evidenceResource;
    }

    public void setEvidenceResource(String evidenceResource) {
        this.evidenceResource = evidenceResource;
    }

    public ManualConfirmation getManualConfirmation() {
        return manualConfirmation;
    }

    public void setManualConfirmation(ManualConfirmation manualConfirmation) {
        this.manualConfirmation = manualConfirmation;
    }

    @Override
    public String toString() {
        return "TaxonomyResult{" +
                "status='" + status + '\'' +
                ", best=" + best +
                ", candidates=" + candidates +
                '}';
    }



    public int getStatus() {
        return status;
    }



    public void setStatus(int status) {
        this.status = status;
    }

    public String getMainBinFastaUrl() {
        return mainBinFastaUrl;
    }

    public void setMainBinFastaUrl(String mainBinFastaUrl) {
        this.mainBinFastaUrl = mainBinFastaUrl;
    }

    public String getContigAssignmentReportUrl() {
        return contigAssignmentReportUrl;
    }

    public void setContigAssignmentReportUrl(String contigAssignmentReportUrl) {
        this.contigAssignmentReportUrl = contigAssignmentReportUrl;
    }

    public TaxonomyBinningMetrics getBinningMetrics() {
        return binningMetrics;
    }

    public void setBinningMetrics(TaxonomyBinningMetrics binningMetrics) {
        this.binningMetrics = binningMetrics;
    }

}
