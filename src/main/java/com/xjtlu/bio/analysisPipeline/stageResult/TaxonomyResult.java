package com.xjtlu.bio.analysisPipeline.stageResult;

import java.util.List;

public class TaxonomyResult implements StageResult{

    public class Taxon {

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

    private String status;      // CONFIDENT / AMBIGUOUS / NO_HIT
    private Taxon best;
    private List<Taxon> candidates;

    public TaxonomyResult() {
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    @Override
    public String toString() {
        return "TaxonomyResult{" +
                "status='" + status + '\'' +
                ", best=" + best +
                ", candidates=" + candidates +
                '}';
    }

    @Override
    public int resultType() {
        // TODO Auto-generated method stub
        return RESULT_TYPE_VALUE;
    }
}
