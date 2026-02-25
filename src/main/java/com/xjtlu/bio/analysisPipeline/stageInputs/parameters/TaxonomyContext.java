package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;

public class TaxonomyContext {

    private Integer taxid;
    private String speciesName;
    private String rank; // S/G/...
    private String status; // CONFIDENT/AMBIGUOUS/NO_HIT
    private Double score;

    private TaxonomyContext() {

    }

    public Integer getTaxid() {
        return taxid;
    }

    public void setTaxid(Integer taxid) {
        this.taxid = taxid;
    }

    public String getSpeciesName() {
        return speciesName;
    }

    public void setSpeciesName(String speciesName) {
        this.speciesName = speciesName;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public static TaxonomyContext of(TaxonomyResult r) {
        TaxonomyContext ctx = new TaxonomyContext();
        ctx.status = r.getStatus();
        ctx.taxid = r.getBest().getTaxid();
        ctx.speciesName = r.getBest().getName();
        ctx.rank = r.getBest().getRank();
        ctx.score = r.getBest().getScore();

        return ctx;
    }

}
