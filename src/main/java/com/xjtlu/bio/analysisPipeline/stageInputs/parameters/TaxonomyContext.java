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
