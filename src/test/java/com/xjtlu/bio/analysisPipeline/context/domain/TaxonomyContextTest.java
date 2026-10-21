package com.xjtlu.bio.analysisPipeline.context.domain;

import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_AMBIGUOUS;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_LOW_CONFIDENCE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.Taxon;

class TaxonomyContextTest {

    @Test
    void missingResultIsUnresolvedRatherThanConfident() {
        TaxonomyContext context = TaxonomyContext.of(null);

        assertEquals(STATUS_LOW_CONFIDENCE, context.getStatus());
        assertNull(context.getTaxid());
        assertNull(context.getSpeciesName());
        assertNull(context.getRank());
        assertNull(context.getScore());
    }

    @Test
    void unresolvedResultKeepsItsStatusAndDoesNotSelectTheFirstCandidate() {
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_AMBIGUOUS);
        Taxon candidate = new Taxon();
        candidate.setTaxid(111);
        candidate.setName("Candidate A");
        result.setCandidates(List.of(candidate));

        TaxonomyContext context = TaxonomyContext.of(result);

        assertEquals(STATUS_AMBIGUOUS, context.getStatus());
        assertNull(context.getTaxid());
        assertNull(context.getSpeciesName());
        assertNull(context.getRank());
        assertNull(context.getScore());
    }

    @Test
    void resolvedResultRetainsAllClassificationFields() {
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_CONFIDENT);
        Taxon best = new Taxon();
        best.setTaxid(111);
        best.setName("Candidate A");
        best.setRank("S");
        best.setScore(98.5);
        result.setBest(best);

        TaxonomyContext context = TaxonomyContext.of(result);

        assertEquals(STATUS_CONFIDENT, context.getStatus());
        assertEquals(111, context.getTaxid());
        assertEquals("Candidate A", context.getSpeciesName());
        assertEquals("S", context.getRank());
        assertEquals(98.5, context.getScore());
    }
}
