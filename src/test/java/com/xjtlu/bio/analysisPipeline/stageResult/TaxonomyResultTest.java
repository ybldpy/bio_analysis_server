package com.xjtlu.bio.analysisPipeline.stageResult;

import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.EVIDENCE_FASTANI;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.EVIDENCE_USER_CONFIRMED;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_AMBIGUOUS;
import static com.xjtlu.bio.analysisPipeline.Constants.TaxonomyClassification.STATUS_CONFIDENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.xjtlu.bio.analysisPipeline.stageMetrics.TaxonomyBinningMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.ManualConfirmation;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.Taxon;
import com.xjtlu.bio.utils.JsonUtil;

class TaxonomyResultTest {

    @Test
    void legacyResultWithoutAuditPropertiesRemainsReadable() throws Exception {
        TaxonomyResult result = JsonUtil.toObject("""
                {
                  "status": 1,
                  "best": null,
                  "candidates": [
                    {"taxid": 111, "name": "Candidate A", "score": 97.5}
                  ]
                }
                """, TaxonomyResult.class);

        assertEquals(STATUS_AMBIGUOUS, result.getStatus());
        assertNull(result.getBest());
        assertNull(result.getEvidenceResource());
        assertNull(result.getManualConfirmation());
        assertEquals(111, result.getCandidates().get(0).getTaxid());
    }

    @Test
    void auditRoundTripRetainsOriginalEvidenceCandidatesAndBinningArtifacts() throws Exception {
        TaxonomyResult result = new TaxonomyResult();
        result.setStatus(STATUS_CONFIDENT);
        result.setEvidenceResource(EVIDENCE_USER_CONFIRMED);
        Taxon candidate = new Taxon();
        candidate.setTaxid(111);
        candidate.setName("Candidate A");
        candidate.setRank("S");
        candidate.setScore(97.5);
        result.setBest(candidate);
        result.setCandidates(List.of(candidate));
        result.setMainBinFastaUrl("stored/main_bin.fasta");
        result.setContigAssignmentReportUrl("stored/contig_assignments.tsv");
        TaxonomyBinningMetrics metrics = new TaxonomyBinningMetrics();
        metrics.setTotalContigs(7);
        metrics.setDominantSpeciesTaxId(111);
        result.setBinningMetrics(metrics);

        Date confirmedAt = new Date(1_800_000_000_000L);
        ManualConfirmation audit = new ManualConfirmation();
        audit.setSpeciesTaxId(111);
        audit.setConfirmedAt(confirmedAt);
        audit.setConfirmedBy("reviewer");
        audit.setReason("Independent identification reviewed");
        audit.setOriginalStatus(STATUS_AMBIGUOUS);
        audit.setOriginalEvidenceResource(EVIDENCE_FASTANI);
        result.setManualConfirmation(audit);

        TaxonomyResult restored = JsonUtil.toObject(JsonUtil.toJson(result), TaxonomyResult.class);

        assertEquals(STATUS_CONFIDENT, restored.getStatus());
        assertEquals(EVIDENCE_USER_CONFIRMED, restored.getEvidenceResource());
        assertEquals(111, restored.getBest().getTaxid());
        assertEquals("Candidate A", restored.getBest().getName());
        assertEquals("S", restored.getBest().getRank());
        assertEquals(97.5, restored.getBest().getScore());
        assertEquals(1, restored.getCandidates().size());
        assertEquals(97.5, restored.getCandidates().get(0).getScore());
        assertEquals("stored/main_bin.fasta", restored.getMainBinFastaUrl());
        assertEquals("stored/contig_assignments.tsv", restored.getContigAssignmentReportUrl());
        assertEquals(7, restored.getBinningMetrics().getTotalContigs());
        assertEquals(111, restored.getBinningMetrics().getDominantSpeciesTaxId());
        ManualConfirmation restoredAudit = restored.getManualConfirmation();
        assertEquals(111, restoredAudit.getSpeciesTaxId());
        assertEquals(confirmedAt, restoredAudit.getConfirmedAt());
        assertEquals("reviewer", restoredAudit.getConfirmedBy());
        assertEquals("Independent identification reviewed", restoredAudit.getReason());
        assertEquals(STATUS_AMBIGUOUS, restoredAudit.getOriginalStatus());
        assertEquals(EVIDENCE_FASTANI, restoredAudit.getOriginalEvidenceResource());
    }

    @Test
    void nullableReviewerSurvivesJsonRoundTrip() throws Exception {
        ManualConfirmation audit = new ManualConfirmation();
        audit.setSpeciesTaxId(111);
        audit.setConfirmedBy(null);
        TaxonomyResult result = new TaxonomyResult();
        result.setManualConfirmation(audit);

        TaxonomyResult restored = JsonUtil.toObject(JsonUtil.toJson(result), TaxonomyResult.class);

        assertEquals(111, restored.getManualConfirmation().getSpeciesTaxId());
        assertNull(restored.getManualConfirmation().getConfirmedBy());
    }
}
