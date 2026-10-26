package com.xjtlu.bio.analysisPipeline.stageResult;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Compact artifact index; complete candidate details are stored in comparisonJsonUrl. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReferenceComparisonResult implements StageResult {

    private Integer schemaVersion;
    private Integer candidateCount;
    private Integer matchedCandidateCount;
    private Integer unmatchedCandidateCount;
    private String comparisonJsonUrl;
    private String comparisonTsvUrl;

    // Retained for reading and completing historical single-reference stages.
    private String pafPath;
    private String differenceTsv;

    public ReferenceComparisonResult() {
    }

    public ReferenceComparisonResult(int candidateCount, int matchedCandidateCount,
            int unmatchedCandidateCount, String comparisonJsonUrl, String comparisonTsvUrl) {
        this.schemaVersion = 2;
        this.candidateCount = candidateCount;
        this.matchedCandidateCount = matchedCandidateCount;
        this.unmatchedCandidateCount = unmatchedCandidateCount;
        this.comparisonJsonUrl = comparisonJsonUrl;
        this.comparisonTsvUrl = comparisonTsvUrl;
    }

    public ReferenceComparisonResult(String pafPath, String differenceTsv) {
        this.pafPath = pafPath;
        this.differenceTsv = differenceTsv;
    }

    public Integer getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(Integer schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public Integer getCandidateCount() {
        return candidateCount;
    }

    public void setCandidateCount(Integer candidateCount) {
        this.candidateCount = candidateCount;
    }

    public Integer getMatchedCandidateCount() {
        return matchedCandidateCount;
    }

    public void setMatchedCandidateCount(Integer matchedCandidateCount) {
        this.matchedCandidateCount = matchedCandidateCount;
    }

    public Integer getUnmatchedCandidateCount() {
        return unmatchedCandidateCount;
    }

    public void setUnmatchedCandidateCount(Integer unmatchedCandidateCount) {
        this.unmatchedCandidateCount = unmatchedCandidateCount;
    }

    public String getComparisonJsonUrl() {
        return comparisonJsonUrl;
    }

    public void setComparisonJsonUrl(String comparisonJsonUrl) {
        this.comparisonJsonUrl = comparisonJsonUrl;
    }

    public String getComparisonTsvUrl() {
        return comparisonTsvUrl;
    }

    public void setComparisonTsvUrl(String comparisonTsvUrl) {
        this.comparisonTsvUrl = comparisonTsvUrl;
    }

    public String getPafPath() {
        return pafPath;
    }

    public void setPafPath(String pafPath) {
        this.pafPath = pafPath;
    }

    public String getDifferenceTsv() {
        return differenceTsv;
    }

    public void setDifferenceTsv(String differenceTsv) {
        this.differenceTsv = differenceTsv;
    }
}
