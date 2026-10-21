package com.xjtlu.bio.response;

/** Public option fields; reference membership and database metadata stay internal. */
public class VirusTargetOption {

    private final String label;
    private final Integer taxId;
    private final Integer pipelineType;

    public VirusTargetOption(String label, Integer taxId, Integer pipelineType) {
        this.label = label;
        this.taxId = taxId;
        this.pipelineType = pipelineType;
    }

    public String getLabel() {
        return label;
    }

    public Integer getTaxId() {
        return taxId;
    }

    public Integer getPipelineType() {
        return pipelineType;
    }
}
