package com.xjtlu.bio.requestParameters;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public class ConfirmTaxonomyRequest {

    @NotNull
    @Positive
    private Long stageId;

    @NotNull
    @Positive
    private Integer taxId;

    @Size(max = 2000)
    private String reason;

    public ConfirmTaxonomyRequest() {
    }

    public ConfirmTaxonomyRequest(Long stageId, Integer taxId, String reason) {
        this.stageId = stageId;
        this.taxId = taxId;
        this.reason = reason;
    }

    public Long getStageId() {
        return stageId;
    }

    public void setStageId(Long stageId) {
        this.stageId = stageId;
    }

    public Integer getTaxId() {
        return taxId;
    }

    public void setTaxId(Integer taxId) {
        this.taxId = taxId;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
}
