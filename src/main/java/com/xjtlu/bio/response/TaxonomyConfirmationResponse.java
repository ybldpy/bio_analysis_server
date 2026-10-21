package com.xjtlu.bio.response;

public class TaxonomyConfirmationResponse {

    private long stageId;
    private int taxId;
    private int version;
    private boolean downstreamScheduled;
    private boolean alreadyConfirmed;

    public TaxonomyConfirmationResponse() {
    }

    public TaxonomyConfirmationResponse(long stageId, int taxId, int version,
                                        boolean downstreamScheduled, boolean alreadyConfirmed) {
        this.stageId = stageId;
        this.taxId = taxId;
        this.version = version;
        this.downstreamScheduled = downstreamScheduled;
        this.alreadyConfirmed = alreadyConfirmed;
    }

    public long getStageId() {
        return stageId;
    }

    public void setStageId(long stageId) {
        this.stageId = stageId;
    }

    public int getTaxId() {
        return taxId;
    }

    public void setTaxId(int taxId) {
        this.taxId = taxId;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean isDownstreamScheduled() {
        return downstreamScheduled;
    }

    public void setDownstreamScheduled(boolean downstreamScheduled) {
        this.downstreamScheduled = downstreamScheduled;
    }

    public boolean isAlreadyConfirmed() {
        return alreadyConfirmed;
    }

    public void setAlreadyConfirmed(boolean alreadyConfirmed) {
        this.alreadyConfirmed = alreadyConfirmed;
    }
}
