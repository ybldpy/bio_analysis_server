package com.xjtlu.bio.response;

import com.xjtlu.bio.common.Result;

/** Upload-only business response; existing success and error response shapes stay unchanged. */
public final class ReferenceMembershipConfirmationResult extends Result<ReferenceLibraryItem> {
    public static final String CODE = "REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED";

    public record Confirmation(int targetTaxId, int pipelineType, int taxId) { }

    private final Confirmation confirmation;

    public ReferenceMembershipConfirmationResult(int targetTaxId, int pipelineType, int taxId) {
        super(Result.BUSINESS_FAIL, null,
                "参考 taxId 尚未纳入所选专项，请确认是否将该分类加入专项范围；该分类下的已有和后续参考均会受此关联影响，"
                        + "也会影响共享此目标 taxId 的其他流水线选项");
        confirmation = new Confirmation(targetTaxId, pipelineType, taxId);
    }

    public String getCode() { return CODE; }
    public Confirmation getConfirmation() { return confirmation; }
}
