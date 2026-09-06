package com.xjtlu.bio.response;

import java.util.List;

import com.xjtlu.bio.entity.BioRefseq;

public class ReferenceGenomeQueryResponse {

    private long total;
    
    public ReferenceGenomeQueryResponse(long total, List<BioRefseq> referenceGenomes) {
        this.total = total;
        this.referenceGenomes = referenceGenomes;
    }

    private List<BioRefseq> referenceGenomes;

    public ReferenceGenomeQueryResponse() {
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List<BioRefseq> getReferenceGenomes() {
        return referenceGenomes;
    }

    public void setReferenceGenomes(List<BioRefseq> referenceGenomes) {
        this.referenceGenomes = referenceGenomes;
    }
    
    

}
