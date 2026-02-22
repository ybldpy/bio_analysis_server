package com.xjtlu.bio.analysisPipeline.stageInputs.parameters;

public class RefSeqConfig {

    private boolean isInnerRefSeq;
    public boolean isInnerRefSeq() {
        return isInnerRefSeq;
    }
    public void setInnerRefSeq(boolean isInnerRefSeq) {
        this.isInnerRefSeq = isInnerRefSeq;
    }
    public RefSeqConfig(boolean isInnerRefSeq, String refseqObjectName, long refseqId) {
        this.isInnerRefSeq = isInnerRefSeq;
        this.refseqObjectName = refseqObjectName;
        this.refseqId = refseqId;
    }

    public RefSeqConfig(){
        
    }

    public RefSeqConfig(String refSeqObjName){
        this(false, refSeqObjName, -1);
    }
    public String getRefseqObjectName() {
        return refseqObjectName;
    }
    public void setRefseqObjectName(String refseqObjectName) {
        this.refseqObjectName = refseqObjectName;
    }
    public long getRefseqId() {
        return refseqId;
    }
    public void setRefseqId(long refseqId) {
        this.refseqId = refseqId;
    }
    private String refseqObjectName;
    private long refseqId;

    
}
