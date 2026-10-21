package com.xjtlu.bio.requestParameters;

/** Multipart metadata only; file paths, sourceDb and database IDs are server-owned. */
public class UploadReferenceRequest {
    private String accession;
    private Integer taxId;
    private Integer orgType;
    private String organismName;
    private String segment;
    private String completeness;
    private Boolean enabled = true;

    public String getAccession() { return accession; }
    public void setAccession(String value) { accession = value; }
    public Integer getTaxId() { return taxId; }
    public void setTaxId(Integer value) { taxId = value; }
    public Integer getOrgType() { return orgType; }
    public void setOrgType(Integer value) { orgType = value; }
    public String getOrganismName() { return organismName; }
    public void setOrganismName(String value) { organismName = value; }
    public String getSegment() { return segment; }
    public void setSegment(String value) { segment = value; }
    public String getCompleteness() { return completeness; }
    public void setCompleteness(String value) { completeness = value; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean value) { enabled = value; }
}
