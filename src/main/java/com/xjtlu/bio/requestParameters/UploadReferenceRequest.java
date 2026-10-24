package com.xjtlu.bio.requestParameters;

/** Multipart metadata only; file paths, sourceDb and database IDs are server-owned. */
public class UploadReferenceRequest {
    private String accession;
    private Integer taxId;
    private Integer targetTaxId;
    private Integer pipelineType;
    private Boolean confirmAddTaxonomyMember = false;
    private Integer orgType;
    private String organismName;
    private String segment;
    private String completeness;
    private Boolean enabled = true;

    public String getAccession() { return accession; }
    public void setAccession(String value) { accession = value; }
    public Integer getTaxId() { return taxId; }
    public void setTaxId(Integer value) { taxId = value; }
    public Integer getTargetTaxId() { return targetTaxId; }
    public void setTargetTaxId(Integer value) { targetTaxId = value; }
    public Integer getPipelineType() { return pipelineType; }
    public void setPipelineType(Integer value) { pipelineType = value; }
    public Boolean getConfirmAddTaxonomyMember() { return confirmAddTaxonomyMember; }
    public void setConfirmAddTaxonomyMember(Boolean value) { confirmAddTaxonomyMember = value; }
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
