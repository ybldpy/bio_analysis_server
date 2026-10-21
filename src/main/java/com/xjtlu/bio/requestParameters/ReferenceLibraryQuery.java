package com.xjtlu.bio.requestParameters;

/** Management query: null enabled includes both enabled and disabled references. */
public class ReferenceLibraryQuery {
    private Integer start = 0;
    private Integer pageSize = 20;
    private Integer taxId;
    private Integer orgType;
    private Boolean enabled;
    private String accession;

    public Integer getStart() { return start; }
    public void setStart(Integer value) { start = value; }
    public Integer getPageSize() { return pageSize; }
    public void setPageSize(Integer value) { pageSize = value; }
    public Integer getTaxId() { return taxId; }
    public void setTaxId(Integer value) { taxId = value; }
    public Integer getOrgType() { return orgType; }
    public void setOrgType(Integer value) { orgType = value; }
    public Boolean getEnabled() { return enabled; }
    public void setEnabled(Boolean value) { enabled = value; }
    public String getAccession() { return accession; }
    public void setAccession(String value) { accession = value; }
}
