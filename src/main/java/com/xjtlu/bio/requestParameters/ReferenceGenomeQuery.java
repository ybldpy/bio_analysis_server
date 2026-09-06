package com.xjtlu.bio.requestParameters;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public class ReferenceGenomeQuery {

    @NotBlank
    private String query;
    @NotNull
    private Integer start;
    @NotNull
    private Integer pageSize;
    public ReferenceGenomeQuery(@NotBlank String query, @NotNull Integer start, @NotNull Integer pageSize) {
        this.query = query;
        this.start = start;
        this.pageSize = pageSize;
    }
    public ReferenceGenomeQuery() {
    }
    public String getQuery() {
        return query;
    }
    public void setQuery(String query) {
        this.query = query;
    }
    public Integer getStart() {
        return start;
    }
    public void setStart(Integer start) {
        this.start = start;
    }
    public Integer getPageSize() {
        return pageSize;
    }
    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }
    

    

}
