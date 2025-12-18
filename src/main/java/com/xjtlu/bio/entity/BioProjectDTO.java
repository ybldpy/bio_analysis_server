package com.xjtlu.bio.entity;

public class BioProjectDTO {

    private Long pid;
    private String projectName;

    private Long creatorId;
    private String creatorName;
    public Long getPid() {
        return pid;
    }
    public void setPid(Long pid) {
        this.pid = pid;
    }
    public BioProjectDTO(Long pid, String projectName, Long creatorId, String creatorName) {
        this.pid = pid;
        this.projectName = projectName;
        this.creatorId = creatorId;
        this.creatorName = creatorName;
    }
    public String getProjectName() {
        return projectName;
    }
    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }
    public Long getCreatorId() {
        return creatorId;
    }
    public void setCreatorId(Long creatorId) {
        this.creatorId = creatorId;
    }
    public String getCreatorName() {
        return creatorName;
    }
    public void setCreatorName(String creatorName) {
        this.creatorName = creatorName;
    }

}
