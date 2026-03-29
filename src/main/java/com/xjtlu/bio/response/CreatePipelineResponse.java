package com.xjtlu.bio.response;

import java.util.List;

public class CreatePipelineResponse {


    public static class InputFile{
        private Long inputId;
        private int inputRole;
        public Long getInputId() {
            return inputId;
        }
        public void setInputId(Long inputId) {
            this.inputId = inputId;
        }
        public int getInputRole() {
            return inputRole;
        }
        public void setInputRole(int inputRole) {
            this.inputRole = inputRole;
        }

        
    }

    public Long getPipelineId() {
        return pipelineId;
    }
    public void setPipelineId(Long pipelineId) {
        this.pipelineId = pipelineId;
    }
    public List<InputFile> getInputFiles() {
        return inputFiles;
    }
    public void setInputFiles(List<InputFile> inputFiles) {
        this.inputFiles = inputFiles;
    }
    public CreatePipelineResponse() {
    }
    private Long pipelineId;
    private List<InputFile> inputFiles;
    

}
