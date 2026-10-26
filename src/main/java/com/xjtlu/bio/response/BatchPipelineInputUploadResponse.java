package com.xjtlu.bio.response;

import java.util.List;

/** The committed complete sample-input set; not individual upload progress. */
public class BatchPipelineInputUploadResponse {
    private Long pipelineId;
    private Integer sequenceLevel;
    private List<Input> inputs;

    public Long getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(Long pipelineId) {
        this.pipelineId = pipelineId;
    }

    public Integer getSequenceLevel() {
        return sequenceLevel;
    }

    public void setSequenceLevel(Integer sequenceLevel) {
        this.sequenceLevel = sequenceLevel;
    }

    public List<Input> getInputs() {
        return inputs;
    }

    public void setInputs(List<Input> inputs) {
        this.inputs = inputs;
    }

    public static class Input {
        private Long inputFileId;
        private String inputKey;
        private String fileName;

        public Long getInputFileId() {
            return inputFileId;
        }

        public void setInputFileId(Long inputFileId) {
            this.inputFileId = inputFileId;
        }

        public String getInputKey() {
            return inputKey;
        }

        public void setInputKey(String inputKey) {
            this.inputKey = inputKey;
        }

        public String getFileName() {
            return fileName;
        }

        public void setFileName(String fileName) {
            this.fileName = fileName;
        }
    }
}
