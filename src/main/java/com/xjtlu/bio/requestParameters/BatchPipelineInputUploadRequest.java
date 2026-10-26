package com.xjtlu.bio.requestParameters;

import java.util.List;

/** Initial complete upload manifest; every input refers to a file in this request. */
public class BatchPipelineInputUploadRequest {
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
        private String inputKey;
        private String filePart;

        public String getInputKey() {
            return inputKey;
        }

        public void setInputKey(String inputKey) {
            this.inputKey = inputKey;
        }

        public String getFilePart() {
            return filePart;
        }

        public void setFilePart(String filePart) {
            this.filePart = filePart;
        }
    }
}
