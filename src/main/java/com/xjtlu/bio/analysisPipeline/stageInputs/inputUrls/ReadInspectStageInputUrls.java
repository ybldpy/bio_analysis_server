package com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls;

public class ReadInspectStageInputUrls implements StageInputUrls{

    /**
     * 用户提供的输入数据声明，不是 ReadInspect 的检测结果。
     * 长短读和质量编码由 ReadInspect 确定；平台声明优先，未声明时才尝试识别长读 header。
     */
    public static class SequenceInputMeta {

        /**
         * {@link com.xjtlu.bio.analysisPipeline.Constants.SequencingPlatform}；
         * null、未提供或 UNKNOWN（0）均允许自动检测；明确指定的平台直接采用并跳过平台检测。
         */
        private Integer sequencingPlatform;

        public Integer getSequencingPlatform() {
            return sequencingPlatform;
        }

        public void setSequencingPlatform(Integer sequencingPlatform) {
            this.sequencingPlatform = sequencingPlatform;
        }
    }

    private String read1Url;
    private String read2Url;
    private SequenceInputMeta sequenceInputMeta;

    public SequenceInputMeta getSequenceInputMeta() {
        return sequenceInputMeta;
    }

    public void setSequenceInputMeta(SequenceInputMeta sequenceInputMeta) {
        this.sequenceInputMeta = sequenceInputMeta;
    }



    public ReadInspectStageInputUrls(String read1Url,String read2Url) {
        this.read1Url = read1Url;
        this.read2Url = read2Url;
    }

    public ReadInspectStageInputUrls() {
    }

    public String getRead1Url() {
        return read1Url;
    }


    public void setRead1Url(String read1Url) {
        this.read1Url = read1Url;
    }

    public String getRead2Url() {
        return read2Url;
    }

    public void setRead2Url(String read2Url) {
        this.read2Url = read2Url;
    }

    

    


}
