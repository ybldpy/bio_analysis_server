package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;
import java.util.Objects;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.common.SequenceMeta;

public class ReadInspectStageOutput implements StageOutput{



    

    private SequenceMeta readMeta;

    public SequenceMeta getReadMeta() {
        return readMeta;
    }

    public void setReadMeta(SequenceMeta readMeta) {
        this.readMeta = Objects.requireNonNull(readMeta, "ReadInspect output metadata must not be null");
    }

    private Path r1Path;
    private Path r2Path;

    private boolean useOriginalSequence;
    private String originalSequenceUrl;

    public boolean isUseOriginalSequence() {
        return useOriginalSequence;
    }

    public void setUseOriginalSequence(boolean useOriginalSequence) {
        this.useOriginalSequence = useOriginalSequence;
    }

    public String getOriginalSequenceUrl() {
        return originalSequenceUrl;
    }

    public void setOriginalSequenceUrl(String originalSequenceUrl) {
        this.originalSequenceUrl = originalSequenceUrl;
    }

    private Path workDir;



    public Path getR1Path() {
        return r1Path;
    }

    public void setR1Path(Path r1Path) {
        this.r1Path = r1Path;
    }

    public Path getR2Path() {
        return r2Path;
    }

    public void setR2Path(Path r2Path) {
        this.r2Path = r2Path;
    }

    public ReadInspectStageOutput(int qualityEncoding, int readLenType, boolean useOriginalSequence, String originalSequenceUrl, Path r1Path, Path r2Path, Path workDir) {
        this.readMeta = new SequenceMeta();
        this.readMeta.setQualityEncoding(qualityEncoding);
        this.readMeta.setReadLenType(readLenType);
        this.readMeta.setSequenceLevel(Constants.SequenceInput.SEQUENCE_LEVEL_READ);
        this.readMeta.setSequencingPlatform(Constants.SequencingPlatform.UNKNOWN);
        this.r1Path = r1Path;
        this.r2Path = r2Path;
        this.workDir = workDir;
        this.useOriginalSequence = useOriginalSequence;
        this.originalSequenceUrl = originalSequenceUrl;
    }


    public int getQualityEncoding() {
        return readMeta.getQualityEncoding();
    }



    public void setQualityEncoding(int qualityEncoding) {
        readMeta.setQualityEncoding(qualityEncoding);
    }







    public int getReadLenType() {
        return readMeta.getReadLenType();
    }







    public void setReadLenType(int readLenType) {
        readMeta.setReadLenType(readLenType);
    }







    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return workDir;
    }

    public Path getWorkDir() {
        return workDir;
    }

    public void setWorkDir(Path workDir) {
        this.workDir = workDir;
    }


}
