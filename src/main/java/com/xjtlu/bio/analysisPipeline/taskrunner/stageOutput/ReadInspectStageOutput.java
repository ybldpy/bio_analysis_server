package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class ReadInspectStageOutput implements StageOutput{



    

    private int qualityEncoding;
    private int readLenType;

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
        this.qualityEncoding = qualityEncoding;
        this.readLenType = readLenType;
        this.r1Path = r1Path;
        this.r2Path = r2Path;
        this.workDir = workDir;
        this.useOriginalSequence = useOriginalSequence;
        this.originalSequenceUrl = originalSequenceUrl;
    }


    public int getQualityEncoding() {
        return qualityEncoding;
    }



    public void setQualityEncoding(int qualityEncoding) {
        this.qualityEncoding = qualityEncoding;
    }







    public int getReadLenType() {
        return readLenType;
    }







    public void setReadLenType(int readLenType) {
        this.readLenType = readLenType;
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
