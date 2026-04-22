package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class ReadInspectStageOutput implements StageOutput{


    private int qualityEncoding;
    private int readLenType;

    private Path r1Path;
    private Path r2Path;




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

    public ReadInspectStageOutput(int qualityEncoding, int readLenType, Path r1Path, Path r2Path) {
        this.qualityEncoding = qualityEncoding;
        this.readLenType = readLenType;
        this.r1Path = r1Path;
        this.r2Path = r2Path;
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
        throw new UnsupportedOperationException("Unimplemented method 'getParentPath'");
    }



}
