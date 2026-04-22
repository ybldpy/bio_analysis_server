package com.xjtlu.bio.analysisPipeline.context;

public class ReadMeta {

    public static final int QUALITY_ENCODING_33 = 0;
    public static final int QUALITY_ENCODING_64 = 1;

    public static final int READ_LEN_TYPE_SHORT = 0;
    public static final int READ_LEN_TYPE_LONG = 1;

    public ReadMeta(int qualityEncoding, int readLenType) {
        this.qualityEncoding = qualityEncoding;
        this.readLenType = readLenType;
    }
    public ReadMeta() {
    }
    private int qualityEncoding;
    private int readLenType;
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

    

}
