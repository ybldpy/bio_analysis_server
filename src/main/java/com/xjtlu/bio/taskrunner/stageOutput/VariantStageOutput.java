package com.xjtlu.bio.taskrunner.stageOutput;

public class VariantStageOutput {


    public VariantStageOutput(String vcfGz, String vcfTbi) {
        this.vcfGz = vcfGz;
        this.vcfTbi = vcfTbi;
    }
    public String getVcfGz() {
        return vcfGz;
    }
    public void setVcfGz(String vcfGz) {
        this.vcfGz = vcfGz;
    }
    public String getVcfTbi() {
        return vcfTbi;
    }
    public void setVcfTbi(String vcfTbi) {
        this.vcfTbi = vcfTbi;
    }
    private String vcfGz;
    private String vcfTbi;
    

}
