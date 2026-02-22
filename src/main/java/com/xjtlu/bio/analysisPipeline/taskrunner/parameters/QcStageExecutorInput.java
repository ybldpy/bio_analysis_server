package com.xjtlu.bio.analysisPipeline.taskrunner.parameters;

import java.util.Map;

public class QcStageExecutorInput {

    private String r1;
    private String r2;

    public QcStageExecutorInput(String r1, String r2, boolean longRead, Map<String, Object> params) {
        this.r1 = r1;
        this.r2 = r2;
        this.longRead = longRead;
        this.params = params;
    }

    private boolean longRead;

    private Map<String,Object> params;

    public String getR1() {
        return r1;
    }

    public void setR1(String r1) {
        this.r1 = r1;
    }

    public String getR2() {
        return r2;
    }

    public void setR2(String r2) {
        this.r2 = r2;
    }

    public boolean isLongRead() {
        return longRead;
    }

    public void setLongRead(boolean longRead) {
        this.longRead = longRead;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}
