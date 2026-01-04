package com.xjtlu.bio.common;

public class Result<T>{


    public static final int SUCCESS = 0;

    public static final int BUSINESS_FAIL = 1;
    public static final int INTERNAL_FAIL = 2;

    public static final int DUPLICATE_OPERATION = 3;


    public static final int PARAMETER_NOT_VALID = 4;



    public int getStatus() {
        return status;
    }


    public void setStatus(int status) {
        this.status = status;
    }


    public T getData() {
        return data;
    }


    public void setData(T data) {
        this.data = data;
    }


    public String getFailMsg() {
        return failMsg;
    }


    public void setFailMsg(String failMsg) {
        this.failMsg = failMsg;
    }


    private int status;
    private T data;
    private String failMsg;


    public Result(int status, T data, String failDescrip) {
        this.status = status;
        this.data = data;
        this.failMsg = failDescrip;
    }
    
}
