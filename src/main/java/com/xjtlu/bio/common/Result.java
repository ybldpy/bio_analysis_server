package com.xjtlu.bio.common;

public class Result<T>{


    public static final int SUCCESS = 0;

    public static final int BUSINESS_FAIL = 1;
    public static final int INTERNAL_FAIL = 2;

    public static final int DUPLICATE_OPERATION = 3;



    private int success;
    private T data;
    private String failMsg;


    public Result(int status, T data, String failDescrip) {
        this.success = success;
        this.data = data;
        this.failMsg = failDescrip;
    }
    
}
