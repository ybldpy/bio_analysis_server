package com.xjtlu.bio.utils;

import java.util.Map;

public class ParameterUtil {


    public static String getStrFromMap(String key, Map<String, Object> map){
        Object o = map.get(key);
        if (o == null || !o.getClass().equals(String.class)) {
            return null;
        }
        return (String) o;
    }


}
