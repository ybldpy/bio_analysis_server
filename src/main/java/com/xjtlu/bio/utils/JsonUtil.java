package com.xjtlu.bio.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.xjtlu.bio.service.stage.RefSeqConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JsonUtil {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);

    // ObjectMapper 是线程安全的，建议静态单例
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 配置：遇到未知属性不报错（非常重要，防止前端加个字段后端就挂）
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 配置：允许空对象序列化
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * 核心需求：Json 转 Map
     * 特性：输入 null 或 空字符串，直接返回空 Map，不报错
     * 
     * @throws JsonProcessingException
     * @throws JsonMappingException
     */
    public static <V> Map<String, V> toMap(String json, Class<V> valueType)
            throws JsonMappingException, JsonProcessingException {
        if (!StringUtils.hasText(json)) {
            return new HashMap<>(); // 或者 Collections.emptyMap() 看你需要可变还是不可变
        }
        // 1. 获取 TypeFactory
        // 2. 构造 MapType: Map<String, V>
        JavaType mapType = objectMapper.getTypeFactory()
                .constructMapType(HashMap.class, String.class, valueType);

        // 3. 解析
        return objectMapper.readValue(json, mapType);

    }

    public static Map<String, Object> toMap(String json) throws JsonMappingException, JsonProcessingException {
        if(org.apache.commons.lang3.StringUtils.isBlank(json)){
            return new HashMap<>();
        }
        return toMap(json, Object.class);
    }

    /**
     * 通用：转对象
     */
    public static <T> T toObject(String json, Class<T> clazz) {
        if (!StringUtils.hasText(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            logger.error("JSON to Object failed. json: {}, class: {}", json, clazz.getName(), e);
            return null;
        }
    }

    /**
     * 通用：对象转 Json 字符串
     * 
     * @throws JsonProcessingException
     */
    public static String toJson(Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    public static <T> T mapToPojo(Map<String,Object> map, Class<T> clazz) {
        if (map == null)
            return null;

        return objectMapper.convertValue(map, clazz);
    }
}
