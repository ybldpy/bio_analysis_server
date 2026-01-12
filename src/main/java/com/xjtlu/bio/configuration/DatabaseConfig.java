package com.xjtlu.bio.configuration;


import jakarta.annotation.Resource;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabaseConfig {

    @Resource
    private SqlSessionFactory sqlSessionFactory;

    @Bean(name = "batchSqlSessionTemplate")
    public SqlSessionTemplate batchSqlSessionTemplate() {
        // ExecutorType.BATCH 是关键
        return new SqlSessionTemplate(sqlSessionFactory, ExecutorType.BATCH);
    }

}
