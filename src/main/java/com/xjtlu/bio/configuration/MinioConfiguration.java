package com.xjtlu.bio.configuration;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;


public class MinioConfiguration {

    private String user;
    private String password;
    private String url;
    private String exposeUrl;
    private String bucket;





    @Bean
    public MinioClient minioClient() {


        MinioClient minioClient;
        try {

            minioClient =  MinioClient.builder()
                .endpoint(url)
                .credentials(user,password)
                .build();
            BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder().bucket(bucket).build();
            boolean found = minioClient.bucketExists(bucketExistsArgs);
            if(!found){
                MakeBucketArgs makeBucketArgs = MakeBucketArgs.builder().bucket(bucket).build();
                minioClient.makeBucket(makeBucketArgs);
            }
        } catch (Exception e) {
            throw new RuntimeException("Minio初始化失败", e);
        }
        return minioClient;
    }

}