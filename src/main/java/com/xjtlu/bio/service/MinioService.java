package com.xjtlu.bio.service;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import io.minio.ObjectWriteResponse;
import io.minio.PutObjectArgs;
import io.minio.RemoveObjectArgs;
import io.minio.RemoveObjectsArgs;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import jakarta.annotation.Resource;

@Service
public class MinioService {

    @Resource
    private MinioClient minioClient;

    @Value("${minio.bucket}")
    private String bucket;

    public boolean isMinioOk() {
        return minioClient != null;
    }

    public void uploadObject(String objectName, InputStream datastream)
            throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException,
            InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IOException {

        PutObjectArgs putObjectArgs = PutObjectArgs.builder()
                .bucket(bucket)
                .object(objectName)
                .stream(datastream, -1, 10485760)
                .build();
        ObjectWriteResponse objectWriteResponse = minioClient.putObject(putObjectArgs);

    }


    public void uploadObject(String objectName, String fileLocalPath) throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IOException{
        FileInputStream fileInputStream = null;
        try{
            fileInputStream = new FileInputStream(fileLocalPath);
            this.uploadObject(objectName, fileInputStream);
        }catch(Exception e){  
            throw e;
        }finally{
            if (fileInputStream!=null) {
               try{
                    fileInputStream.close();
               }catch(IOException e1){
                //do nothing;
               }
            }
        }
    }

    public List<DeleteError> batchDelete(List<String> objectNames) throws JsonMappingException, JsonParseException, InvalidKeyException, ErrorResponseException, IllegalArgumentException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IOException{
        List<DeleteError> failureDeleteFiles = new ArrayList<>();
        
        List<DeleteObject> deleteObjs = objectNames.stream().map((fileName)->{return new DeleteObject(fileName);}).toList();
        Iterable<Result<DeleteError>> results = minioClient.removeObjects(
            RemoveObjectsArgs.builder()
            .bucket(bucket)
            .objects(deleteObjs)
            .build()
        );

        Iterator<Result<DeleteError>> resultIterator = results.iterator();
        while (resultIterator.hasNext()) {
            Result<DeleteError> err = resultIterator.next();
            failureDeleteFiles.add(err.get());
        }

        return failureDeleteFiles;

        
    }

    public void removeObject(String objectName)
            throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException,
            InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IOException {
        RemoveObjectArgs removeObjectArgs = RemoveObjectArgs.builder().bucket(bucket).object(objectName).build();
        minioClient.removeObject(removeObjectArgs);
    }

    public InputStream getObjectStream(String objectName) throws InvalidKeyException, ErrorResponseException, InsufficientDataException, InternalException, InvalidResponseException, NoSuchAlgorithmException, ServerException, XmlParserException, IllegalArgumentException, IOException {
        return minioClient.getObject(
                GetObjectArgs.builder().bucket(bucket).object(objectName).build()); // 调用方负责关闭
    }

}
