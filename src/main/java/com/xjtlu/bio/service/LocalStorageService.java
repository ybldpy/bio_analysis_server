package com.xjtlu.bio.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.stereotype.Service;

@Service
public class LocalStorageService implements StorageService{



    private String base;

    @Override
    public PutResult putObject(String key, InputStream data) {
        // TODO Auto-generated method stub

        Path objectPath = Paths.get(base, key);
        try {
            Files.copy(data, objectPath);
            return new PutResult(true, null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return new PutResult(false, e);
        }

    }

    @Override
    public GetObjectResult getObject(String key, String writeToPath) {
        // TODO Auto-generated method stub
        Path objectPath = Paths.get(this.base, key);
        if(!Files.exists(objectPath)){
            return new GetObjectResult(false, null, null);
        }

        try {
            Path writeObjectPath = Files.createSymbolicLink(Path.of(writeToPath), objectPath);
            return new GetObjectResult(true, writeObjectPath.toFile(), null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return new GetObjectResult(false, null, e);
        }

    }



    @Override
    public ObjectStat getObjectStream(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getObjectStream'");
    }

    @Override
    public boolean exists(String key) {
        // TODO Auto-generated method stub
        return Files.exists(Paths.get(String.format("%s/%s", base, key)));
    }

    @Override
    public boolean delete(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    @Override
    public PutResult putObject(String key, InputStream data, PutOptions opts) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'putObject'");
    }

}
