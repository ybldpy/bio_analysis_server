package com.xjtlu.bio.service;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.stereotype.Service;

@Service
public class LocalStorageService implements StorageService{

    @Override
    public PutResult putObject(String key, InputStream data) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GetObjectResult getObject(String key, String writeToPath) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ObjectStat getObjectStream(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getObjectStream'");
    }

    @Override
    public boolean exists(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'exists'");
    }

    @Override
    public boolean delete(String key) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

}
