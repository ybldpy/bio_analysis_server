package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class LocalStorageService implements StorageService {

    @Value("${localstorageService.baseDir}")
    private String base;




    private static final Logger logger = LoggerFactory.getLogger(LocalStorageService.class);

    @Override
    public PutResult putObject(String key, InputStream data) {
        // TODO Auto-generated method stub

        Path objectPath = Paths.get(base, key);
        try {
            Path parentDir = objectPath.getParent();
            if (parentDir != null && Files.notExists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            if (Files.exists(objectPath)) {
                Files.delete(objectPath);
            }
            Files.copy(data, objectPath);
            return new PutResult(true, null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return new PutResult(false, e);
        }

    }

    private Path createSymbolicLink(Path src, Path targetFile) throws IOException {
        return Files.createSymbolicLink(src, targetFile);
    }

    private void lockFile(Path filePath) {
        // TODO: to be implemented
    }

    private void unlockFile(Path filePath) {
        // TODO: to be implemented
    }

    private boolean isSymlinkTo(Path link, Path expectedTarget) throws IOException {
        if (!Files.isSymbolicLink(link))
            return false;

        Path rawTarget = Files.readSymbolicLink(link); // 可能是相对路径
        Path resolvedTarget = link.getParent().resolve(rawTarget)
                .normalize().toAbsolutePath();

        Path expected = expectedTarget.normalize().toAbsolutePath();
        return resolvedTarget.equals(expected);
    }


    

    @Override
    public GetObjectResult getObject(String key, String writeToPath) {
        Path objectPath = Paths.get(this.base, key);
        if (!Files.exists(objectPath)) {
            return new GetObjectResult(false, null, null);
        }

        Path writeToFilePath = Path.of(writeToPath);

        Path parentDir = writeToFilePath.getParent();


        //if no parent dir
        if (parentDir != null) {
            try {
                Files.createDirectories(parentDir);
            } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                if (!Files.isDirectory(parentDir)) {
                    return new GetObjectResult(false, null, new NotDirectoryException(parentDir.toString()));
                }
            } catch (IOException e) {
                logger.error("object key: {}, writeToPath: {} get object error: \n", key, writeToPath,e);
                return new GetObjectResult(false, null, e);
            }
        }

        boolean locked = false;
        try {

            this.lockFile(writeToFilePath);
            locked = true;
            // check existence
            if (Files.exists(writeToFilePath)) {
                if(isSymlinkTo(writeToFilePath, objectPath)){
                    return new GetObjectResult(true, writeToFilePath.toFile(), null);
                }

                Files.delete(writeToFilePath);
            }

            Path writeObjectPath = createSymbolicLink(writeToFilePath, objectPath);
            return new GetObjectResult(true, writeObjectPath.toFile(), null);
        } catch (IOException e) {
            logger.error("{}: {} get object error", key, writeToPath,e);
            try {
                Files.deleteIfExists(writeToFilePath);
            } catch (IOException e1) {
                logger.error("{}: {} get object error", key, writeToPath,e1);
                
            }
            return new GetObjectResult(false, null, e);
        } catch (Exception e) {
            logger.error("{}: {} get object error", key, writeToPath,e);
            return new GetObjectResult(false, null, e);
            
        } finally {
            if (locked) {
                unlockFile(writeToFilePath);
            }
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
