package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class LocalStorageService implements StorageService {

    @Value("${localstorageService.baseDir}")
    private String base;

    private static final Logger logger = LoggerFactory.getLogger(LocalStorageService.class);

    private static final int LOCK_NUM = 2048;

    private ReentrantReadWriteLock[] reentrantReadWriteLockPool;

    public LocalStorageService() {
        this.reentrantReadWriteLockPool = new ReentrantReadWriteLock[LOCK_NUM];

        for (int i = 0; i < LOCK_NUM; i++) {
            this.reentrantReadWriteLockPool[i] = new ReentrantReadWriteLock();
        }
    }

    private ReentrantReadWriteLock lockFor(String key) {
        int lockIndex = Math.floorMod(key.hashCode(), LOCK_NUM);
        return reentrantReadWriteLockPool[lockIndex];
    }

    @Override
    public PutResult putObject(String key, InputStream data) {

        ReentrantReadWriteLock readWriteLock = lockFor(key);

        readWriteLock.writeLock().lock();

        Path objectPath = Paths.get(base, key);    
        Path tempWritePath = objectPath.resolveSibling(objectPath.getFileName().toString() + ".tmp");
        
    
        try {
            Path parentDir = objectPath.getParent();
            if (parentDir != null && Files.notExists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            Files.copy(data, tempWritePath, StandardCopyOption.REPLACE_EXISTING);
            Files.move(tempWritePath, objectPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return new PutResult(true, null);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            return new PutResult(false, e);
        } finally {

            try {
                Files.deleteIfExists(tempWritePath);
            } catch (IOException e) {
                // TODO Auto-generated catch block
            }

            readWriteLock.writeLock().unlock();
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

        ReentrantReadWriteLock readWriteLock = lockFor(key);

        readWriteLock.readLock().lock();



        Path writeToFilePath = Path.of(writeToPath);

        try {
            Path objectPath = Paths.get(this.base, key);
            if (!Files.exists(objectPath)) {
                return new GetObjectResult(false, null, null);
            }

            Path parentDir = writeToFilePath.getParent();

            // if no parent dir
            if (parentDir != null) {
                try {
                    Files.createDirectories(parentDir);
                } catch (FileAlreadyExistsException fileAlreadyExistsException) {
                    if (!Files.isDirectory(parentDir)) {
                        return new GetObjectResult(false, null, new NotDirectoryException(parentDir.toString()));
                    }
                } catch (IOException e) {
                    logger.error("object key: {}, writeToPath: {} get object error: \n", key, writeToPath, e);
                    return new GetObjectResult(false, null, e);
                }
            }

            Path tmpPath = writeToFilePath.resolveSibling(writeToFilePath.getFileName()+".tmp");
            Files.copy(objectPath, tmpPath, StandardCopyOption.REPLACE_EXISTING);
            Files.move(tmpPath, writeToFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return new GetObjectResult(true, writeToFilePath.toFile(), null);
        } 
        catch(IOException e){
            logger.error("{}: {} get object error", key, writeToPath, e);
                try {
                    Files.deleteIfExists(writeToFilePath);
                } catch (IOException e1) {
                    logger.error("{}: {} get object error", key, writeToPath, e1);

                }
                return new GetObjectResult(false, null, e);
        }
        catch (Exception e) {
            logger.error("{}: {} get object error", key, writeToPath, e);
            return new GetObjectResult(false, null, e);
        } finally {
            readWriteLock.readLock().unlock();
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
        Path path = Paths.get(base, key);
        boolean exists = Files.exists(path, LinkOption.NOFOLLOW_LINKS);
        return exists;
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
