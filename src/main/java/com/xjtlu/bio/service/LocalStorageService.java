package com.xjtlu.bio.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SecureDirectoryStream;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.HashSet;
import java.util.Objects;
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
        // Legacy callers may spell the same object with './' or repeated separators.
        String lockKey = Path.of(key).normalize().toString();
        int lockIndex = Math.floorMod(lockKey.hashCode(), LOCK_NUM);
        return reentrantReadWriteLockPool[lockIndex];
    }


    public Path getObject(String url){
        return Path.of(base).resolve(url);
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
    public boolean isReadableObject(String key) {
        ReentrantReadWriteLock lock = null;
        try {
            Path relative = validateManagedKey(key);
            lock = lockFor(relative.toString());
            lock.readLock().lock();
            Path root = Paths.get(base).toAbsolutePath().normalize().toRealPath();
            Path object = prepareManagedObject(root, relative, false);
            return Files.isRegularFile(object, LinkOption.NOFOLLOW_LINKS) && Files.isReadable(object);
        } catch (IOException | RuntimeException e) {
            return false;
        } finally {
            if (lock != null) {
                lock.readLock().unlock();
            }
        }
    }

    @Override
    public boolean delete(String key) {
        ReentrantReadWriteLock lock = null;
        try {
            Path relative = validateManagedKey(key);
            lock = lockFor(relative.toString());
            lock.writeLock().lock();
            Path root = Paths.get(base).toAbsolutePath().normalize().toRealPath();
            try (DirectoryStream<Path> directory = Files.newDirectoryStream(root)) {
                if (!(directory instanceof SecureDirectoryStream<Path> secureDirectory)) {
                    throw new IOException("Storage filesystem does not support safe file deletion");
                }
                return deleteRegularFile(secureDirectory, relative);
            }
        } catch (NoSuchFileException e) {
            return false;
        } catch (Exception e) {
            logger.warn("Unable to delete local storage object {}", key, e);
            return false;
        } finally {
            if (lock != null) {
                lock.writeLock().unlock();
            }
        }
    }

    @Override
    public PutResult putObject(String key, InputStream data, PutOptions opts) {
        // The configured storage tree must be service-owned, not concurrently
        // writable by untrusted local users. Parent checks cannot make path-based
        // publication race-free against malicious external directory replacement.
        ReentrantReadWriteLock lock = null;
        Path temporary = null;
        Exception failure = null;
        boolean published = false;
        try {
            Path root;
            Path relative;
            Path object;
            boolean overwrite = opts == null || opts.overwrite;
            // Own the input before validating anything, and close it before publishing.
            try (InputStream input = Objects.requireNonNull(data, "Object input stream is required")) {
                relative = validateManagedKey(key);
                lock = lockFor(relative.toString());
                lock.writeLock().lock();
                Path configuredRoot = Paths.get(base).toAbsolutePath().normalize();
                Files.createDirectories(configuredRoot);
                root = configuredRoot.toRealPath();
                object = prepareManagedObject(root, relative, true);
                checkWritableTarget(object, overwrite);
                // Keeping temporary files in the trusted root also isolates them from
                // changes to nested object directories while the input is being read.
                temporary = Files.createTempFile(root, ".object-write-", ".tmp");
                try (OutputStream output = Files.newOutputStream(temporary,
                        StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING,
                        LinkOption.NOFOLLOW_LINKS)) {
                    input.transferTo(output);
                }
            }

            prepareManagedObject(root, relative, false);
            checkWritableTarget(object, overwrite);
            if (overwrite) {
                Files.move(temporary, object, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } else {
                // Unlike ATOMIC_MOVE, hard-link publication never replaces an existing
                // directory entry, even across processes or for a dangling symlink.
                // Fail closed if the filesystem does not support same-filesystem links.
                Files.createLink(object, temporary);
            }
            published = true;
        } catch (Exception e) {
            failure = e;
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (Exception cleanupFailure) {
                    if (failure != null) {
                        failure.addSuppressed(cleanupFailure);
                    } else {
                        // Publication already committed: do not falsely report a failed
                        // write and cause callers to discard a successfully stored object.
                        logger.warn("Unable to clean local storage temporary file {}", temporary, cleanupFailure);
                    }
                }
            }
            if (lock != null) {
                lock.writeLock().unlock();
            }
        }
        return new PutResult(published, failure);
    }

    private Path validateManagedKey(String key) {
        Objects.requireNonNull(key, "Object key is required");
        Path relative = Path.of(key);
        if (key.isBlank() || relative.isAbsolute()) {
            throw new IllegalArgumentException("Object key must be a relative file path");
        }
        for (Path component : relative) {
            if (component.toString().equals("..")) {
                throw new IllegalArgumentException("Parent traversal is not allowed in object keys");
            }
        }
        Path normalized = relative.normalize();
        if (normalized.toString().isEmpty() || !key.equals(normalized.toString())) {
            throw new IllegalArgumentException("Object key must be a canonical relative file path");
        }
        return normalized;
    }

    private Path prepareManagedObject(Path root, Path relative, boolean createParents) throws IOException {
        Path object = root.resolve(relative).normalize();
        if (object.equals(root) || !object.startsWith(root)) {
            throw new IOException("Object key resolves outside the storage directory");
        }
        Path parent = root;
        for (int index = 0; index < relative.getNameCount() - 1; index++) {
            parent = parent.resolve(relative.getName(index));
            if (createParents) {
                try {
                    Files.createDirectory(parent);
                } catch (FileAlreadyExistsException ignored) {
                    // Validate the existing entry without following symbolic links.
                }
            }
            if (!Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
                throw new NotDirectoryException(parent.toString());
            }
        }
        return object;
    }

    private void checkWritableTarget(Path object, boolean overwrite) throws IOException {
        if (Files.exists(object, LinkOption.NOFOLLOW_LINKS)) {
            if (!overwrite) {
                throw new FileAlreadyExistsException(object.toString());
            }
            if (!Files.isRegularFile(object, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("Only regular storage files can be overwritten");
            }
        }
    }

    private boolean deleteRegularFile(SecureDirectoryStream<Path> directory, Path relative) throws IOException {
        Path name = relative.getName(0);
        if (relative.getNameCount() > 1) {
            try (SecureDirectoryStream<Path> child = directory.newDirectoryStream(name, LinkOption.NOFOLLOW_LINKS)) {
                return deleteRegularFile(child, relative.subpath(1, relative.getNameCount()));
            }
        }
        BasicFileAttributeView attributes = directory.getFileAttributeView(
                name, BasicFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.readAttributes().isRegularFile()) {
            return false;
        }
        // deleteFile cannot remove a directory, even if an external writer replaces
        // the checked entry with an empty directory immediately before deletion.
        directory.deleteFile(name);
        return true;
    }

}
