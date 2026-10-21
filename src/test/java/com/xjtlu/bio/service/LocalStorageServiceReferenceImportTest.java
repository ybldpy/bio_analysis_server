package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;

import com.xjtlu.bio.service.StorageService.PutOptions;
import com.xjtlu.bio.service.StorageService.PutResult;

/** Synthetic files only; never starts Spring or touches the configured reference library. */
class LocalStorageServiceReferenceImportTest {

    private static final PutOptions NO_OVERWRITE = PutOptions.builder().overwrite(false).build();

    @TempDir
    Path temporaryDirectory;

    private Path root;
    private Path outside;
    private LocalStorageService service;

    @BeforeEach
    void setUp() throws IOException {
        root = Files.createDirectory(temporaryDirectory.resolve("storage"));
        outside = Files.createDirectory(temporaryDirectory.resolve("outside"));
        service = serviceAt(root);
    }

    @Test
    void writesCompleteObjectCreatesParentsAndClosesTheInput() throws Exception {
        TrackedInputStream input = input("synthetic object");

        PutResult result = service.putObject("imports/new/object.txt", input, NO_OVERWRITE);

        assertTrue(result.success());
        assertNull(result.e());
        assertTrue(input.closed);
        assertEquals("synthetic object", Files.readString(root.resolve("imports/new/object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void existingObjectIsNotOverwrittenAndInputIsClosedWithoutBeingRead() throws Exception {
        Files.writeString(root.resolve("object.txt"), "existing");
        Files.writeString(root.resolve("object.txt.tmp"), "another writer's temporary file");
        TrackedInputStream input = input("replacement");

        PutResult result = service.putObject("object.txt", input, NO_OVERWRITE);

        assertFalse(result.success());
        assertInstanceOf(FileAlreadyExistsException.class, result.e());
        assertTrue(input.closed);
        assertEquals("replacement".length(), input.available());
        assertEquals("existing", Files.readString(root.resolve("object.txt")));
        assertEquals("another writer's temporary file", Files.readString(root.resolve("object.txt.tmp")));
        assertNoTemporaryWrites();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void existingSymlinkIsNeverFollowedOrReplaced(boolean overwrite) throws Exception {
        Path external = Files.writeString(outside.resolve("sentinel.txt"), "untouched");
        Path link = Files.createSymbolicLink(root.resolve("object.txt"), external);
        TrackedInputStream input = input("replacement");

        PutResult result = service.putObject("object.txt", input,
                PutOptions.builder().overwrite(overwrite).build());

        assertFalse(result.success());
        assertNotNull(result.e());
        assertTrue(input.closed);
        assertTrue(Files.isSymbolicLink(link));
        assertEquals(external, Files.readSymbolicLink(link));
        assertEquals("untouched", Files.readString(external));
        assertNoTemporaryWrites();
    }

    @Test
    void danglingSymlinkCountsAsAnExistingObject() throws Exception {
        Path absent = outside.resolve("absent.txt");
        Path link = Files.createSymbolicLink(root.resolve("object.txt"), absent);

        PutResult result = service.putObject("object.txt", input("replacement"), NO_OVERWRITE);

        assertFalse(result.success());
        assertInstanceOf(FileAlreadyExistsException.class, result.e());
        assertTrue(Files.isSymbolicLink(link));
        assertFalse(Files.exists(absent));
        assertNoTemporaryWrites();
    }

    @Test
    void nullOptionsKeepTheDefaultOverwriteBehaviorForRegularFiles() throws Exception {
        Files.writeString(root.resolve("object.txt"), "old");
        TrackedInputStream input = input("new");

        PutResult result = service.putObject("object.txt", input, null);

        assertTrue(result.success());
        assertTrue(input.closed);
        assertEquals("new", Files.readString(root.resolve("object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void failedReadLeavesNoPublishedObjectOrOwnTemporaryFile() throws Exception {
        PartialFailureInputStream input = new PartialFailureInputStream();

        PutResult result = service.putObject("imports/object.txt", input, NO_OVERWRITE);

        assertFalse(result.success());
        assertEquals("synthetic read failure", result.e().getMessage());
        assertTrue(input.closed);
        assertFalse(Files.exists(root.resolve("imports/object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void failedOverwritePreservesTheOldObject() throws Exception {
        Files.writeString(root.resolve("object.txt"), "old object");
        PartialFailureInputStream input = new PartialFailureInputStream();

        PutResult result = service.putObject("object.txt", input, PutOptions.builder().overwrite(true).build());

        assertFalse(result.success());
        assertTrue(input.closed);
        assertEquals("old object", Files.readString(root.resolve("object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void inputCloseFailureHappensBeforePublication() throws Exception {
        TrackedInputStream input = new TrackedInputStream("complete payload") {
            @Override
            public void close() throws IOException {
                super.close();
                throw new IOException("synthetic close failure");
            }
        };

        PutResult result = service.putObject("object.txt", input, NO_OVERWRITE);

        assertFalse(result.success());
        assertEquals("synthetic close failure", result.e().getMessage());
        assertTrue(input.closed);
        assertFalse(Files.exists(root.resolve("object.txt")));
        assertNoTemporaryWrites();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", ".", "..", "../outside/escape.txt", "folder/../../outside/escape.txt",
            "folder/../object.txt", "./object.txt", "folder//object.txt", "folder/"})
    void invalidKeysFailAndCloseInputWithoutDeletingOrCreatingAnything(String key) throws Exception {
        Path sentinel = Files.writeString(outside.resolve("sentinel.txt"), "untouched");
        TrackedInputStream input = input("not written");

        PutResult result = service.putObject(key, input, NO_OVERWRITE);

        assertFalse(result.success());
        assertNotNull(result.e());
        assertTrue(input.closed);
        assertFalse(service.delete(key));
        assertEquals("untouched", Files.readString(sentinel));
        try (Stream<Path> entries = Files.list(root)) {
            assertEquals(0, entries.count());
        }
    }

    @Test
    void absolutePathsCannotWriteOrDeleteEvenWhenTheyPointToSyntheticFiles() throws Exception {
        Path sentinel = Files.writeString(outside.resolve("sentinel.txt"), "untouched");
        TrackedInputStream input = input("replacement");

        PutResult result = service.putObject(sentinel.toString(), input, NO_OVERWRITE);

        assertFalse(result.success());
        assertTrue(input.closed);
        assertFalse(service.delete(sentinel.toString()));
        assertEquals("untouched", Files.readString(sentinel));
        assertNoTemporaryWrites();
    }

    @Test
    void symlinkParentCannotEscapeTheStorageRootForWriteOrDelete() throws Exception {
        Path sentinel = Files.writeString(outside.resolve("sentinel.txt"), "untouched");
        Files.createSymbolicLink(root.resolve("escape"), outside);
        TrackedInputStream input = input("not written");

        PutResult result = service.putObject("escape/new.txt", input, NO_OVERWRITE);

        assertFalse(result.success());
        assertTrue(input.closed);
        assertFalse(Files.exists(outside.resolve("new.txt")));
        assertFalse(service.delete("escape/sentinel.txt"));
        assertEquals("untouched", Files.readString(sentinel));
        assertTrue(Files.isSymbolicLink(root.resolve("escape")));
        assertNoTemporaryWrites();
    }

    @Test
    void internalSymlinkParentsAreAlsoRejected() throws Exception {
        Path directory = Files.createDirectory(root.resolve("real"));
        Files.writeString(directory.resolve("existing.txt"), "untouched");
        Files.createSymbolicLink(root.resolve("alias"), directory);

        assertFalse(service.putObject("alias/new.txt", input("not written"), NO_OVERWRITE).success());
        assertFalse(service.delete("alias/existing.txt"));
        assertFalse(Files.exists(directory.resolve("new.txt")));
        assertEquals("untouched", Files.readString(directory.resolve("existing.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void parentSymlinkIntroducedDuringReadIsRejectedBeforePublicationAndOwnTempIsCleaned() throws Exception {
        Path parent = Files.createDirectory(root.resolve("imports"));
        Path moved = root.resolve("moved-imports");
        TrackedInputStream input = new TrackedInputStream("not published") {
            @Override
            public void close() throws IOException {
                super.close();
                Files.move(parent, moved);
                Files.createSymbolicLink(parent, outside);
            }
        };

        PutResult result = service.putObject("imports/object.txt", input, NO_OVERWRITE);

        assertFalse(result.success());
        assertTrue(input.closed);
        assertFalse(Files.exists(outside.resolve("object.txt")));
        assertFalse(Files.exists(moved.resolve("object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void competingTargetCreatedDuringReadIsNotOverwritten() throws Exception {
        TrackedInputStream input = new TrackedInputStream("losing payload") {
            @Override
            public void close() throws IOException {
                super.close();
                Files.writeString(root.resolve("object.txt"), "winning payload");
            }
        };

        PutResult result = service.putObject("object.txt", input, NO_OVERWRITE);

        assertFalse(result.success());
        assertInstanceOf(FileAlreadyExistsException.class, result.e());
        assertEquals("winning payload", Files.readString(root.resolve("object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void concurrentWritersHaveExactlyOneWinnerWithoutPartialOrOverwrittenContent() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<PutResult>> writes = new ArrayList<>();
        List<TrackedInputStream> inputs = new ArrayList<>();
        try {
            for (int index = 0; index < 8; index++) {
                TrackedInputStream input = input("payload-" + index);
                inputs.add(input);
                writes.add(executor.submit(() -> {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    return service.putObject("imports/object.txt", input, NO_OVERWRITE);
                }));
            }
            start.countDown();
            int successes = 0;
            for (int index = 0; index < writes.size(); index++) {
                PutResult result = writes.get(index).get(5, TimeUnit.SECONDS);
                if (result.success()) {
                    successes++;
                    assertEquals("payload-" + index, Files.readString(root.resolve("imports/object.txt")));
                } else {
                    assertInstanceOf(FileAlreadyExistsException.class, result.e());
                }
                assertTrue(inputs.get(index).closed);
            }
            assertEquals(1, successes);
            assertNoTemporaryWrites();
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void independentServiceInstancesStillCannotOverwriteEachOther() throws Exception {
        LocalStorageService secondService = serviceAt(root);
        CyclicBarrier readyToPublish = new CyclicBarrier(2);
        TrackedInputStream first = awaitOnClose("first payload", readyToPublish);
        TrackedInputStream second = awaitOnClose("second payload", readyToPublish);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<PutResult> firstWrite = executor.submit(() -> service.putObject("object.txt", first, NO_OVERWRITE));
            Future<PutResult> secondWrite = executor.submit(
                    () -> secondService.putObject("object.txt", second, NO_OVERWRITE));

            PutResult firstResult = firstWrite.get(5, TimeUnit.SECONDS);
            PutResult secondResult = secondWrite.get(5, TimeUnit.SECONDS);

            assertTrue(firstResult.success() ^ secondResult.success());
            assertEquals(firstResult.success() ? "first payload" : "second payload",
                    Files.readString(root.resolve("object.txt")));
            assertTrue(first.closed);
            assertTrue(second.closed);
            assertNoTemporaryWrites();
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void deleteWaitsForTheSameKeyWriteAndOnlySeesTheCompletePublishedFile() throws Exception {
        CountDownLatch inputOpened = new CountDownLatch(1);
        CountDownLatch releaseInput = new CountDownLatch(1);
        CountDownLatch deleteStarted = new CountDownLatch(1);
        InputStream input = blockingInput(inputOpened, releaseInput);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<PutResult> write = executor.submit(() -> service.putObject("object.txt", input, NO_OVERWRITE));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));
            assertFalse(Files.exists(root.resolve("object.txt")), "Do not publish a partially written object");
            Future<Boolean> delete = executor.submit(() -> {
                deleteStarted.countDown();
                return service.delete("object.txt");
            });
            assertTrue(deleteStarted.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> delete.get(100, TimeUnit.MILLISECONDS));

            releaseInput.countDown();

            assertTrue(write.get(5, TimeUnit.SECONDS).success());
            assertTrue(delete.get(5, TimeUnit.SECONDS));
            assertFalse(Files.exists(root.resolve("object.txt")));
            assertNoTemporaryWrites();
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"object.txt", "./object.txt"})
    void legacyTwoArgumentWriterSharesTheLockButKeepsItsOriginalOverwriteAndStreamBehavior(String legacyKey)
            throws Exception {
        CountDownLatch inputOpened = new CountDownLatch(1);
        CountDownLatch releaseInput = new CountDownLatch(1);
        CountDownLatch legacyStarted = new CountDownLatch(1);
        InputStream input = blockingInput(inputOpened, releaseInput);
        TrackedInputStream legacyInput = input("legacy payload");
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<PutResult> write = executor.submit(() -> service.putObject("object.txt", input, NO_OVERWRITE));
            assertTrue(inputOpened.await(5, TimeUnit.SECONDS));
            Future<PutResult> legacyWrite = executor.submit(() -> {
                legacyStarted.countDown();
                return service.putObject(legacyKey, legacyInput);
            });
            assertTrue(legacyStarted.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> legacyWrite.get(100, TimeUnit.MILLISECONDS));

            releaseInput.countDown();

            assertTrue(write.get(5, TimeUnit.SECONDS).success());
            assertTrue(legacyWrite.get(5, TimeUnit.SECONDS).success());
            assertEquals("legacy payload", Files.readString(root.resolve("object.txt")));
            assertFalse(legacyInput.closed, "Preserve the pre-existing two-argument method contract");
            assertNoTemporaryWrites();
        } finally {
            releaseInput.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void deleteOnlyRemovesTheSpecifiedRegularFileAndAbsentFileReturnsFalse() throws Exception {
        Path parent = Files.createDirectory(root.resolve("imports"));
        Files.writeString(parent.resolve("first.txt"), "first");
        Files.writeString(parent.resolve("second.txt"), "second");

        assertTrue(service.delete("imports/first.txt"));
        assertFalse(service.delete("imports/first.txt"));
        assertFalse(service.delete("missing/object.txt"));
        assertTrue(Files.isDirectory(parent));
        assertEquals("second", Files.readString(parent.resolve("second.txt")));
    }

    @Test
    void deleteNeverRemovesEmptyOrNonemptyDirectoriesOrSymlinks() throws Exception {
        Path empty = Files.createDirectory(root.resolve("empty"));
        Path nonempty = Files.createDirectory(root.resolve("nonempty"));
        Files.writeString(nonempty.resolve("sentinel.txt"), "untouched");
        Path external = Files.writeString(outside.resolve("sentinel.txt"), "external");
        Path link = Files.createSymbolicLink(root.resolve("link.txt"), external);
        Path dangling = Files.createSymbolicLink(root.resolve("dangling.txt"), outside.resolve("missing.txt"));

        assertFalse(service.delete("empty"));
        assertFalse(service.delete("nonempty"));
        assertFalse(service.delete("link.txt"));
        assertFalse(service.delete("dangling.txt"));

        assertTrue(Files.isDirectory(empty));
        assertTrue(Files.isDirectory(nonempty));
        assertEquals("untouched", Files.readString(nonempty.resolve("sentinel.txt")));
        assertEquals("external", Files.readString(external));
        assertTrue(Files.exists(link, LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.exists(dangling, LinkOption.NOFOLLOW_LINKS));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void writeNeverReplacesADirectory(boolean overwrite) throws Exception {
        Path directory = Files.createDirectory(root.resolve("object.txt"));
        TrackedInputStream input = input("not written");

        PutResult result = service.putObject("object.txt", input,
                PutOptions.builder().overwrite(overwrite).build());

        assertFalse(result.success());
        assertTrue(input.closed);
        assertTrue(Files.isDirectory(directory));
        assertNoTemporaryWrites();
    }

    @Test
    void defaultReadabilityCheckDelegatesToTheExistingProviderContract() {
        StorageService provider = mock(StorageService.class, CALLS_REAL_METHODS);
        when(provider.exists("present.txt")).thenReturn(true);
        when(provider.exists("missing.txt")).thenReturn(false);

        assertTrue(provider.isReadableObject("present.txt"));
        assertFalse(provider.isReadableObject("missing.txt"));

        verify(provider).exists("present.txt");
        verify(provider).exists("missing.txt");
    }

    @Test
    void readableObjectCheckAcceptsNormalFilesWithoutValidatingTheirContents() throws Exception {
        Path parent = Files.createDirectory(root.resolve("imports"));
        Files.writeString(parent.resolve("object.txt"), "synthetic text, deliberately not FASTA");
        Files.createFile(parent.resolve("empty.txt"));

        assertTrue(service.isReadableObject("imports/object.txt"));
        assertTrue(service.isReadableObject("imports/empty.txt"));
        assertFalse(service.isReadableObject("imports/missing.txt"));
        assertFalse(service.isReadableObject("missing/object.txt"));
        assertFalse(Files.exists(root.resolve("missing")), "Readability checks must not create directories");
        assertNoTemporaryWrites();
    }

    @Test
    void readableObjectCheckRejectsDirectoriesAndDanglingSymlinksWithoutChangingExists() throws Exception {
        Files.createDirectory(root.resolve("directory"));
        Files.createSymbolicLink(root.resolve("dangling.txt"), outside.resolve("missing.txt"));

        assertTrue(service.exists("directory"));
        assertTrue(service.exists("dangling.txt"));
        assertFalse(service.isReadableObject("directory"));
        assertFalse(service.isReadableObject("dangling.txt"));
        assertTrue(Files.isDirectory(root.resolve("directory")));
        assertTrue(Files.isSymbolicLink(root.resolve("dangling.txt")));
    }

    @Test
    void readableObjectCheckRejectsBothInternalAndExternalFileSymlinks() throws Exception {
        Path internal = Files.writeString(root.resolve("original.txt"), "internal");
        Path external = Files.writeString(outside.resolve("original.txt"), "external");
        Files.createSymbolicLink(root.resolve("internal-link.txt"), internal);
        Files.createSymbolicLink(root.resolve("external-link.txt"), external);

        assertTrue(Files.isReadable(internal));
        assertTrue(Files.isReadable(external));
        assertTrue(service.exists("internal-link.txt"));
        assertTrue(service.exists("external-link.txt"));
        assertFalse(service.isReadableObject("internal-link.txt"));
        assertFalse(service.isReadableObject("external-link.txt"));
        assertEquals("internal", Files.readString(internal));
        assertEquals("external", Files.readString(external));
    }

    @Test
    void readableObjectCheckRejectsSymlinkParentsEvenWhenTheFinalFileIsReadable() throws Exception {
        Path internal = Files.createDirectory(root.resolve("original"));
        Files.writeString(internal.resolve("object.txt"), "internal");
        Files.writeString(outside.resolve("object.txt"), "external");
        Files.createSymbolicLink(root.resolve("internal-alias"), internal);
        Files.createSymbolicLink(root.resolve("external-alias"), outside);

        assertFalse(service.isReadableObject("internal-alias/object.txt"));
        assertFalse(service.isReadableObject("external-alias/object.txt"));
        assertTrue(service.isReadableObject("original/object.txt"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", ".", "..", "../outside/object.txt", "nested/../../outside/object.txt",
            "./object.txt", "nested/../object.txt", "nested//object.txt", "nested/"})
    void readableObjectCheckReturnsFalseForInvalidKeys(String key) throws Exception {
        Files.writeString(root.resolve("object.txt"), "internal");
        Files.writeString(outside.resolve("object.txt"), "external");

        assertFalse(service.isReadableObject(key));
        assertEquals("internal", Files.readString(root.resolve("object.txt")));
        assertEquals("external", Files.readString(outside.resolve("object.txt")));
        assertNoTemporaryWrites();
    }

    @Test
    void readableObjectCheckRejectsAbsolutePathsAndDoesNotCreateAMissingStorageRoot() throws Exception {
        Path external = Files.writeString(outside.resolve("object.txt"), "external");
        Path absentRoot = temporaryDirectory.resolve("absent-storage");
        LocalStorageService absentStorage = serviceAt(absentRoot);

        assertFalse(service.isReadableObject(external.toString()));
        assertFalse(service.isReadableObject(root.toString()));
        assertFalse(absentStorage.isReadableObject("object.txt"));
        assertFalse(Files.exists(absentRoot));
        assertEquals("external", Files.readString(external));
    }

    private static LocalStorageService serviceAt(Path root) {
        LocalStorageService service = new LocalStorageService();
        ReflectionTestUtils.setField(service, "base", root.toString());
        return service;
    }

    private static TrackedInputStream input(String content) {
        return new TrackedInputStream(content);
    }

    private static TrackedInputStream awaitOnClose(String content, CyclicBarrier barrier) {
        return new TrackedInputStream(content) {
            @Override
            public void close() throws IOException {
                super.close();
                try {
                    barrier.await(5, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IOException("Synthetic concurrent publication failed", e);
                }
            }
        };
    }

    private static InputStream blockingInput(CountDownLatch opened, CountDownLatch release) {
        return new InputStream() {
            private boolean finished;

            @Override
            public int read() throws IOException {
                if (finished) {
                    return -1;
                }
                opened.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new IOException("Synthetic input timed out");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
                finished = true;
                return 'x';
            }
        };
    }

    private void assertNoTemporaryWrites() throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().startsWith(".object-write-")));
        }
    }

    private static class TrackedInputStream extends ByteArrayInputStream {
        boolean closed;

        TrackedInputStream(String content) {
            super(content.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }

    private static class PartialFailureInputStream extends InputStream {
        boolean closed;
        private boolean firstRead = true;

        @Override
        public int read(byte[] data, int offset, int length) throws IOException {
            if (firstRead) {
                firstRead = false;
                data[offset] = 'x';
                return 1;
            }
            throw new IOException("synthetic read failure");
        }

        @Override
        public int read() throws IOException {
            throw new IOException("synthetic read failure");
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
