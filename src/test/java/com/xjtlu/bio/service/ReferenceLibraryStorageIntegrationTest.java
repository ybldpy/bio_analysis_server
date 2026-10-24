package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.requestParameters.UploadReferenceRequest;
import com.xjtlu.bio.response.ReferenceLibraryItem;

/** Real service/storage composition on synthetic temporary files, with no database or Spring context. */
class ReferenceLibraryStorageIntegrationTest {

    private static final byte[] SYNTHETIC_FASTA =
            ">fixture_a\nACGTN\n>fixture_b\nTTAA\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] EXISTING_FASTA = ">existing_fixture\nNNNN\n".getBytes(StandardCharsets.UTF_8);
    private static final String ACCESSION = "CUSTOM_STORAGE_FIXTURE";
    private static final long REFERENCE_ID = 701L;

    @TempDir
    Path temporaryDirectory;

    private Path storageRoot;
    private Path existingFile;
    private BioReferenceSequenceMapper mapper;
    private LocalStorageService storage;
    private ReferenceLibraryService service;
    private DiskLifecycleTransactionManager transactionManager;
    private AtomicReference<BioReferenceSequence> attemptedInsert;

    @BeforeEach
    void setUp() throws IOException {
        storageRoot = Files.createDirectory(temporaryDirectory.resolve("storage"));
        existingFile = storageRoot.resolve("existing/preexisting.fna");
        Files.createDirectories(existingFile.getParent());
        Files.write(existingFile, EXISTING_FASTA);
        mapper = mock(BioReferenceSequenceMapper.class);
        storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "base", storageRoot.toString());
        transactionManager = new DiskLifecycleTransactionManager();
        attemptedInsert = new AtomicReference<>();
        service = new ReferenceLibraryService(mapper, storage, transactionManager, mock(ReferenceTargetScopeService.class));
        ReflectionTestUtils.setField(service, "maxUploadBytes", 4096L);
        when(mapper.insertSelective(any(BioReferenceSequence.class))).thenAnswer(invocation -> {
            BioReferenceSequence row = invocation.getArgument(0);
            captureStoredInsert(row);
            row.setReferenceId(REFERENCE_ID);
            return 1;
        });
        when(mapper.selectByPrimaryKey(REFERENCE_ID)).thenAnswer(invocation -> attemptedInsert.get());
        when(mapper.updateByPrimaryKeySelective(any(BioReferenceSequence.class))).thenAnswer(invocation -> {
            BioReferenceSequence patch = invocation.getArgument(0);
            assertEquals(REFERENCE_ID, patch.getReferenceId());
            attemptedInsert.get().setEnabled(patch.getEnabled());
            return 1;
        });
    }

    @AfterEach
    void transactionStateDoesNotLeakBetweenCalls() {
        assertFalse(TransactionSynchronizationManager.isSynchronizationActive());
        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
    }

    @ParameterizedTest
    @ValueSource(strings = {"plain", "gzip", "zip"})
    void importAndEnabledLifecyclePreserveTheActualStoredBytes(String format) throws Exception {
        Result<ReferenceLibraryItem> imported = service.upload(request(), multipart(format));

        assertEquals(Result.SUCCESS, imported.getStatus());
        assertNull(imported.getFailMsg());
        assertNotNull(imported.getData());
        assertEquals(REFERENCE_ID, imported.getData().referenceId());
        assertTrue(imported.getData().enabled());
        String objectKey = imported.getData().fastaObjectName();
        Path published = storageRoot.resolve(objectKey);
        assertTrue(published.normalize().startsWith(storageRoot));
        assertArrayEquals(SYNTHETIC_FASTA, Files.readAllBytes(published));
        assertTrue(storage.isReadableObject(objectKey));
        assertEquals(1, transactionManager.commits);
        assertEquals(0, transactionManager.rollbacks);

        Result<ReferenceLibraryItem> disabled = service.setEnabled(REFERENCE_ID, false);

        assertEquals(Result.SUCCESS, disabled.getStatus());
        assertFalse(disabled.getData().enabled());
        assertArrayEquals(SYNTHETIC_FASTA, Files.readAllBytes(published),
                "Disabling must not modify or remove the published reference");
        assertTrue(storage.isReadableObject(objectKey));

        Result<ReferenceLibraryItem> enabled = service.setEnabled(REFERENCE_ID, true);

        assertEquals(Result.SUCCESS, enabled.getStatus());
        assertTrue(enabled.getData().enabled());
        assertArrayEquals(SYNTHETIC_FASTA, Files.readAllBytes(published));
        assertEquals(Result.SUCCESS, service.setEnabled(REFERENCE_ID, false).getStatus());

        // Remove only this test's own imported file to exercise real missing-file detection.
        Files.delete(published);
        Result<ReferenceLibraryItem> unavailable = service.setEnabled(REFERENCE_ID, true);

        assertEquals(Result.BUSINESS_FAIL, unavailable.getStatus());
        assertFalse(attemptedInsert.get().getEnabled());
        assertFalse(storage.isReadableObject(objectKey));
        assertOnlyExistingFixtureRemains();
    }

    @Test
    void definiteInsertRollbackDeletesOnlyTheNewlyStoredObject() throws Exception {
        doAnswer(invocation -> {
            captureStoredInsert(invocation.getArgument(0));
            throw new DataIntegrityViolationException("synthetic insert refusal");
        }).when(mapper).insertSelective(any(BioReferenceSequence.class));

        Result<ReferenceLibraryItem> result = service.upload(request(), multipart("plain"));

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertNull(result.getData());
        assertNotNull(attemptedInsert.get());
        assertFalse(Files.exists(storageRoot.resolve(attemptedInsert.get().getPath()), LinkOption.NOFOLLOW_LINKS));
        assertEquals(0, transactionManager.commits);
        assertEquals(1, transactionManager.rollbacks);
        assertOnlyExistingFixtureRemains();
    }

    @Test
    void unknownCommitOutcomeRetainsTheActualFileForReconciliation() throws Exception {
        transactionManager.unknownCommit = true;

        Result<ReferenceLibraryItem> result = service.upload(request(), multipart("gzip"));

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertNull(result.getData());
        assertTrue(result.getFailMsg().contains("待确认"));
        assertTrue(result.getFailMsg().contains(ACCESSION));
        assertNotNull(attemptedInsert.get());
        Path retained = storageRoot.resolve(attemptedInsert.get().getPath());
        assertArrayEquals(SYNTHETIC_FASTA, Files.readAllBytes(retained),
                "An uncertain commit must not remove a potentially referenced file");
        assertTrue(storage.isReadableObject(attemptedInsert.get().getPath()));
        assertArrayEquals(EXISTING_FASTA, Files.readAllBytes(existingFile));
        assertEquals(2, storedFiles().size());
        assertEquals(1, transactionManager.commits);
        assertEquals(0, transactionManager.rollbacks);
        assertNoStorageTemporaryFiles();
    }

    private void captureStoredInsert(BioReferenceSequence row) throws IOException {
        attemptedInsert.set(row);
        assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
        assertTrue(TransactionSynchronizationManager.isSynchronizationActive());
        assertArrayEquals(SYNTHETIC_FASTA, Files.readAllBytes(storageRoot.resolve(row.getPath())),
                "The complete object must exist before the database insert is attempted");
    }

    private void assertOnlyExistingFixtureRemains() throws IOException {
        assertArrayEquals(EXISTING_FASTA, Files.readAllBytes(existingFile));
        assertEquals(List.of(existingFile), storedFiles());
        assertNoStorageTemporaryFiles();
    }

    private List<Path> storedFiles() throws IOException {
        try (Stream<Path> paths = Files.walk(storageRoot)) {
            return paths.filter(path -> Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).sorted().toList();
        }
    }

    private void assertNoStorageTemporaryFiles() throws IOException {
        try (Stream<Path> paths = Files.walk(storageRoot)) {
            assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith(".object-write-")));
        }
    }

    private static UploadReferenceRequest request() {
        UploadReferenceRequest request = new UploadReferenceRequest();
        request.setAccession(ACCESSION);
        request.setTaxId(42);
        request.setOrgType(RefSeqService.VIRUS_TYPE);
        request.setEnabled(true);
        return request;
    }

    private static MockMultipartFile multipart(String format) throws IOException {
        if (format.equals("plain")) {
            return new MockMultipartFile("file", "fixture.fna", "text/plain", SYNTHETIC_FASTA);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        if (format.equals("gzip")) {
            try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
                gzip.write(SYNTHETIC_FASTA);
            }
            return new MockMultipartFile("file", "fixture.fna.gz", "application/gzip", bytes.toByteArray());
        }
        if (format.equals("zip")) {
            try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
                zip.putNextEntry(new ZipEntry("fixture.fna"));
                zip.write(SYNTHETIC_FASTA);
                zip.closeEntry();
            }
            return new MockMultipartFile("file", "fixture.zip", "application/zip", bytes.toByteArray());
        }
        throw new IllegalArgumentException("Unknown synthetic upload format: " + format);
    }

    /** Uses Spring's real synchronization/completion flow, but never opens a connection. */
    private static class DiskLifecycleTransactionManager extends AbstractPlatformTransactionManager {
        private int commits;
        private int rollbacks;
        private boolean unknownCommit;

        DiskLifecycleTransactionManager() {
            setRollbackOnCommitFailure(false);
        }

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            assertEquals(TransactionDefinition.PROPAGATION_REQUIRES_NEW, definition.getPropagationBehavior());
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, definition.getIsolationLevel());
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
            commits++;
            if (unknownCommit) {
                throw new TransactionSystemException("synthetic uncertain commit");
            }
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
            rollbacks++;
        }
    }
}
