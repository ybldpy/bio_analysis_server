package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.multipart.MultipartFile;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineInputFile;
import com.xjtlu.bio.entity.BioPipelineInputFileExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioPipelineInputFileMapper;
import com.xjtlu.bio.requestParameters.BatchPipelineInputUploadRequest;
import com.xjtlu.bio.response.BatchPipelineInputUploadResponse;

/** Protocol and failure-path tests. Real SQL atomicity is covered by the isolated HTTP integration test. */
class PipelineInputBatchServiceTest {
    private static final long PIPELINE_ID = 123L;
    private static final String OLD_R1 = "pipelineInput/p123/10/sample_0_0/old_R1.fastq";
    private static final String OLD_R2 = "pipelineInput/p123/10/sample_0_1/old_R2.fastq";
    private static final byte[] BYTES = "ordinary upload fixture".getBytes(StandardCharsets.UTF_8);
    private BioAnalysisPipelineMapper pipelineMapper;
    private BioPipelineInputFileMapper inputMapper;
    private StorageService storage;
    private PipelineInputOperationCoordinator coordinator;
    private RecordingTransactionManager transactions;
    private PipelineInputBatchService service;
    private BioAnalysisPipeline pipeline;
    private List<BioPipelineInputFile> previous;
    private List<BioPipelineInputFile> inserted;
    private Map<String, byte[]> objects;
    private List<String> writes;
    private AtomicLong nextId;

    @BeforeEach
    void setUp() {
        pipelineMapper = mock(BioAnalysisPipelineMapper.class);
        inputMapper = mock(BioPipelineInputFileMapper.class);
        storage = mock(StorageService.class);
        coordinator = new PipelineInputOperationCoordinator();
        transactions = new RecordingTransactionManager();
        service = new PipelineInputBatchService(pipelineMapper, inputMapper, storage, transactions, coordinator);
        pipeline = pipeline(100, PipelineService.PIPELINE_STATUS_PENDING);
        previous = new ArrayList<>();
        inserted = new ArrayList<>();
        objects = new LinkedHashMap<>();
        writes = new ArrayList<>();
        nextId = new AtomicLong(500);
        when(pipelineMapper.selectByPrimaryKey(PIPELINE_ID)).thenAnswer(call -> pipeline);
        when(inputMapper.selectByExample(any())).thenAnswer(call -> new ArrayList<>(previous));
        when(inputMapper.insertSelective(any())).thenAnswer(call -> {
            assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
            BioPipelineInputFile row = call.getArgument(0);
            row.setInputFileId(nextId.incrementAndGet());
            inserted.add(row);
            return 1;
        });
        when(storage.exists(anyString())).thenAnswer(call -> objects.containsKey(call.getArgument(0)));
        when(storage.isReadableObject(anyString())).thenAnswer(call -> objects.containsKey(call.getArgument(0)));
        when(storage.delete(anyString())).thenAnswer(call -> objects.remove(call.getArgument(0)) != null);
        when(storage.putObject(anyString(), any(InputStream.class), any(StorageService.PutOptions.class)))
                .thenAnswer(call -> {
                    assertFalse(TransactionSynchronizationManager.isActualTransactionActive(), "Do not hold SQL transactions while streaming");
                    String key = call.getArgument(0);
                    StorageService.PutOptions options = call.getArgument(2);
                    assertFalse(options.overwrite);
                    assertFalse(objects.containsKey(key));
                    writes.add(key);
                    objects.put(key, ((InputStream) call.getArgument(1)).readAllBytes());
                    return new StorageService.PutResult(true, null);
                });
    }

    @AfterEach
    void releasesTransactionAndOperationState() {
        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
        assertFalse(TransactionSynchronizationManager.isSynchronizationActive());
        assertEquals(0, coordinator.activePipelineCount());
        verify(inputMapper, never()).deleteByExample(any());
        verify(inputMapper, never()).countByExample(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "null-request", "null-id", "zero-id", "negative-id", "null-level", "unknown-level", "reference-level", "file-role-level",
            "null-inputs", "empty-inputs", "three-inputs", "null-input", "null-files", "empty-files", "null-slot", "invalid-slot",
            "other-sample", "r2-only", "duplicate-slot", "duplicate-part", "null-part", "empty-part", "reserved-part", "invalid-part",
            "long-part", "missing-part", "null-part-list", "empty-part-list", "duplicate-files", "null-file", "empty-file", "extra-file",
            "assembly-r2", "read-with-fasta", "assembly-with-fastq"
    })
    void invalidManifestRejectsTheEntireRequestBeforeAnyDatabaseOrStorageWork(String scenario) {
        BatchPipelineInputUploadRequest request = request(0, item("sample/0/0", "reads"));
        MultiValueMap<String, MultipartFile> files = files(file("reads", "sample.fastq"));
        switch (scenario) {
            case "null-request" -> request = null;
            case "null-id" -> request.setPipelineId(null);
            case "zero-id" -> request.setPipelineId(0L);
            case "negative-id" -> request.setPipelineId(-1L);
            case "null-level" -> request.setSequenceLevel(null);
            case "unknown-level" -> request.setSequenceLevel(-1);
            case "reference-level" -> request.setSequenceLevel(2);
            case "file-role-level" -> request.setSequenceLevel(10);
            case "null-inputs" -> request.setInputs(null);
            case "empty-inputs" -> request.setInputs(List.of());
            case "three-inputs" -> request.setInputs(List.of(item("sample/0/0", "a"), item("sample/0/1", "b"), item("sample/1/0", "c")));
            case "null-input" -> request.setInputs(Arrays.asList((BatchPipelineInputUploadRequest.Input) null));
            case "null-files" -> files = null;
            case "empty-files" -> files.clear();
            case "null-slot" -> request.getInputs().get(0).setInputKey(null);
            case "invalid-slot" -> request.getInputs().get(0).setInputKey("sample/0/2");
            case "other-sample" -> request.getInputs().get(0).setInputKey("sample/1/0");
            case "r2-only" -> request.getInputs().get(0).setInputKey("sample/0/1");
            case "duplicate-slot" -> request.setInputs(List.of(item("sample/0/0", "reads"), item("sample/0/0", "other")));
            case "duplicate-part" -> request.setInputs(List.of(item("sample/0/0", "reads"), item("sample/0/1", "reads")));
            case "null-part" -> request.getInputs().get(0).setFilePart(null);
            case "empty-part" -> request.getInputs().get(0).setFilePart("");
            case "reserved-part" -> request.getInputs().get(0).setFilePart("manifest");
            case "invalid-part" -> request.getInputs().get(0).setFilePart("../reads");
            case "long-part" -> request.getInputs().get(0).setFilePart("x".repeat(65));
            case "missing-part" -> request.getInputs().get(0).setFilePart("other");
            case "null-part-list" -> files.put("reads", null);
            case "empty-part-list" -> files.put("reads", List.of());
            case "duplicate-files" -> files.add("reads", file("reads", "second.fastq"));
            case "null-file" -> files.put("reads", Arrays.asList((MultipartFile) null));
            case "empty-file" -> files.set("reads", new MockMultipartFile("reads", "empty.fastq", null, new byte[0]));
            case "extra-file" -> files.add("extra", file("extra", "extra.fastq"));
            case "assembly-r2" -> {
                request.setSequenceLevel(1);
                request.getInputs().get(0).setInputKey("sample/0/1");
            }
            case "read-with-fasta" -> files.set("reads", file("reads", "sample.fasta"));
            case "assembly-with-fastq" -> request.setSequenceLevel(1);
            default -> fail("Unknown test case " + scenario);
        }
        Result<?> result = service.uploadBatch(request, files);
        assertEquals(Result.PARAMETER_NOT_VALID, result.getStatus(), scenario);
        assertNull(result.getData());
        assertNotNull(result.getFailMsg());
        verifyNoInteractions(pipelineMapper, inputMapper, storage);
        assertEquals(0, transactions.begins);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", " ", "../x.fastq", "/tmp/x.fastq", "C:\\temp\\x.fastq", "x\n.fastq", ".hidden.fastq", "x.fastq.tar.gz", "x.txt", "x.fasta.gz"})
    void rejectsUnsafeOrWrongFormatFileNames(String name) {
        Result<?> result = service.uploadBatch(single(0), files(file("reads", name)));
        assertEquals(Result.PARAMETER_NOT_VALID, result.getStatus());
        verifyNoInteractions(pipelineMapper, inputMapper, storage);
    }

    @Test
    void rejectsNamesExceedingCharacterOrEncodedFilesystemLimits() {
        for (String name : List.of("a".repeat(513) + ".fastq", "测".repeat(90) + ".fastq")) {
            assertEquals(Result.PARAMETER_NOT_VALID, service.uploadBatch(single(0), files(file("reads", name))).getStatus());
        }
        verifyNoInteractions(pipelineMapper, inputMapper, storage);
    }

    @ParameterizedTest
    @ValueSource(strings = {"sample.fastq", "sample.fq", "sample.fastq.gz", "sample.fq.gzip", "sample.fastq.zip", "SAMPLE.FQ.GZ"})
    void readFormatsUseRole10AndPreserveTheirCompressionSuffix(String name) {
        Result<BatchPipelineInputUploadResponse> result = service.uploadBatch(single(0), files(file("reads", name)));
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(0, result.getData().getSequenceLevel());
        assertEquals(10, inserted.get(0).getFileRole());
        assertTrue(writes.get(0).endsWith("/" + name));
        assertEquals(TransactionDefinition.PROPAGATION_REQUIRES_NEW, transactions.propagation);
        assertEquals(1, transactions.commits);
        assertEquals(0, transactions.rollbacks);
    }

    @ParameterizedTest
    @ValueSource(strings = {"sample.fasta", "sample.fa", "sample.fna", "sample.contig", "sample.contigs", "sample.fasta.gz", "sample.fna.gzip", "sample.fa.zip"})
    void assemblyFormatsUseRole11(String name) {
        Result<BatchPipelineInputUploadResponse> result = service.uploadBatch(single(1), files(file("reads", name)));
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(1, result.getData().getSequenceLevel());
        assertEquals(11, inserted.get(0).getFileRole());
    }

    @ParameterizedTest
    @ValueSource(ints = {100, 101, 200})
    void supportsTheThreeAgreedUserPipelineTypes(int type) {
        pipeline.setPipelineType(type);
        assertEquals(Result.SUCCESS, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {102, 300, 301, 302, 999})
    void doesNotImplicitlyOpenOtherPipelineTypes(Integer type) {
        pipeline.setPipelineType(type);
        assertEquals(Result.BUSINESS_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        verifyNoInteractions(inputMapper, storage);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(ints = {0, 1, 2, 4})
    void onlyPendingPipelineStatus3AcceptsInput(Integer state) {
        pipeline.setStatus(state);
        assertEquals(Result.BUSINESS_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        verifyNoInteractions(inputMapper, storage);
    }

    @Test
    void missingPipelineAndDerivedPipelineCannotAcceptUploads() {
        pipeline = null;
        assertEquals(Result.BUSINESS_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        pipeline = pipeline(100, 3);
        pipeline.setParentPipelineId(77L);
        assertEquals(Result.BUSINESS_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        verifyNoInteractions(inputMapper, storage);
    }

    @Test
    void heldStartPermitRejectsWithoutReleasingTheOtherCallersPermit() {
        var held = coordinator.tryAcquire(PIPELINE_ID, PipelineInputOperationCoordinator.Operation.START);
        try (var permit = held.permit()) {
            assertEquals(Result.DUPLICATE_OPERATION, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
            assertEquals(1, coordinator.activePipelineCount());
            verifyNoInteractions(pipelineMapper, inputMapper, storage);
        }
    }

    @Test
    void pairedInputsPublishTogetherInManifestOrderAndUseDistinctPathsEvenWithIdenticalNames() {
        Result<BatchPipelineInputUploadResponse> result = service.uploadBatch(paired(), pairedFiles("same.fastq"));
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(List.of("sample/0/0", "sample/0/1"), result.getData().getInputs().stream()
                .map(BatchPipelineInputUploadResponse.Input::getInputKey).toList());
        assertEquals(List.of(501L, 502L), result.getData().getInputs().stream()
                .map(BatchPipelineInputUploadResponse.Input::getInputFileId).toList());
        assertEquals(2, objects.size());
        assertNotEquals(writes.get(0), writes.get(1));
        assertEquals(batchPrefix(writes.get(0)), batchPrefix(writes.get(1)));
        assertEquals(1, transactions.commits);
        verify(inputMapper, never()).deleteByExample(any());
        verify(inputMapper, times(2)).selectByExample(argThat(query -> sampleScope(query)));
    }

    @Test
    void retryAfterFailedInitialUploadUsesANewBatchNamespace() {
        transactions.beginFailure = new CannotCreateTransactionException("temporarily unavailable");
        assertEquals(Result.INTERNAL_FAIL, service.uploadBatch(single(0), files(file("reads", "same.fastq"))).getStatus());
        assertTrue(objects.isEmpty());
        transactions.beginFailure = null;
        assertEquals(Result.SUCCESS, service.uploadBatch(single(0), files(file("reads", "same.fastq"))).getStatus());
        assertNotEquals(batchPrefix(writes.get(0)), batchPrefix(writes.get(1)));
        assertEquals(1, objects.size());
    }

    @Test
    void safeDisplayNameDoesNotDiscardTheOriginalName() {
        assertEquals(Result.SUCCESS, service.uploadBatch(single(0), files(file("reads", " sample reads.fastq "))).getStatus());
        assertEquals("sample_reads.fastq", inserted.get(0).getFileName());
        assertEquals(" sample reads.fastq ", inserted.get(0).getOriginalFilename());
        assertTrue(writes.get(0).endsWith("/sample_reads.fastq"));
    }

    @Test
    void existingPairedReadsRejectAnAssemblyUploadWithoutWritingOrReplacingAnything() {
        existingPair();
        Result<BatchPipelineInputUploadResponse> result = service.uploadBatch(single(1), files(file("reads", "contigs.fasta")));
        assertEquals(Result.BUSINESS_FAIL, result.getStatus());
        assertNull(result.getData());
        assertTrue(result.getFailMsg().contains("仅支持首次完整上传"));
        assertEquals(0, transactions.begins);
        assertEquals(2, objects.size());
        verifyNoInteractions(storage);
        verify(inputMapper, never()).insertSelective(any());
        verify(inputMapper).selectByExample(argThat(PipelineInputBatchServiceTest::sampleScope));
    }

    @ParameterizedTest
    @ValueSource(strings = {"read-r1", "read-r2", "assembly", "pending-read"})
    void anyExistingSampleRowIncludingPartialLegacyUploadsRejectsAnotherBatch(String kind) {
        BioPipelineInputFile existing = row(11, kind.equals("read-r2") ? "sample/0/1" : "sample/0/0",
                OLD_R1, kind.equals("assembly") ? 11 : 10);
        if (kind.equals("pending-read")) existing.setStatus(0);
        previous.add(existing);
        Result<?> result = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.BUSINESS_FAIL, result.getStatus());
        assertNull(result.getData());
        assertEquals(0, transactions.begins);
        assertTrue(inserted.isEmpty());
        verifyNoInteractions(storage);
        verify(inputMapper, never()).insertSelective(any());
    }

    @Test
    void aSuccessfulInitialUploadCannotBeRepeatedEvenWithTheSameFiles() {
        Result<?> first = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.SUCCESS, first.getStatus());
        previous.addAll(inserted);
        int writesBeforeRetry = writes.size();
        Result<?> retry = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.BUSINESS_FAIL, retry.getStatus());
        assertNull(retry.getData());
        assertEquals(writesBeforeRetry, writes.size());
        assertEquals(2, inserted.size());
        assertEquals(2, objects.size());
        assertEquals(1, transactions.begins);
        verify(storage, never()).delete(anyString());
    }

    @Test
    void referenceRole20DoesNotBlockTheInitialSampleUploadAndIsNeverRemoved() {
        BioPipelineInputFile reference = row(90, "refseq", "pipelineInput/p123/reference.fna", 20);
        objects.put(reference.getFilePath(), BYTES);
        when(inputMapper.selectByExample(any())).thenAnswer(call -> {
            BioPipelineInputFileExample query = call.getArgument(0);
            assertTrue(sampleScope(query));
            List<?> roles = (List<?>) criterion(query, "file_role in");
            return roles.contains(reference.getFileRole()) ? List.of(reference) : List.of();
        });
        Result<?> result = service.uploadBatch(single(0), files(file("reads", "sample.fastq")));
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(2, objects.size());
        assertTrue(objects.containsKey(reference.getFilePath()));
        verify(storage, never()).delete(anyString());
    }

    @Test
    void failureWritingTheSecondFileNeverCreatesAnEffectiveHalfBatch() {
        AtomicInteger attempt = new AtomicInteger();
        doAnswer(call -> {
            String key = call.getArgument(0);
            writes.add(key);
            objects.put(key, BYTES); // Simulate partial bytes left by the failing second writer.
            return attempt.incrementAndGet() == 2
                    ? new StorageService.PutResult(false, new IOException("second stream failed"))
                    : new StorageService.PutResult(true, null);
        }).when(storage).putObject(anyString(), any(), any());
        Result<?> result = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertNull(result.getData());
        assertEquals(0, objects.size());
        writes.forEach(key -> assertFalse(objects.containsKey(key)));
        verify(inputMapper, never()).insertSelective(any());
        verify(inputMapper, never()).deleteByExample(any());
        assertEquals(0, transactions.begins);
    }

    @ParameterizedTest
    @ValueSource(strings = {"null-put", "unreadable", "stream-open", "put-throw"})
    void storageFailuresNeverPublishSampleAssociations(String kind) throws Exception {
        MultiValueMap<String, MultipartFile> files = files(file("reads", "sample.fastq"));
        if (kind.equals("null-put")) doReturn(null).when(storage).putObject(anyString(), any(), any());
        if (kind.equals("unreadable")) when(storage.isReadableObject(anyString())).thenReturn(false);
        if (kind.equals("put-throw")) doThrow(new IllegalStateException("disk unavailable")).when(storage).putObject(anyString(), any(), any());
        if (kind.equals("stream-open")) {
            MultipartFile failing = mock(MultipartFile.class);
            when(failing.getSize()).thenReturn(10L);
            when(failing.getOriginalFilename()).thenReturn("sample.fastq");
            when(failing.getInputStream()).thenThrow(new IOException("read failed"));
            files.set("reads", failing);
        }
        assertEquals(Result.INTERNAL_FAIL, service.uploadBatch(single(0), files).getStatus());
        assertEquals(0, objects.size());
        verify(inputMapper, never()).insertSelective(any());
        verify(inputMapper, never()).deleteByExample(any());
        assertEquals(0, transactions.begins);
    }

    @Test
    void unexpectedExistingObjectAtGeneratedKeyIsNotClaimedOrDeleted() {
        when(storage.exists(anyString())).thenReturn(true);
        assertEquals(Result.INTERNAL_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        verify(storage, never()).putObject(anyString(), any(), any());
        verify(storage, never()).delete(anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"second-insert", "zero-insert", "missing-generated-id"})
    void databaseErrorsRequestRollbackAndCleanOnlyNewObjects(String failure) {
        objects.put("unrelated/other-pipeline.fastq", BYTES);
        if (failure.equals("second-insert")) {
            AtomicInteger attempt = new AtomicInteger();
            doAnswer(call -> {
                BioPipelineInputFile row = call.getArgument(0);
                if (attempt.incrementAndGet() == 2) throw new DataAccessResourceFailureException("second insert failed");
                row.setInputFileId(501L);
                return 1;
            }).when(inputMapper).insertSelective(any());
        }
        if (failure.equals("zero-insert")) doReturn(0).when(inputMapper).insertSelective(any());
        if (failure.equals("missing-generated-id")) doReturn(1).when(inputMapper).insertSelective(any());
        Result<?> result = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertNull(result.getData());
        assertEquals(1, transactions.rollbacks);
        assertEquals(0, transactions.commits);
        assertEquals(1, objects.size());
        assertTrue(objects.containsKey("unrelated/other-pipeline.fastq"));
        verify(storage, never()).delete("unrelated/other-pipeline.fastq");
    }

    @Test
    void failedTransactionBeginCleansNewFilesWithoutOpeningADatabaseMutation() {
        transactions.beginFailure = new CannotCreateTransactionException("database unreachable");
        assertEquals(Result.INTERNAL_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        assertEquals(0, objects.size());
        verify(inputMapper, never()).insertSelective(any());
        assertEquals(0, transactions.rollbacks);
    }

    @Test
    void aSampleAppearingDuringUploadIsDetectedInsideTransactionBeforeAnyMutation() {
        BioPipelineInputFile changed = row(99, "sample/0/0", OLD_R1, 10);
        objects.put(OLD_R1, BYTES);
        when(inputMapper.selectByExample(any())).thenReturn(List.of(), List.of(changed));
        Result<?> result = service.uploadBatch(single(0), files(file("reads", "sample.fastq")));
        assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
        assertEquals(1, transactions.rollbacks);
        assertEquals(1, objects.size());
        assertTrue(objects.containsKey(OLD_R1));
        verify(storage, never()).delete(OLD_R1);
        verify(inputMapper, never()).insertSelective(any());
        verify(inputMapper, never()).deleteByExample(any());
    }

    @Test
    void pipelineStatusIsRecheckedAfterAllFilesAreStored() {
        when(pipelineMapper.selectByPrimaryKey(PIPELINE_ID)).thenReturn(pipeline, pipeline(100, 1));
        assertEquals(Result.BUSINESS_FAIL, service.uploadBatch(single(0), files(file("reads", "sample.fastq"))).getStatus());
        assertEquals(1, transactions.rollbacks);
        assertEquals(0, objects.size());
        verify(inputMapper, never()).insertSelective(any());
    }

    @Test
    void rollbackCleanupFailureDoesNotEscapeOrChangeTheOriginalFailureOutcome() {
        doThrow(new DataAccessResourceFailureException("original SQL failure")).when(inputMapper).insertSelective(any());
        when(storage.delete(anyString())).thenThrow(new IllegalStateException("cleanup also failed"));
        Result<?> result = assertDoesNotThrow(() -> service.uploadBatch(paired(), pairedFiles("sample.fastq")));
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertTrue(result.getFailMsg().contains("本批次未生效"));
        assertFalse(result.getFailMsg().contains("cleanup"));
        assertEquals(1, transactions.rollbacks);
        assertEquals(2, objects.size(), "Unreferenced new objects may remain when rollback cleanup also fails");
    }

    @Test
    void unknownCommitOutcomeRetainsNewFilesInsteadOfClaimingRollback() {
        transactions.commitFailure = new TransactionSystemException("connection lost during COMMIT");
        Result<?> result = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertTrue(result.getFailMsg().contains("尚未确认"));
        assertFalse(result.getFailMsg().contains("未生效"));
        assertEquals(1, transactions.commits);
        assertEquals(0, transactions.rollbacks);
        assertEquals(2, objects.size());
        verify(storage, never()).delete(anyString());
    }

    @Test
    void unknownCommitWithACompleteMatchingReadBackIsRecoveredAsSuccess() {
        transactions.commitFailure = new TransactionSystemException("connection lost after COMMIT");
        AtomicInteger reads = new AtomicInteger();
        when(inputMapper.selectByExample(any())).thenAnswer(call -> reads.incrementAndGet() <= 2
                ? new ArrayList<>(previous) : new ArrayList<>(inserted));
        Result<BatchPipelineInputUploadResponse> result = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(List.of(501L, 502L), result.getData().getInputs().stream()
                .map(BatchPipelineInputUploadResponse.Input::getInputFileId).toList());
        assertEquals(2, objects.size());
        writes.forEach(key -> assertTrue(objects.containsKey(key)));
        verify(storage, never()).delete(anyString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing-row", "extra-row", "wrong-slot", "wrong-role", "wrong-path", "wrong-status", "wrong-name", "null-id", "lookup-fails"})
    void incompleteOrUnverifiableReadBackCannotTurnAnUnknownCommitIntoSuccess(String mismatch) {
        transactions.commitFailure = new TransactionSystemException("lost COMMIT reply");
        AtomicInteger reads = new AtomicInteger();
        when(inputMapper.selectByExample(any())).thenAnswer(call -> {
            if (reads.incrementAndGet() <= 2) return new ArrayList<>(previous);
            if (mismatch.equals("lookup-fails")) throw new DataAccessResourceFailureException("reconciliation unavailable");
            List<BioPipelineInputFile> actual = inserted.stream().map(PipelineInputBatchServiceTest::copy).collect(java.util.stream.Collectors.toCollection(ArrayList::new));
            switch (mismatch) {
                case "missing-row" -> actual.remove(1);
                case "extra-row" -> actual.add(row(999, "sample/8/0", "unrelated/key", 10));
                case "wrong-slot" -> actual.get(0).setInputKey("sample/4/0");
                case "wrong-role" -> actual.get(0).setFileRole(11);
                case "wrong-path" -> actual.get(0).setFilePath("unrelated/key");
                case "wrong-status" -> actual.get(0).setStatus(0);
                case "wrong-name" -> actual.get(0).setFileName("different.fastq");
                case "null-id" -> actual.get(0).setInputFileId(null);
                default -> fail("Unknown mismatch " + mismatch);
            }
            return actual;
        });
        Result<?> result = service.uploadBatch(paired(), pairedFiles("sample.fastq"));
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertTrue(result.getFailMsg().contains("尚未确认"));
        assertEquals(2, objects.size());
        verify(storage, never()).delete(anyString());
    }

    @Test
    void rollbackFailureAlsoLeavesOutcomeUnknownAndRetainsObjects() {
        transactions.rollbackFailure = new TransactionSystemException("rollback reply lost");
        doThrow(new DataAccessResourceFailureException("insert failed")).when(inputMapper).insertSelective(any());
        Result<?> result = service.uploadBatch(single(0), files(file("reads", "sample.fastq")));
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertTrue(result.getFailMsg().contains("尚未确认"));
        assertEquals(1, objects.size());
        verify(storage, never()).delete(anyString());
    }

    @Test
    void exceptionAfterCommittedTransactionDoesNotDeleteNewObjectsOrReportRollback() {
        doAnswer(call -> {
            BioPipelineInputFile row = call.getArgument(0);
            row.setInputFileId(501L);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() { throw new IllegalStateException("post-commit listener failed"); }
            });
            return 1;
        }).when(inputMapper).insertSelective(any());
        Result<?> result = service.uploadBatch(single(0), files(file("reads", "sample.fastq")));
        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(1, transactions.commits);
        assertEquals(0, transactions.rollbacks);
        assertEquals(1, objects.size());
        verify(storage, never()).delete(anyString());
    }

    private void existingPair() {
        previous.add(row(11, "sample/0/0", OLD_R1, 10));
        previous.add(row(12, "sample/0/1", OLD_R2, 10));
        objects.put(OLD_R1, BYTES);
        objects.put(OLD_R2, BYTES);
    }

    private static BioAnalysisPipeline pipeline(int type, int status) {
        BioAnalysisPipeline row = new BioAnalysisPipeline();
        row.setPipelineId(PIPELINE_ID);
        row.setPipelineType(type);
        row.setStatus(status);
        return row;
    }

    private static BioPipelineInputFile row(long id, String slot, String path, int role) {
        BioPipelineInputFile row = new BioPipelineInputFile();
        row.setInputFileId(id);
        row.setPipelineId(PIPELINE_ID);
        row.setInputKey(slot);
        row.setFilePath(path);
        row.setFileRole(role);
        row.setFileName(path.substring(path.lastIndexOf('/') + 1));
        row.setOriginalFilename(row.getFileName());
        row.setStatus(1);
        return row;
    }

    private static BioPipelineInputFile copy(BioPipelineInputFile original) {
        BioPipelineInputFile row = row(original.getInputFileId(), original.getInputKey(), original.getFilePath(), original.getFileRole());
        row.setFileName(original.getFileName());
        row.setOriginalFilename(original.getOriginalFilename());
        row.setStatus(original.getStatus());
        return row;
    }

    private static BatchPipelineInputUploadRequest single(int level) {
        return request(level, item("sample/0/0", "reads"));
    }

    private static BatchPipelineInputUploadRequest paired() {
        return request(0, item("sample/0/0", "r1"), item("sample/0/1", "r2"));
    }

    private static BatchPipelineInputUploadRequest request(int level, BatchPipelineInputUploadRequest.Input... inputs) {
        BatchPipelineInputUploadRequest request = new BatchPipelineInputUploadRequest();
        request.setPipelineId(PIPELINE_ID);
        request.setSequenceLevel(level);
        request.setInputs(List.of(inputs));
        return request;
    }

    private static BatchPipelineInputUploadRequest.Input item(String slot, String part) {
        BatchPipelineInputUploadRequest.Input input = new BatchPipelineInputUploadRequest.Input();
        input.setInputKey(slot);
        input.setFilePart(part);
        return input;
    }

    private static MockMultipartFile file(String part, String name) {
        return new MockMultipartFile(part, name, "application/octet-stream", BYTES);
    }

    private static MultiValueMap<String, MultipartFile> files(MultipartFile... files) {
        MultiValueMap<String, MultipartFile> result = new LinkedMultiValueMap<>();
        for (MultipartFile file : files) result.add(file.getName(), file);
        return result;
    }

    private static MultiValueMap<String, MultipartFile> pairedFiles(String fileName) {
        return files(file("r1", fileName), file("r2", fileName));
    }

    private static String batchPrefix(String path) {
        String[] components = path.split("/");
        assertEquals("pipelineInput", components[0]);
        assertEquals("p123", components[1]);
        assertEquals("batches", components[2]);
        assertEquals(6, components.length);
        java.util.UUID.fromString(components[3]);
        java.util.UUID.fromString(components[4]);
        return components[3];
    }

    private static boolean sampleScope(BioPipelineInputFileExample query) {
        return Long.valueOf(PIPELINE_ID).equals(criterion(query, "pipeline_id ="))
                && List.of(10, 11).equals(criterion(query, "file_role in"));
    }

    private static Object criterion(BioPipelineInputFileExample query, String name) {
        return query.getOredCriteria().get(0).getAllCriteria().stream()
                .filter(item -> item.getCondition().equals(name)).findFirst().orElseThrow().getValue();
    }

    /** No resource emulation: Spring itself drives synchronization and exception outcomes. */
    private static final class RecordingTransactionManager extends AbstractPlatformTransactionManager {
        private int begins;
        private int commits;
        private int rollbacks;
        private int propagation;
        private RuntimeException beginFailure;
        private RuntimeException commitFailure;
        private RuntimeException rollbackFailure;

        private RecordingTransactionManager() {
            setRollbackOnCommitFailure(false);
        }

        @Override protected Object doGetTransaction() { return new Object(); }

        @Override protected void doBegin(Object transaction, TransactionDefinition definition) {
            begins++;
            propagation = definition.getPropagationBehavior();
            if (beginFailure != null) throw beginFailure;
        }

        @Override protected void doCommit(DefaultTransactionStatus status) {
            commits++;
            if (commitFailure != null) throw commitFailure;
        }

        @Override protected void doRollback(DefaultTransactionStatus status) {
            rollbacks++;
            if (rollbackFailure != null) throw rollbackFailure;
        }
    }
}
