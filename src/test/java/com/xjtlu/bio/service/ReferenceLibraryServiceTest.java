package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.web.multipart.MultipartFile;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.entity.BioReferenceSequenceExample;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.requestParameters.ReferenceLibraryQuery;
import com.xjtlu.bio.requestParameters.UploadReferenceRequest;
import com.xjtlu.bio.response.ReferenceLibraryItem;
import com.xjtlu.bio.response.ReferenceLibraryPage;
import com.xjtlu.bio.service.reference.ReferenceFastaUpload;
import com.xjtlu.bio.utils.JsonUtil;

/** Synthetic bytes and mocked persistence only; no Spring context or database connection. */
class ReferenceLibraryServiceTest {
    private static final byte[] FASTA = ">fixture_a synthetic\nACGT\n>fixture_b\nNNNN\n"
            .getBytes(StandardCharsets.UTF_8);
    private static final String EXISTING_KEY = "refseq/existing/reference.fna";
    private static final byte[] EXISTING_BYTES = "old object must survive".getBytes(StandardCharsets.UTF_8);

    @TempDir Path temporaryDirectory;
    private BioReferenceSequenceMapper mapper;
    private StorageService storage;
    private RecordingTransactionManager transactions;
    private ReferenceLibraryService service;
    private Map<String, byte[]> objects;
    private StorageService.PutOptions lastPutOptions;
    private String lastPutKey;

    @BeforeEach
    void setUp() {
        mapper = mock(BioReferenceSequenceMapper.class);
        storage = mock(StorageService.class);
        transactions = new RecordingTransactionManager();
        service = new ReferenceLibraryService(mapper, storage, transactions, mock(ReferenceTargetScopeService.class));
        objects = new LinkedHashMap<>();
        objects.put(EXISTING_KEY, EXISTING_BYTES);
        when(storage.putObject(anyString(), any(InputStream.class), any(StorageService.PutOptions.class)))
                .thenAnswer(invocation -> {
                    lastPutKey = invocation.getArgument(0);
                    lastPutOptions = invocation.getArgument(2);
                    InputStream input = invocation.getArgument(1);
                    assertFalse(objects.containsKey(lastPutKey));
                    objects.put(lastPutKey, input.readAllBytes());
                    return new StorageService.PutResult(true, null);
                });
        when(storage.delete(anyString())).thenAnswer(invocation -> objects.remove(invocation.getArgument(0)) != null);
        when(storage.exists(anyString())).thenAnswer(invocation -> objects.containsKey(invocation.getArgument(0)));
        when(storage.isReadableObject(anyString())).thenAnswer(invocation -> objects.containsKey(invocation.getArgument(0)));
        when(mapper.insertSelective(any(BioReferenceSequence.class))).thenAnswer(invocation -> {
            assertTrue(TransactionSynchronizationManager.isSynchronizationActive());
            assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
            BioReferenceSequence row = invocation.getArgument(0);
            row.setReferenceId(321L);
            return 1;
        });
    }

    @AfterEach
    void noThreadLocalStateLeaks() {
        PageHelper.clearPage();
        assertFalse(TransactionSynchronizationManager.isSynchronizationActive());
        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
    }

    @Test
    void queryCombinesAllFiltersAndClearsTheOffsetPage() {
        ReferenceLibraryQuery query = new ReferenceLibraryQuery();
        query.setStart(7);
        query.setPageSize(3);
        query.setTaxId(101);
        query.setOrgType(2);
        query.setEnabled(false);
        query.setAccession("  SYNTHETIC_001  ");
        when(mapper.countByExample(any())).thenAnswer(invocation -> {
            assertNull(PageHelper.getLocalPage());
            BioReferenceSequenceExample example = invocation.getArgument(0);
            assertEquals(Map.of("tax_id =", 101, "org_type =", 2, "enabled =", false,
                    "accession =", "SYNTHETIC_001"), conditions(example));
            return 12L;
        });
        when(mapper.selectByExample(any())).thenAnswer(invocation -> {
            BioReferenceSequenceExample example = invocation.getArgument(0);
            assertEquals("reference_id desc", example.getOrderByClause());
            Page<?> page = PageHelper.getLocalPage();
            assertNotNull(page);
            assertEquals(7, page.getStartRow());
            assertEquals(3, page.getPageSize());
            assertFalse(page.isCount());
            return List.of(reference(false));
        });

        Result<ReferenceLibraryPage> result = service.query(query);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(12, result.getData().total());
        assertEquals(7, result.getData().start());
        assertEquals(3, result.getData().pageSize());
        assertFalse(result.getData().items().get(0).enabled());
        assertNull(PageHelper.getLocalPage());
        verifyNoInteractions(storage);
    }

    @Test
    void queryDefaultsIncludeDisabledRowsAndDoNotAddAnEnabledPredicate() {
        when(mapper.selectByExample(any())).thenReturn(List.of(reference(true), reference(false)));

        Result<ReferenceLibraryPage> result = service.query(null);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(0, result.getData().start());
        assertEquals(20, result.getData().pageSize());
        assertEquals(List.of(true, false), result.getData().items().stream().map(ReferenceLibraryItem::enabled).toList());
        ArgumentCaptor<BioReferenceSequenceExample> examples = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(mapper).selectByExample(examples.capture());
        assertTrue(conditions(examples.getValue()).isEmpty());
        assertNull(PageHelper.getLocalPage());
    }

    @Test
    void queryClearsPageOnSelectFailureAndDoesNotExposeDatabaseDetails() {
        when(mapper.selectByExample(any())).thenThrow(new DataAccessResourceFailureException("private SQL fixture"));

        Result<?> result = service.query(null);

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertFalse(result.getFailMsg().contains("private SQL"));
        assertNull(PageHelper.getLocalPage());
    }

    @ParameterizedTest
    @MethodSource("invalidQueries")
    void invalidQueryDoesNotReachPersistence(ReferenceLibraryQuery query) {
        assertEquals(Result.PARAMETER_NOT_VALID, service.query(query).getStatus());
        verifyNoInteractions(mapper, storage);
    }

    static Stream<Arguments> invalidQueries() {
        ReferenceLibraryQuery negativeStart = new ReferenceLibraryQuery(); negativeStart.setStart(-1);
        ReferenceLibraryQuery zeroSize = new ReferenceLibraryQuery(); zeroSize.setPageSize(0);
        ReferenceLibraryQuery largeSize = new ReferenceLibraryQuery(); largeSize.setPageSize(101);
        ReferenceLibraryQuery tax = new ReferenceLibraryQuery(); tax.setTaxId(0);
        ReferenceLibraryQuery type = new ReferenceLibraryQuery(); type.setOrgType(3);
        ReferenceLibraryQuery accession = new ReferenceLibraryQuery(); accession.setAccession("x".repeat(65));
        ReferenceLibraryQuery control = new ReferenceLibraryQuery(); control.setAccession("a\u0000b");
        return Stream.of(negativeStart, zeroSize, largeSize, tax, type, accession, control).map(Arguments::of);
    }

    @Test
    void detailReturnsDisabledReferenceWithoutFileExistenceCheck() {
        when(mapper.selectByPrimaryKey(42L)).thenReturn(reference(false));

        Result<ReferenceLibraryItem> result = service.detail(42L);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertFalse(result.getData().enabled());
        assertEquals(EXISTING_KEY, result.getData().fastaObjectName());
        verifyNoInteractions(storage);
    }

    @Test
    void detailRejectsInvalidIdAndReportsMissingRecord() {
        assertEquals(Result.PARAMETER_NOT_VALID, service.detail(0).getStatus());
        verifyNoInteractions(mapper);
        assertEquals(Result.BUSINESS_FAIL, service.detail(99).getStatus());
        verify(mapper).selectByPrimaryKey(99L);
    }

    @Test
    void enableRequiresReadableObjectAndUpdatesOnlyEnabledAndPrimaryKey() throws Exception {
        when(mapper.selectByPrimaryKey(42L)).thenReturn(reference(false));
        when(mapper.updateByPrimaryKeySelective(any())).thenReturn(1);

        Result<ReferenceLibraryItem> result = service.setEnabled(42L, true);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertTrue(result.getData().enabled());
        verify(storage).isReadableObject(EXISTING_KEY);
        ArgumentCaptor<BioReferenceSequence> patch = ArgumentCaptor.forClass(BioReferenceSequence.class);
        verify(mapper).updateByPrimaryKeySelective(patch.capture());
        Map<String, Object> nonNullFields = new LinkedHashMap<>(JsonUtil.toMap(JsonUtil.toJson(patch.getValue())));
        nonNullFields.values().removeIf(value -> value == null);
        assertEquals(Map.of("referenceId", 42, "enabled", true), nonNullFields);
        verify(storage, never()).delete(anyString());
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", " ", "refseq/missing.fna"})
    void enableRefusesMissingOrUnreadableObject(String path) {
        BioReferenceSequence row = reference(false);
        row.setPath(path);
        when(mapper.selectByPrimaryKey(42L)).thenReturn(row);

        assertEquals(Result.BUSINESS_FAIL, service.setEnabled(42L, true).getStatus());

        verify(mapper, never()).updateByPrimaryKeySelective(any());
        verify(storage, never()).delete(anyString());
    }

    @Test
    void disableNeedsNoObjectAndDoesNotDeleteOrRewriteIt() {
        when(mapper.selectByPrimaryKey(42L)).thenReturn(reference(true));
        when(mapper.updateByPrimaryKeySelective(any())).thenReturn(1);

        Result<ReferenceLibraryItem> result = service.setEnabled(42L, false);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertFalse(result.getData().enabled());
        verifyNoInteractions(storage);
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    @Test
    void enabledUpdateHandlesMissingInvalidIdMissingFlagAndNoOp() {
        assertEquals(Result.PARAMETER_NOT_VALID, service.setEnabled(0, true).getStatus());
        assertEquals(Result.PARAMETER_NOT_VALID, service.setEnabled(42, null).getStatus());
        verifyNoInteractions(mapper, storage);
        assertEquals(Result.BUSINESS_FAIL, service.setEnabled(99, false).getStatus());
        when(mapper.selectByPrimaryKey(42L)).thenReturn(reference(false));
        assertEquals(Result.SUCCESS, service.setEnabled(42, false).getStatus());
        verify(mapper, never()).updateByPrimaryKeySelective(any());
        verifyNoInteractions(storage);
    }

    @Test
    void unsuccessfulEnabledUpdateIsNotReportedAsSuccess() {
        when(mapper.selectByPrimaryKey(42L)).thenReturn(reference(true));
        assertEquals(Result.BUSINESS_FAIL, service.setEnabled(42, false).getStatus());
        when(mapper.updateByPrimaryKeySelective(any())).thenThrow(new IllegalStateException("private SQL fixture"));
        Result<?> result = service.setEnabled(42, false);
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertFalse(result.getFailMsg().contains("private SQL"));
    }

    @ParameterizedTest
    @MethodSource("invalidMetadata")
    void metadataValidationPrecedesStorageAndDatabase(UploadReferenceRequest request) {
        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(request, plainFile()).getStatus());
        verifyNoInteractions(mapper, storage);
        assertEquals(0, transactions.begins);
    }

    static Stream<Arguments> invalidMetadata() {
        UploadReferenceRequest noTax = metadata(); noTax.setTaxId(null);
        UploadReferenceRequest tax = metadata(); tax.setTaxId(0);
        UploadReferenceRequest noType = metadata(); noType.setOrgType(null);
        UploadReferenceRequest type = metadata(); type.setOrgType(3);
        UploadReferenceRequest accession = metadata(); accession.setAccession("not a valid accession");
        UploadReferenceRequest longAccession = metadata(); longAccession.setAccession("x".repeat(65));
        UploadReferenceRequest control = metadata(); control.setOrganismName("fixture\u0000name");
        UploadReferenceRequest organism = metadata(); organism.setOrganismName("x".repeat(256));
        UploadReferenceRequest segment = metadata(); segment.setSegment("x".repeat(65));
        UploadReferenceRequest completeness = metadata(); completeness.setCompleteness("x".repeat(33));
        return Stream.of(null, noTax, tax, noType, type, accession, longAccession, control, organism, segment,
                completeness).map(value -> Arguments.of((Object) value));
    }

    @ParameterizedTest
    @MethodSource("invalidFiles")
    void invalidFastaCannotInsertOrStore(MultipartFile file) {
        Result<?> result = service.upload(metadata(), file);
        assertEquals(Result.PARAMETER_NOT_VALID, result.getStatus());
        verifyNoInteractions(storage);
        verify(mapper, never()).insertSelective(any());
        assertEquals(0, transactions.begins);
    }

    static Stream<Arguments> invalidFiles() {
        return Stream.of(
                Arguments.of((Object) null),
                Arguments.of(file("empty.fa", new byte[0])),
                Arguments.of(file("fixture.txt", FASTA)),
                Arguments.of(file("fixture.fa", "ACGT\n".getBytes(StandardCharsets.UTF_8))),
                Arguments.of(file("fixture.fa", ">missing\n".getBytes(StandardCharsets.UTF_8))),
                Arguments.of(file("fixture.fa", ">a\nACGT\n>a\nNNNN\n".getBytes(StandardCharsets.UTF_8))),
                Arguments.of(file("fixture.fa", ">a\nAC$T\n".getBytes(StandardCharsets.UTF_8))),
                Arguments.of(file("fixture.fa.gz", FASTA)));
    }

    @Test
    void declaredSizeAndDecompressedSizeAreBothBounded() throws IOException {
        ReflectionTestUtils.setField(service, "maxUploadBytes", 10L);
        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(metadata(), plainFile()).getStatus());
        verifyNoInteractions(mapper, storage);

        ReflectionTestUtils.setField(service, "maxUploadBytes", 80L);
        byte[] content = (">bounded_fixture\n" + "A".repeat(200) + "\n").getBytes(StandardCharsets.UTF_8);
        byte[] compressed = gzip(content);
        assertTrue(compressed.length < 80);
        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(metadata(), file("bounded.fa.gz", compressed)).getStatus());
        verifyNoInteractions(storage);
        verify(mapper, never()).insertSelective(any());
    }

    @Test
    void uploadReadFailureDoesNotStoreOrInsert() throws IOException {
        MultipartFile broken = mock(MultipartFile.class);
        when(broken.getInputStream()).thenThrow(new IOException("fixture read failure"));

        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(metadata(), broken).getStatus());

        verifyNoInteractions(storage);
        verify(mapper, never()).insertSelective(any());
    }

    @Test
    void duplicatePrecheckIncludesDisabledRowsAndPreservesExistingFile() {
        when(mapper.countByExample(any())).thenReturn(1L);

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
        ArgumentCaptor<BioReferenceSequenceExample> duplicate = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(mapper).countByExample(duplicate.capture());
        assertEquals(Map.of("accession =", "SYNTHETIC_001"), conditions(duplicate.getValue()));
        verify(mapper, never()).insertSelective(any());
        verifyNoInteractions(storage);
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
        assertEquals(0, transactions.begins);
    }

    @ParameterizedTest
    @ValueSource(strings = {"fa", "fasta.gz", "fna.gzip", "zip"})
    void validUploadUsesPreparedBytesAndGeneratedIdAndServerOwnedMetadata(String extension) throws Exception {
        byte[] uploaded = extension.endsWith("gz") || extension.endsWith("gzip") ? gzip(FASTA)
                : extension.equals("zip") ? zip(FASTA) : FASTA;
        String filename = "C:\\fakepath\\synthetic." + extension;
        ReferenceFastaUpload.Prepared expected = ReferenceFastaUpload.prepare(
                new ByteArrayInputStream(uploaded), filename, temporaryDirectory, 4096);
        UploadReferenceRequest request = metadata();
        request.setOrganismName("  synthetic organism  ");
        request.setSegment("  fixture segment  ");
        request.setCompleteness("  fixture status  ");
        request.setEnabled(false);

        Result<ReferenceLibraryItem> result = service.upload(request, file(filename, uploaded));

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(321L, result.getData().referenceId());
        assertFalse(result.getData().enabled());
        assertEquals("USER_UPLOAD", result.getData().sourceDb());
        assertEquals("synthetic organism", result.getData().organismName());
        assertEquals("fixture segment", result.getData().segment());
        assertEquals("fixture status", result.getData().completeness());
        assertEquals(8, result.getData().genomeLength());
        assertFalse(result.getData().isAnnotated());
        assertEquals(lastPutKey, result.getData().fastaObjectName());
        assertTrue(lastPutKey.matches("refseq/custom/[0-9a-f-]{36}/reference\\.fna"));
        assertArrayEquals(Files.readAllBytes(expected.path()), objects.get(lastPutKey));
        assertArrayEquals(FASTA, objects.get(lastPutKey));
        assertFalse(lastPutOptions.overwrite);
        assertEquals("text/plain", lastPutOptions.contentType);
        assertEquals((long) FASTA.length, lastPutOptions.size);
        ArgumentCaptor<BioReferenceSequence> record = ArgumentCaptor.forClass(BioReferenceSequence.class);
        verify(mapper).insertSelective(record.capture());
        BioReferenceSequence inserted = record.getValue();
        assertNull(inserted.getAnnotationFile());
        assertNull(inserted.getGeneCount());
        assertNull(inserted.getProteinCount());
        Map<String, Object> raw = JsonUtil.toMap(inserted.getRawMetadata());
        assertEquals(Set.of("importedAt", "originalFilename", "recordCount", "sha256"), raw.keySet());
        assertDoesNotThrow(() -> Instant.parse((String) raw.get("importedAt")));
        assertEquals("synthetic." + extension, raw.get("originalFilename"));
        assertEquals(2, raw.get("recordCount"));
        assertEquals(expected.sha256(), raw.get("sha256"));
        assertEquals(1, transactions.commits);
        assertEquals(0, transactions.rollbacks);
        assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, transactions.isolation);
        assertEquals(TransactionDefinition.PROPAGATION_REQUIRES_NEW, transactions.propagation);
        verify(storage, never()).delete(anyString());
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    @Test
    void omittedAccessionIsGeneratedAndOmittedEnabledDefaultsToTrue() {
        UploadReferenceRequest request = metadata();
        request.setAccession(null);
        request.setEnabled(null);

        Result<ReferenceLibraryItem> result = service.upload(request, plainFile());

        assertEquals(Result.SUCCESS, result.getStatus());
        assertTrue(result.getData().accession().matches("CUSTOM_[0-9a-f]{32}"));
        assertTrue(result.getData().enabled());
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "null", "throw"})
    void storageFailureNeverStartsInsertTransaction(String failure) {
        if (failure.equals("throw")) {
            doThrow(new IllegalStateException("private storage fixture")).when(storage).putObject(anyString(), any(), any());
        } else {
            doReturn(failure.equals("null") ? null
                    : new StorageService.PutResult(false, new IOException("private storage fixture")))
                    .when(storage).putObject(anyString(), any(), any());
        }

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertFalse(result.getFailMsg().contains("private storage"));
        verify(mapper, never()).insertSelective(any());
        assertEquals(0, transactions.begins);
        verify(storage, never()).delete(anyString());
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @Test
    void insertRaceRollsBackAndDeletesOnlyTheNewObject() {
        doThrow(new DuplicateKeyException("fixture accession race")).when(mapper).insertSelective(any());

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
        assertEquals(1, transactions.rollbacks);
        assertEquals(0, transactions.commits);
        verify(storage).delete(lastPutKey);
        verify(storage, never()).delete(EXISTING_KEY);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"exception", "zeroRows", "missingId"})
    void failedInsertOrMissingGeneratedIdRollsBackAndRemovesOnlyNewObject(String failure) {
        if (failure.equals("exception")) {
            doThrow(new IllegalStateException("private SQL fixture")).when(mapper).insertSelective(any());
        } else {
            doReturn(failure.equals("zeroRows") ? 0 : 1).when(mapper).insertSelective(any());
        }

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertFalse(result.getFailMsg().contains("private SQL"));
        assertEquals(1, transactions.rollbacks);
        verify(storage).delete(lastPutKey);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @Test
    void transactionBeginFailureCleansNewObjectWithoutAnInsert() {
        transactions.beginFailure = new CannotCreateTransactionException("fixture begin failure");

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        verify(mapper, never()).insertSelective(any());
        verify(storage).delete(lastPutKey);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @ParameterizedTest
    @ValueSource(strings = {"transaction", "duplicate", "argument"})
    void unknownCommitOutcomeRetainsObjectAndRequiresAccessionConfirmation(String exceptionType) {
        transactions.commitFailure = switch (exceptionType) {
            case "duplicate" -> new DuplicateKeyException("private uncertain commit fixture");
            case "argument" -> new IllegalArgumentException("private uncertain commit fixture");
            default -> new TransactionSystemException("private uncertain commit fixture");
        };
        // Spring attempts rollback after a plain RuntimeException from commit.
        // Make that rollback fail too, so the actual completion is UNKNOWN;
        // a successful rollback would legitimately report ROLLED_BACK instead.
        if (!exceptionType.equals("transaction")) transactions.rollbackFailure = transactions.commitFailure;

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertTrue(result.getFailMsg().contains("待确认"));
        assertTrue(result.getFailMsg().contains("SYNTHETIC_001"));
        assertFalse(result.getFailMsg().contains("private uncertain"));
        assertNull(result.getData());
        assertEquals(1, transactions.commits);
        assertEquals(exceptionType.equals("transaction") ? 0 : 1, transactions.rollbacks);
        verify(storage, never()).delete(anyString());
        assertArrayEquals(FASTA, objects.get(lastPutKey));
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cleanupFailureDoesNotMaskTheOriginalDuplicateResult(boolean throwing) {
        doThrow(new DuplicateKeyException("fixture accession race")).when(mapper).insertSelective(any());
        if (throwing) when(storage.delete(anyString())).thenThrow(new IllegalStateException("fixture cleanup failure"));
        else when(storage.delete(anyString())).thenReturn(false);

        Result<?> result = service.upload(metadata(), plainFile());

        assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
        assertFalse(result.getFailMsg().contains("cleanup"));
        assertEquals(1, transactions.rollbacks);
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
        assertArrayEquals(FASTA, objects.get(lastPutKey));
    }

    private static Map<String, Object> conditions(BioReferenceSequenceExample example) {
        assertEquals(1, example.getOredCriteria().size());
        Map<String, Object> result = new LinkedHashMap<>();
        for (var condition : example.getOredCriteria().get(0).getAllCriteria()) {
            assertNull(result.put(condition.getCondition(), condition.getValue()));
        }
        return result;
    }

    private static BioReferenceSequence reference(boolean enabled) {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setReferenceId(42L);
        row.setAccession("SYNTHETIC_001");
        row.setTaxId(101);
        row.setOrgType(2);
        row.setEnabled(enabled);
        row.setPath(EXISTING_KEY);
        row.setRawMetadata("{\"internalFixture\":true}");
        return row;
    }

    private static UploadReferenceRequest metadata() {
        UploadReferenceRequest request = new UploadReferenceRequest();
        request.setAccession("  SYNTHETIC_001  ");
        request.setTaxId(101);
        request.setOrgType(2);
        return request;
    }

    private static MockMultipartFile plainFile() { return file("synthetic.fa", FASTA); }

    private static MockMultipartFile file(String filename, byte[] bytes) {
        return new MockMultipartFile("file", filename, "application/octet-stream", bytes);
    }

    private static byte[] gzip(byte[] content) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream compressed = new GZIPOutputStream(bytes)) { compressed.write(content); }
        return bytes.toByteArray();
    }

    private static byte[] zip(byte[] content) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream archive = new ZipOutputStream(bytes)) {
            archive.putNextEntry(new ZipEntry("../../synthetic.fna"));
            archive.write(content);
            archive.closeEntry();
        }
        return bytes.toByteArray();
    }

    /** Spring drives synchronization callbacks: the fake controls only the resource outcome. */
    private static final class RecordingTransactionManager extends AbstractPlatformTransactionManager {
        private int begins;
        private int commits;
        private int rollbacks;
        private int isolation;
        private int propagation;
        private RuntimeException beginFailure;
        private RuntimeException commitFailure;
        private RuntimeException rollbackFailure;

        @Override protected Object doGetTransaction() { return new Object(); }

        @Override protected void doBegin(Object transaction, TransactionDefinition definition) {
            begins++;
            isolation = definition.getIsolationLevel();
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
