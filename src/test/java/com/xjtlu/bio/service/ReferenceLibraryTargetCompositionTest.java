package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioPipelineTarget;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;
import com.xjtlu.bio.requestParameters.UploadReferenceRequest;
import com.xjtlu.bio.response.ReferenceLibraryItem;
import com.xjtlu.bio.response.ReferenceMembershipConfirmationResult;

/**
 * Composes the actual upload, target-scope and taxonomy-resolver services with stateful mapper
 * doubles. Spring drives transaction synchronization; snapshots model rollback of both tables.
 * This is not a database integration test and makes no SQL locking or concurrency guarantees.
 */
class ReferenceLibraryTargetCompositionTest {
    private static final int TARGET = 70001;
    private static final int ACTUAL = 70002;
    private static final int OTHER = 70003;
    private static final int PIPELINE_TYPE = 100;
    private static final long NEW_REFERENCE_ID = 321L;
    private static final String EXISTING_KEY = "refseq/existing/reference.fna";
    private static final byte[] EXISTING_BYTES = "existing object must survive".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FASTA = ">synthetic_composition_fixture\nACGT\n".getBytes(StandardCharsets.UTF_8);
    private static final StoredReference EXISTING_REFERENCE =
            new StoredReference(42L, OTHER, "EXISTING_SYNTHETIC", EXISTING_KEY);

    private BioPipelineTargetMapper targets;
    private BioReferenceTaxonomyMemberMapper members;
    private BioReferenceSequenceMapper references;
    private StorageService storage;
    private ReferenceTaxonomyResolver resolver;
    private ReferenceLibraryService service;
    private DatabaseState database;
    private RecordingTransactionManager transactions;
    private Map<String, byte[]> objects;
    private final List<String> writes = new ArrayList<>();
    private final List<Object> writeTransactions = new ArrayList<>();
    private String newObjectKey;
    private Integer memberInsertFailure;
    private List<Integer> membersSeenAtFailure;
    private boolean referenceInsertFailure;

    @BeforeEach
    void setUp() {
        targets = mock(BioPipelineTargetMapper.class);
        members = mock(BioReferenceTaxonomyMemberMapper.class);
        references = mock(BioReferenceSequenceMapper.class);
        storage = mock(StorageService.class);
        database = new DatabaseState();
        database.references.put(EXISTING_REFERENCE.referenceId(), EXISTING_REFERENCE);
        transactions = new RecordingTransactionManager(database);
        resolver = new ReferenceTaxonomyResolver(members);
        ReferenceTargetScopeService scopes = new ReferenceTargetScopeService(targets, members, resolver);
        service = new ReferenceLibraryService(references, storage, transactions, scopes);
        objects = new LinkedHashMap<>();
        objects.put(EXISTING_KEY, EXISTING_BYTES);

        BioPipelineTarget target = new BioPipelineTarget();
        target.setId(1L);
        target.setTaxId(TARGET);
        target.setPipelineType(PIPELINE_TYPE);
        when(targets.selectByTaxIdAndPipelineType(TARGET, PIPELINE_TYPE)).thenReturn(target);
        when(targets.lockIdsByTaxId(TARGET)).thenAnswer(invocation -> {
            assertImportTransaction();
            return List.of(1L);
        });
        when(members.selectMemberTaxIds(TARGET)).thenAnswer(invocation ->
                database.members.entrySet().stream().sorted(Map.Entry.comparingByValue())
                        .map(Map.Entry::getKey).toList());
        when(members.selectNextSortOrder(TARGET)).thenAnswer(invocation -> {
            assertImportTransaction();
            return database.members.values().stream().mapToLong(Integer::longValue).max().orElse(-1L) + 1L;
        });
        when(members.insertMemberIfAbsent(eq(TARGET), anyInt(), anyInt())).thenAnswer(invocation -> {
            int member = invocation.getArgument(1);
            int sortOrder = invocation.getArgument(2);
            recordWrite("member:" + member);
            if (Integer.valueOf(member).equals(memberInsertFailure)) {
                membersSeenAtFailure = List.copyOf(database.members.keySet());
                throw new DataAccessResourceFailureException("private synthetic member-write failure");
            }
            return database.members.putIfAbsent(member, sortOrder) == null ? 1 : 0;
        });
        when(references.insertSelective(any())).thenAnswer(invocation -> {
            BioReferenceSequence row = invocation.getArgument(0);
            recordWrite("reference:" + NEW_REFERENCE_ID);
            row.setReferenceId(NEW_REFERENCE_ID);
            database.references.put(NEW_REFERENCE_ID,
                    new StoredReference(NEW_REFERENCE_ID, row.getTaxId(), row.getAccession(), row.getPath()));
            // Mutate before throwing to prove rollback restores both state collections.
            if (referenceInsertFailure) {
                throw new DataAccessResourceFailureException("private synthetic reference-write failure");
            }
            return 1;
        });
        when(storage.putObject(anyString(), any(InputStream.class), any(StorageService.PutOptions.class)))
                .thenAnswer(invocation -> {
                    assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
                    newObjectKey = invocation.getArgument(0);
                    assertFalse(objects.containsKey(newObjectKey));
                    InputStream input = invocation.getArgument(1);
                    objects.put(newObjectKey, input.readAllBytes());
                    return new StorageService.PutResult(true, null);
                });
        when(storage.delete(anyString())).thenAnswer(invocation -> objects.remove(invocation.getArgument(0)) != null);
        when(storage.exists(anyString())).thenAnswer(invocation -> objects.containsKey(invocation.getArgument(0)));
    }

    @AfterEach
    void transactionThreadStateIsClean() {
        assertFalse(TransactionSynchronizationManager.isSynchronizationActive());
        assertFalse(TransactionSynchronizationManager.isActualTransactionActive());
        assertNull(transactions.activeTransaction);
    }

    @Test
    void firstConfirmedNonSelfUploadPreservesImplicitSelfAndStoresTheActualReferenceTaxId() {
        assertEquals(List.of(TARGET), resolver.resolveMemberTaxIds(TARGET));

        Result<ReferenceLibraryItem> result = service.upload(metadata(true), syntheticFile());

        assertSuccessfulImport(result);
        assertEquals(Map.of(TARGET, 0, ACTUAL, 1), database.members);
        assertEquals(List.of(TARGET, ACTUAL), resolver.resolveMemberTaxIds(TARGET));
        assertEquals(List.of("member:" + TARGET, "member:" + ACTUAL, "reference:" + NEW_REFERENCE_ID), writes);
        assertOneCommittedTransaction();
    }

    @Test
    void explicitNonSelfScopePreservesItsMembersWithoutInventingSelfMembership() {
        database.members.put(OTHER, 7);

        Result<ReferenceLibraryItem> result = service.upload(metadata(true), syntheticFile());

        assertSuccessfulImport(result);
        assertEquals(Map.of(OTHER, 7, ACTUAL, 8), database.members);
        assertEquals(List.of(OTHER, ACTUAL), resolver.resolveMemberTaxIds(TARGET));
        assertFalse(database.members.containsKey(TARGET));
        verify(members, never()).insertMemberIfAbsent(eq(TARGET), eq(TARGET), anyInt());
        assertEquals(List.of("member:" + ACTUAL, "reference:" + NEW_REFERENCE_ID), writes);
        assertOneCommittedTransaction();
    }

    @Test
    void referenceWriteFailureRollsBackBothMemberWritesAndTheRowAndDeletesOnlyTheNewFile() {
        referenceInsertFailure = true;

        Result<?> result = service.upload(metadata(true), syntheticFile());

        assertDefiniteRollback(result);
        assertEquals(List.of("member:" + TARGET, "member:" + ACTUAL, "reference:" + NEW_REFERENCE_ID), writes);
        assertTrue(database.members.isEmpty());
        assertEquals(List.of(TARGET), resolver.resolveMemberTaxIds(TARGET));
        assertEquals(1, new HashSet<>(writeTransactions).size());
    }

    @Test
    void memberWriteFailureRollsBackEarlierSelfMaterializationAndPreventsReferenceInsert() {
        memberInsertFailure = ACTUAL;

        Result<?> result = service.upload(metadata(true), syntheticFile());

        assertDefiniteRollback(result);
        assertEquals(List.of(TARGET), membersSeenAtFailure);
        assertEquals(List.of("member:" + TARGET, "member:" + ACTUAL), writes);
        assertTrue(database.members.isEmpty());
        verify(references, never()).insertSelective(any());
        assertEquals(1, new HashSet<>(writeTransactions).size());
    }

    @Test
    void unconfirmedOutsideMemberIsRejectedByRealPreflightWithoutReadingBytesOrWritingState() throws Exception {
        MockMultipartFile file = spy(syntheticFile());

        Result<?> result = service.upload(metadata(false), file);

        ReferenceMembershipConfirmationResult confirmation = assertInstanceOf(
                ReferenceMembershipConfirmationResult.class, result);
        assertEquals(Result.BUSINESS_FAIL, result.getStatus());
        assertNull(result.getData());
        assertEquals("REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED", confirmation.getCode());
        assertEquals(TARGET, confirmation.getConfirmation().targetTaxId());
        assertEquals(PIPELINE_TYPE, confirmation.getConfirmation().pipelineType());
        assertEquals(ACTUAL, confirmation.getConfirmation().taxId());
        assertTrue(database.members.isEmpty());
        assertEquals(Map.of(EXISTING_REFERENCE.referenceId(), EXISTING_REFERENCE), database.references);
        assertTrue(writes.isEmpty());
        assertEquals(0, transactions.begins);
        verify(file, never()).getInputStream();
        verify(targets, never()).lockIdsByTaxId(anyInt());
        verify(members, never()).insertMemberIfAbsent(anyInt(), anyInt(), anyInt());
        verifyNoInteractions(references, storage);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @Test
    void existingExplicitMemberUploadsWithoutConfirmationOrAnyMembershipWrite() {
        database.members.put(ACTUAL, 2);
        database.members.put(OTHER, 9);

        Result<ReferenceLibraryItem> result = service.upload(metadata(false), syntheticFile());

        assertSuccessfulImport(result);
        assertEquals(Map.of(ACTUAL, 2, OTHER, 9), database.members);
        verify(members, never()).selectNextSortOrder(anyInt());
        verify(members, never()).insertMemberIfAbsent(anyInt(), anyInt(), anyInt());
        assertEquals(List.of("reference:" + NEW_REFERENCE_ID), writes);
        assertOneCommittedTransaction();
    }

    private void assertSuccessfulImport(Result<ReferenceLibraryItem> result) {
        assertEquals(Result.SUCCESS, result.getStatus());
        assertFalse(result instanceof ReferenceMembershipConfirmationResult);
        assertEquals(NEW_REFERENCE_ID, result.getData().referenceId());
        assertEquals(ACTUAL, result.getData().taxId());
        assertEquals(ACTUAL, database.references.get(NEW_REFERENCE_ID).taxId());
        assertEquals(newObjectKey, database.references.get(NEW_REFERENCE_ID).path());
        assertEquals(EXISTING_REFERENCE, database.references.get(EXISTING_REFERENCE.referenceId()));
        assertArrayEquals(FASTA, objects.get(newObjectKey));
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
        verify(storage, never()).delete(anyString());
    }

    private void assertOneCommittedTransaction() {
        assertEquals(1, transactions.begins);
        assertEquals(1, transactions.commits);
        assertEquals(0, transactions.rollbacks);
        assertEquals(1, new HashSet<>(writeTransactions).size());
        verify(targets).lockIdsByTaxId(TARGET);
    }

    private void assertDefiniteRollback(Result<?> result) {
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertNull(result.getData());
        assertFalse(result.getFailMsg().contains("private synthetic"));
        assertEquals(1, transactions.begins);
        assertEquals(0, transactions.commits);
        assertEquals(1, transactions.rollbacks);
        assertEquals(Map.of(EXISTING_REFERENCE.referenceId(), EXISTING_REFERENCE), database.references);
        verify(storage).delete(newObjectKey);
        verify(storage, never()).delete(EXISTING_KEY);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    private void recordWrite(String description) {
        assertImportTransaction();
        assertNotNull(transactions.activeTransaction);
        writes.add(description);
        writeTransactions.add(transactions.activeTransaction);
    }

    private static void assertImportTransaction() {
        assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
        assertTrue(TransactionSynchronizationManager.isSynchronizationActive());
        assertFalse(TransactionSynchronizationManager.isCurrentTransactionReadOnly());
        assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED,
                TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
    }

    private static UploadReferenceRequest metadata(boolean confirmed) {
        UploadReferenceRequest request = new UploadReferenceRequest();
        request.setAccession("SYNTHETIC_COMPOSITION_001");
        request.setTargetTaxId(TARGET);
        request.setPipelineType(PIPELINE_TYPE);
        request.setTaxId(ACTUAL);
        request.setOrgType(1);
        request.setConfirmAddTaxonomyMember(confirmed);
        return request;
    }

    private static MockMultipartFile syntheticFile() {
        return new MockMultipartFile("file", "synthetic.fa", "application/octet-stream", FASTA);
    }

    private record StoredReference(long referenceId, int taxId, String accession, String path) {}

    private static final class DatabaseState {
        private final Map<Integer, Integer> members = new LinkedHashMap<>();
        private final Map<Long, StoredReference> references = new LinkedHashMap<>();
    }

    private static final class TransactionSnapshot {
        private Map<Integer, Integer> members;
        private Map<Long, StoredReference> references;
    }

    /** File objects are deliberately excluded: only the service's compensation removes them. */
    private static final class RecordingTransactionManager extends AbstractPlatformTransactionManager {
        private final DatabaseState database;
        private TransactionSnapshot activeTransaction;
        private int begins;
        private int commits;
        private int rollbacks;

        private RecordingTransactionManager(DatabaseState database) { this.database = database; }

        @Override protected Object doGetTransaction() { return new TransactionSnapshot(); }

        @Override protected void doBegin(Object transaction, TransactionDefinition definition) {
            assertNull(activeTransaction);
            assertEquals(TransactionDefinition.PROPAGATION_REQUIRES_NEW, definition.getPropagationBehavior());
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, definition.getIsolationLevel());
            assertFalse(definition.isReadOnly());
            activeTransaction = (TransactionSnapshot) transaction;
            activeTransaction.members = new LinkedHashMap<>(database.members);
            activeTransaction.references = new LinkedHashMap<>(database.references);
            begins++;
        }

        @Override protected void doCommit(DefaultTransactionStatus status) {
            assertSame(activeTransaction, status.getTransaction());
            commits++;
        }

        @Override protected void doRollback(DefaultTransactionStatus status) {
            assertSame(activeTransaction, status.getTransaction());
            database.members.clear();
            database.members.putAll(activeTransaction.members);
            database.references.clear();
            database.references.putAll(activeTransaction.references);
            rollbacks++;
        }

        @Override protected void doCleanupAfterCompletion(Object transaction) {
            assertSame(activeTransaction, transaction);
            activeTransaction = null;
        }
    }
}
