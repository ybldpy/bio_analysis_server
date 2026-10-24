package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

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
import com.xjtlu.bio.response.ReferenceMembershipConfirmationResult;
import com.xjtlu.bio.service.ReferenceTargetScopeService.MembershipConfirmationRequiredException;
import com.xjtlu.bio.service.ReferenceTargetScopeService.Scope;

/** Scope/lifecycle boundary tests using synthetic bytes and mocked persistence only. */
class ReferenceLibraryTargetScopeTest {
    private static final int TARGET_TAX_ID = 900;
    private static final int ACTUAL_TAX_ID = 101;
    private static final int PIPELINE_TYPE = 100;
    private static final List<Integer> MEMBERS = List.of(TARGET_TAX_ID, ACTUAL_TAX_ID, 102);
    private static final Scope SCOPE = new Scope(TARGET_TAX_ID, PIPELINE_TYPE, MEMBERS);
    private static final Scope OUTSIDE_SCOPE = new Scope(TARGET_TAX_ID, PIPELINE_TYPE, List.of(TARGET_TAX_ID, 102));
    private static final String EXISTING_KEY = "refseq/existing/reference.fna";
    private static final byte[] EXISTING_BYTES = "existing object".getBytes(StandardCharsets.UTF_8);
    private static final byte[] FASTA = ">synthetic_scope_fixture\nACGT\n".getBytes(StandardCharsets.UTF_8);

    private BioReferenceSequenceMapper mapper;
    private StorageService storage;
    private ReferenceTargetScopeService targetScopes;
    private RecordingTransactionManager transactions;
    private ReferenceLibraryService service;
    private Map<String, byte[]> objects;
    private String newObjectKey;

    @BeforeEach
    void setUp() {
        mapper = mock(BioReferenceSequenceMapper.class);
        storage = mock(StorageService.class);
        targetScopes = mock(ReferenceTargetScopeService.class);
        transactions = new RecordingTransactionManager();
        service = new ReferenceLibraryService(mapper, storage, transactions, targetScopes);
        objects = new LinkedHashMap<>();
        objects.put(EXISTING_KEY, EXISTING_BYTES);
        when(targetScopes.resolve(TARGET_TAX_ID, PIPELINE_TYPE)).thenReturn(SCOPE);
        when(storage.putObject(anyString(), any(InputStream.class), any(StorageService.PutOptions.class)))
                .thenAnswer(invocation -> {
                    newObjectKey = invocation.getArgument(0);
                    assertFalse(objects.containsKey(newObjectKey));
                    InputStream input = invocation.getArgument(1);
                    objects.put(newObjectKey, input.readAllBytes());
                    return new StorageService.PutResult(true, null);
                });
        when(storage.delete(anyString())).thenAnswer(invocation -> objects.remove(invocation.getArgument(0)) != null);
        when(storage.exists(anyString())).thenAnswer(invocation -> objects.containsKey(invocation.getArgument(0)));
        when(mapper.insertSelective(any())).thenAnswer(invocation -> {
            assertImportTransaction();
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
    void targetQueryUsesTheSameSingleAndScopeForCountAndRows() {
        ReferenceLibraryQuery query = scopedQuery();
        query.setStart(7);
        query.setPageSize(3);
        query.setTaxId(ACTUAL_TAX_ID);
        query.setOrgType(1);
        query.setEnabled(false);
        query.setAccession("  SYNTHETIC_SCOPE_001  ");
        Map<String, Object> expected = Map.of("tax_id in", MEMBERS, "tax_id =", ACTUAL_TAX_ID,
                "org_type =", 1, "enabled =", false, "accession =", "SYNTHETIC_SCOPE_001");
        when(mapper.countByExample(any())).thenAnswer(invocation -> {
            assertNull(PageHelper.getLocalPage());
            assertEquals(expected, conditions(invocation.getArgument(0)));
            return 12L;
        });
        when(mapper.selectByExample(any())).thenAnswer(invocation -> {
            BioReferenceSequenceExample example = invocation.getArgument(0);
            assertEquals(expected, conditions(example));
            assertEquals("reference_id desc", example.getOrderByClause());
            Page<?> page = PageHelper.getLocalPage();
            assertNotNull(page);
            assertEquals(7, page.getStartRow());
            assertEquals(3, page.getPageSize());
            assertFalse(page.isCount());
            return List.of(reference());
        });

        Result<ReferenceLibraryPage> result = service.query(query);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(12, result.getData().total());
        assertEquals(ACTUAL_TAX_ID, result.getData().items().get(0).taxId());
        assertFalse(result.getData().items().get(0).enabled());
        assertNull(PageHelper.getLocalPage());
        verify(targetScopes).resolve(TARGET_TAX_ID, PIPELINE_TYPE);
        verifyNoInteractions(storage);
    }

    @Test
    void targetOnlyQueryDoesNotReplaceTheMemberScopeWithAnExactTargetTaxId() {
        when(mapper.selectByExample(any())).thenReturn(List.of());

        assertEquals(Result.SUCCESS, service.query(scopedQuery()).getStatus());

        ArgumentCaptor<BioReferenceSequenceExample> examples = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(mapper).countByExample(examples.capture());
        verify(mapper).selectByExample(examples.capture());
        for (BioReferenceSequenceExample example : examples.getAllValues()) {
            assertEquals(Map.of("tax_id in", MEMBERS), conditions(example));
        }
    }

    @Test
    void exactTaxIdOutsideScopeRemainsAnIntersectionNotAnUnscopedFallback() {
        ReferenceLibraryQuery query = scopedQuery();
        query.setTaxId(777);
        when(mapper.selectByExample(any())).thenReturn(List.of());

        Result<ReferenceLibraryPage> result = service.query(query);

        assertEquals(Result.SUCCESS, result.getStatus());
        assertTrue(result.getData().items().isEmpty());
        ArgumentCaptor<BioReferenceSequenceExample> example = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(mapper).selectByExample(example.capture());
        assertEquals(Map.of("tax_id in", MEMBERS, "tax_id =", 777), conditions(example.getValue()));
    }

    @Test
    void globalQueryPreservesTheExistingExactTaxIdContractWithoutResolvingTargetScope() {
        ReferenceLibraryQuery query = new ReferenceLibraryQuery();
        query.setTaxId(ACTUAL_TAX_ID);
        query.setOrgType(2);
        when(mapper.selectByExample(any())).thenReturn(List.of());

        assertEquals(Result.SUCCESS, service.query(query).getStatus());

        ArgumentCaptor<BioReferenceSequenceExample> example = ArgumentCaptor.forClass(BioReferenceSequenceExample.class);
        verify(mapper).selectByExample(example.capture());
        assertEquals(Map.of("tax_id =", ACTUAL_TAX_ID, "org_type =", 2), conditions(example.getValue()));
        verifyNoInteractions(targetScopes, storage);
    }

    @ParameterizedTest
    @MethodSource("invalidContexts")
    void invalidOrUnknownQueryContextNeverFallsBackToTheFullLibrary(Integer target, Integer pipeline) {
        ReferenceLibraryQuery query = new ReferenceLibraryQuery();
        query.setTargetTaxId(target);
        query.setPipelineType(pipeline);
        when(targetScopes.resolve(target, pipeline)).thenThrow(new IllegalArgumentException("invalid target context"));

        assertEquals(Result.PARAMETER_NOT_VALID, service.query(query).getStatus());

        verifyNoInteractions(mapper, storage);
    }

    static Stream<Arguments> invalidContexts() {
        return Stream.of(Arguments.of(TARGET_TAX_ID, null), Arguments.of(null, PIPELINE_TYPE),
                Arguments.of(999, PIPELINE_TYPE), Arguments.of(TARGET_TAX_ID, 400));
    }

    @Test
    void scopeResolutionFailureDoesNotRunAnUnscopedReferenceQuery() {
        when(targetScopes.resolve(TARGET_TAX_ID, PIPELINE_TYPE))
                .thenThrow(new DataAccessResourceFailureException("private target SQL fixture"));

        Result<?> result = service.query(scopedQuery());

        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertFalse(result.getFailMsg().contains("private target SQL"));
        verifyNoInteractions(mapper, storage);
    }

    @Test
    void unconfirmedOutsideMemberReturnsStructuredConfirmationBeforeReadingOrWritingAnything() throws Exception {
        UploadReferenceRequest request = scopedMetadata();
        assertFalse(request.getConfirmAddTaxonomyMember());
        MockMultipartFile file = spy(syntheticFile());
        when(targetScopes.resolve(TARGET_TAX_ID, PIPELINE_TYPE)).thenReturn(OUTSIDE_SCOPE);
        doThrow(confirmationRequired()).when(targetScopes).checkMembership(OUTSIDE_SCOPE, ACTUAL_TAX_ID, false);

        Result<ReferenceLibraryItem> result = service.upload(request, file);

        assertConfirmation(result);
        verify(file, never()).getInputStream();
        verifyNoInteractions(mapper, storage);
        verify(targetScopes, never()).ensureMembership(any(), any(), anyInt(), anyBoolean());
        assertEquals(0, transactions.begins);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @Test
    void confirmedUploadRechecksMembershipAndInsertsTheActualTaxIdInTheSameTransaction() {
        UploadReferenceRequest request = scopedMetadata();
        request.setConfirmAddTaxonomyMember(true);
        when(targetScopes.resolve(TARGET_TAX_ID, PIPELINE_TYPE)).thenReturn(OUTSIDE_SCOPE);
        doAnswer(invocation -> { assertImportTransaction(); return null; })
                .when(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, true);

        Result<ReferenceLibraryItem> result = service.upload(request, syntheticFile());

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(ACTUAL_TAX_ID, result.getData().taxId());
        assertEquals(1, result.getData().orgType());
        assertFalse(result instanceof ReferenceMembershipConfirmationResult);
        ArgumentCaptor<BioReferenceSequence> inserted = ArgumentCaptor.forClass(BioReferenceSequence.class);
        InOrder order = inOrder(targetScopes, storage, mapper);
        order.verify(targetScopes).checkMembership(OUTSIDE_SCOPE, ACTUAL_TAX_ID, true);
        order.verify(storage).putObject(anyString(), any(), any());
        order.verify(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, true);
        order.verify(mapper).insertSelective(inserted.capture());
        assertEquals(ACTUAL_TAX_ID, inserted.getValue().getTaxId());
        assertEquals(1, transactions.commits);
        assertEquals(0, transactions.rollbacks);
        verify(storage, never()).delete(anyString());
        assertArrayEquals(FASTA, objects.get(newObjectKey));
    }

    @Test
    void existingMemberDoesNotNeedConfirmationButIsStillRecheckedInsideTheTransaction() {
        doAnswer(invocation -> { assertImportTransaction(); return null; })
                .when(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, false);

        Result<?> result = service.upload(scopedMetadata(), syntheticFile());

        assertEquals(Result.SUCCESS, result.getStatus());
        verify(targetScopes).checkMembership(SCOPE, ACTUAL_TAX_ID, false);
        verify(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, false);
        assertEquals(1, transactions.commits);
    }

    @Test
    void virusTargetRejectsBacterialUploadBeforeFileOrReferenceWrites() {
        UploadReferenceRequest request = scopedMetadata();
        request.setOrgType(2);
        request.setConfirmAddTaxonomyMember(true);

        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(request, syntheticFile()).getStatus());

        verifyNoInteractions(mapper, storage);
        verify(targetScopes, never()).ensureMembership(any(), any(), anyInt(), anyBoolean());
        assertEquals(0, transactions.begins);
    }

    @Test
    void globalBacterialUploadRemainsValidAndDoesNotTouchTargetMembership() {
        UploadReferenceRequest request = scopedMetadata();
        request.setTargetTaxId(null);
        request.setPipelineType(null);
        request.setOrgType(2);

        Result<ReferenceLibraryItem> result = service.upload(request, syntheticFile());

        assertEquals(Result.SUCCESS, result.getStatus());
        assertEquals(2, result.getData().orgType());
        assertEquals(ACTUAL_TAX_ID, result.getData().taxId());
        verifyNoInteractions(targetScopes);
    }

    @Test
    void confirmationWithoutTargetContextIsInvalidAndCannotAddMembership() {
        UploadReferenceRequest request = scopedMetadata();
        request.setTargetTaxId(null);
        request.setPipelineType(null);
        request.setConfirmAddTaxonomyMember(true);

        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(request, syntheticFile()).getStatus());

        verifyNoInteractions(mapper, storage);
        verify(targetScopes, never()).ensureMembership(any(), any(), anyInt(), anyBoolean());
        assertEquals(0, transactions.begins);
    }

    @Test
    void partialUploadContextFailsBeforeFileOrReferenceWrites() {
        UploadReferenceRequest request = scopedMetadata();
        request.setPipelineType(null);
        when(targetScopes.resolve(TARGET_TAX_ID, null)).thenThrow(new IllegalArgumentException("partial context"));

        assertEquals(Result.PARAMETER_NOT_VALID, service.upload(request, syntheticFile()).getStatus());

        verifyNoInteractions(mapper, storage);
        assertEquals(0, transactions.begins);
    }

    @Test
    void duplicateAccessionCannotBeAutomaticallyReassociatedWithTheConfirmedTarget() {
        UploadReferenceRequest request = scopedMetadata();
        request.setConfirmAddTaxonomyMember(true);
        when(mapper.countByExample(any())).thenReturn(1L);

        Result<?> result = service.upload(request, syntheticFile());

        assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
        verify(targetScopes, never()).ensureMembership(any(), any(), anyInt(), anyBoolean());
        verify(mapper, never()).insertSelective(any());
        verify(mapper, never()).updateByPrimaryKeySelective(any());
        verifyNoInteractions(storage);
        assertEquals(0, transactions.begins);
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    @Test
    void membershipDriftRequiringConfirmationRollsBackBeforeInsertAndRemovesOnlyTheNewFile() {
        doAnswer(invocation -> { assertImportTransaction(); throw confirmationRequired(); })
                .when(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, false);

        Result<?> result = service.upload(scopedMetadata(), syntheticFile());

        assertConfirmation(result);
        verify(mapper, never()).insertSelective(any());
        assertEquals(1, transactions.rollbacks);
        assertEquals(0, transactions.commits);
        verify(storage).delete(newObjectKey);
        verify(storage, never()).delete(EXISTING_KEY);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @Test
    void uncertainMembershipRollbackIsInternalFailureNotASafeConfirmationRetry() {
        doThrow(confirmationRequired()).when(targetScopes)
                .ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, false);
        transactions.rollbackFailure = new TransactionSystemException("private uncertain rollback fixture");

        Result<?> result = service.upload(scopedMetadata(), syntheticFile());

        assertUncertainResultRetainsObjects(result);
        verify(mapper, never()).insertSelective(any());
        assertEquals(1, transactions.rollbacks);
    }

    @Test
    void confirmedMembershipAndReferenceInsertShareTheSameRollbackOnDuplicateRace() {
        UploadReferenceRequest request = scopedMetadata();
        request.setConfirmAddTaxonomyMember(true);
        doAnswer(invocation -> { assertImportTransaction(); return null; })
                .when(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, true);
        doThrow(new DuplicateKeyException("private duplicate fixture")).when(mapper).insertSelective(any());

        Result<?> result = service.upload(request, syntheticFile());

        assertEquals(Result.DUPLICATE_OPERATION, result.getStatus());
        verify(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, true);
        assertEquals(1, transactions.begins);
        assertEquals(1, transactions.rollbacks);
        assertEquals(0, transactions.commits);
        verify(storage).delete(newObjectKey);
        assertEquals(Set.of(EXISTING_KEY), objects.keySet());
    }

    @Test
    void confirmedImportWithUnknownCommitKeepsItsFileAndReportsPendingOutcome() {
        UploadReferenceRequest request = scopedMetadata();
        request.setConfirmAddTaxonomyMember(true);
        transactions.commitFailure = new TransactionSystemException("private uncertain commit fixture");

        Result<?> result = service.upload(request, syntheticFile());

        assertUncertainResultRetainsObjects(result);
        assertEquals(1, transactions.commits);
        verify(targetScopes).ensureMembership(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID, true);
    }

    @Test
    void unknownCommitCannotBeMisreportedAsMembershipConfirmationByExceptionType() {
        transactions.commitFailure = confirmationRequired();
        transactions.rollbackFailure = new TransactionSystemException("private rollback fixture");

        Result<?> result = service.upload(scopedMetadata(), syntheticFile());

        assertUncertainResultRetainsObjects(result);
        assertEquals(1, transactions.commits);
        assertEquals(1, transactions.rollbacks);
    }

    private void assertUncertainResultRetainsObjects(Result<?> result) {
        assertEquals(Result.INTERNAL_FAIL, result.getStatus());
        assertFalse(result instanceof ReferenceMembershipConfirmationResult);
        assertNull(result.getData());
        assertTrue(result.getFailMsg().contains("待确认"));
        assertTrue(result.getFailMsg().contains("SYNTHETIC_SCOPE_001"));
        assertFalse(result.getFailMsg().contains("private"));
        verify(storage, never()).delete(anyString());
        assertArrayEquals(FASTA, objects.get(newObjectKey));
        assertArrayEquals(EXISTING_BYTES, objects.get(EXISTING_KEY));
    }

    private static void assertConfirmation(Result<?> result) {
        assertEquals(Result.BUSINESS_FAIL, result.getStatus());
        assertNull(result.getData());
        ReferenceMembershipConfirmationResult confirmation = assertInstanceOf(
                ReferenceMembershipConfirmationResult.class, result);
        assertEquals("REFERENCE_TARGET_MEMBERSHIP_CONFIRMATION_REQUIRED", confirmation.getCode());
        assertEquals(TARGET_TAX_ID, confirmation.getConfirmation().targetTaxId());
        assertEquals(PIPELINE_TYPE, confirmation.getConfirmation().pipelineType());
        assertEquals(ACTUAL_TAX_ID, confirmation.getConfirmation().taxId());
    }

    private static void assertImportTransaction() {
        assertTrue(TransactionSynchronizationManager.isSynchronizationActive());
        assertTrue(TransactionSynchronizationManager.isActualTransactionActive());
        assertFalse(TransactionSynchronizationManager.isCurrentTransactionReadOnly());
        assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED,
                TransactionSynchronizationManager.getCurrentTransactionIsolationLevel());
    }

    private static Map<String, Object> conditions(BioReferenceSequenceExample example) {
        assertEquals(1, example.getOredCriteria().size(), "target scope and optional filters must share one AND group");
        Map<String, Object> result = new LinkedHashMap<>();
        for (var condition : example.getOredCriteria().get(0).getAllCriteria()) {
            assertNull(result.put(condition.getCondition(), condition.getValue()));
        }
        return result;
    }

    private static ReferenceLibraryQuery scopedQuery() {
        ReferenceLibraryQuery query = new ReferenceLibraryQuery();
        query.setTargetTaxId(TARGET_TAX_ID);
        query.setPipelineType(PIPELINE_TYPE);
        return query;
    }

    private static UploadReferenceRequest scopedMetadata() {
        UploadReferenceRequest request = new UploadReferenceRequest();
        request.setAccession("SYNTHETIC_SCOPE_001");
        request.setTaxId(ACTUAL_TAX_ID);
        request.setOrgType(1);
        request.setTargetTaxId(TARGET_TAX_ID);
        request.setPipelineType(PIPELINE_TYPE);
        return request;
    }

    private static MembershipConfirmationRequiredException confirmationRequired() {
        return new MembershipConfirmationRequiredException(TARGET_TAX_ID, PIPELINE_TYPE, ACTUAL_TAX_ID);
    }

    private static BioReferenceSequence reference() {
        BioReferenceSequence row = new BioReferenceSequence();
        row.setReferenceId(42L);
        row.setTaxId(ACTUAL_TAX_ID);
        row.setOrgType(1);
        row.setAccession("SYNTHETIC_SCOPE_001");
        row.setEnabled(false);
        row.setPath(EXISTING_KEY);
        return row;
    }

    private static MockMultipartFile syntheticFile() {
        return new MockMultipartFile("file", "synthetic.fa", "application/octet-stream", FASTA);
    }

    /** Spring performs synchronization callbacks; this fake controls resource completion only. */
    private static final class RecordingTransactionManager extends AbstractPlatformTransactionManager {
        private int begins;
        private int commits;
        private int rollbacks;
        private RuntimeException commitFailure;
        private RuntimeException rollbackFailure;

        @Override protected Object doGetTransaction() { return new Object(); }

        @Override protected void doBegin(Object transaction, TransactionDefinition definition) {
            begins++;
            assertEquals(TransactionDefinition.PROPAGATION_REQUIRES_NEW, definition.getPropagationBehavior());
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, definition.getIsolationLevel());
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
