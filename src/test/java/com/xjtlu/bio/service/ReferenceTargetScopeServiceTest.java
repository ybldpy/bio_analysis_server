package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.xjtlu.bio.entity.BioPipelineTarget;
import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;
import com.xjtlu.bio.service.ReferenceTargetScopeService.MembershipConfirmationRequiredException;
import com.xjtlu.bio.service.ReferenceTargetScopeService.Scope;

/** Synthetic metadata and transaction state only; never connects to a database. */
class ReferenceTargetScopeServiceTest {

    private static final int TARGET = 70001;
    private static final int MEMBER = 70002;
    private static final int OTHER = 70003;
    private static final int VIRUS = 100;
    private static final int COVID = 101;
    private static final int COVID_TARGET = 2697049;

    private BioPipelineTargetMapper targets;
    private BioReferenceTaxonomyMemberMapper members;
    private ReferenceTargetScopeService service;

    @BeforeEach
    void setUp() {
        targets = mock(BioPipelineTargetMapper.class);
        members = mock(BioReferenceTaxonomyMemberMapper.class);
        service = new ReferenceTargetScopeService(targets, members, new ReferenceTaxonomyResolver(members));
    }

    @AfterEach
    void clearTransactionState() {
        TransactionSynchronizationManager.clear();
    }

    @Test
    void constructionAndGlobalResolutionDoNotQueryOrWrite() {
        assertNull(service.resolve(null, null));
        assertDoesNotThrow(() -> service.checkMembership(null, MEMBER, false));
        assertThrows(IllegalArgumentException.class, () -> service.checkMembership(null, MEMBER, true));

        verifyNoInteractions(targets, members);
    }

    @ParameterizedTest
    @MethodSource("invalidContexts")
    void invalidContextNeverFallsBackToGlobalOrQueriesMetadata(Integer targetTaxId, Integer pipelineType) {
        assertThrows(IllegalArgumentException.class, () -> service.resolve(targetTaxId, pipelineType));
        beginTransaction();
        assertThrows(IllegalArgumentException.class,
                () -> service.ensureMembership(targetTaxId, pipelineType, MEMBER, true));

        verifyNoInteractions(targets, members);
    }

    @Test
    void acceptsRegisteredDisabledTargetAndPreservesExplicitMemberOrderWithoutAddingSelf() {
        BioPipelineTarget target = registeredTarget(TARGET, VIRUS);
        target.setEnabled(false);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER, MEMBER));

        Scope scope = service.resolve(TARGET, VIRUS);

        assertEquals(new Scope(TARGET, VIRUS, List.of(OTHER, MEMBER)), scope);
        verify(targets).selectByTaxIdAndPipelineType(TARGET, VIRUS);
        verify(members).selectMemberTaxIds(TARGET);
        verifyNoMoreInteractions(targets, members);
    }

    @Test
    void scopeOwnsAnImmutableSnapshotOfItsMembers() {
        List<Integer> values = new ArrayList<>(List.of(MEMBER));
        Scope scope = new Scope(TARGET, VIRUS, values);
        values.add(OTHER);

        assertEquals(List.of(MEMBER), scope.memberTaxIds());
        assertThrows(UnsupportedOperationException.class, () -> scope.memberTaxIds().add(OTHER));
    }

    @Test
    void absenceOfMappingsKeepsExistingSelfFallbackWithoutWriting() {
        registeredTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of());

        assertEquals(List.of(TARGET), service.resolve(TARGET, VIRUS).memberTaxIds());

        verify(members).selectMemberTaxIds(TARGET);
        verifyNoMoreInteractions(members);
    }

    @Test
    void unknownTargetPairFailsBeforeResolvingMembers() {
        assertThrows(IllegalArgumentException.class, () -> service.resolve(TARGET, VIRUS));

        verify(targets).selectByTaxIdAndPipelineType(TARGET, VIRUS);
        verifyNoInteractions(members);
    }

    @Test
    void acceptsCovidOnlyForItsRegisteredExactTargetWithoutRewritingIt() {
        registeredTarget(COVID_TARGET, COVID);
        when(members.selectMemberTaxIds(COVID_TARGET)).thenReturn(List.of(MEMBER));

        assertEquals(new Scope(COVID_TARGET, COVID, List.of(MEMBER)), service.resolve(COVID_TARGET, COVID));

        verify(targets).selectByTaxIdAndPipelineType(COVID_TARGET, COVID);
        verify(members).selectMemberTaxIds(COVID_TARGET);
        verifyNoMoreInteractions(targets, members);
    }

    @Test
    void targetLookupFailurePropagatesWithoutGlobalOrSelfFallback() {
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("fixture target failure");
        when(targets.selectByTaxIdAndPipelineType(TARGET, VIRUS)).thenThrow(failure);

        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> service.resolve(TARGET, VIRUS)));

        verifyNoInteractions(members);
    }

    @Test
    void memberLookupFailurePropagatesWithoutGlobalOrSelfFallback() {
        registeredTarget(TARGET, VIRUS);
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("fixture member failure");
        when(members.selectMemberTaxIds(TARGET)).thenThrow(failure);

        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> service.resolve(TARGET, VIRUS)));
    }

    @Test
    void preflightRequiresConfirmationWithMachineReadableContextAndNeverWrites() {
        Scope scope = new Scope(TARGET, VIRUS, List.of(OTHER));

        MembershipConfirmationRequiredException failure = assertThrows(
                MembershipConfirmationRequiredException.class, () -> service.checkMembership(scope, MEMBER, false));

        assertEquals(TARGET, failure.getTargetTaxId());
        assertEquals(VIRUS, failure.getPipelineType());
        assertEquals(MEMBER, failure.getTaxId());
        assertDoesNotThrow(() -> service.checkMembership(scope, MEMBER, true));
        assertDoesNotThrow(() -> service.checkMembership(scope, OTHER, false));
        assertDoesNotThrow(() -> service.checkMembership(scope, OTHER, true));
        verifyNoInteractions(targets, members);
    }

    @Test
    void rejectsInvalidActualReferenceTaxIdEvenWithConfirmation() {
        Scope scope = new Scope(TARGET, VIRUS, List.of(MEMBER));

        assertThrows(IllegalArgumentException.class, () -> service.checkMembership(scope, 0, true));
        assertThrows(IllegalArgumentException.class, () -> service.checkMembership(scope, -1, false));
        verifyNoInteractions(targets, members);
    }

    @Test
    void synchronizationAloneDoesNotSatisfyTheActualTransactionGuard() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(
                TransactionDefinition.ISOLATION_READ_COMMITTED);

        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));
        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(null, null, MEMBER, false));

        verifyNoInteractions(targets, members);
    }

    @ParameterizedTest
    @ValueSource(ints = {TransactionDefinition.ISOLATION_DEFAULT, TransactionDefinition.ISOLATION_READ_UNCOMMITTED,
            TransactionDefinition.ISOLATION_REPEATABLE_READ, TransactionDefinition.ISOLATION_SERIALIZABLE})
    void requiresExplicitReadCommittedIsolationForThePostLockRead(int isolation) {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(isolation);

        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verifyNoInteractions(targets, members);
    }

    @Test
    void rejectsReadOnlyTransactionAndMissingIsolationMetadata() {
        beginTransaction();
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(true);
        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(null);
        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verifyNoInteractions(targets, members);
    }

    @Test
    void globalTransactionWritesNoAssociationAndStillRejectsConfirmationWithoutContext() {
        beginTransaction();

        assertDoesNotThrow(() -> service.ensureMembership(null, null, MEMBER, false));
        assertThrows(IllegalArgumentException.class,
                () -> service.ensureMembership(null, null, MEMBER, true));

        verifyNoInteractions(targets, members);
    }

    @Test
    void missingTargetLockDoesNotProceedWithUnlockedAssociation() {
        beginTransaction();
        when(targets.lockIdsByTaxId(TARGET)).thenReturn(List.of());

        assertThrows(IllegalArgumentException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verify(targets).lockIdsByTaxId(TARGET);
        verifyNoMoreInteractions(targets);
        verifyNoInteractions(members);
    }

    @Test
    void nullLockResultFailsRatherThanTreatingItAsAnUnconfiguredTarget() {
        beginTransaction();
        when(targets.lockIdsByTaxId(TARGET)).thenReturn(null);

        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verifyNoInteractions(members);
    }

    @Test
    void anotherPipelineTargetWithTheSameTaxIdDoesNotAuthorizeAnUnregisteredPair() {
        beginTransaction();
        when(targets.lockIdsByTaxId(TARGET)).thenReturn(List.of(2L));

        assertThrows(IllegalArgumentException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        InOrder order = inOrder(targets);
        order.verify(targets).lockIdsByTaxId(TARGET);
        order.verify(targets).selectByTaxIdAndPipelineType(TARGET, VIRUS);
        verifyNoInteractions(members);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void existingExplicitMemberNeverWritesRegardlessOfConfirmation(boolean confirmed) {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER, MEMBER));

        service.ensureMembership(TARGET, VIRUS, MEMBER, confirmed);

        assertLockThenResolve(TARGET, VIRUS);
        verifyNoMoreInteractions(targets, members);
    }

    @Test
    void uploadingImplicitSelfNeverMaterializesAMapping() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of());

        service.ensureMembership(TARGET, VIRUS, TARGET, true);

        verify(members).selectMemberTaxIds(TARGET);
        verifyNoMoreInteractions(members);
    }

    @Test
    void unconfirmedAbsentMemberIsRejectedOnlyAfterLockedFreshResolutionWithoutWriting() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER));

        assertThrows(MembershipConfirmationRequiredException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, false));

        assertLockThenResolve(TARGET, VIRUS);
        verifyNoMoreInteractions(targets, members);
    }

    @Test
    void confirmedFirstAdditionMaterializesSelfBeforeNewMemberAndVerifiesBoth() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(), List.of(TARGET, MEMBER));
        when(members.selectNextSortOrder(TARGET)).thenReturn(0L, 1L);

        service.ensureMembership(TARGET, VIRUS, MEMBER, true);

        InOrder order = inOrder(targets, members);
        order.verify(targets).lockIdsByTaxId(TARGET);
        order.verify(targets).selectByTaxIdAndPipelineType(TARGET, VIRUS);
        order.verify(members).selectMemberTaxIds(TARGET);
        order.verify(members).selectNextSortOrder(TARGET);
        order.verify(members).insertMemberIfAbsent(TARGET, TARGET, 0);
        order.verify(members).selectNextSortOrder(TARGET);
        order.verify(members).insertMemberIfAbsent(TARGET, MEMBER, 1);
        order.verify(members).selectMemberTaxIds(TARGET);
        order.verifyNoMoreInteractions();
    }

    @Test
    void explicitSelfOnlyScopeAllowsIdempotentSelfInsertWithoutRelyingOnAffectedRowCount() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(TARGET), List.of(TARGET, MEMBER));
        when(members.selectNextSortOrder(TARGET)).thenReturn(9L);
        when(members.insertMemberIfAbsent(TARGET, TARGET, 9)).thenReturn(0);
        when(members.insertMemberIfAbsent(TARGET, MEMBER, 9)).thenReturn(0);

        assertDoesNotThrow(() -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verify(members).insertMemberIfAbsent(TARGET, TARGET, 9);
        verify(members).insertMemberIfAbsent(TARGET, MEMBER, 9);
        verify(members, times(2)).selectMemberTaxIds(TARGET);
    }

    @Test
    void explicitNonSelfScopeAddsOnlyConfirmedMemberAndNeverInventsSelf() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER), List.of(OTHER, MEMBER));
        when(members.selectNextSortOrder(TARGET)).thenReturn(6L);

        service.ensureMembership(TARGET, VIRUS, MEMBER, true);

        verify(members).insertMemberIfAbsent(TARGET, MEMBER, 6);
        verify(members, never()).insertMemberIfAbsent(TARGET, TARGET, 6);
        verify(members, times(2)).selectMemberTaxIds(TARGET);
        verify(members).selectNextSortOrder(TARGET);
        verifyNoMoreInteractions(members);
    }

    @Test
    void postLockResolutionSeesMemberAddedAfterPreflightAndDoesNotWriteItAgain() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER), List.of(OTHER, MEMBER));
        Scope earlierScope = service.resolve(TARGET, VIRUS);
        assertThrows(MembershipConfirmationRequiredException.class,
                () -> service.checkMembership(earlierScope, MEMBER, false));

        service.ensureMembership(TARGET, VIRUS, MEMBER, true);

        verify(members, times(2)).selectMemberTaxIds(TARGET);
        verify(members, never()).insertMemberIfAbsent(anyInt(), anyInt(), anyInt());
        verify(members, never()).selectNextSortOrder(anyInt());
    }

    @Test
    void postLockResolutionDoesNotTrustAMemberRemovedAfterPreflight() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(MEMBER), List.of(OTHER));
        service.checkMembership(service.resolve(TARGET, VIRUS), MEMBER, false);

        assertThrows(MembershipConfirmationRequiredException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, false));

        verify(members, times(2)).selectMemberTaxIds(TARGET);
        verify(members, never()).insertMemberIfAbsent(anyInt(), anyInt(), anyInt());
    }

    @Test
    void twoPipelineTypesSharingTaxIdUseTheSameTaxOnlyLockBeforePairResolution() {
        lockedTarget(COVID_TARGET, VIRUS);
        registeredTarget(COVID_TARGET, COVID);
        when(targets.lockIdsByTaxId(COVID_TARGET)).thenReturn(List.of(2L, 9L));
        when(members.selectMemberTaxIds(COVID_TARGET)).thenReturn(List.of(MEMBER));

        service.ensureMembership(COVID_TARGET, VIRUS, MEMBER, false);
        service.ensureMembership(COVID_TARGET, COVID, MEMBER, true);

        InOrder order = inOrder(targets, members);
        order.verify(targets).lockIdsByTaxId(COVID_TARGET);
        order.verify(targets).selectByTaxIdAndPipelineType(COVID_TARGET, VIRUS);
        order.verify(members).selectMemberTaxIds(COVID_TARGET);
        order.verify(targets).lockIdsByTaxId(COVID_TARGET);
        order.verify(targets).selectByTaxIdAndPipelineType(COVID_TARGET, COVID);
        order.verify(members).selectMemberTaxIds(COVID_TARGET);
        order.verifyNoMoreInteractions();
    }

    @Test
    void writeFailurePropagatesWithoutUsingFallbackOrVerifyingAnUncommittedSuccess() {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER));
        when(members.selectNextSortOrder(TARGET)).thenReturn(1L);
        DataAccessResourceFailureException failure = new DataAccessResourceFailureException("fixture insert failure");
        when(members.insertMemberIfAbsent(TARGET, MEMBER, 1)).thenThrow(failure);

        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true)));

        verify(members).selectMemberTaxIds(TARGET);
    }

    @ParameterizedTest
    @MethodSource("missingPostWriteMembers")
    void postWriteVerificationRejectsMissingNewMemberOrLostPriorSelfScope(List<Integer> actualMembers) {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(), actualMembers);
        when(members.selectNextSortOrder(TARGET)).thenReturn(0L, 1L);

        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verify(members, times(2)).selectMemberTaxIds(TARGET);
    }

    @ParameterizedTest
    @MethodSource("invalidSortOrders")
    void invalidOrOverflowedAppendOrderFailsBeforeAnyWrite(Long nextOrder) {
        lockedTarget(TARGET, VIRUS);
        when(members.selectMemberTaxIds(TARGET)).thenReturn(List.of(OTHER));
        when(members.selectNextSortOrder(TARGET)).thenReturn(nextOrder);

        assertThrows(IllegalStateException.class,
                () -> service.ensureMembership(TARGET, VIRUS, MEMBER, true));

        verify(members, never()).insertMemberIfAbsent(anyInt(), anyInt(), anyInt());
    }

    private BioPipelineTarget registeredTarget(int targetTaxId, int pipelineType) {
        BioPipelineTarget target = new BioPipelineTarget();
        target.setId(2L);
        target.setTaxId(targetTaxId);
        target.setPipelineType(pipelineType);
        target.setEnabled(true);
        when(targets.selectByTaxIdAndPipelineType(targetTaxId, pipelineType)).thenReturn(target);
        return target;
    }

    private void lockedTarget(int targetTaxId, int pipelineType) {
        beginTransaction();
        registeredTarget(targetTaxId, pipelineType);
        when(targets.lockIdsByTaxId(targetTaxId)).thenReturn(List.of(2L));
    }

    private void assertLockThenResolve(int targetTaxId, int pipelineType) {
        InOrder order = inOrder(targets, members);
        order.verify(targets).lockIdsByTaxId(targetTaxId);
        order.verify(targets).selectByTaxIdAndPipelineType(targetTaxId, pipelineType);
        order.verify(members).selectMemberTaxIds(targetTaxId);
    }

    private static void beginTransaction() {
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.setCurrentTransactionIsolationLevel(
                TransactionDefinition.ISOLATION_READ_COMMITTED);
    }

    private static Stream<Arguments> invalidContexts() {
        return Stream.of(
                Arguments.of(null, VIRUS), Arguments.of(TARGET, null),
                Arguments.of(0, VIRUS), Arguments.of(-1, VIRUS),
                Arguments.of(TARGET, 0), Arguments.of(TARGET, -1),
                Arguments.of(TARGET, 1), Arguments.of(TARGET, 102),
                Arguments.of(TARGET, COVID));
    }

    private static Stream<Arguments> missingPostWriteMembers() {
        return Stream.of(Arguments.of(List.of(TARGET)), Arguments.of(List.of(MEMBER)));
    }

    private static Stream<Arguments> invalidSortOrders() {
        return Stream.of(Arguments.of((Long) null), Arguments.of(-1L),
                Arguments.of((long) Integer.MAX_VALUE + 1));
    }
}
