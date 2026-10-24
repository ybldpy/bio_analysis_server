package com.xjtlu.bio.service;

import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS;
import static com.xjtlu.bio.analysisPipeline.Constants.PipelineType.PIPELINE_VIRUS_COVID;

import java.util.List;
import java.util.Objects;

import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.xjtlu.bio.mapper.BioPipelineTargetMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;

/** Resolves library target scopes and appends explicitly confirmed metadata associations. */
@Service
public class ReferenceTargetScopeService {

    private static final int COVID_TARGET_TAX_ID = 2697049;

    private final BioPipelineTargetMapper targetMapper;
    private final BioReferenceTaxonomyMemberMapper memberMapper;
    private final ReferenceTaxonomyResolver taxonomyResolver;

    public ReferenceTargetScopeService(BioPipelineTargetMapper targetMapper,
            BioReferenceTaxonomyMemberMapper memberMapper, ReferenceTaxonomyResolver taxonomyResolver) {
        this.targetMapper = Objects.requireNonNull(targetMapper, "targetMapper");
        this.memberMapper = Objects.requireNonNull(memberMapper, "memberMapper");
        this.taxonomyResolver = Objects.requireNonNull(taxonomyResolver, "taxonomyResolver");
    }

    /** An immutable snapshot; it is never reused in place of the locked upload-time lookup. */
    public record Scope(int targetTaxId, int pipelineType, List<Integer> memberTaxIds) {
        public Scope {
            memberTaxIds = List.copyOf(memberTaxIds);
        }
    }

    /** Null means an explicitly global request, not a failed target lookup. */
    public Scope resolve(Integer targetTaxId, Integer pipelineType) {
        validateContext(targetTaxId, pipelineType);
        if (targetTaxId == null) {
            return null;
        }
        if (targetMapper.selectByTaxIdAndPipelineType(targetTaxId, pipelineType) == null) {
            throw new IllegalArgumentException("未登记的参考库目标：targetTaxId=" + targetTaxId
                    + ", pipelineType=" + pipelineType);
        }
        return new Scope(targetTaxId, pipelineType, taxonomyResolver.resolveMemberTaxIds(targetTaxId));
    }

    /** Performs a read-only preflight; the transaction must repeat it after locking. */
    public void checkMembership(Scope scope, int referenceTaxId, boolean confirmed) {
        if (scope == null) {
            if (confirmed) {
                throw new IllegalArgumentException("确认添加成员时必须提供 targetTaxId 和 pipelineType");
            }
            return;
        }
        if (referenceTaxId <= 0) {
            throw new IllegalArgumentException("参考序列 taxId 必须大于 0");
        }
        if (!scope.memberTaxIds().contains(referenceTaxId) && !confirmed) {
            throw new MembershipConfirmationRequiredException(
                    scope.targetTaxId(), scope.pipelineType(), referenceTaxId);
        }
    }

    /**
     * Joins the reference-record transaction: locks, mappings and record must commit together.
     * A current read after obtaining the shared taxId locks requires READ_COMMITTED isolation.
     */
    public void ensureMembership(Integer targetTaxId, Integer pipelineType, int referenceTaxId,
            boolean confirmed) {
        requireWritableReadCommittedTransaction();
        validateContext(targetTaxId, pipelineType);
        if (targetTaxId == null) {
            checkMembership(null, referenceTaxId, confirmed);
            return;
        }

        List<Long> lockedIds = targetMapper.lockIdsByTaxId(targetTaxId);
        if (lockedIds == null) {
            throw new IllegalStateException("Target lock lookup returned null, taxId=" + targetTaxId);
        }
        if (lockedIds.isEmpty()) {
            throw new IllegalArgumentException("未登记的参考库目标：targetTaxId=" + targetTaxId);
        }
        Scope scope = resolve(targetTaxId, pipelineType);
        checkMembership(scope, referenceTaxId, confirmed);
        if (scope.memberTaxIds().contains(referenceTaxId)) {
            return;
        }

        // With no explicit rows the resolver's self fallback is part of the effective scope.
        // Materialize it before the first non-self addition, without inventing self membership
        // for an explicitly configured non-self scope. The duplicate-key path preserves rows.
        if (scope.memberTaxIds().equals(List.of(targetTaxId))) {
            appendMember(targetTaxId, targetTaxId);
        }
        appendMember(targetTaxId, referenceTaxId);

        // MySQL affected-row counts vary for duplicate-key no-ops; verify the actual state.
        List<Integer> members = taxonomyResolver.resolveMemberTaxIds(targetTaxId);
        if (!members.contains(referenceTaxId) || !members.containsAll(scope.memberTaxIds())) {
            throw new IllegalStateException("Reference taxonomy membership was not preserved, taxId="
                    + targetTaxId + ", memberTaxId=" + referenceTaxId);
        }
    }

    private void appendMember(int targetTaxId, int memberTaxId) {
        Long nextOrder = memberMapper.selectNextSortOrder(targetTaxId);
        if (nextOrder == null || nextOrder < 0 || nextOrder > Integer.MAX_VALUE) {
            throw new IllegalStateException("Invalid reference taxonomy sort order, taxId=" + targetTaxId);
        }
        memberMapper.insertMemberIfAbsent(targetTaxId, memberTaxId, nextOrder.intValue());
    }

    private static void validateContext(Integer targetTaxId, Integer pipelineType) {
        if (targetTaxId == null && pipelineType == null) {
            return;
        }
        if (targetTaxId == null || pipelineType == null || targetTaxId <= 0 || pipelineType <= 0) {
            throw new IllegalArgumentException("targetTaxId 和 pipelineType 必须同时提供且大于 0");
        }
        if (pipelineType != PIPELINE_VIRUS && pipelineType != PIPELINE_VIRUS_COVID) {
            throw new IllegalArgumentException("参考库目标只支持 pipelineType=100 或 101");
        }
        if (pipelineType == PIPELINE_VIRUS_COVID && targetTaxId != COVID_TARGET_TAX_ID) {
            throw new IllegalArgumentException("pipelineType=101 必须使用 targetTaxId=2697049");
        }
    }

    private static void requireWritableReadCommittedTransaction() {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || TransactionSynchronizationManager.isCurrentTransactionReadOnly()
                || !Objects.equals(TransactionSynchronizationManager.getCurrentTransactionIsolationLevel(),
                        TransactionDefinition.ISOLATION_READ_COMMITTED)) {
            throw new IllegalStateException(
                    "Reference target membership requires an active writable READ_COMMITTED transaction");
        }
    }

    /** Identifies a requested association needing user confirmation, without writing it. */
    public static class MembershipConfirmationRequiredException extends IllegalArgumentException {
        private final int targetTaxId;
        private final int pipelineType;
        private final int taxId;

        public MembershipConfirmationRequiredException(int targetTaxId, int pipelineType, int taxId) {
            super("参考序列 taxId 不在目标成员范围内，添加关联前需要确认");
            this.targetTaxId = targetTaxId;
            this.pipelineType = pipelineType;
            this.taxId = taxId;
        }

        public int getTargetTaxId() {
            return targetTaxId;
        }

        public int getPipelineType() {
            return pipelineType;
        }

        public int getTaxId() {
            return taxId;
        }
    }
}
