package com.xjtlu.bio.service;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;

/** Resolves explicit reference-taxonomy membership without changing source taxIds. */
@Service
public class ReferenceTaxonomyResolver {

    private final BioReferenceTaxonomyMemberMapper memberMapper;

    public ReferenceTaxonomyResolver(BioReferenceTaxonomyMemberMapper memberMapper) {
        this.memberMapper = Objects.requireNonNull(memberMapper, "memberMapper");
    }

    /**
     * Read for each new lookup so database edits apply to subsequent pipeline starts.
     * No recursive expansion or implicit addition of the requested target is performed.
     * Database failures must propagate, rather than silently changing reference scope.
     */
    public List<Integer> resolveMemberTaxIds(int taxId) {
        List<Integer> members = memberMapper.selectMemberTaxIds(taxId);
        if (members == null) {
            throw new IllegalStateException("Reference taxonomy lookup returned null, taxId=" + taxId);
        }
        if (members.isEmpty()) {
            return List.of(taxId);
        }

        Set<Integer> unique = new HashSet<>();
        for (Integer member : members) {
            if (member == null || member <= 0 || !unique.add(member)) {
                throw new IllegalStateException(
                        "Invalid or duplicate reference taxonomy member, taxId=" + taxId
                                + ", memberTaxId=" + member);
            }
        }
        return List.copyOf(members);
    }
}
