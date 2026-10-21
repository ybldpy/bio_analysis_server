package com.xjtlu.bio.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataAccessResourceFailureException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.mapper.BioReferenceTaxonomyMemberMapper;

/** Exercises database-backed metadata resolution with mocks, never a live database. */
class ReferenceTaxonomyResolverTest {

    private static final Path MIGRATION_PATH = Path.of("scripts", "migrations",
            "20261020_add_bio_reference_taxonomy_member.sql");
    private static final Pattern SEED_ROW = Pattern.compile(
            "(?im)^\\s*(?:UNION\\s+ALL\\s+)?SELECT\\s+(\\d+)(?:\\s+AS\\s+tax_id)?\\s*,"
                    + "\\s*(\\d+)(?:\\s+AS\\s+member_tax_id)?\\s*,"
                    + "\\s*(\\d+)(?:\\s+AS\\s+sort_order)?\\s*$");

    private BioReferenceTaxonomyMemberMapper mapper;
    private ReferenceTaxonomyResolver resolver;

    @BeforeEach
    void setUp() {
        mapper = mock(BioReferenceTaxonomyMemberMapper.class);
        resolver = new ReferenceTaxonomyResolver(mapper);
    }

    @Test
    void constructionDoesNotPreloadOrQueryMappings() {
        verifyNoInteractions(mapper);
    }

    @ParameterizedTest
    @ValueSource(ints = {42, 2697049, 11676, 641809, 0, -1, Integer.MIN_VALUE, Integer.MAX_VALUE})
    void absentRowsKeepExactMatchFallbackForTheOriginalTarget(int taxId) {
        when(mapper.selectMemberTaxIds(taxId)).thenReturn(List.of());

        assertEquals(List.of(taxId), resolver.resolveMemberTaxIds(taxId));

        verify(mapper).selectMemberTaxIds(taxId);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void everyResolutionReadsTheLatestMappingWithoutCachingIncludingAfterDeletion() {
        when(mapper.selectMemberTaxIds(101))
                .thenReturn(List.of(202, 303), List.of(404), List.of(), List.of(505, 606));

        List<Integer> firstSnapshot = resolver.resolveMemberTaxIds(101);
        assertEquals(List.of(202, 303), firstSnapshot);
        assertEquals(List.of(404), resolver.resolveMemberTaxIds(101));
        assertEquals(List.of(101), resolver.resolveMemberTaxIds(101));
        assertEquals(List.of(505, 606), resolver.resolveMemberTaxIds(101));
        assertEquals(List.of(202, 303), firstSnapshot,
                "Later configuration changes must not change an earlier result");

        verify(mapper, times(4)).selectMemberTaxIds(101);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void configuredAndFallbackResultsAreImmutableSnapshots() {
        List<Integer> databaseRows = new ArrayList<>(List.of(303, 202));
        when(mapper.selectMemberTaxIds(101)).thenReturn(databaseRows);
        when(mapper.selectMemberTaxIds(999)).thenReturn(List.of());

        List<Integer> mapped = resolver.resolveMemberTaxIds(101);
        List<Integer> fallback = resolver.resolveMemberTaxIds(999);
        databaseRows.set(0, 404);
        databaseRows.add(505);

        assertEquals(List.of(303, 202), mapped, "Return a copy, not a view of the mapper's list");
        for (List<Integer> result : List.of(mapped, fallback)) {
            assertThrows(UnsupportedOperationException.class, () -> result.add(707));
            assertThrows(UnsupportedOperationException.class, () -> result.set(0, 707));
            assertThrows(UnsupportedOperationException.class, () -> result.remove(0));
        }
        assertEquals(List.of(999), fallback);
    }

    @Test
    void preservesExplicitOrderWithoutAddingTargetOrRecursivelyExpandingMembers() {
        when(mapper.selectMemberTaxIds(101)).thenReturn(List.of(303, 202));
        when(mapper.selectMemberTaxIds(202)).thenReturn(List.of(404));
        when(mapper.selectMemberTaxIds(303)).thenReturn(List.of(505));

        assertEquals(List.of(303, 202), resolver.resolveMemberTaxIds(101));

        verify(mapper).selectMemberTaxIds(101);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void acceptsExplicitSelfMembershipWithoutRecursing() {
        when(mapper.selectMemberTaxIds(101)).thenReturn(List.of(101, 202));
        when(mapper.selectMemberTaxIds(202)).thenReturn(List.of(101));

        assertEquals(List.of(101, 202), resolver.resolveMemberTaxIds(101));
        assertEquals(List.of(101), resolver.resolveMemberTaxIds(202));

        verify(mapper).selectMemberTaxIds(101);
        verify(mapper).selectMemberTaxIds(202);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void acceptsPositiveIntegerMemberBoundaries() {
        when(mapper.selectMemberTaxIds(101)).thenReturn(List.of(Integer.MAX_VALUE, 1));

        assertEquals(List.of(Integer.MAX_VALUE, 1), resolver.resolveMemberTaxIds(101));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidMemberRows")
    void invalidDatabaseMembersFailInsteadOfBeingFilteredOrSilentlyReplaced(
            String description, List<Integer> rows) {
        when(mapper.selectMemberTaxIds(101)).thenReturn(rows);

        assertThrows(IllegalStateException.class, () -> resolver.resolveMemberTaxIds(101), description);

        verify(mapper).selectMemberTaxIds(101);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void nullMapperResultIsNotTreatedAsAnUnconfiguredTarget() {
        when(mapper.selectMemberTaxIds(101)).thenReturn(null);

        assertThrows(IllegalStateException.class, () -> resolver.resolveMemberTaxIds(101));

        verify(mapper).selectMemberTaxIds(101);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void databaseFailurePropagatesUnchangedInsteadOfExactFallbackOrStaleCache() {
        DataAccessResourceFailureException failure =
                new DataAccessResourceFailureException("metadata database unavailable");
        when(mapper.selectMemberTaxIds(101))
                .thenReturn(List.of(202))
                .thenThrow(failure)
                .thenReturn(List.of(303));

        assertEquals(List.of(202), resolver.resolveMemberTaxIds(101));
        assertSame(failure, assertThrows(DataAccessResourceFailureException.class,
                () -> resolver.resolveMemberTaxIds(101)));
        assertEquals(List.of(303), resolver.resolveMemberTaxIds(101));

        verify(mapper, times(3)).selectMemberTaxIds(101);
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void migrationContainsSevenExplicitSeedGroupsAndEighteenRows() throws IOException {
        Map<Integer, List<Integer>> seeds = readMigrationSeeds();
        assertEquals(Set.of(11320, 11520, 11552, 2560526, 11250, 12637, 1003835), seeds.keySet());
        assertEquals(18, seeds.values().stream().mapToInt(List::size).sum());

        for (Map.Entry<Integer, List<Integer>> entry : seeds.entrySet()) {
            when(mapper.selectMemberTaxIds(entry.getKey())).thenReturn(entry.getValue());
            assertEquals(entry.getValue(), resolver.resolveMemberTaxIds(entry.getKey()));
            verify(mapper).selectMemberTaxIds(entry.getKey());
        }
        verifyNoMoreInteractions(mapper);
    }

    @Test
    void actualMigrationSeedsAndExactFallbackMatchAllSixtySevenFrontendOptions() throws Exception {
        Map<Integer, List<Integer>> seeds = readMigrationSeeds();
        JsonNode document = new ObjectMapper().readTree(
                Files.readString(Path.of("docs", "frontend", "virus-taxonomy-options.json")));
        JsonNode options = document.path("options");
        Map<Integer, List<Integer>> nonIdentityOptions = new LinkedHashMap<>();
        Set<Integer> seenTaxIds = new HashSet<>();

        assertTrue(options.isArray(), "Frontend options must remain a flat array");
        assertEquals(67, options.size(), "The consistency check must cover the full agreed dictionary");
        assertEquals(options.size(), document.path("optionCount").intValue());
        for (JsonNode option : options) {
            JsonNode taxIdNode = option.path("taxId");
            JsonNode memberNodes = option.path("memberTaxIds");
            String description = option.path("label").asText() + " (taxId=" + taxIdNode + ")";
            assertTrue(taxIdNode.isIntegralNumber() && taxIdNode.canConvertToInt()
                    && taxIdNode.intValue() > 0, "Invalid option taxId: " + description);
            assertTrue(memberNodes.isArray() && !memberNodes.isEmpty(),
                    "Missing explicit members: " + description);
            int taxId = taxIdNode.intValue();
            assertTrue(seenTaxIds.add(taxId), "Duplicate frontend taxId: " + description);
            List<Integer> expected = new ArrayList<>();
            for (JsonNode member : memberNodes) {
                assertTrue(member.isIntegralNumber() && member.canConvertToInt() && member.intValue() > 0,
                        "Invalid option member: " + description);
                expected.add(member.intValue());
            }
            if (!expected.equals(List.of(taxId))) {
                nonIdentityOptions.put(taxId, expected);
            }
            when(mapper.selectMemberTaxIds(taxId)).thenReturn(seeds.getOrDefault(taxId, List.of()));

            assertEquals(expected, resolver.resolveMemberTaxIds(taxId), description);
            verify(mapper).selectMemberTaxIds(taxId);
        }
        assertEquals(nonIdentityOptions, seeds,
                "Only the migration supplies production seed rows; do not maintain a second JSON mapping");
        verifyNoMoreInteractions(mapper);
    }

    private static Stream<Arguments> invalidMemberRows() {
        return Stream.of(
                Arguments.of("null member", Arrays.asList(202, null)),
                Arguments.of("zero member", List.of(202, 0)),
                Arguments.of("negative member", List.of(202, -1)),
                Arguments.of("minimum integer member", List.of(202, Integer.MIN_VALUE)),
                Arguments.of("duplicate member", List.of(202, 303, 202)));
    }

    /**
     * Reads only numeric rows in the checked-in migration's SELECT/UNION seed block.
     * This deliberately does not execute SQL or duplicate the production seed data.
     */
    private static Map<Integer, List<Integer>> readMigrationSeeds() throws IOException {
        String migration = Files.readString(MIGRATION_PATH);
        Matcher rows = SEED_ROW.matcher(migration);
        Map<Integer, TreeMap<Integer, Integer>> orderedSeeds = new LinkedHashMap<>();
        while (rows.find()) {
            int taxId = Integer.parseInt(rows.group(1));
            int memberTaxId = Integer.parseInt(rows.group(2));
            int sortOrder = Integer.parseInt(rows.group(3));
            assertTrue(taxId > 0 && memberTaxId > 0);
            TreeMap<Integer, Integer> members = orderedSeeds.computeIfAbsent(taxId,
                    ignored -> new TreeMap<>());
            assertFalse(members.containsKey(sortOrder), "Duplicate seed order for taxId " + taxId);
            assertFalse(members.containsValue(memberTaxId), "Duplicate seed member for taxId " + taxId);
            members.put(sortOrder, memberTaxId);
        }
        assertFalse(orderedSeeds.isEmpty(), "No seed rows parsed from " + MIGRATION_PATH);
        Map<Integer, List<Integer>> seeds = new LinkedHashMap<>();
        orderedSeeds.forEach((taxId, members) -> seeds.put(taxId, List.copyOf(members.values())));
        return seeds;
    }
}
