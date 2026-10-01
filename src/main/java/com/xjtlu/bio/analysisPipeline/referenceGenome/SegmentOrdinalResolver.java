package com.xjtlu.bio.analysisPipeline.referenceGenome;

import java.text.Normalizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resolves a biological segment position without changing the source segment name.
 *
 * <p>Only naming schemes whose order is unambiguous are handled here. Callers may
 * set {@link ReferenceSequence#setSegmentOrdinal(Integer)} before invoking this
 * resolver when an import-time or taxon-specific rule provides a better answer.</p>
 */
public final class SegmentOrdinalResolver {
    private static final String GENERIC_WRAPPER =
            "(?:SEGMENT|COMPONENT|SEG|DSRNA|SSRNA|RNA|DNA)";
    private static final Pattern PREFIXED_NUMBER_PATTERN = Pattern.compile(
            "^(?:" + GENERIC_WRAPPER + "[\\s_:\\-]*)?0*([1-9]\\d*)$");
    private static final Pattern SUFFIXED_NUMBER_PATTERN = Pattern.compile(
            "^0*([1-9]\\d*)[\\s_:\\-]+" + GENERIC_WRAPPER + "$");
    private static final Pattern GENERIC_PREFIX_PATTERN = Pattern.compile(
            "^" + GENERIC_WRAPPER + "[\\s_:\\-]*(.+)$");
    private static final Pattern GENERIC_SUFFIX_PATTERN = Pattern.compile(
            "^(.+?)[\\s_:\\-]+" + GENERIC_WRAPPER + "$");
    private static final Pattern ORDERED_SUFFIX_PATTERN = Pattern.compile(
            "^([LMS])[\\s_:\\-]*0*([1-9]\\d*)$");
    private static final Pattern UNICODE_DASH_PATTERN = Pattern.compile("[‐‑‒–—−]");
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    private static final Set<String> MISSING_SEGMENT_NAMES = Set.of(
            "UNKNOWN",
            "UNASSIGNED",
            "UNSPECIFIED",
            "NOT KNOWN",
            "NOT-KNOWN",
            "NOT_KNOWN",
            "NOT APPLICABLE",
            "NOT-APPLICABLE",
            "NOT_APPLICABLE",
            "NOT AVAILABLE",
            "NOT-AVAILABLE",
            "NOT_AVAILABLE",
            "N/A");

    private static final Map<String, Integer> INFLUENZA_AB_SEGMENTS = Map.of(
            "PB2", 1,
            "PB1", 2,
            "PA", 3,
            "HA", 4,
            "NP", 5,
            "NA", 6,
            "M", 7,
            "MP", 7,
            "NS", 8);

    private static final Map<String, Integer> INFLUENZA_CD_SEGMENTS = Map.of(
            "PB2", 1,
            "PB1", 2,
            "P3", 3,
            "HE", 4,
            "HEF", 4,
            "NP", 5,
            "M", 6,
            "NS", 7);

    private static final Map<String, Integer> THREE_SIZE_SEGMENTS = Map.of(
            "L", 1,
            "M", 2,
            "S", 3);
    private static final Map<String, Integer> TWO_SIZE_SEGMENTS = Map.of(
            "L", 1,
            "S", 2);
    private static final Map<String, Integer> TWO_LETTER_SEGMENTS = Map.of(
            "A", 1,
            "B", 2);
    private static final Map<String, Integer> ORTHOREOVIRUS_SEGMENTS = Map.of(
            "L1", 1,
            "L2", 2,
            "L3", 3,
            "M1", 4,
            "M2", 5,
            "M3", 6,
            "S1", 7,
            "S2", 8,
            "S3", 9,
            "S4", 10);
    private static final Map<String, Integer> AQUAREOVIRUS_SEGMENTS = Map.ofEntries(
            Map.entry("L1", 1),
            Map.entry("L2", 2),
            Map.entry("L3", 3),
            Map.entry("M4", 4),
            Map.entry("M5", 5),
            Map.entry("M6", 6),
            Map.entry("S7", 7),
            Map.entry("S8", 8),
            Map.entry("S9", 9),
            Map.entry("S10", 10),
            Map.entry("S11", 11));
    private static final Map<String, Integer> NANOVIRUS_EIGHT_SEGMENTS = Map.of(
            "R", 1,
            "S", 2,
            "M", 3,
            "C", 4,
            "N", 5,
            "U1", 6,
            "U2", 7,
            "U4", 8);
    private static final Map<String, Integer> BABUVIRUS_SEGMENTS = Map.of(
            "R", 1,
            "S", 2,
            "M", 3,
            "C", 4,
            "N", 5,
            "U3", 6);
    private static final List<Map<String, Integer>> EXACT_COHORT_PROFILES = List.of(
            THREE_SIZE_SEGMENTS,
            ORTHOREOVIRUS_SEGMENTS,
            AQUAREOVIRUS_SEGMENTS);

    private SegmentOrdinalResolver() {
    }

    /** Assigns every ordinal that can be resolved safely, preserving explicit values. */
    public static void assignKnownOrdinals(List<ReferenceSequence> references) {
        if (references == null) {
            return;
        }

        Map<String, List<ReferenceSequence>> cohorts = new LinkedHashMap<>();
        for (ReferenceSequence reference : references) {
            if (reference == null) {
                continue;
            }

            if (reference.getSegmentOrdinal() == null) {
                Integer ordinal = resolveKnownOrdinal(reference);
                if (ordinal != null) {
                    reference.setSegmentOrdinal(ordinal);
                }
            }

            String cohortKey = cohortKey(reference);
            if (cohortKey != null) {
                cohorts.computeIfAbsent(cohortKey, ignored -> new ArrayList<>())
                        .add(reference);
            }
        }

        for (List<ReferenceSequence> cohort : cohorts.values()) {
            propagateResolvedLabelOrdinals(cohort);
            assignTaxonSpecificProfiles(cohort);
            assignExactCohortProfile(cohort);
            assignConflictFreeSuffixOrdinals(cohort);
        }
    }

    public static Integer resolveKnownOrdinal(ReferenceSequence reference) {
        if (reference == null) {
            return null;
        }

        Integer explicitOrdinal = resolveExplicitOrdinal(reference.getSegment());
        if (explicitOrdinal != null) {
            return explicitOrdinal;
        }

        Integer explicitDnaComponent = resolveExplicitDnaComponent(reference.getSegment());
        if (explicitDnaComponent != null) {
            return explicitDnaComponent;
        }

        String normalizedName = semanticLabel(reference.getSegment());
        if (normalizedName == null) {
            return null;
        }

        if (isInfluenzaAOrB(reference)) {
            return INFLUENZA_AB_SEGMENTS.get(normalizedName);
        }

        if (isInfluenzaCOrD(reference)) {
            return INFLUENZA_CD_SEGMENTS.get(normalizedName);
        }

        if (isColoradoTickFeverVirus(reference)
                && "S1".equals(normalizedName)) {
            return 11;
        }

        if (isBombyxMoriDensovirus(reference)) {
            if ("VD1".equals(normalizedName)) {
                return 1;
            }
            if ("VD2".equals(normalizedName)) {
                return 2;
            }
        }

        return null;
    }

    /**
     * Resolves only an explicit numeric position, optionally surrounded by a
     * generic wrapper such as RNA, DNA, segment, seg, or component.
     */
    public static Integer resolveExplicitOrdinal(String rawSegmentName) {
        String normalizedName = normalizeName(rawSegmentName);
        if (normalizedName == null) {
            return null;
        }

        Matcher prefixMatcher = PREFIXED_NUMBER_PATTERN.matcher(normalizedName);
        if (prefixMatcher.matches()) {
            return parsePositiveInteger(prefixMatcher.group(1));
        }

        Matcher suffixMatcher = SUFFIXED_NUMBER_PATTERN.matcher(normalizedName);
        if (suffixMatcher.matches()) {
            return parsePositiveInteger(suffixMatcher.group(1));
        }
        return null;
    }

    public static boolean isMissingSegmentName(String rawSegmentName) {
        if (rawSegmentName == null || rawSegmentName.isBlank()) {
            return true;
        }
        return MISSING_SEGMENT_NAMES.contains(normalizeText(rawSegmentName));
    }

    private static String normalizeName(String rawSegmentName) {
        if (rawSegmentName == null || rawSegmentName.isBlank()) {
            return null;
        }
        String normalizedName = normalizeText(rawSegmentName);
        return MISSING_SEGMENT_NAMES.contains(normalizedName) ? null : normalizedName;
    }

    private static String semanticLabel(String rawSegmentName) {
        String current = normalizeName(rawSegmentName);
        if (current == null) {
            return null;
        }
        for (int i = 0; i < 4; i++) {
            Matcher prefixMatcher = GENERIC_PREFIX_PATTERN.matcher(current);
            if (prefixMatcher.matches()) {
                current = prefixMatcher.group(1).trim();
                continue;
            }
            Matcher suffixMatcher = GENERIC_SUFFIX_PATTERN.matcher(current);
            if (suffixMatcher.matches()) {
                current = suffixMatcher.group(1).trim();
                continue;
            }
            break;
        }
        return current;
    }

    private static String profileLabel(String rawSegmentName) {
        String label = semanticLabel(rawSegmentName);
        if (label == null) {
            return null;
        }
        return switch (label) {
            case "LARGE" -> "L";
            case "MEDIUM" -> "M";
            case "SMALL" -> "S";
            default -> label;
        };
    }

    private static Integer resolveExplicitDnaComponent(String rawSegmentName) {
        String normalizedName = normalizeName(rawSegmentName);
        if (normalizedName == null) {
            return null;
        }
        if (normalizedName.matches("^DNA[\\s_:\\-]*A$")) {
            return 1;
        }
        if (normalizedName.matches("^DNA[\\s_:\\-]*B$")) {
            return 2;
        }
        return null;
    }

    private static void propagateResolvedLabelOrdinals(List<ReferenceSequence> cohort) {
        Map<String, Integer> resolvedByLabel = new HashMap<>();
        Set<String> conflictingLabels = new HashSet<>();

        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() == null) {
                continue;
            }
            String label = profileLabel(reference.getSegment());
            if (label == null) {
                continue;
            }
            Integer previous = resolvedByLabel.putIfAbsent(label, reference.getSegmentOrdinal());
            if (previous != null && !previous.equals(reference.getSegmentOrdinal())) {
                conflictingLabels.add(label);
            }
        }

        if (!conflictingLabels.isEmpty()) {
            throw new IllegalArgumentException(
                    "Conflicting explicit segment ordinals for labels: " + conflictingLabels);
        }

        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() != null) {
                continue;
            }
            String label = profileLabel(reference.getSegment());
            if (label != null && !conflictingLabels.contains(label)) {
                Integer ordinal = resolvedByLabel.get(label);
                if (ordinal != null) {
                    reference.setSegmentOrdinal(ordinal);
                }
            }
        }
    }

    private static void assignTaxonSpecificProfiles(List<ReferenceSequence> cohort) {
        if (cohortHasContext(cohort, "ARENAVIRIDAE", "ARENAVIRUS")) {
            boolean isThreeSegmentGenus = cohortHasContext(
                    cohort, "ANTENNAVIRUS", "INNMOVIRUS");
            boolean isTwoSegmentGenus = cohortHasContext(
                    cohort, "HARTMANIVIRUS", "MAMMARENAVIRUS", "REPTARENAVIRUS");
            if (isThreeSegmentGenus) {
                assignProfile(cohort, THREE_SIZE_SEGMENTS);
            } else if (isTwoSegmentGenus) {
                assignProfile(cohort, TWO_SIZE_SEGMENTS);
            }
        } else if (cohortHasContext(cohort, "BUNYAVIRICETES", "BUNYAVIRUS")) {
            // L/M/S describe fixed large/medium/small positions. If M is absent
            // from an incomplete candidate set, S must remain position 3.
            assignProfile(cohort, THREE_SIZE_SEGMENTS);
        } else if (cohortHasContext(cohort, "PICOBIRNAVIRIDAE", "PICOBIRNAVIRUS")) {
            assignProfile(cohort, TWO_SIZE_SEGMENTS);
        }

        if (cohortHasContext(cohort, "ORTHOREOVIRUS")) {
            assignProfile(cohort, ORTHOREOVIRUS_SEGMENTS);
        }

        boolean isPicobirnavirus = cohortHasContext(
                cohort, "PICOBIRNAVIRIDAE", "PICOBIRNAVIRUS");
        if (!isPicobirnavirus
                && cohortHasContext(cohort, "BIRNAVIRIDAE", "BIRNAVIRUS", "GEMINIVIRIDAE")) {
            assignProfile(cohort, TWO_LETTER_SEGMENTS);
        }

        if (cohortHasContext(cohort, "NANOVIRIDAE", "NANOVIRUS", "BABUVIRUS")) {
            if (cohortHasContext(cohort, "BABUVIRUS")) {
                assignProfile(cohort, BABUVIRUS_SEGMENTS);
            } else {
                assignProfile(cohort, NANOVIRUS_EIGHT_SEGMENTS);
            }
        }

        if (cohortHasContext(cohort, "FIJIVIRUS", "ORYZAVIRUS", "AQUAREOVIRUS")) {
            assignNumberedSuffixes(cohort);
        }
    }

    private static void assignProfile(List<ReferenceSequence> cohort,
            Map<String, Integer> profile) {
        if (!isCompatibleProfile(cohort, profile)) {
            return;
        }
        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() != null) {
                continue;
            }
            Integer ordinal = profile.get(profileLabel(reference.getSegment()));
            if (ordinal != null) {
                reference.setSegmentOrdinal(ordinal);
            }
        }
    }

    private static void assignNumberedSuffixes(List<ReferenceSequence> cohort) {
        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() != null) {
                continue;
            }
            Integer ordinal = orderedSuffixOrdinal(profileLabel(reference.getSegment()));
            if (ordinal != null) {
                reference.setSegmentOrdinal(ordinal);
            }
        }
    }

    private static void assignExactCohortProfile(List<ReferenceSequence> cohort) {
        Set<String> labels = cohort.stream()
                .map(ReferenceSequence::getSegment)
                .filter(segment -> !isMissingSegmentName(segment))
                .map(SegmentOrdinalResolver::profileLabel)
                .filter(label -> label != null)
                .collect(java.util.stream.Collectors.toSet());

        for (Map<String, Integer> profile : EXACT_COHORT_PROFILES) {
            if (!labels.equals(profile.keySet()) || !isCompatibleProfile(cohort, profile)) {
                continue;
            }
            assignProfile(cohort, profile);
            return;
        }
    }

    private static boolean isCompatibleProfile(List<ReferenceSequence> cohort,
            Map<String, Integer> profile) {
        for (ReferenceSequence reference : cohort) {
            Integer expectedOrdinal = profile.get(profileLabel(reference.getSegment()));
            if (expectedOrdinal != null && reference.getSegmentOrdinal() != null
                    && !expectedOrdinal.equals(reference.getSegmentOrdinal())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Supports explicit labels such as S1..S10 or L1..L3/M4..M6/S7..S11.
     * The suffix is accepted only when every unresolved label in the cohort has
     * this form and no suffix collides with another label or resolved segment.
     */
    private static void assignConflictFreeSuffixOrdinals(List<ReferenceSequence> cohort) {
        long distinctSegmentLabels = cohort.stream()
                .map(ReferenceSequence::getSegment)
                .filter(segment -> !isMissingSegmentName(segment))
                .map(SegmentOrdinalResolver::profileLabel)
                .filter(label -> label != null)
                .distinct()
                .count();
        if (distinctSegmentLabels < 2) {
            return;
        }

        boolean hasExplicitNumericSegment = cohort.stream()
                .map(ReferenceSequence::getSegment)
                .anyMatch(segment -> resolveExplicitOrdinal(segment) != null);
        if (!hasExplicitNumericSegment) {
            return;
        }

        Map<String, Integer> proposals = new LinkedHashMap<>();
        Map<Integer, String> proposedLabelsByOrdinal = new LinkedHashMap<>();

        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() != null
                    || isMissingSegmentName(reference.getSegment())) {
                continue;
            }

            String label = profileLabel(reference.getSegment());
            Integer ordinal = orderedSuffixOrdinal(label);
            if (ordinal == null) {
                return;
            }
            proposals.put(label, ordinal);
            String previousLabel = proposedLabelsByOrdinal.putIfAbsent(ordinal, label);
            if (previousLabel != null && !previousLabel.equals(label)) {
                return;
            }
        }

        if (proposals.isEmpty()) {
            return;
        }

        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() == null) {
                continue;
            }
            String proposedLabel = proposedLabelsByOrdinal.get(reference.getSegmentOrdinal());
            if (proposedLabel != null
                    && !proposedLabel.equals(profileLabel(reference.getSegment()))) {
                return;
            }
        }

        for (ReferenceSequence reference : cohort) {
            if (reference.getSegmentOrdinal() == null) {
                Integer ordinal = proposals.get(profileLabel(reference.getSegment()));
                if (ordinal != null) {
                    reference.setSegmentOrdinal(ordinal);
                }
            }
        }
    }

    private static String cohortKey(ReferenceSequence reference) {
        if (reference.getTaxId() != null) {
            return "TAX:" + reference.getTaxId();
        }
        String organismName = normalizeName(reference.getOrganismName());
        return organismName == null ? null : "ORGANISM:" + organismName;
    }

    private static boolean cohortHasContext(List<ReferenceSequence> cohort, String... markers) {
        for (ReferenceSequence reference : cohort) {
            if (referenceHasContext(reference, markers)) {
                return true;
            }
        }
        return false;
    }

    private static boolean referenceHasContext(ReferenceSequence reference, String... markers) {
        String organism = uppercase(reference.getOrganismName());
        String metadata = uppercase(reference.getRawMetadata());
        for (String marker : markers) {
            if (organism.contains(marker) || metadata.contains(marker)) {
                return true;
            }
        }
        return false;
    }

    private static String uppercase(String value) {
        return value == null ? "" : value.toUpperCase(Locale.ROOT);
    }

    private static Integer orderedSuffixOrdinal(String label) {
        if (label == null) {
            return null;
        }
        Matcher matcher = ORDERED_SUFFIX_PATTERN.matcher(label);
        return matcher.matches() ? parsePositiveInteger(matcher.group(2)) : null;
    }

    private static String normalizeText(String value) {
        String normalized = Normalizer.normalize(value, Normalizer.Form.NFKC)
                .trim()
                .toUpperCase(Locale.ROOT);
        normalized = UNICODE_DASH_PATTERN.matcher(normalized).replaceAll("-");
        return WHITESPACE_PATTERN.matcher(normalized).replaceAll(" ");
    }

    private static boolean isInfluenzaAOrB(ReferenceSequence reference) {
        return referenceHasContext(
                reference, "INFLUENZA A VIRUS", "INFLUENZA B VIRUS");
    }

    private static boolean isInfluenzaCOrD(ReferenceSequence reference) {
        return referenceHasContext(
                reference, "INFLUENZA C VIRUS", "INFLUENZA D VIRUS");
    }

    private static boolean isColoradoTickFeverVirus(ReferenceSequence reference) {
        return referenceHasContext(reference, "COLORADO TICK FEVER VIRUS");
    }

    private static boolean isBombyxMoriDensovirus(ReferenceSequence reference) {
        return referenceHasContext(reference, "BOMBYX MORI DENSOVIRUS");
    }

    private static Integer parsePositiveInteger(String value) {
        try {
            int ordinal = Integer.parseInt(value);
            return ordinal > 0 ? ordinal : null;
        } catch (NumberFormatException ignored) {
            return null;
        }
    }
}
