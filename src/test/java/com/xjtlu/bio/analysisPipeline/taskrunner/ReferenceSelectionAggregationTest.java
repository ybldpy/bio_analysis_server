package com.xjtlu.bio.analysisPipeline.taskrunner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCompositionStatus;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.referenceGenome.SegmentOrdinalResolver;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.stageDoneHandler.ReferenceSelectionStageDoneHandler;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceSelectionStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.ReferenceGenomeFastaBuilder;
import com.xjtlu.bio.utils.JsonUtil;

import org.apache.commons.lang3.tuple.Pair;

class ReferenceSelectionAggregationTest {

    @TempDir
    Path tempDir;

    @Test
    void resolvesExplicitNumericStylesToTheSameOrdinalAndPreservesNames() {
        List<String> segmentNames = List.of(
                "1", "01", "RNA 1", "RNA1", "dsRNA_1", "DNA-1",
                "segment 1", "Seg:1", "component 1", "RNA\u00a0–\u00a001");

        for (String segmentName : segmentNames) {
            ReferenceSequence sequence = referenceSequence("REF_" + segmentName, segmentName);
            SegmentOrdinalResolver.assignKnownOrdinals(List.of(sequence));

            assertEquals(segmentName, sequence.getSegment());
            assertEquals(1, sequence.getSegmentOrdinal());
            assertEquals("SEGMENT_1", sequence.getSegmentKey());
        }

        assertNull(SegmentOrdinalResolver.resolveExplicitOrdinal("12345678901234567890"));
    }

    @Test
    void resolvesSupportedSemanticSchemesByBiologicalOrder() {
        ReferenceSequence pb2 = referenceSequence("PB2", "PB2");
        pb2.setOrganismName("Influenza A virus test isolate");
        ReferenceSequence pb1 = referenceSequence("PB1", "PB1");
        pb1.setOrganismName("Influenza A virus test isolate");
        ReferenceSequence na = referenceSequence("NA", "NA");
        na.setOrganismName("Influenza A virus test isolate");
        ReferenceSequence large = referenceSequence("L", "Large");
        large.setTaxId(2001);
        ReferenceSequence medium = referenceSequence("M", "M");
        medium.setTaxId(2001);
        ReferenceSequence small = referenceSequence("S", "RNA S");
        small.setTaxId(2001);
        ReferenceSequence arenavirusLarge = referenceSequence("ARENA_L", "L");
        arenavirusLarge.setTaxId(2002);
        arenavirusLarge.setOrganismName("Mammarenavirus test virus");
        ReferenceSequence arenavirusSmall = referenceSequence("ARENA_S", "S");
        arenavirusSmall.setTaxId(2002);
        arenavirusSmall.setOrganismName("Mammarenavirus test virus");
        ReferenceSequence componentA = referenceSequence("A", "DNA-A");
        componentA.setTaxId(2003);
        componentA.setRawMetadata("{\"family\":\"Birnaviridae\"}");
        ReferenceSequence componentB = referenceSequence("B", "B component");
        componentB.setTaxId(2003);
        componentB.setRawMetadata("{\"family\":\"Birnaviridae\"}");

        List<ReferenceSequence> references = List.of(
                pb2, pb1, na,
                large, medium, small,
                arenavirusLarge, arenavirusSmall,
                componentA, componentB);
        SegmentOrdinalResolver.assignKnownOrdinals(references);

        assertEquals(1, pb2.getSegmentOrdinal());
        assertEquals(2, pb1.getSegmentOrdinal());
        assertEquals(6, na.getSegmentOrdinal());
        assertEquals(1, large.getSegmentOrdinal());
        assertEquals(2, medium.getSegmentOrdinal());
        assertEquals(3, small.getSegmentOrdinal());
        assertEquals(1, arenavirusLarge.getSegmentOrdinal());
        assertEquals(2, arenavirusSmall.getSegmentOrdinal());
        assertEquals(1, componentA.getSegmentOrdinal());
        assertEquals(2, componentB.getSegmentOrdinal());
    }

    @Test
    void resolvesInfluenzaCDSegmentsByProteinName() {
        List<String> labels = List.of("PB2", "PB1", "P3", "HEF", "NP", "M", "NS");
        List<ReferenceSequence> references = new ArrayList<>();
        for (String label : labels) {
            ReferenceSequence sequence = referenceSequence("FLU_C_" + label, label);
            sequence.setTaxId(11552);
            sequence.setOrganismName("Influenza C virus test isolate");
            references.add(sequence);
        }

        SegmentOrdinalResolver.assignKnownOrdinals(references);

        assertEquals(
                List.of(1, 2, 3, 4, 5, 6, 7),
                references.stream().map(ReferenceSequence::getSegmentOrdinal).toList());

        ReferenceSequence heAlias = referenceSequence("FLU_D_HE", "HE");
        heAlias.setOrganismName("Influenza D virus test isolate");
        SegmentOrdinalResolver.assignKnownOrdinals(List.of(heAlias));
        assertEquals(4, heAlias.getSegmentOrdinal());
    }

    @Test
    void resolvesOrthoreovirusLengthClassProfile() {
        List<String> labels = List.of(
                "L1", "L2", "L3", "M1", "M2", "M3", "S1", "S2", "S3", "S4");
        List<ReferenceSequence> references = new ArrayList<>();
        for (String label : labels) {
            ReferenceSequence sequence = referenceSequence("REO_" + label, label);
            sequence.setTaxId(10886);
            sequence.setOrganismName("Mammalian orthoreovirus 3 Dearing");
            references.add(sequence);
        }

        SegmentOrdinalResolver.assignKnownOrdinals(references);

        assertEquals(
                List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                references.stream().map(ReferenceSequence::getSegmentOrdinal).toList());
    }

    @Test
    void resolvesAnIncompleteOrthoreovirusFromTaxonContext() {
        ReferenceSequence s1 = referenceSequence("REO_S1", "S1");
        s1.setTaxId(10886);
        s1.setRawMetadata("{\"lineage\":[{\"name\":\"Orthoreovirus\"}]}");
        ReferenceSequence s2 = referenceSequence("REO_S2", "S2");
        s2.setTaxId(10886);
        s2.setRawMetadata("{\"lineage\":[{\"name\":\"Orthoreovirus\"}]}");

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(s1, s2));

        assertEquals(7, s1.getSegmentOrdinal());
        assertEquals(8, s2.getSegmentOrdinal());
    }

    @Test
    void keepsBunyaviralSmallAtThreeWhenTheMediumCandidateIsMissing() {
        ReferenceSequence large = referenceSequence("BUNYA_L", "L");
        large.setTaxId(9100);
        large.setOrganismName("Example virus");
        large.setRawMetadata("{\"lineage\":[{\"name\":\"Bunyaviricetes\"}]}");
        ReferenceSequence small = referenceSequence("BUNYA_S", "S");
        small.setTaxId(9100);
        small.setOrganismName("Example virus");
        small.setRawMetadata("{\"lineage\":[{\"name\":\"Bunyaviricetes\"}]}");

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(large, small));

        assertEquals(1, large.getSegmentOrdinal());
        assertEquals(3, small.getSegmentOrdinal());
    }

    @Test
    void leavesArenaviralSizeLabelsUnresolvedWhenTheGenusIsUnknown() {
        ReferenceSequence large = referenceSequence("UNKNOWN_ARENA_L", "L");
        large.setTaxId(9101);
        large.setOrganismName("Unclassified virus");
        large.setRawMetadata("{\"lineage\":[{\"name\":\"Arenaviridae\"}]}");
        ReferenceSequence small = referenceSequence("UNKNOWN_ARENA_S", "S");
        small.setTaxId(9101);
        small.setOrganismName("Unclassified virus");
        small.setRawMetadata("{\"lineage\":[{\"name\":\"Arenaviridae\"}]}");

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(large, small));

        assertNull(large.getSegmentOrdinal());
        assertNull(small.getSegmentOrdinal());
    }

    @Test
    void doesNotMistakePicobirnavirusForBirnavirus() {
        ReferenceSequence a = referenceSequence("PICOBIRNA_A", "A");
        a.setTaxId(9102);
        a.setRawMetadata("{\"lineage\":[{\"name\":\"Picobirnaviridae\"}]}");
        ReferenceSequence b = referenceSequence("PICOBIRNA_B", "B");
        b.setTaxId(9102);
        b.setRawMetadata("{\"lineage\":[{\"name\":\"Picobirnaviridae\"}]}");

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(a, b));

        assertNull(a.getSegmentOrdinal());
        assertNull(b.getSegmentOrdinal());
    }

    @Test
    void resolvesNanovirusComponentsWithoutTreatingNamesAsSizes() {
        List<String> labels = List.of(
                "DNA R", "DNA S", "DNA M", "DNA C", "DNA N",
                "DNA U1", "DNA U2", "DNA U4");
        List<ReferenceSequence> references = new ArrayList<>();
        for (String label : labels) {
            ReferenceSequence sequence = referenceSequence("NANO_" + label, label);
            sequence.setTaxId(283824);
            sequence.setOrganismName("Faba bean necrotic stunt virus");
            sequence.setRawMetadata("{\"family\":\"Nanoviridae\"}");
            references.add(sequence);
        }

        SegmentOrdinalResolver.assignKnownOrdinals(references);

        assertEquals(
                List.of(1, 2, 3, 4, 5, 6, 7, 8),
                references.stream().map(ReferenceSequence::getSegmentOrdinal).toList());

        ReferenceSequence dnaMedium = referenceSequence("INCOMPLETE_M", "DNA M");
        dnaMedium.setTaxId(9001);
        ReferenceSequence dnaSmall = referenceSequence("INCOMPLETE_S", "DNA S");
        dnaSmall.setTaxId(9001);
        SegmentOrdinalResolver.assignKnownOrdinals(List.of(dnaMedium, dnaSmall));
        assertNull(dnaMedium.getSegmentOrdinal());
        assertNull(dnaSmall.getSegmentOrdinal());
    }

    @Test
    void resolvesConflictFreeNumberedSuffixesAcrossTheCandidateCohort() {
        List<String> labels = List.of(
                "S1", "S2", "S3", "S4", "5", "S6", "S7", "S8", "9", "S10");
        List<ReferenceSequence> references = new ArrayList<>();
        for (String label : labels) {
            ReferenceSequence sequence = referenceSequence("RRSV_" + label, label);
            sequence.setTaxId(42475);
            sequence.setOrganismName("Rice ragged stunt virus");
            references.add(sequence);
        }

        SegmentOrdinalResolver.assignKnownOrdinals(references);

        assertEquals(
                List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                references.stream().map(ReferenceSequence::getSegmentOrdinal).toList());
    }

    @Test
    void usesExplicitVirusProfileButRejectsTheSameAmbiguousLabelWithoutIt() {
        ReferenceSequence coloradoS1 = referenceSequence("CTFV_S1", "S1");
        coloradoS1.setTaxId(46839);
        coloradoS1.setOrganismName("Colorado tick fever virus");
        SegmentOrdinalResolver.assignKnownOrdinals(List.of(coloradoS1));
        assertEquals(11, coloradoS1.getSegmentOrdinal());

        ReferenceSequence genericOne = referenceSequence("GENERIC_1", "1");
        genericOne.setTaxId(9002);
        ReferenceSequence genericS1 = referenceSequence("GENERIC_S1", "S1");
        genericS1.setTaxId(9002);
        SegmentOrdinalResolver.assignKnownOrdinals(List.of(genericOne, genericS1));
        assertEquals(1, genericOne.getSegmentOrdinal());
        assertNull(genericS1.getSegmentOrdinal());
    }

    @Test
    void propagatesAnExplicitOrdinalToTheSameLabelInTheCohort() {
        ReferenceSequence curated = referenceSequence("CURATED_S1", "S1");
        curated.setTaxId(9003);
        curated.setSegmentOrdinal(11);
        ReferenceSequence duplicateLabel = referenceSequence("OTHER_S1", "S1");
        duplicateLabel.setTaxId(9003);

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(curated, duplicateLabel));

        assertEquals(11, curated.getSegmentOrdinal());
        assertEquals(11, duplicateLabel.getSegmentOrdinal());
    }

    @Test
    void rejectsConflictingExplicitOrdinalsForTheSameLabel() {
        ReferenceSequence first = referenceSequence("CONFLICT_A", "S1");
        first.setTaxId(9004);
        first.setSegmentOrdinal(7);
        ReferenceSequence second = referenceSequence("CONFLICT_B", "S1");
        second.setTaxId(9004);
        second.setSegmentOrdinal(11);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> SegmentOrdinalResolver.assignKnownOrdinals(List.of(first, second)));

        assertTrue(exception.getMessage().contains("S1"));
    }

    @Test
    void resolvesDirectProfilesFromRawMetadataWhenOrganismNameIsMissing() {
        ReferenceSequence hef = referenceSequence("FLU_C_HEF", "HEF");
        hef.setTaxId(9005);
        hef.setOrganismName(null);
        hef.setRawMetadata("{\"organism_name\":\"Influenza C virus\"}");

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(hef));

        assertEquals(4, hef.getSegmentOrdinal());
    }

    @Test
    void doesNotInferAcrossReferencesWithoutACohortIdentity() {
        ReferenceSequence large = referenceSequence("UNKNOWN_L", "L");
        large.setTaxId(null);
        large.setOrganismName(null);
        ReferenceSequence medium = referenceSequence("UNKNOWN_M", "M");
        medium.setTaxId(null);
        medium.setOrganismName(null);
        ReferenceSequence small = referenceSequence("UNKNOWN_S", "S");
        small.setTaxId(null);
        small.setOrganismName(null);

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(large, medium, small));

        assertNull(large.getSegmentOrdinal());
        assertNull(medium.getSegmentOrdinal());
        assertNull(small.getSegmentOrdinal());
    }

    @Test
    void resolvesBombyxDensovirusVDLabels() {
        ReferenceSequence vd1 = referenceSequence("BMDV_VD1", "VD1");
        vd1.setOrganismName("Bombyx mori densovirus 3");
        ReferenceSequence vd2 = referenceSequence("BMDV_VD2", "VD2");
        vd2.setOrganismName("Bombyx mori densovirus 3");

        SegmentOrdinalResolver.assignKnownOrdinals(List.of(vd1, vd2));

        assertEquals(1, vd1.getSegmentOrdinal());
        assertEquals(2, vd2.getSegmentOrdinal());
    }

    @Test
    void leavesAmbiguousNamesUnresolvedUntilAnExplicitOrdinalIsProvided() {
        List<String> ambiguousNames = List.of(
                "PB2", "L1", "M1", "S1", "DNA M", "RNA3a", "Seg4-1", "15.8",
                "CiV16.8");

        for (String segmentName : ambiguousNames) {
            ReferenceSequence sequence = referenceSequence("REF_" + segmentName, segmentName);
            SegmentOrdinalResolver.assignKnownOrdinals(List.of(sequence));
            assertNull(sequence.getSegmentOrdinal(), segmentName);
            assertNull(sequence.getSegmentKey(), segmentName);
        }

        ReferenceSequence explicitlyResolved = referenceSequence("CTFV_S1", "S1");
        explicitlyResolved.setSegmentOrdinal(11);
        assertEquals("S1", explicitlyResolved.getSegment());
        assertEquals("SEGMENT_11", explicitlyResolved.getSegmentKey());
    }

    @Test
    void recognizesOnlyExplicitMissingSegmentNamesAsMissing() {
        assertTrue(SegmentOrdinalResolver.isMissingSegmentName(null));
        assertTrue(SegmentOrdinalResolver.isMissingSegmentName("  "));
        assertTrue(SegmentOrdinalResolver.isMissingSegmentName("Unknown"));
        assertTrue(SegmentOrdinalResolver.isMissingSegmentName("unassigned"));
        assertTrue(SegmentOrdinalResolver.isMissingSegmentName("not_applicable"));
        assertTrue(SegmentOrdinalResolver.isMissingSegmentName("not-available"));
        assertFalse(SegmentOrdinalResolver.isMissingSegmentName("NA"));
    }

    @Test
    void rejectsCandidateSetsContainingOnlyUnresolvedSegmentNames() {
        ReferenceSequence first = referenceSequence("UNKNOWN_1", "Unknown");
        ReferenceSequence second = referenceSequence("UNKNOWN_2", "Unassigned");
        ReferenceSelectionStageInputUrls input = new ReferenceSelectionStageInputUrls();
        input.setContigsUrl("query.fasta");
        ReferenceSelectionStageParameters parameters = new ReferenceSelectionStageParameters();
        parameters.setCandidateReferences(List.of(first, second));

        String validationError = ReferenceSelectionStageExecutor.validateInput(input, parameters);

        assertTrue(validationError.contains("no resolved ordinal"));
    }

    @Test
    void rejectsAValidButUnsupportedNameInsteadOfGuessingItsOrder() {
        ReferenceSequence candidate = referenceSequence("AMBIGUOUS_S1", "S1");
        ReferenceSelectionStageInputUrls input = new ReferenceSelectionStageInputUrls();
        input.setContigsUrl("query.fasta");
        ReferenceSelectionStageParameters parameters = new ReferenceSelectionStageParameters();
        parameters.setCandidateReferences(List.of(candidate));

        String validationError = ReferenceSelectionStageExecutor.validateInput(input, parameters);

        assertTrue(validationError.contains("Unable to resolve segment ordinal"));
        assertTrue(validationError.contains("S1"));
    }

    @Test
    void derivesSegmentKeyOnlyFromOrdinalAndAcceptsLegacyNumericKeys() {
        ReferenceSequence referenceSequence = new ReferenceSequence();
        referenceSequence.setSegment("PB2");
        referenceSequence.setSegmentOrdinal(1);
        referenceSequence.setSegmentKey("SEGMENT_8");

        assertEquals("PB2", referenceSequence.getSegment());
        assertEquals("SEGMENT_1", referenceSequence.getSegmentKey());

        ReferenceSequence legacySequence = new ReferenceSequence();
        legacySequence.setSegmentKey("SEGMENT_2");
        assertEquals(2, legacySequence.getSegmentOrdinal());
        assertEquals("SEGMENT_2", legacySequence.getSegmentKey());
    }

    @Test
    void preservesExplicitOrdinalAndOriginalNameAcrossJsonRoundTrip() throws Exception {
        ReferenceSequence source = referenceSequence("FLU_PB2", "PB2");
        source.setSegmentOrdinal(1);

        ReferenceSequence restored = JsonUtil.toObject(
                JsonUtil.toJson(source), ReferenceSequence.class);

        assertEquals("PB2", restored.getSegment());
        assertEquals(1, restored.getSegmentOrdinal());
        assertEquals("SEGMENT_1", restored.getSegmentKey());
    }

    @Test
    void ordersResolvedSegmentKeysByOrdinal() {
        List<ReferenceCandidateScore> scores = new ArrayList<>(List.of(
                score("N10", "SEGMENT_10", 100.0d, 100L),
                score("N2", "SEGMENT_2", 100.0d, 100L),
                score("N1", "SEGMENT_1", 100.0d, 100L)));

        ReferenceSelectionStageExecutor.CandidateSelection selection =
                ReferenceSelectionStageExecutor.selectBestCandidatesBySegment(scores);

        assertEquals(
                List.of(
                        "SEGMENT_1",
                        "SEGMENT_2",
                        "SEGMENT_10"),
                selection.winners().stream()
                        .map(ReferenceCandidateScore::getSegmentKey)
                        .toList());
    }

    @Test
    void usesExplicitBiologicalOrderForColoradoTickFeverVirusLabels() {
        List<String> rawSegments = List.of(
                "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "12", "S1");
        List<ReferenceCandidateScore> scores = new ArrayList<>();
        for (String rawSegment : rawSegments) {
            int ordinal = "S1".equals(rawSegment) ? 11 : Integer.parseInt(rawSegment);
            scores.add(score(
                    "CTFV_" + rawSegment,
                    ReferenceSequence.segmentKeyForOrdinal(ordinal),
                    100.0d,
                    100L));
        }

        ReferenceSelectionStageExecutor.CandidateSelection selection =
                ReferenceSelectionStageExecutor.selectBestCandidatesBySegment(scores);

        assertEquals(12, selection.winners().size());
        assertEquals(
                List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
                selection.winners().stream()
                        .map(ReferenceCandidateScore::getSegmentKey)
                        .map(ReferenceSequence::segmentOrdinalFromKey)
                        .toList());
    }

    @Test
    void selectsOneWinnerPerSegmentAndBuildsReferenceGenome() {
        List<ReferenceCandidateScore> scores = new ArrayList<>(List.of(
                score("SEG1_LOSE", "SEGMENT_1", 90.0d, 90L),
                score("SEG2_LOSE", "SEGMENT_2", 50.0d, 50L),
                score("SEG1_WIN", "SEGMENT_1", 100.0d, 100L),
                score("SEG2_WIN", "SEGMENT_2", 60.0d, 60L)));

        ReferenceSelectionStageExecutor.CandidateSelection selection =
                ReferenceSelectionStageExecutor.selectBestCandidatesBySegment(scores);

        assertEquals(
                List.of("SEG1_WIN", "SEG2_WIN"),
                selection.winners().stream()
                        .map(ReferenceCandidateScore::getReferenceAccession)
                        .toList());

        Map<String, Set<String>> selectedBySegment = selection.winners().stream()
                .collect(Collectors.groupingBy(
                        ReferenceCandidateScore::getSegmentKey,
                        Collectors.mapping(
                                ReferenceCandidateScore::getReferenceAccession,
                                Collectors.toSet())));
        assertEquals(Set.of("SEG1_WIN"), selectedBySegment.get("SEGMENT_1"));
        assertEquals(Set.of("SEG2_WIN"), selectedBySegment.get("SEGMENT_2"));

        ReferenceSequence segment1 = referenceSequence("SEG1_WIN", "RNA 1");
        ReferenceSequence segment2 = referenceSequence("SEG2_WIN", "RNA 2");
        ReferenceGenome referenceGenome = ReferenceGenome.fromSelectedSequences(
                List.of(segment2, segment1));

        assertEquals(List.of(segment1, segment2), referenceGenome.getSequences());
        assertFalse(referenceGenome.isComplete());
        assertEquals(ReferenceCompositionStatus.UNKNOWN, referenceGenome.getCompositionStatus());
    }

    @Test
    void materializesAllSelectedSegmentsIntoOneFasta() throws Exception {
        ReferenceSequence segment1 = referenceSequence("SEG1_WIN", "RNA 1");
        ReferenceSequence segment2 = referenceSequence("SEG2_WIN", "RNA 2");

        Path segment1Fasta = tempDir.resolve("segment1.fasta");
        Path segment2Fasta = tempDir.resolve("segment2.fasta");
        Files.writeString(segment1Fasta, ">rna1\nAAAA", StandardCharsets.UTF_8);
        Files.writeString(segment2Fasta, ">rna2 description\nCCCC\n", StandardCharsets.UTF_8);

        Map<String, Path> pathsByAccession = new LinkedHashMap<>();
        pathsByAccession.put(segment1.getAccession(), segment1Fasta);
        pathsByAccession.put(segment2.getAccession(), segment2Fasta);

        Path materializedFasta = tempDir.resolve("reference_genome.fasta");
        ReferenceGenomeFastaBuilder.write(
                materializedFasta,
                List.of(segment1, segment2),
                pathsByAccession);

        assertEquals(
                ">rna1\nAAAA\n>rna2 description\nCCCC\n",
                Files.readString(materializedFasta, StandardCharsets.UTF_8));
    }

    @Test
    void keepsGlobalSingleWinnerBehaviorForUnsegmentedCandidates() {
        List<ReferenceCandidateScore> scores = new ArrayList<>(List.of(
                score("LOWER", null, 90.0d, 90L),
                score("WINNER", null, 100.0d, 100L)));

        ReferenceSelectionStageExecutor.CandidateSelection selection =
                ReferenceSelectionStageExecutor.selectBestCandidatesBySegment(scores);

        assertEquals(1, selection.winners().size());
        assertEquals("WINNER", selection.winners().get(0).getReferenceAccession());
    }

    @Test
    void treatsMissingSegmentMetadataAsUnassignedWhenSegmentedCandidatesExist() {
        List<ReferenceCandidateScore> scores = new ArrayList<>(List.of(
                score("MISSING_SEGMENT", null, 100.0d, 100L),
                score("SEGMENT_1_WINNER", "SEGMENT_1", 90.0d, 90L),
                score("SEGMENT_2_WINNER", "SEGMENT_2", 80.0d, 80L)));

        ReferenceSelectionStageExecutor.CandidateSelection selection =
                ReferenceSelectionStageExecutor.selectBestCandidatesBySegment(scores);

        assertEquals(
                List.of("SEGMENT_1_WINNER", "SEGMENT_2_WINNER"),
                selection.winners().stream()
                        .map(ReferenceCandidateScore::getReferenceAccession)
                        .toList());
        ReferenceCandidateScore unassigned = selection.orderedScores().stream()
                .filter(score -> "MISSING_SEGMENT".equals(score.getReferenceAccession()))
                .findFirst()
                .orElseThrow();
        assertFalse(selection.winners().contains(unassigned));
    }

    @Test
    void rejectsSegmentGroupWithoutUsableAlignment() {
        List<ReferenceCandidateScore> scores = new ArrayList<>(List.of(
                score("SEG1", "SEGMENT_1", 100.0d, 100L),
                score("SEG2", "SEGMENT_2", 0.0d, 0L)));

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> ReferenceSelectionStageExecutor.selectBestCandidatesBySegment(scores));

        assertTrue(exception.getMessage().contains("SEGMENT_2"));
    }

    @Test
    void doneHandlerReturnsDomainResultWithoutUploadingFiles() {
        ReferenceSequence segment1 = referenceSequence("SEG1_WIN", "RNA 1");
        ReferenceSequence segment2 = referenceSequence("SEG2_WIN", "RNA 2");
        ReferenceGenome referenceGenome = ReferenceGenome.fromSelectedSequences(
                List.of(segment1, segment2));

        Path comparisonReport = tempDir.resolve("reference_selection.tsv");

        ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                referenceGenome,
                List.of(),
                comparisonReport);
        StageContext stageContext = new StageContext(42L, 0, 0, 7);
        StageRunResult<ReferenceSelectionStageOutput> stageRunResult = StageRunResult.OK(
                output,
                stageContext,
                tempDir);

        Pair<Map<String, String>, ReferenceSelectionStageResult> uploadPlan =
                new TestReferenceSelectionStageDoneHandler().buildUploadPlan(stageRunResult);

        assertTrue(uploadPlan.getLeft().isEmpty());
        assertEquals(referenceGenome, uploadPlan.getRight().getSelectedReference());
        assertTrue(uploadPlan.getRight().getCandidateScores().isEmpty());
    }

    private static ReferenceCandidateScore score(
            String accession,
            String segmentKey,
            double score,
            long alignedReferenceBases) {
        return new ReferenceCandidateScore(
                accession,
                segmentKey,
                score,
                alignedReferenceBases == 0L ? 0.0d : 1.0d,
                alignedReferenceBases,
                100L,
                alignedReferenceBases,
                alignedReferenceBases,
                alignedReferenceBases == 0L ? 0 : 1,
                score);
    }

    private static ReferenceSequence referenceSequence(String accession, String segment) {
        ReferenceSequence referenceSequence = new ReferenceSequence();
        referenceSequence.setReferenceId((long) accession.hashCode());
        referenceSequence.setAccession(accession);
        referenceSequence.setTaxId(1511847);
        referenceSequence.setOrganismName("Segmented test virus");
        referenceSequence.setCompleteness("COMPLETE");
        referenceSequence.setSegment(segment);
        referenceSequence.setSegmentOrdinal(
                SegmentOrdinalResolver.resolveKnownOrdinal(referenceSequence));
        referenceSequence.setPath("references/" + accession + ".fasta");
        return referenceSequence;
    }

    private static final class TestReferenceSelectionStageDoneHandler
            extends ReferenceSelectionStageDoneHandler {

        Pair<Map<String, String>, ReferenceSelectionStageResult> buildUploadPlan(
                StageRunResult<ReferenceSelectionStageOutput> stageRunResult) {
            return buildUploadConfigAndOutputUrlMap(stageRunResult);
        }
    }
}
