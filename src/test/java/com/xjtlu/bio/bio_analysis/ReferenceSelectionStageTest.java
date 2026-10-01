package com.xjtlu.bio.bio_analysis;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_PENDING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.ReferenceSelectionStageExecutor;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.ReferenceGenomeFastaBuilder;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioReferenceSequence;
import com.xjtlu.bio.mapper.BioReferenceSequenceMapper;
import com.xjtlu.bio.utils.JsonUtil;

import jakarta.annotation.Resource;

@SpringBootTest(properties = {
        "localstorageService.baseDir=/home/jcy/bioTest",
        "analysis-pipeline.stage.baseWorkDir=/home/jcy/bioTest/workDir/referenceSelectionTest",
        "analysis-pipeline.stage.baseInputDir=/home/jcy/bioTest/inputDir/referenceSelectionTest"
})
@ActiveProfiles("dev")
public class ReferenceSelectionStageTest {

    private static final Path TEST_STORAGE_ROOT = Path.of("/home/jcy/bioTest");

    private static final String QUERY_OBJECT_NAME =
            "inputDir/referenceSelectionTest/maize_rough_dwarf_virus_query.fna";

    // One complete segment 1-10 set used to build a deterministic multi-FASTA query.
    private static final List<Long> QUERY_REFERENCE_IDS = List.of(
            28732L, 28724L, 28731L, 28730L, 24469L,
            28729L, 28728L, 28727L, 28726L, 28725L);

    // Two candidates for every Maize rough dwarf virus segment.
    private static final List<Long> CANDIDATE_REFERENCE_IDS = List.of(
            28732L, 28724L, 28731L, 28730L, 24469L,
            28729L, 28728L, 28727L, 28726L, 28725L,
            36016L, 36013L, 36015L, 36014L, 36017L,
            36012L, 36021L, 36019L, 36020L, 36018L);

    @Resource
    private ReferenceSelectionStageExecutor referenceSelectionStageExecutor;

    @Resource
    private BioReferenceSequenceMapper bioReferenceSequenceMapper;

    @Test
    public void doTest() throws Exception {
        List<ReferenceSequence> candidateReferences = loadReferenceSequences(
                CANDIDATE_REFERENCE_IDS);
        writeQueryFasta(candidateReferences);

        BioPipelineStage stage = new BioPipelineStage();
        stage.setPipelineId(0L);
        stage.setStageId(0L);
        stage.setVersion(0);
        stage.setStageType(PIPELINE_STAGE_REFERENCE_SELECTION);
        stage.setStageIndex(0);
        stage.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        ReferenceSelectionStageInputUrls inputUrls = new ReferenceSelectionStageInputUrls();
        inputUrls.setContigsUrl(QUERY_OBJECT_NAME);
        stage.setInputUrl(JsonUtil.toJson(inputUrls));

        ReferenceSelectionStageParameters parameters = new ReferenceSelectionStageParameters();
        parameters.setCandidateReferences(candidateReferences);
        parameters.setThreads(2);
        stage.setParameters(JsonUtil.toJson(parameters));

        StageRunResult<ReferenceSelectionStageOutput> result =
                referenceSelectionStageExecutor.execute(stage);

        assertNotNull(result);
        assertTrue(result.isSuccess(), result.getErrorLog());

        ReferenceSelectionStageOutput output = result.getStageOutput();
        assertNotNull(output);
        assertNotNull(output.getSelectedReference());
        ReferenceGenome selectedReference = output.getSelectedReference();
        assertEquals(QUERY_REFERENCE_IDS.size(), selectedReference.getSequences().size());
        assertEquals(
                selectedReference.getSequences().size(),
                selectedReference.getSequences().stream()
                        .map(ReferenceSequence::getSegmentKey)
                        .distinct()
                        .count());
        assertEquals(
                List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
                selectedReference.getSequences().stream()
                        .map(ReferenceSequence::getSegmentOrdinal)
                        .toList());

        List<ReferenceCandidateScore> candidateScores = output.getCandidateScores();
        assertNotNull(candidateScores);
        assertEquals(CANDIDATE_REFERENCE_IDS.size(), candidateScores.size());

        for (ReferenceSequence selectedSequence : selectedReference.getSequences()) {
            ReferenceCandidateScore selectedScore = candidateScores.stream()
                    .filter(score -> selectedSequence.getAccession().equals(
                            score.getReferenceAccession()))
                    .findFirst()
                    .orElseThrow();
            double bestSegmentScore = candidateScores.stream()
                    .filter(score -> selectedSequence.getSegmentKey().equals(
                            score.getSegmentKey()))
                    .mapToDouble(ReferenceCandidateScore::getScore)
                    .max()
                    .orElseThrow();

            assertEquals(bestSegmentScore, selectedScore.getScore(), 0.0d);
            assertTrue(selectedScore.getAlignedReferenceBases() > 0L);
            assertTrue(selectedScore.getReferenceCoverage() > 0.0d);
            assertTrue(selectedScore.getSequenceIdentity() > 0.0d);
            assertTrue(selectedScore.getScore() > 0.0d);
        }

        assertNotNull(output.getComparisonReportPath());
        assertTrue(Files.isRegularFile(output.getComparisonReportPath()));
        assertTrue(Files.size(output.getComparisonReportPath()) > 0L);

        printSelectionResult(
                selectedReference,
                candidateScores,
                output.getComparisonReportPath());
    }

    private static void printSelectionResult(
            ReferenceGenome selectedReference,
            List<ReferenceCandidateScore> candidateScores,
            Path comparisonReportPath) {
        Set<String> selectedAccessions = selectedReference.getSequences().stream()
                .map(ReferenceSequence::getAccession)
                .collect(Collectors.toSet());

        System.out.println();
        System.out.println("================ Reference selection result ================");
        System.out.printf(
                Locale.ROOT,
                "%-9s %-12s %-16s %12s %12s %15s %12s%n",
                "selected",
                "segment",
                "accession",
                "identity(%)",
                "coverage(%)",
                "aligned/ref",
                "score");

        for (ReferenceCandidateScore score : candidateScores) {
            System.out.printf(
                    Locale.ROOT,
                    "%-9s %-12s %-16s %12.4f %12.4f %7d/%-7d %12.6f%n",
                    selectedAccessions.contains(score.getReferenceAccession()) ? "YES" : "NO",
                    score.getSegmentKey(),
                    score.getReferenceAccession(),
                    score.getSequenceIdentity(),
                    score.getReferenceCoverage() * 100.0d,
                    score.getAlignedReferenceBases(),
                    score.getReferenceBases(),
                    score.getScore());
        }

        System.out.println("Selected reference genome:");
        for (ReferenceSequence sequence : selectedReference.getSequences()) {
            System.out.printf(
                    Locale.ROOT,
                    "  %s -> referenceId=%d, accession=%s, path=%s%n",
                    sequence.getSegmentKey(),
                    sequence.getReferenceId(),
                    sequence.getAccession(),
                    sequence.getPath());
        }
        System.out.println("Comparison report: " + comparisonReportPath.toAbsolutePath());
        System.out.println("============================================================");
    }

    private List<ReferenceSequence> loadReferenceSequences(List<Long> referenceIds) {
        return referenceIds.stream()
                .map(this::loadReferenceSequence)
                .toList();
    }

    private ReferenceSequence loadReferenceSequence(Long referenceId) {
        BioReferenceSequence source = bioReferenceSequenceMapper.selectByPrimaryKey(referenceId);
        assertNotNull(source, "Reference sequence does not exist: " + referenceId);
        return new ReferenceSequence(
                source.getReferenceId(),
                source.getAccession(),
                source.getSourceDb(),
                source.getTaxId(),
                source.getOrganismName(),
                source.getGenomeLength(),
                source.getCompleteness(),
                source.getIsAnnotated(),
                source.getGeneCount(),
                source.getProteinCount(),
                source.getSegment(),
                source.getBioproject(),
                source.getReleaseDate(),
                source.getUpdateDate(),
                source.getOrgType(),
                source.getPath(),
                source.getAnnotationFile(),
                source.getRawMetadata());
    }

    private static void writeQueryFasta(List<ReferenceSequence> candidateReferences)
            throws Exception {
        Map<Long, ReferenceSequence> candidatesById = new LinkedHashMap<>();
        for (ReferenceSequence candidate : candidateReferences) {
            candidatesById.put(candidate.getReferenceId(), candidate);
        }

        List<ReferenceSequence> queryReferences = QUERY_REFERENCE_IDS.stream()
                .map(referenceId -> {
                    ReferenceSequence referenceSequence = candidatesById.get(referenceId);
                    assertNotNull(referenceSequence,
                            "Query reference is not a candidate: " + referenceId);
                    return referenceSequence;
                })
                .toList();

        Map<String, Path> sourcePathsByAccession = new LinkedHashMap<>();
        for (ReferenceSequence queryReference : queryReferences) {
            sourcePathsByAccession.put(
                    queryReference.getAccession(),
                    TEST_STORAGE_ROOT.resolve(queryReference.getPath()));
        }

        ReferenceGenomeFastaBuilder.write(
                TEST_STORAGE_ROOT.resolve(QUERY_OBJECT_NAME),
                queryReferences,
                sourcePathsByAccession);
    }
}
