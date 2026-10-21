package com.xjtlu.bio.analysisPipeline.taskrunner;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_REFERENCE_SELECTION;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.Constants.SequenceInput;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore.EvidenceSource;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.referenceGenome.SegmentOrdinalResolver;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceSelectionReadFallbackMetrics.Status;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser.AlignmentStats;
import com.xjtlu.bio.utils.JsonUtil;

@Component
public class ReferenceSelectionStageExecutor extends
        AbstractPipelineStageExector<ReferenceSelectionStageOutput, ReferenceSelectionStageInputUrls, ReferenceSelectionStageParameters> {

    private static final String MINIMAP2_PRESET = "asm20";
    private static final String SELECTION_REPORT_FILE_NAME = "reference_selection.tsv";
    private static final String WHOLE_GENOME_GROUP_KEY = "__WHOLE_GENOME__";

    static record CandidateSelection(
            List<ReferenceCandidateScore> orderedScores,
            List<ReferenceCandidateScore> winners,
            List<String> unmatchedSegmentKeys) {
    }

    private static class CandidateFile {
        private final int index;
        private final ReferenceSequence referenceSequence;
        private Path localPath;

        CandidateFile(int index, ReferenceSequence referenceSequence, Path localPath) {
            this.index = index;
            this.referenceSequence = referenceSequence;
            this.localPath = localPath;
        }
    }

    @Override
    protected Class<ReferenceSelectionStageInputUrls> stageInputType() {
        return ReferenceSelectionStageInputUrls.class;
    }

    @Override
    protected Class<ReferenceSelectionStageParameters> stageParameterType() {
        return ReferenceSelectionStageParameters.class;
    }

    @Override
    protected StageRunResult<ReferenceSelectionStageOutput> _execute(StageExecutionInput stageExecutionInput)
            throws LoadFailException {

        ReferenceSelectionStageInputUrls input = stageExecutionInput.input;
        ReferenceSelectionStageParameters parameters = stageExecutionInput.stageParameters;

        if (parameters != null) {
            SegmentOrdinalResolver.assignKnownOrdinals(parameters.getCandidateReferences());
        }

        String validationError = validateInput(input, parameters);
        if (validationError != null) {
            return runFail(stageExecutionInput.stageContext, validationError, stageExecutionInput.workDir);
        }

        Path referencesDir = stageExecutionInput.inputDir.resolve("references");
        try {
            Files.createDirectories(referencesDir);
        } catch (IOException e) {
            return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                    "Failed to create candidate reference directory", e);
        }

        String contigsUrl = input.getContigsUrl();
        Path contigsPath = stageExecutionInput.inputDir.resolve("query_" + safeFileName(contigsUrl));
        Map<String, Path> loadMap = new LinkedHashMap<>();
        loadMap.put(contigsUrl, contigsPath);

        List<CandidateFile> candidateFiles = new ArrayList<>();
        Map<String, ReferenceSequence> candidatesByAccession = new LinkedHashMap<>();
        Set<String> candidateObjectNames = new HashSet<>();
        int candidateIndex = 0;
        for (ReferenceSequence referenceSequence : parameters.getCandidateReferences()) {
            String objectName = referenceSequence.getPath();
            candidatesByAccession.put(referenceSequence.getAccession(), referenceSequence);
            if (!candidateObjectNames.add(objectName)) {
                return runFail(stageExecutionInput.stageContext,
                        "Candidate reference path is duplicated: " + objectName,
                        stageExecutionInput.workDir);
            }

            int currentCandidateIndex = candidateIndex++;
            Path localPath = referencesDir.resolve(String.format(
                    Locale.ROOT,
                    "%04d_%s",
                    currentCandidateIndex,
                    safeFileName(objectName)));
            candidateFiles.add(new CandidateFile(
                    currentCandidateIndex, referenceSequence, localPath));
            loadMap.put(objectName, localPath);
        }

        loadInput(loadMap);

        try {
            contigsPath = uncompressIfCompressedFormat(contigsPath);
            for (CandidateFile candidateFile : candidateFiles) {
                candidateFile.localPath = uncompressIfCompressedFormat(candidateFile.localPath);
            }
        } catch (IOException e) {
            return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                    "Failed to uncompress reference-selection input", e);
        }

        Path candidateAlignmentsDir = stageExecutionInput.workDir.resolve("candidate_alignments");
        try {
            Files.createDirectories(candidateAlignmentsDir);
        } catch (IOException e) {
            return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                    "Failed to create candidate alignment directory", e);
        }

        List<ReferenceCandidateScore> candidateScores = new ArrayList<>(candidateFiles.size());
        for (CandidateFile candidateFile : candidateFiles) {
            Path alignmentPafPath = candidateAlignmentsDir.resolve(String.format(
                    Locale.ROOT, "%04d.paf", candidateFile.index));

            List<String> command = analysisPipelineToolsConfig.getMinimap2();
            command.add("-x");
            command.add(MINIMAP2_PRESET);
            command.add("-c");
            command.add("--secondary=no");
            command.add("-t");
            command.add(String.valueOf(Math.max(1, parameters.getThreads())));
            command.add("-o");
            command.add(alignmentPafPath.toAbsolutePath().toString());
            command.add(candidateFile.localPath.toAbsolutePath().toString());
            command.add(contigsPath.toAbsolutePath().toString());

            ExecuteResult executeResult = _execute(command, stageExecutionInput.workDir);
            if (!executeResult.success()) {
                String message = String.format(
                        "Reference selection alignment failed for candidate %s. "
                                + "exitCode=%d, command=%s",
                        candidateFile.referenceSequence.getAccession(),
                        executeResult.runCode,
                        String.join(" ", command));
                return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                        message, executeResult.ex);
            }

            try {
                AlignmentStats stats = PafAlignmentStatsParser.parse(
                        alignmentPafPath, candidateFile.localPath);
                candidateScores.add(candidateScore(candidateFile.referenceSequence, stats));
            } catch (IOException e) {
                return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                        "Failed to parse minimap2 PAF for candidate "
                                + candidateFile.referenceSequence.getAccession(),
                        e);
            }
        }

        CandidateSelection selection;
        ReferenceSelectionReadFallbackMetrics readFallbackMetrics;
        try {
            readFallbackMetrics = evaluateReadFallback(stageExecutionInput, candidateFiles, candidateScores);
            selection = orderCandidatesBySegment(candidateScores);
        } catch (IllegalArgumentException e) {
            return runFail(stageExecutionInput.stageContext, e.getMessage(), stageExecutionInput.workDir);
        } catch (IOException e) {
            return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                    "Reference selection read fallback failed: " + e.getMessage(), e);
        }
        candidateScores = new ArrayList<>(selection.orderedScores());

        List<ReferenceSequence> selectedSequences = new ArrayList<>(selection.winners().size());
        Set<String> selectedAccessions = new HashSet<>(selection.winners().size());
        for (ReferenceCandidateScore winner : selection.winners()) {
            ReferenceSequence selectedSequence = candidatesByAccession.get(
                    winner.getReferenceAccession());
            if (selectedSequence == null) {
                return runFail(stageExecutionInput.stageContext,
                        "Selected candidate does not exist: " + winner.getReferenceAccession(),
                        stageExecutionInput.workDir);
            }
            selectedSequences.add(selectedSequence);
            selectedAccessions.add(winner.getReferenceAccession());
        }

        Path selectionReportPath = stageExecutionInput.workDir.resolve(SELECTION_REPORT_FILE_NAME);
        try {
            writeSelectionReport(
                    selectionReportPath,
                    candidateScores,
                    candidatesByAccession,
                    selectedAccessions);
            Files.writeString(stageExecutionInput.workDir.resolve("reference_selection_read_fallback.json"),
                    JsonUtil.toJson(readFallbackMetrics), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                    "Failed to write reference-selection report", e);
        }

        if (selectedSequences.isEmpty()) {
            return runFail(stageExecutionInput.stageContext,
                    "Minimap2 did not find a usable match for any candidate reference group. "
                            + "readFallbackStatus=" + readFallbackMetrics.status()
                            + ", report=" + selectionReportPath,
                    stageExecutionInput.workDir);
        }
        ReferenceGenome selectedGenome;
        try {
            selectedGenome = ReferenceGenome.fromSelectedSequences(selectedSequences);
        } catch (IllegalArgumentException e) {
            return runFail(stageExecutionInput.stageContext, e.getMessage(), stageExecutionInput.workDir);
        }

        ReferenceSelectionStageOutput output = new ReferenceSelectionStageOutput(
                selectedGenome,
                candidateScores,
                selectionReportPath,
                selection.unmatchedSegmentKeys());
        output.setReadFallbackMetrics(readFallbackMetrics);
        return OK(output, stageExecutionInput);
    }

    private ReferenceSelectionReadFallbackMetrics evaluateReadFallback(
            StageExecutionInput executionInput,
            List<CandidateFile> candidateFiles,
            List<ReferenceCandidateScore> scores) throws IOException, LoadFailException {
        ReferenceSelectionStageInputUrls input = executionInput.input;
        ReferenceSelectionStageParameters parameters = executionInput.stageParameters;
        if (!parameters.isReadFallbackEnabled()) {
            return fallbackMetrics(Status.DISABLED, null, parameters, 0, 0);
        }
        if (!hasText(input.getR1Url())) {
            return fallbackMetrics(Status.NO_READS, null, parameters, 0, 0);
        }

        CandidateSelection contigSelection = orderCandidatesBySegment(scores);
        Set<String> matchedGroups = new HashSet<>();
        contigSelection.winners().forEach(score -> matchedGroups.add(groupKey(score.getSegmentKey())));
        boolean segmented = scores.stream().anyMatch(score -> hasText(score.getSegmentKey()));
        List<CandidateFile> unmatchedCandidates = candidateFiles.stream()
                .filter(file -> !segmented || hasText(file.referenceSequence.getSegmentKey()))
                .filter(file -> !matchedGroups.contains(groupKey(file.referenceSequence.getSegmentKey())))
                .toList();
        if (unmatchedCandidates.isEmpty()) {
            return fallbackMetrics(Status.NOT_NEEDED, null, parameters, 0, 0);
        }

        String preset = readFallbackPreset(parameters);
        if (preset == null) {
            logger.warn("Read fallback skipped: long-read technology requires an explicit readFallbackPreset");
            return fallbackMetrics(Status.UNSUPPORTED_READ_TYPE, null, parameters, 0, 0);
        }
        logger.info("Read fallback uses configurable provisional acceptance thresholds, pending dataset validation: "
                        + "referenceCoverage={}, sequenceIdentityPercent={}, minimumMappingQuality={}",
                parameters.getMinimumReadReferenceCoverage(), parameters.getMinimumReadSequenceIdentity(),
                parameters.getMinimumReadMappingQuality());

        Map<String, Path> readLoadMap = new LinkedHashMap<>();
        Path r1Path = executionInput.inputDir.resolve("reads_r1_" + safeFileName(input.getR1Url()));
        readLoadMap.put(input.getR1Url(), r1Path);
        Path r2Path = null;
        if (hasText(input.getR2Url())) {
            r2Path = executionInput.inputDir.resolve("reads_r2_" + safeFileName(input.getR2Url()));
            readLoadMap.put(input.getR2Url(), r2Path);
        }
        loadInput(readLoadMap);
        r1Path = uncompressIfCompressedFormat(r1Path);
        if (r2Path != null) {
            r2Path = uncompressIfCompressedFormat(r2Path);
        }

        Path alignmentsDir = executionInput.workDir.resolve("read_candidate_alignments");
        Files.createDirectories(alignmentsDir);
        Map<String, Integer> scoreIndexes = new LinkedHashMap<>();
        for (int i = 0; i < scores.size(); i++) {
            scoreIndexes.put(scores.get(i).getReferenceAccession(), i);
        }
        Set<String> acceptedGroups = new HashSet<>();
        for (CandidateFile file : unmatchedCandidates) {
            Path pafPath = alignmentsDir.resolve(String.format(Locale.ROOT, "%04d.paf", file.index));
            List<String> command = buildReadFallbackCommand(
                    analysisPipelineToolsConfig.getMinimap2(), preset, parameters.getThreads(),
                    file.localPath, r1Path, r2Path, pafPath);
            ExecuteResult execution = _execute(command, executionInput.workDir);
            if (!execution.success()) {
                throw new IOException("Read alignment failed for " + file.referenceSequence.getAccession()
                        + ", exitCode=" + execution.runCode, execution.ex);
            }
            AlignmentStats stats = PafAlignmentStatsParser.parse(
                    pafPath, file.localPath, parameters.getMinimumReadMappingQuality());
            ReferenceCandidateScore score = candidateScore(file.referenceSequence, stats);
            score.setEvidenceSource(EvidenceSource.READS);
            score.setRejectionReason(readRejectionReason(stats, parameters));
            scores.set(scoreIndexes.get(score.getReferenceAccession()), score);
            if (score.getRejectionReason() == null) {
                acceptedGroups.add(groupKey(score.getSegmentKey()));
            }
        }
        return fallbackMetrics(Status.COMPLETED, preset, parameters,
                unmatchedCandidates.size(), acceptedGroups.size());
    }

    private static ReferenceSelectionReadFallbackMetrics fallbackMetrics(Status status, String preset,
            ReferenceSelectionStageParameters parameters, int evaluatedCandidates, int acceptedGroups) {
        return new ReferenceSelectionReadFallbackMetrics(status, preset,
                parameters.getMinimumReadReferenceCoverage(), parameters.getMinimumReadSequenceIdentity(),
                parameters.getMinimumReadMappingQuality(), evaluatedCandidates, acceptedGroups);
    }

    static List<String> buildReadFallbackCommand(List<String> executable, String preset, int threads,
            Path reference, Path r1, Path r2, Path paf) {
        List<String> command = new ArrayList<>(executable);
        command.addAll(List.of("-x", preset, "-c", "--secondary=no", "-t",
                String.valueOf(Math.max(1, threads)), "-o", paf.toAbsolutePath().toString(),
                reference.toAbsolutePath().toString(), r1.toAbsolutePath().toString()));
        if (r2 != null) {
            command.add(r2.toAbsolutePath().toString());
        }
        return command;
    }

    private static ReferenceCandidateScore candidateScore(ReferenceSequence reference, AlignmentStats stats) {
        return new ReferenceCandidateScore(reference.getAccession(), reference.getSegmentKey(),
                stats.sequenceIdentity(), stats.referenceCoverage(), stats.alignedReferenceBases(),
                stats.referenceBases(), stats.matchingBases(), stats.alignmentBlockBases(), stats.alignmentCount(),
                stats.sequenceIdentity() * stats.referenceCoverage());
    }

    private static String readRejectionReason(AlignmentStats stats, ReferenceSelectionStageParameters parameters) {
        if (stats.alignedReferenceBases() == 0L) {
            return "NO_ALIGNMENT_PASSING_MAPPING_QUALITY";
        }
        List<String> reasons = new ArrayList<>();
        if (stats.referenceCoverage() < parameters.getMinimumReadReferenceCoverage()) {
            reasons.add("REFERENCE_COVERAGE_BELOW_THRESHOLD");
        }
        if (stats.sequenceIdentity() < parameters.getMinimumReadSequenceIdentity()) {
            reasons.add("SEQUENCE_IDENTITY_BELOW_THRESHOLD");
        }
        return reasons.isEmpty() ? null : String.join(";", reasons);
    }

    private static String readFallbackPreset(ReferenceSelectionStageParameters parameters) {
        if (hasText(parameters.getReadFallbackPreset())) {
            return parameters.getReadFallbackPreset();
        }
        return parameters.getReadMeta() != null
                && parameters.getReadMeta().getReadLenType() == SequenceInput.READ_LEN_TYPE_SHORT ? "sr" : null;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String groupKey(String segmentKey) {
        return hasText(segmentKey) ? segmentKey : WHOLE_GENOME_GROUP_KEY;
    }

    static String validateInput(ReferenceSelectionStageInputUrls input,
            ReferenceSelectionStageParameters parameters) {
        if (input == null || input.getContigsUrl() == null || input.getContigsUrl().isBlank()) {
            return "Reference selection requires a contigs URL";
        }
        if (parameters == null || parameters.getCandidateReferences() == null
                || parameters.getCandidateReferences().isEmpty()) {
            return "Reference selection requires at least one candidate reference";
        }
        if (hasText(input.getR2Url()) && !hasText(input.getR1Url())) {
            return "Reference selection read fallback requires R1 when R2 is provided";
        }
        if (hasText(input.getR1Url()) && input.getR1Url().equals(input.getR2Url())) {
            return "Reference selection R1 and R2 must have distinct URLs";
        }
        if (parameters.isReadFallbackEnabled()) {
            if (!Double.isFinite(parameters.getMinimumReadReferenceCoverage())
                    || parameters.getMinimumReadReferenceCoverage() < 0.0d
                    || parameters.getMinimumReadReferenceCoverage() > 1.0d) {
                return "minimumReadReferenceCoverage must be a fraction between 0 and 1";
            }
            if (!Double.isFinite(parameters.getMinimumReadSequenceIdentity())
                    || parameters.getMinimumReadSequenceIdentity() < 0.0d
                    || parameters.getMinimumReadSequenceIdentity() > 100.0d) {
                return "minimumReadSequenceIdentity must be a percentage between 0 and 100";
            }
            if (parameters.getMinimumReadMappingQuality() < 0
                    || parameters.getMinimumReadMappingQuality() > 254) {
                return "minimumReadMappingQuality must be between 0 and 254";
            }
            if (hasText(input.getR1Url())) {
                if (parameters.getReadMeta() == null) {
                    return "Reference selection read fallback requires read metadata";
                }
                int readLenType = parameters.getReadMeta().getReadLenType();
                if (readLenType != SequenceInput.READ_LEN_TYPE_SHORT
                        && readLenType != SequenceInput.READ_LEN_TYPE_LONG) {
                    return "Reference selection read fallback requires a supported read length type";
                }
                String preset = parameters.getReadFallbackPreset();
                if (hasText(preset) && !(readLenType == SequenceInput.READ_LEN_TYPE_SHORT
                        ? "sr".equals(preset) : Set.of("map-ont", "map-pb", "map-hifi").contains(preset))) {
                    return "readFallbackPreset is unsupported or incompatible with the read length type";
                }
                if (readLenType == SequenceInput.READ_LEN_TYPE_LONG && hasText(input.getR2Url())) {
                    return "Long-read reference selection expects a single reads file";
                }
            }
        }
        SegmentOrdinalResolver.assignKnownOrdinals(parameters.getCandidateReferences());

        Set<String> accessions = new HashSet<>();
        Integer expectedTaxId = null;
        boolean taxIdInitialized = false;
        boolean hasDeclaredSegment = false;
        boolean hasResolvedSegment = false;
        for (ReferenceSequence candidate : parameters.getCandidateReferences()) {
            if (candidate == null) {
                return "Candidate reference must not be null";
            }
            if (!taxIdInitialized) {
                expectedTaxId = candidate.getTaxId();
                taxIdInitialized = true;
            } else if (!Objects.equals(expectedTaxId, candidate.getTaxId())) {
                return "Candidate references must have the same tax ID";
            }
            if (candidate.getAccession() == null || candidate.getAccession().isBlank()) {
                return "Candidate reference accession must not be blank";
            }
            if (!accessions.add(candidate.getAccession())) {
                return "Candidate reference accession is duplicated: " + candidate.getAccession();
            }
            if (candidate.getPath() == null || candidate.getPath().isBlank()) {
                return "Candidate reference path must not be blank: " + candidate.getAccession();
            }
            String segmentName = candidate.getSegment();
            Integer segmentOrdinal = candidate.getSegmentOrdinal();
            if (segmentOrdinal != null && (segmentName == null || segmentName.isBlank())) {
                return "Candidate segment ordinal requires the original segment name: "
                        + candidate.getAccession();
            }
            if (segmentName != null && !segmentName.isBlank()) {
                hasDeclaredSegment = true;
                if (segmentOrdinal != null) {
                    hasResolvedSegment = true;
                } else if (!SegmentOrdinalResolver.isMissingSegmentName(segmentName)) {
                    return "Unable to resolve segment ordinal for candidate "
                            + candidate.getAccession() + ": " + segmentName;
                }
            }
        }
        if (hasDeclaredSegment && !hasResolvedSegment) {
            return "Candidate segment metadata is present but contains no resolved ordinal";
        }
        return null;
    }

    static CandidateSelection selectBestCandidatesBySegment(
            List<ReferenceCandidateScore> candidateScores) {
        CandidateSelection selection = orderCandidatesBySegment(candidateScores);
        if (selection.winners().isEmpty()) {
            throw new IllegalArgumentException(
                    "Minimap2 did not find a usable match for any candidate reference group");
        }
        return selection;
    }

    private static CandidateSelection orderCandidatesBySegment(List<ReferenceCandidateScore> candidateScores) {
        if (candidateScores == null || candidateScores.isEmpty()) {
            throw new IllegalArgumentException("Reference selection requires candidate scores");
        }

        boolean hasSegmentedCandidate = false;
        Set<String> scoreAccessions = new HashSet<>();
        for (ReferenceCandidateScore candidateScore : candidateScores) {
            if (candidateScore == null
                    || candidateScore.getReferenceAccession() == null
                    || candidateScore.getReferenceAccession().isBlank()) {
                throw new IllegalArgumentException(
                        "Candidate score must identify a reference accession");
            }
            if (!scoreAccessions.add(candidateScore.getReferenceAccession())) {
                throw new IllegalArgumentException(
                        "Candidate score accession is duplicated: "
                                + candidateScore.getReferenceAccession());
            }
            String segmentKey = candidateScore.getSegmentKey();
            if (segmentKey != null && !segmentKey.isBlank()) {
                if (ReferenceSequence.segmentOrdinalFromKey(segmentKey) == null) {
                    throw new IllegalArgumentException(
                            "Candidate score has a non-ordinal segment key: " + segmentKey);
                }
                hasSegmentedCandidate = true;
            }
        }

        Map<String, List<ReferenceCandidateScore>> scoresBySegment = new TreeMap<>(
                ReferenceSelectionStageExecutor::compareSegmentKeys);
        List<ReferenceCandidateScore> candidatesWithoutSegment = new ArrayList<>();
        for (ReferenceCandidateScore candidateScore : candidateScores) {
            // Once segmented candidates exist, a blank segment is treated as
            // missing metadata rather than as a whole-genome candidate.
            if (hasSegmentedCandidate
                    && (candidateScore.getSegmentKey() == null
                            || candidateScore.getSegmentKey().isBlank())) {
                candidatesWithoutSegment.add(candidateScore);
                continue;
            }
            String groupKey = hasSegmentedCandidate
                    ? candidateScore.getSegmentKey()
                    : WHOLE_GENOME_GROUP_KEY;
            scoresBySegment.computeIfAbsent(groupKey, ignored -> new ArrayList<>())
                    .add(candidateScore);
        }

        List<ReferenceCandidateScore> orderedScores = new ArrayList<>(candidateScores.size());
        List<ReferenceCandidateScore> winners = new ArrayList<>(scoresBySegment.size());
        List<String> unmatchedSegmentKeys = new ArrayList<>();
        for (Map.Entry<String, List<ReferenceCandidateScore>> entry : scoresBySegment.entrySet()) {
            List<ReferenceCandidateScore> segmentScores = entry.getValue();
            segmentScores.sort(ReferenceSelectionStageExecutor::compareScoreDescending);
            orderedScores.addAll(segmentScores);

            ReferenceCandidateScore winner = segmentScores.stream()
                    .filter(score -> score.getAlignedReferenceBases() > 0L && score.getRejectionReason() == null)
                    .findFirst().orElse(null);
            if (winner == null) {
                if (!WHOLE_GENOME_GROUP_KEY.equals(entry.getKey())) {
                    unmatchedSegmentKeys.add(entry.getKey());
                }
                continue;
            }
            winners.add(winner);
        }
        candidatesWithoutSegment.sort(ReferenceSelectionStageExecutor::compareScoreDescending);
        orderedScores.addAll(candidatesWithoutSegment);

        return new CandidateSelection(
                List.copyOf(orderedScores),
                List.copyOf(winners),
                List.copyOf(unmatchedSegmentKeys));
    }

    private static int compareSegmentKeys(String left, String right) {
        if (WHOLE_GENOME_GROUP_KEY.equals(left) || WHOLE_GENOME_GROUP_KEY.equals(right)) {
            if (WHOLE_GENOME_GROUP_KEY.equals(left) && WHOLE_GENOME_GROUP_KEY.equals(right)) {
                return 0;
            }
            return WHOLE_GENOME_GROUP_KEY.equals(left) ? -1 : 1;
        }
        Integer leftOrdinal = ReferenceSequence.segmentOrdinalFromKey(left);
        Integer rightOrdinal = ReferenceSequence.segmentOrdinalFromKey(right);
        if (leftOrdinal == null || rightOrdinal == null) {
            throw new IllegalArgumentException("Segment keys must contain resolved ordinals");
        }
        return Integer.compare(leftOrdinal, rightOrdinal);
    }

    private static int compareScoreDescending(ReferenceCandidateScore left, ReferenceCandidateScore right) {
        int comparison = Double.compare(right.getScore(), left.getScore());
        if (comparison != 0) {
            return comparison;
        }
        comparison = Double.compare(right.getReferenceCoverage(), left.getReferenceCoverage());
        if (comparison != 0) {
            return comparison;
        }
        comparison = Double.compare(right.getSequenceIdentity(), left.getSequenceIdentity());
        if (comparison != 0) {
            return comparison;
        }

        String leftAccession = left.getReferenceAccession();
        String rightAccession = right.getReferenceAccession();
        if (leftAccession == null) {
            return rightAccession == null ? 0 : 1;
        }
        if (rightAccession == null) {
            return -1;
        }
        return leftAccession.compareTo(rightAccession);
    }

    private static void writeSelectionReport(Path reportPath,
            List<ReferenceCandidateScore> candidateScores,
            Map<String, ReferenceSequence> candidatesByAccession,
            Set<String> selectedAccessions) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(reportPath, StandardCharsets.UTF_8)) {
            writer.write("reference_id\taccession\tsegment_name\tsegment_ordinal\tsegment_key"
                    + "\ttax_id\tpath\tsequence_identity_percent"
                    + "\treference_coverage\taligned_reference_bases\treference_bases"
                    + "\tmatching_bases\talignment_block_bases\talignment_count\tscore\tselected"
                    + "\tevidence_source\trejection_reason");
            writer.newLine();

            for (ReferenceCandidateScore candidateScore : candidateScores) {
                ReferenceSequence referenceSequence = candidatesByAccession.get(
                        candidateScore.getReferenceAccession());
                if (referenceSequence == null) {
                    throw new IOException("No candidate reference found for accession: "
                            + candidateScore.getReferenceAccession());
                }
                writer.write(reportValue(referenceSequence.getReferenceId()));
                writer.write('\t');
                writer.write(reportValue(candidateScore.getReferenceAccession()));
                writer.write('\t');
                writer.write(reportValue(referenceSequence.getSegment()));
                writer.write('\t');
                writer.write(reportValue(referenceSequence.getSegmentOrdinal()));
                writer.write('\t');
                writer.write(reportValue(candidateScore.getSegmentKey()));
                writer.write('\t');
                writer.write(reportValue(referenceSequence.getTaxId()));
                writer.write('\t');
                writer.write(reportValue(referenceSequence.getPath()));
                writer.write('\t');
                writer.write(Double.toString(candidateScore.getSequenceIdentity()));
                writer.write('\t');
                writer.write(Double.toString(candidateScore.getReferenceCoverage()));
                writer.write('\t');
                writer.write(Long.toString(candidateScore.getAlignedReferenceBases()));
                writer.write('\t');
                writer.write(Long.toString(candidateScore.getReferenceBases()));
                writer.write('\t');
                writer.write(Long.toString(candidateScore.getMatchingBases()));
                writer.write('\t');
                writer.write(Long.toString(candidateScore.getAlignmentBlockBases()));
                writer.write('\t');
                writer.write(Integer.toString(candidateScore.getAlignmentCount()));
                writer.write('\t');
                writer.write(Double.toString(candidateScore.getScore()));
                writer.write('\t');
                writer.write(Boolean.toString(
                        selectedAccessions.contains(candidateScore.getReferenceAccession())));
                writer.write('\t');
                writer.write(reportValue(candidateScore.getEvidenceSource()));
                writer.write('\t');
                writer.write(reportValue(candidateScore.getRejectionReason()));
                writer.newLine();
            }
        }
    }

    private static String safeFileName(String objectName) {
        int slashIndex = objectName.lastIndexOf('/');
        String fileName = slashIndex >= 0 ? objectName.substring(slashIndex + 1) : objectName;
        String safeName = fileName.replaceAll("[^A-Za-z0-9._-]", "_");
        return safeName.isBlank() ? "reference.fasta" : safeName;
    }

    private static String reportValue(Object value) {
        if (value == null) {
            return "";
        }
        return value.toString().replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    @Override
    public int id() {
        return PIPELINE_STAGE_REFERENCE_SELECTION;
    }
}
