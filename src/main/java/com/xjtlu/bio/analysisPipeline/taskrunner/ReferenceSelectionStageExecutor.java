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

import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceCandidateScore;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceGenome;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.referenceGenome.SegmentOrdinalResolver;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceSelectionStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceSelectionStageParameters;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceSelectionStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser.AlignmentStats;

@Component
public class ReferenceSelectionStageExecutor extends
        AbstractPipelineStageExector<ReferenceSelectionStageOutput, ReferenceSelectionStageInputUrls, ReferenceSelectionStageParameters> {

    private static final String MINIMAP2_PRESET = "asm20";
    private static final String SELECTION_REPORT_FILE_NAME = "reference_selection.tsv";
    private static final String WHOLE_GENOME_GROUP_KEY = "__WHOLE_GENOME__";

    static record CandidateSelection(
            List<ReferenceCandidateScore> orderedScores,
            List<ReferenceCandidateScore> winners) {
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
                double score = stats.sequenceIdentity() * stats.referenceCoverage();
                candidateScores.add(new ReferenceCandidateScore(
                        candidateFile.referenceSequence.getAccession(),
                        candidateFile.referenceSequence.getSegmentKey(),
                        stats.sequenceIdentity(),
                        stats.referenceCoverage(),
                        stats.alignedReferenceBases(),
                        stats.referenceBases(),
                        stats.matchingBases(),
                        stats.alignmentBlockBases(),
                        stats.alignmentCount(),
                        score));
            } catch (IOException e) {
                return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                        "Failed to parse minimap2 PAF for candidate "
                                + candidateFile.referenceSequence.getAccession(),
                        e);
            }
        }

        CandidateSelection selection;
        try {
            selection = selectBestCandidatesBySegment(candidateScores);
        } catch (IllegalArgumentException e) {
            return runFail(stageExecutionInput.stageContext, e.getMessage(), stageExecutionInput.workDir);
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

        ReferenceGenome selectedGenome;
        try {
            selectedGenome = ReferenceGenome.fromSelectedSequences(selectedSequences);
        } catch (IllegalArgumentException e) {
            return runFail(stageExecutionInput.stageContext, e.getMessage(), stageExecutionInput.workDir);
        }

        Path selectionReportPath = stageExecutionInput.workDir.resolve(SELECTION_REPORT_FILE_NAME);
        try {
            writeSelectionReport(
                    selectionReportPath,
                    candidateScores,
                    candidatesByAccession,
                    selectedAccessions);
        } catch (IOException e) {
            return runFail(stageExecutionInput.stageContext, stageExecutionInput.workDir,
                    "Failed to write reference-selection report", e);
        }

        return OK(new ReferenceSelectionStageOutput(
                selectedGenome,
                candidateScores,
                selectionReportPath), stageExecutionInput);
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
        for (Map.Entry<String, List<ReferenceCandidateScore>> entry : scoresBySegment.entrySet()) {
            List<ReferenceCandidateScore> segmentScores = entry.getValue();
            segmentScores.sort(ReferenceSelectionStageExecutor::compareScoreDescending);
            ReferenceCandidateScore winner = segmentScores.get(0);
            if (winner.getAlignedReferenceBases() <= 0L) {
                String groupName = WHOLE_GENOME_GROUP_KEY.equals(entry.getKey())
                        ? "whole genome"
                        : entry.getKey();
                throw new IllegalArgumentException(
                        "Minimap2 did not find a usable match for reference group: " + groupName);
            }
            winners.add(winner);
            orderedScores.addAll(segmentScores);
        }
        candidatesWithoutSegment.sort(ReferenceSelectionStageExecutor::compareScoreDescending);
        orderedScores.addAll(candidatesWithoutSegment);

        return new CandidateSelection(List.copyOf(orderedScores), List.copyOf(winners));
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
                    + "\tmatching_bases\talignment_block_bases\talignment_count\tscore\tselected");
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
