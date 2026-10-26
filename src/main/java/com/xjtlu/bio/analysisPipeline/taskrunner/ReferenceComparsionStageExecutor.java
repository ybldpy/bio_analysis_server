package com.xjtlu.bio.analysisPipeline.taskrunner;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.referenceGenome.ReferenceSequence;
import com.xjtlu.bio.analysisPipeline.stageInputs.inputUrls.ReferenceComparisonStageInputUrls;
import com.xjtlu.bio.analysisPipeline.stageInputs.parameters.ReferenceComparisonStageParameters;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceComparisonMetrics;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput.CandidateComparison;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafAlignmentStatsParser.AlignmentStats;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafParser;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.PafParser.PafParseResult;

@Component
public class ReferenceComparsionStageExecutor extends
        AbstractPipelineStageExector<ReferenceComparisonStageOutput, ReferenceComparisonStageInputUrls, ReferenceComparisonStageParameters> {

    // Operations understood by the assembly difference parser. Do not silently
    // accept truncated/unknown operations (including unsupported splice tags).
    private static final Pattern ASSEMBLY_CS_TOKEN = Pattern.compile(
            ":[0-9]+|=[A-Za-z]+|\\*[A-Za-z]{2}|[+-][A-Za-z]+");

    @Override
    protected Class<ReferenceComparisonStageInputUrls> stageInputType() {
        return ReferenceComparisonStageInputUrls.class;
    }

    @Override
    protected Class<ReferenceComparisonStageParameters> stageParameterType() {
        return ReferenceComparisonStageParameters.class;
    }

    @Override
    protected StageRunResult<ReferenceComparisonStageOutput> _execute(StageExecutionInput execution) {
        String context = "Reference comparison";
        try {
            List<ReferenceSequence> candidates = resolveCandidates(execution.stageParameters);
            if (execution.input == null || isBlank(execution.input.getFastaUrl())) {
                throw new IllegalArgumentException("Reference comparison requires a sample FASTA URL");
            }
            Path queryPath = loadFasta(execution.input.getFastaUrl(), execution.inputDir, "query_");
            List<CandidateComparison> comparisons = new ArrayList<>(candidates.size());
            for (int index = 0; index < candidates.size(); index++) {
                ReferenceSequence reference = candidates.get(index);
                context = "Reference comparison candidate " + (index + 1) + " (" + reference.getAccession() + ")";
                // Index-based directories keep distinct records with identical names/paths separate.
                String candidateDirectory = "candidate_" + (index + 1);
                Path candidateWorkDir = Files.createDirectories(execution.workDir.resolve(candidateDirectory));
                Path referencePath = loadFasta(reference.getPath(),
                        execution.inputDir.resolve(candidateDirectory), "reference_");
                Path pafPath = candidateWorkDir.resolve("alignment.paf");
                Path differencePath = candidateWorkDir.resolve("reference_difference.tsv");

                List<String> command = new ArrayList<>(analysisPipelineToolsConfig.getMinimap2());
                command.addAll(List.of("-x", "asm5", "--cs=long", "--secondary=no",
                        referencePath.toString(), queryPath.toString(), "-o", pafPath.toString()));
                ExecuteResult result = _execute(command, candidateWorkDir);
                if (!result.success()) {
                    return runFail(execution.stageContext, execution.workDir,
                            context + " failed. exitCode=" + result.runCode
                                    + ", workDir=" + candidateWorkDir, result.ex);
                }

                // A missing/malformed PAF is an error. A present empty PAF is a valid NO_MATCH.
                AlignmentStats stats = PafAlignmentStatsParser.parse(pafPath, referencePath);
                validateDifferenceTags(pafPath);
                PafParseResult parsed = PafParser.parseToDifferenceTsv(pafPath, differencePath);
                if (!parsed.isSuccess()) {
                    throw new IOException("Unable to parse comparison differences: " + pafPath,
                            parsed.getException());
                }
                comparisons.add(new CandidateComparison(metrics(reference, stats), pafPath, differencePath));
            }
            context = "Reference comparison summary";
            Path summaryPath = execution.workDir.resolve("reference_comparison.tsv");
            writeSummary(summaryPath, comparisons);
            return OK(new ReferenceComparisonStageOutput(comparisons, summaryPath), execution);
        } catch (IOException | LoadFailException | IllegalArgumentException e) {
            return runFail(execution.stageContext, execution.workDir, context + " failed: " + e.getMessage(), e);
        }
    }

    private static List<ReferenceSequence> resolveCandidates(ReferenceComparisonStageParameters parameters) {
        if (parameters == null) {
            throw new IllegalArgumentException("Reference comparison parameters are required");
        }
        List<ReferenceSequence> candidates = parameters.getCandidateReferences();
        if (candidates == null && parameters.getReferenceGenome() != null) {
            candidates = parameters.getReferenceGenome().getSequences();
        }
        if (candidates == null || candidates.isEmpty()) {
            throw new IllegalArgumentException("Reference comparison requires at least one candidate reference");
        }
        Set<Long> referenceIds = new HashSet<>();
        for (int index = 0; index < candidates.size(); index++) {
            ReferenceSequence reference = candidates.get(index);
            if (reference == null || isBlank(reference.getAccession()) || isBlank(reference.getPath())) {
                throw new IllegalArgumentException("Candidate " + (index + 1) + " requires an accession and file path");
            }
            if (reference.getReferenceId() != null && !referenceIds.add(reference.getReferenceId())) {
                throw new IllegalArgumentException("Duplicate candidate reference ID: " + reference.getReferenceId());
            }
        }
        return candidates;
    }

    private Path loadFasta(String objectName, Path directory, String prefix) throws IOException, LoadFailException {
        Files.createDirectories(directory);
        Path localPath = directory.resolve(prefix + objectName.substring(objectName.lastIndexOf('/') + 1));
        loadInput(Map.of(objectName, localPath));
        return uncompressIfCompressedFormat(localPath);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /** Do not publish an empty differences report for a missing or malformed cs tag. */
    private static void validateDifferenceTags(Path pafPath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(pafPath, StandardCharsets.UTF_8)) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (line.isBlank()) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                boolean hasCs = false;
                for (int index = 12; index < fields.length; index++) {
                    if (fields[index].startsWith("cs:Z:")) {
                        if (!hasValidCsOperations(fields[index].substring(5))) {
                            throw new IOException("Malformed or unsupported cs tag at " + pafPath + ":" + lineNumber);
                        }
                        hasCs = true;
                        break;
                    }
                }
                if (!hasCs) {
                    throw new IOException("Missing or empty cs tag at " + pafPath + ":" + lineNumber);
                }
            }
        }
    }

    private static boolean hasValidCsOperations(String cs) {
        // Scan tokens iteratively: a recursively repeated regex can overflow
        // the Java regex stack on long alignments with many difference tokens.
        Matcher matcher = ASSEMBLY_CS_TOKEN.matcher(cs);
        int end = 0;
        while (matcher.find()) {
            if (matcher.start() != end) {
                return false;
            }
            end = matcher.end();
        }
        return end > 0 && end == cs.length();
    }

    private static ReferenceComparisonMetrics metrics(ReferenceSequence reference, AlignmentStats stats) {
        return new ReferenceComparisonMetrics(
                reference.getReferenceId(), reference.getAccession(), reference.getSourceDb(),
                reference.getTaxId(), reference.getOrganismName(), reference.getSegment(), reference.getSegmentKey(),
                stats.alignedReferenceBases() > 0L
                        ? ReferenceComparisonMetrics.MATCHED : ReferenceComparisonMetrics.NO_MATCH,
                stats.sequenceIdentity(), stats.referenceCoverage(), stats.alignedReferenceBases(),
                stats.referenceBases(), stats.matchingBases(), stats.alignmentBlockBases(), stats.alignmentCount());
    }

    private static void writeSummary(Path path, List<CandidateComparison> comparisons) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write("reference_id\treference_accession\tsource_db\ttax_id\torganism_name\tsegment\tsegment_key"
                    + "\tstatus\tsequence_identity_percent\treference_coverage\taligned_reference_bases"
                    + "\treference_bases\tmatching_bases\talignment_block_bases\talignment_count\n");
            for (CandidateComparison comparison : comparisons) {
                ReferenceComparisonMetrics m = comparison.metrics();
                Object[] fields = {m.referenceId(), m.referenceAccession(), m.sourceDb(), m.taxId(),
                        m.organismName(), m.segment(), m.segmentKey(), m.status(), m.sequenceIdentity(),
                        m.referenceCoverage(), m.alignedReferenceBases(), m.referenceBases(),
                        m.matchingBases(), m.alignmentBlockBases(), m.alignmentCount()};
                for (int index = 0; index < fields.length; index++) {
                    if (index > 0) {
                        writer.write('\t');
                    }
                    if (fields[index] != null) {
                        writer.write(fields[index].toString().replace('\t', ' ').replace('\r', ' ').replace('\n', ' '));
                    }
                }
                writer.write('\n');
            }
        }
    }

    @Override
    public int id() {
        return Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
    }
}
