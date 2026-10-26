package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceComparisonMetrics;
import com.xjtlu.bio.analysisPipeline.stageResult.ReferenceComparisonResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.ReferenceComparisonStageOutput.CandidateComparison;

@Component
public class ReferenceComparisonStageDoneHandler extends AbstractStageDoneHandler<ReferenceComparisonStageOutput> {

    private static final String COMPARISON_JSON = "reference_comparison.json";
    private static final String COMPARISON_TSV = "reference_comparison.tsv";

    @Override
    public int getType() {
        return Constants.StageType.PIPELINE_STAGE_REFERENCE_COMPARISON;
    }

    @Override
    protected Pair<Map<String, String>, ReferenceComparisonResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<ReferenceComparisonStageOutput> stageRunResult) {

        ReferenceComparisonStageOutput output = stageRunResult.getStageOutput();
        if (output.getComparisonTsvPath() == null && output.getComparisons().isEmpty()) {
            return legacyUploadPlan(stageRunResult);
        }

        Map<String, String> uploadConfig = new LinkedHashMap<>();
        List<ObjectNode> comparisons = new ArrayList<>();
        int matchedCount = 0;
        for (int index = 0; index < output.getComparisons().size(); index++) {
            CandidateComparison candidate = output.getComparisons().get(index);
            ReferenceComparisonMetrics metrics = candidate.metrics();
            if (ReferenceComparisonMetrics.MATCHED.equals(metrics.status())) {
                matchedCount++;
            } else if (!ReferenceComparisonMetrics.NO_MATCH.equals(metrics.status())) {
                throw new IllegalArgumentException("Unsupported comparison status: " + metrics.status());
            }

            // Indices avoid collisions when accession/segment metadata are missing or repeated.
            String prefix = String.format(Locale.ROOT, "candidates/%04d/", index + 1);
            String pafUrl = createStoreObjectName(stageRunResult.getStageContext(), prefix + "alignment.paf");
            String differenceUrl = createStoreObjectName(stageRunResult.getStageContext(),
                    prefix + "reference_difference.tsv");
            uploadConfig.put(candidate.alignmentPafPath().toString(), pafUrl);
            uploadConfig.put(candidate.differenceTsvPath().toString(), differenceUrl);

            ObjectNode comparison = jsonMapper.valueToTree(metrics);
            comparison.put("alignmentPafUrl", pafUrl);
            comparison.put("differenceTsvUrl", differenceUrl);
            comparisons.add(comparison);
        }

        int candidateCount = comparisons.size();
        String comparisonJsonUrl = createStoreObjectName(stageRunResult.getStageContext(), COMPARISON_JSON);
        String comparisonTsvUrl = createStoreObjectName(stageRunResult.getStageContext(), COMPARISON_TSV);
        ReferenceComparisonResult result = new ReferenceComparisonResult(candidateCount, matchedCount,
                candidateCount - matchedCount, comparisonJsonUrl, comparisonTsvUrl);

        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("schemaVersion", 2);
        manifest.put("candidateCount", candidateCount);
        manifest.put("matchedCandidateCount", matchedCount);
        manifest.put("unmatchedCandidateCount", candidateCount - matchedCount);
        manifest.put("comparisons", comparisons);

        Path manifestPath = output.getComparisonTsvPath().resolveSibling(COMPARISON_JSON);
        try {
            jsonMapper.writeValue(manifestPath.toFile(), manifest);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write reference comparison manifest", e);
        }
        uploadConfig.put(output.getComparisonTsvPath().toString(), comparisonTsvUrl);
        uploadConfig.put(manifestPath.toString(), comparisonJsonUrl);
        return Pair.of(uploadConfig, result);
    }

    private Pair<Map<String, String>, ReferenceComparisonResult> legacyUploadPlan(
            StageRunResult<ReferenceComparisonStageOutput> stageRunResult) {
        ReferenceComparisonStageOutput output = stageRunResult.getStageOutput();
        String pafUrl = createStoreObjectName(stageRunResult.getStageContext(), "alignment.paf");
        String differenceTsvUrl = createStoreObjectName(stageRunResult.getStageContext(), "reference_difference.tsv");
        return Pair.of(Map.of(output.getAlignmentPafPath().toString(), pafUrl,
                output.getDifferenceTsvPath().toString(), differenceTsvUrl),
                new ReferenceComparisonResult(pafUrl, differenceTsvUrl));
    }
}
