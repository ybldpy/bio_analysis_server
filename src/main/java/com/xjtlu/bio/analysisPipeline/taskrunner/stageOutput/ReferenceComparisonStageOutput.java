package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;
import java.util.List;

import com.xjtlu.bio.analysisPipeline.stageMetrics.ReferenceComparisonMetrics;

public class ReferenceComparisonStageOutput implements StageOutput {

    public record CandidateComparison(
            ReferenceComparisonMetrics metrics,
            Path alignmentPafPath,
            Path differenceTsvPath) {
    }

    private List<CandidateComparison> comparisons = List.of();
    private Path comparisonTsvPath;

    // Kept for compatibility with the historical single-comparison contract.
    private Path alignmentPafPath;
    private Path differenceTsvPath;

    public ReferenceComparisonStageOutput(List<CandidateComparison> comparisons, Path comparisonTsvPath) {
        this.comparisons = List.copyOf(comparisons);
        this.comparisonTsvPath = comparisonTsvPath;
    }

    public ReferenceComparisonStageOutput(Path alignmentPafPath, Path differenceTsvPath) {
        this.alignmentPafPath = alignmentPafPath;
        this.differenceTsvPath = differenceTsvPath;
    }

    public Path getAlignmentPafPath() {
        return alignmentPafPath;
    }

    public ReferenceComparisonStageOutput() {
    }

    public List<CandidateComparison> getComparisons() {
        return comparisons;
    }

    public void setComparisons(List<CandidateComparison> comparisons) {
        this.comparisons = comparisons == null ? List.of() : List.copyOf(comparisons);
    }

    public Path getComparisonTsvPath() {
        return comparisonTsvPath;
    }

    public void setComparisonTsvPath(Path comparisonTsvPath) {
        this.comparisonTsvPath = comparisonTsvPath;
    }

    public void setAlignmentPafPath(Path alignmentPafPath) {
        this.alignmentPafPath = alignmentPafPath;
    }

    public Path getDifferenceTsvPath() {
        return differenceTsvPath;
    }

    public void setDifferenceTsvPath(Path differenceTsvPath) {
        this.differenceTsvPath = differenceTsvPath;
    }

    @Override
    public Path getParentPath() {
        if (comparisonTsvPath != null) {
            return comparisonTsvPath.getParent();
        }
        return differenceTsvPath == null ? null : differenceTsvPath.getParent();
    }

}
