package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_COVID_WASTEWATER;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.CovidWastewaterStageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.CovidWastewaterStageOutput;

@Component
public class CovidWastewaterStageDoneHandler extends AbstractStageDoneHandler<CovidWastewaterStageOutput> {

    @Override
    public int getType() {
        return PIPELINE_STAGE_COVID_WASTEWATER;
    }

    @Override
    protected Pair<Map<String, String>, CovidWastewaterStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<CovidWastewaterStageOutput> stageRunResult) {
        CovidWastewaterStageOutput output = stageRunResult.getStageOutput();
        if (output == null) {
            throw new IllegalArgumentException("COVID wastewater stage output is missing");
        }
        requireArtifact(output.getReportPath(), "report");
        requireArtifact(output.getMetricsPath(), "metrics");
        requireArtifact(output.getProvenancePath(), "provenance");
        requireArtifact(output.getExecutionLogPath(), "execution log");

        Map<String, String> uploads = new LinkedHashMap<>();
        StageContext context = stageRunResult.getStageContext();
        CovidWastewaterStageResult result = new CovidWastewaterStageResult();
        result.setReportUrl(addArtifact(uploads, context, output.getReportPath(), "freyja.demix.tsv"));
        result.setMetricsUrl(addArtifact(uploads, context, output.getMetricsPath(), "covid-wastewater.metrics.json"));
        result.setProvenanceUrl(addArtifact(uploads, context, output.getProvenancePath(), "provenance.json"));
        result.setExecutionLogUrl(addArtifact(uploads, context, output.getExecutionLogPath(), "execution.log"));

        // Report imports need not supply alignments. Publish only a complete usable pair.
        if (nonemptyRegularFile(output.getBamPath()) && nonemptyRegularFile(output.getBamIndexPath())) {
            result.setBamUrl(addArtifact(uploads, context, output.getBamPath(), "aligned.bam"));
            result.setBamIndexUrl(addArtifact(uploads, context, output.getBamIndexPath(), "aligned.bam.bai"));
        }
        return Pair.of(uploads, result);
    }

    private String addArtifact(Map<String, String> uploads, StageContext context, Path path, String filename) {
        String key = createStoreObjectName(context, filename);
        String source = path.toAbsolutePath().toString();
        if (uploads.putIfAbsent(source, key) != null) {
            throw new IllegalArgumentException("COVID wastewater artifacts must have distinct source files");
        }
        return key;
    }

    private static void requireArtifact(Path path, String label) {
        if (!nonemptyRegularFile(path)) {
            throw new IllegalArgumentException("COVID wastewater " + label + " must be a nonempty regular file");
        }
    }

    private static boolean nonemptyRegularFile(Path path) {
        if (path == null || !Files.isRegularFile(path)) {
            return false;
        }
        try {
            return Files.size(path) > 0L;
        } catch (IOException e) {
            return false;
        }
    }
}
