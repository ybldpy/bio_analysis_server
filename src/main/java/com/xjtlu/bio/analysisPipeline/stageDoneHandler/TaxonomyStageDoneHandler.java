package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.context.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.Taxon;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;


class TaxonomyParser {

    private static final int MAX_CANDIDATES = 50;

    // 第一名最低支持度
    private static final double MIN_CONFIDENT_SCORE = 3.0;

    // 第一名至少比第二名高 20%
    private static final double MIN_CONFIDENT_RATIO = 1.2;

    public static TaxonomyResult parseKraken2Output(Path reportFile) throws IOException {
        TaxonomyResult result = new TaxonomyResult();

        if (reportFile == null || !Files.exists(reportFile)) {
            result.setStatus("NO_HIT");
            result.setCandidates(Collections.emptyList());
            return result;
        }

        List<TaxonomyResult.Taxon> taxa = parseSpeciesOnly(reportFile);

        if (taxa.isEmpty()) {
            result.setStatus("NO_HIT");
            result.setCandidates(Collections.emptyList());
            return result;
        }

        taxa.sort(Comparator.comparingDouble(TaxonomyResult.Taxon::getScore).reversed());

        List<TaxonomyResult.Taxon> topTaxa =
                new ArrayList<>(taxa.subList(0, Math.min(MAX_CANDIDATES, taxa.size())));

        TaxonomyResult.Taxon best = topTaxa.get(0);
        result.setBest(best);
        result.setCandidates(topTaxa);

        if (topTaxa.size() == 1) {
            if (best.getScore() >= MIN_CONFIDENT_SCORE) {
                result.setStatus("CONFIDENT");
            } else {
                result.setStatus("AMBIGUOUS");
            }
            return result;
        }

        TaxonomyResult.Taxon second = topTaxa.get(1);
        double ratio = calcRatio(best.getScore(), second.getScore());

        if (best.getScore() >= MIN_CONFIDENT_SCORE && ratio >= MIN_CONFIDENT_RATIO) {
            result.setStatus("CONFIDENT");
        } else {
            result.setStatus("AMBIGUOUS");
        }

        return result;
    }

    private static List<TaxonomyResult.Taxon> parseSpeciesOnly(Path reportFile) throws IOException {
        List<TaxonomyResult.Taxon> taxa = new ArrayList<>();

        for (String line : Files.readAllLines(reportFile)) {
            if (line == null || line.isBlank()) {
                continue;
            }

            String[] parts = line.trim().split("\\s+", 6);
            if (parts.length < 6) {
                continue;
            }

            String rank = parts[3];

            // 只取 species
            if (!"S".equals(rank)) {
                continue;
            }

            try {
                double score = Double.parseDouble(parts[0]);
                int taxid = Integer.parseInt(parts[4]);
                String name = parts[5];

                TaxonomyResult.Taxon taxon = new TaxonomyResult.Taxon();
                taxon.setScore(score);
                taxon.setTaxid(taxid);
                taxon.setRank(rank);
                taxon.setName(name);

                taxa.add(taxon);
            } catch (NumberFormatException ignored) {
            }
        }

        return taxa;
    }

    private static double calcRatio(double best, double second) {
        if (second <= 0) {
            return Double.MAX_VALUE;
        }
        return best / second;
    }
}





@Component
public class TaxonomyStageDoneHandler extends AbstractStageDoneHandler<TaxonomyStageOutput> {

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PIPELINE_STAGE_TAXONOMY;
    }

    @Override
    public void handleStageDone(StageRunResult<TaxonomyStageOutput> stageRunResult) {
        StageContext taxonomyStage = stageRunResult.getStageContext();
        TaxonomyStageOutput taxonomyStageOutput = stageRunResult.getStageOutput();

        TaxonomyResult taxonomySortedResults = null;
        try {
            taxonomySortedResults = TaxonomyParser.parseKraken2Output(taxonomyStageOutput.getReport());

        } catch (IOException e) {
            // TODO Auto-generated catch block
            logger.error(
                    "Failed to parse Kraken report. reportPath={}",
                    taxonomyStageOutput.getReport(),
                    e);

            this.handleFail(taxonomyStage, taxonomyStageOutput.getParentPath().toString());
            return;
        }

        String serializedResult = null;

        try {
            serializedResult = JsonUtil.toJson(taxonomySortedResults);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            logger.error("stage = {}.json processing exception", taxonomyStage, e);
            this.handleFail(taxonomyStage, taxonomyStageOutput.getParentPath().toString());
        }
        BioPipelineStage patch = new BioPipelineStage();
        patch.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        patch.setEndTime(new Date());
        patch.setOutputInline(serializedResult);

        
        int updateRes = updateStageFromVersion(patch, taxonomyStage.getRunStageId(), taxonomyStage.getVersion());
        this.deleteStageResultDir(taxonomyStageOutput.getParentPath().toString());
        return;
    }

    @Override
    protected Pair<Map<String, String>, StageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<TaxonomyStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'buildUploadConfigAndOutputUrlMap'");
    }



    

}
