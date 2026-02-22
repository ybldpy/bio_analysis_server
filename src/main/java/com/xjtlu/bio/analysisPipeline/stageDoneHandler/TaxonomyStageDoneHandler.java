package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.service.PipelineService.PIPELINE_STAGE_STATUS_FINISHED;

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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.Taxon;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.service.PipelineService;
import com.xjtlu.bio.utils.JsonUtil;

class TaxonomyParser {

    public static TaxonomyResult parseKraken2Output(Path reportFile) throws IOException {

        List<Taxon> taxa = new ArrayList<>();

        if (reportFile == null || !Files.exists(reportFile)) {
            TaxonomyResult empty = new TaxonomyResult();
            empty.setStatus("NO_HIT");
            empty.setCandidates(Collections.emptyList());
            return empty;
        }

        List<String> lines = Files.readAllLines(reportFile);

        for (String line : lines) {

            if (line == null || line.isBlank()) {
                continue;
            }

            line = line.trim();

            String[] parts = line.split("\\s+", 6);

            if (parts.length < 6) {
                continue;
            }

            String rank = parts[3];

            // 只取 species
            if (!"S".equals(rank)) {
                continue;
            }

            Taxon taxon = new Taxon();

            try {
                taxon.setScore(Double.parseDouble(parts[0]));
                taxon.setTaxid(Integer.parseInt(parts[4]));
            } catch (NumberFormatException e) {
                continue;
            }

            taxon.setRank(rank);
            taxon.setName(parts[5]);

            taxa.add(taxon);
        }

        // 按 score 降序排序
        taxa.sort(
                Comparator.comparingDouble(Taxon::getScore)
                        .reversed());

        TaxonomyResult result = new TaxonomyResult();

        if (taxa.isEmpty()) {
            result.setStatus("NO_HIT");
            result.setCandidates(Collections.emptyList());
            return result;
        }

        // 取前 50
        int limit = Math.min(50, taxa.size());
        List<Taxon> topTaxa = new ArrayList<>(taxa.subList(0, limit));

        Taxon best = topTaxa.get(0);

        result.setBest(best);
        result.setCandidates(topTaxa);

        // 状态判断
        if (topTaxa.size() == 1) {
            result.setStatus("CONFIDENT");
        } else {

            Taxon second = topTaxa.get(1);

            if (best.getScore() >= 60 &&
                    best.getScore() - second.getScore() >= 20) {
                result.setStatus("CONFIDENT");
            } else {
                result.setStatus("AMBIGUOUS");
            }
        }

        return result;
    }

}

public class TaxonomyStageDoneHandler extends AbstractStageDoneHandler<TaxonomyStageOutput> {

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return PipelineService.PIPELINE_STAGE_TAXONOMY;
    }

    @Override
    public boolean handleStageDone(StageRunResult<TaxonomyStageOutput> stageRunResult) {
        BioPipelineStage taxonomyStage = stageRunResult.getStage();
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
            return false;
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
        patch.setOutputUrl(serializedResult);
        int updateRes = updateStageFromVersion(patch, taxonomyStage.getStageId(), taxonomyStage.getVersion());
        this.deleteStageResultDir(taxonomyStageOutput.getParentPath().toString());
        return updateRes > 0;
    }

    @Override
    protected Pair<Map<String, String>, Map<String, Object>> buildUploadConfigAndOutputUrlMap(
            StageRunResult<TaxonomyStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'buildUploadConfigAndOutputUrlMap'");
    }

    

}
