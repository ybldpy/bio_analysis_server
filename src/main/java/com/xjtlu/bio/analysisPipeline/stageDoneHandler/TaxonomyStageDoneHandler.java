package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_ACTION_REQUIRED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_FINISHED;
import static com.xjtlu.bio.analysisPipeline.Constants.StageStatus.PIPELINE_STAGE_STATUS_NOT_APPLICABLE;
import static com.xjtlu.bio.analysisPipeline.Constants.StageType.PIPELINE_STAGE_TAXONOMY;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult;
import com.xjtlu.bio.analysisPipeline.stageResult.TaxonomyResult.Taxon;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.TaxonomyStageOutput.TaxonomyClassificationOutput;
import com.xjtlu.bio.analysisPipeline.taskrunner.util.TaxonomicContigBinner;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.utils.JsonUtil;


class TaxonomyParser {

    private static final int MAX_CANDIDATES = 50;

    // 第一名最低支持度
    private static final double MIN_CONFIDENT_SCORE = 3.0;

    // 第一名至少比第二名高 20%
    private static final double MIN_CONFIDENT_RATIO = 1.2;

    // public static TaxonomyResult parseKraken2Output(Path reportFile) throws IOException {
    //     TaxonomyResult result = new TaxonomyResult();

    //     if (reportFile == null || !Files.exists(reportFile)) {
    //         result.setStatus("NO_HIT");
    //         result.setCandidates(Collections.emptyList());
    //         return result;
    //     }

    //     List<TaxonomyResult.Taxon> taxa = parseSpeciesOnly(reportFile);

    //     if (taxa.isEmpty()) {
    //         result.setStatus("NO_HIT");
    //         result.setCandidates(Collections.emptyList());
    //         return result;
    //     }

    //     taxa.sort(Comparator.comparingDouble(TaxonomyResult.Taxon::getScore).reversed());

    //     List<TaxonomyResult.Taxon> topTaxa =
    //             new ArrayList<>(taxa.subList(0, Math.min(MAX_CANDIDATES, taxa.size())));

    //     TaxonomyResult.Taxon best = topTaxa.get(0);
    //     result.setBest(best);
    //     result.setCandidates(topTaxa);

    //     if (topTaxa.size() == 1) {
    //         if (best.getScore() >= MIN_CONFIDENT_SCORE) {
    //             result.setStatus("CONFIDENT");
    //         } else {
    //             result.setStatus("AMBIGUOUS");
    //         }
    //         return result;
    //     }

    //     TaxonomyResult.Taxon second = topTaxa.get(1);
    //     double ratio = calcRatio(best.getScore(), second.getScore());

    //     if (best.getScore() >= MIN_CONFIDENT_SCORE && ratio >= MIN_CONFIDENT_RATIO) {
    //         result.setStatus("CONFIDENT");
    //     } else {
    //         result.setStatus("AMBIGUOUS");
    //     }

    //     return result;
    // }

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
    protected void doHandleStageDone(StageRunResult<TaxonomyStageOutput> stageRunResult)
            throws JsonProcessingException {
        StageContext taxonomyStage = stageRunResult.getStageContext();
        TaxonomyStageOutput taxonomyStageOutput = stageRunResult.getStageOutput();

        // TaxonomyResult taxonomySortedResults = null;
        // try {
        //     taxonomySortedResults = TaxonomyParser.parseKraken2Output(taxonomyStageOutput.getReport());

        // } catch (IOException e) {
        //     // TODO Auto-generated catch block
        //     logger.error(
        //             "Failed to parse Kraken report. reportPath={}",
        //             taxonomyStageOutput.getReport(),
        //             e);

        //     this.handleFail(taxonomyStage, stageRunResult.getWorkDir().toString());
        //     return;
        // }

        List<TaxonomyClassificationOutput> candicates = taxonomyStageOutput.getCandicates() == null
                ? Collections.emptyList()
                : taxonomyStageOutput.getCandicates();
        TaxonomyClassificationOutput comfirmedSpecies = taxonomyStageOutput.getComfirmedTaxonomy();
        
        TaxonomyResult taxonomyResult = new TaxonomyResult();
        Taxon best = null;

        boolean isConfident = taxonomyStageOutput.getStatus() == Constants.TaxonomyClassification.STATUS_CONFIDENT;
        if (isConfident && comfirmedSpecies == null) {
            logger.error("Confident taxonomy stage has no confirmed species. stage={}", taxonomyStage);
            this.handleFail(taxonomyStage, stageRunResult.getWorkDir() == null
                    ? null : stageRunResult.getWorkDir().toString(),
                    "Confident taxonomy stage has no confirmed species");
            return;
        }
        if(isConfident){
            best = new Taxon();
            best.setName(comfirmedSpecies.getSpeciesName());
            best.setScore(comfirmedSpecies.getScore());
            best.setRank("S");
            best.setTaxid(comfirmedSpecies.getSpeciesTaxId());
        }

        taxonomyResult.setBest(best);
        taxonomyResult.setStatus(taxonomyStageOutput.getStatus());
        taxonomyResult.setEvidenceResource(taxonomyStageOutput.getEvidenceResource());
        List<Taxon> resultCandicates = candicates.stream().map(
            (s)->{
                Taxon taxon = new Taxon();
                taxon.setName(s.getSpeciesName());
                taxon.setScore(s.getScore());
                taxon.setTaxid(s.getSpeciesTaxId());
                taxon.setRank("S");
                return taxon;
            }
        ).toList();

        taxonomyResult.setCandidates(resultCandicates);
        taxonomyResult.setBinningMetrics(taxonomyStageOutput.getBinningMetrics());

        Map<String, String> uploadConfig = new LinkedHashMap<>();
        if (taxonomyStageOutput.getMainBinFastaPath() != null) {
            String mainBinFastaUrl = this.createStoreObjectName(
                    taxonomyStage,
                    TaxonomicContigBinner.MAIN_BIN_FASTA_FILE_NAME);
            uploadConfig.put(
                    taxonomyStageOutput.getMainBinFastaPath().toAbsolutePath().toString(),
                    mainBinFastaUrl);
            taxonomyResult.setMainBinFastaUrl(mainBinFastaUrl);
        }
        if (taxonomyStageOutput.getContigAssignmentReportPath() != null) {
            String assignmentReportUrl = this.createStoreObjectName(
                    taxonomyStage,
                    TaxonomicContigBinner.ASSIGNMENT_REPORT_FILE_NAME);
            uploadConfig.put(
                    taxonomyStageOutput.getContigAssignmentReportPath().toAbsolutePath().toString(),
                    assignmentReportUrl);
            taxonomyResult.setContigAssignmentReportUrl(assignmentReportUrl);
        }

        if (!uploadConfig.isEmpty() && !this.batchUploadObjectsFromLocal(uploadConfig)) {
            logger.error("Failed to upload taxonomy binning artifacts. stage={}", taxonomyStage);
            this.handleFail(taxonomyStage, stageRunResult.getWorkDir() == null
                    ? null : stageRunResult.getWorkDir().toString(), "Failed to upload taxonomy binning artifacts");
            return;
        }


        String serializedResult = JsonUtil.toJson(taxonomyResult);
        BioPipelineStage patch = new BioPipelineStage();

        if (isConfident) {
            patch.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
        } else if (taxonomyStageOutput.getStatus() == Constants.TaxonomyClassification.STATUS_OUT_PANEL) {
            patch.setStatus(PIPELINE_STAGE_STATUS_NOT_APPLICABLE);
        } else {
            patch.setStatus(PIPELINE_STAGE_STATUS_ACTION_REQUIRED);
        }
        patch.setEndTime(new Date());
        patch.setOutputInline(serializedResult);

        
        int updateRes = updateStageFromVersion(patch, taxonomyStage.getRunStageId(), taxonomyStage.getVersion());
        // this.deleteStageResultDir(taxonomyStageOutput.getParentPath().toString());

        notifyStageDone(taxonomyStage, updateRes >= 1);
        return;
    }

    @Override
    protected Pair<Map<String, String>, StageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<TaxonomyStageOutput> stageRunResult) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'buildUploadConfigAndOutputUrlMap'");
    }



    

}
