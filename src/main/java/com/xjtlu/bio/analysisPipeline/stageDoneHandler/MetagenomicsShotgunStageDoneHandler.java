package com.xjtlu.bio.analysisPipeline.stageDoneHandler;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.xjtlu.bio.analysisPipeline.Constants;
import com.xjtlu.bio.analysisPipeline.context.runtime.StageContext;
import com.xjtlu.bio.analysisPipeline.stageResult.MetagenomicShotgunStageResult;
import com.xjtlu.bio.analysisPipeline.stageResult.StageResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.StageRunResult;
import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MetagenomicsShotgunAnalysisStageOutput;

public class MetagenomicsShotgunStageDoneHandler
        extends AbstractStageDoneHandler<MetagenomicsShotgunAnalysisStageOutput>
        implements StageDoneHandler<MetagenomicsShotgunAnalysisStageOutput> {

    @Override
    public int getType() {
        // TODO Auto-generated method stub
        return Constants.StageType.PIPELINE_STAGE_METAGENOMICS_SHORTGUN;
    }

    @Override
    protected Pair<Map<String, String>, MetagenomicShotgunStageResult> buildUploadConfigAndOutputUrlMap(
            StageRunResult<MetagenomicsShotgunAnalysisStageOutput> stageRunResult) {

        MetagenomicsShotgunAnalysisStageOutput metagenomicsShotgunAnalysisStageOutput = stageRunResult.getStageOutput();
        List<Path> binsPaths = new ArrayList<>();
        List<String> binsUrls = new ArrayList<>();
        Map<String, String> uploadMap = new HashMap<>();

        StageContext stageContext = stageRunResult.getStageContext();


        /*
         * MetaBAT2 bins
         */
        for (File f : metagenomicsShotgunAnalysisStageOutput.getBinsDir().toFile().listFiles()) {
            if (Constants.SequenceInput.isFasta(f.getName())) {
                String binUrl = this.createStoreObjectName(stageContext, f.getName());
                binsUrls.add(binUrl);
                uploadMap.put(f.toPath().toString(), binUrl);
            }
        }

        /*
         * Kraken2 分类报告
         */
        Path kraken2ReportPath = metagenomicsShotgunAnalysisStageOutput.getKraken2ReportPath();

        String kraken2ReportUrl = this.createStoreObjectName(
                stageContext,
                kraken2ReportPath.getFileName().toString());

        uploadMap.put(
                kraken2ReportPath.toString(),
                kraken2ReportUrl);

        /*
         * Bracken 物种丰度
         */
        Path speciesAbundancePath = metagenomicsShotgunAnalysisStageOutput.getSpeciesAbundancePath();

        String speciesAbundanceUrl = this.createStoreObjectName(
                stageContext,
                speciesAbundancePath.getFileName().toString());

        uploadMap.put(
                speciesAbundancePath.toString(),
                speciesAbundanceUrl);

        /*
         * Alpha diversity
         */
        Path alphaDiversityPath = metagenomicsShotgunAnalysisStageOutput.getAlphaDiversityPath();

        String alphaDiversityUrl = this.createStoreObjectName(
                stageContext,
                alphaDiversityPath.getFileName().toString());

        uploadMap.put(
                alphaDiversityPath.toString(),
                alphaDiversityUrl);

        /*
         * MEGAHIT contigs
         */
        Path contigsPath = metagenomicsShotgunAnalysisStageOutput.getContigsPath();

        String contigsUrl = this.createStoreObjectName(
                stageContext,
                contigsPath.getFileName().toString());

        uploadMap.put(
                contigsPath.toString(),
                contigsUrl);

        /*
         * 组装统计
         */
        Path assemblySummaryPath = metagenomicsShotgunAnalysisStageOutput.getAssemblySummaryPath();

        String assemblySummaryUrl = this.createStoreObjectName(
                stageContext,
                assemblySummaryPath.getFileName().toString());

        uploadMap.put(
                assemblySummaryPath.toString(),
                assemblySummaryUrl);

        /*
         * Prodigal 预测基因
         */
        Path predictedGenesPath = metagenomicsShotgunAnalysisStageOutput.getPredictedGenesPath();

        String predictedGenesUrl = this.createStoreObjectName(
                stageContext,
                predictedGenesPath.getFileName().toString());

        uploadMap.put(
                predictedGenesPath.toString(),
                predictedGenesUrl);

        /*
         * Prodigal 预测蛋白
         */
        Path predictedProteinsPath = metagenomicsShotgunAnalysisStageOutput.getPredictedProteinsPath();

        String predictedProteinsUrl = this.createStoreObjectName(
                stageContext,
                predictedProteinsPath.getFileName().toString());

        uploadMap.put(
                predictedProteinsPath.toString(),
                predictedProteinsUrl);

        /*
         * eggNOG 功能注释
         */
        Path functionalAnnotationPath = metagenomicsShotgunAnalysisStageOutput.getFunctionalAnnotationPath();

        String functionalAnnotationUrl = this.createStoreObjectName(
                stageContext,
                functionalAnnotationPath.getFileName().toString());

        uploadMap.put(
                functionalAnnotationPath.toString(),
                functionalAnnotationUrl);

        /*
         * 功能丰度目前未实现
         */
        String functionalAbundanceUrl = null;

        
        

        /*
         * CheckM2 质量报告
         */
        Path binQualityPath = metagenomicsShotgunAnalysisStageOutput.getBinQualityPath();

        String binQualityUrl = this.createStoreObjectName(
                stageContext,
                binQualityPath.getFileName().toString());

        uploadMap.put(
                binQualityPath.toString(),
                binQualityUrl);

        MetagenomicShotgunStageResult stageResult = new MetagenomicShotgunStageResult(
                kraken2ReportUrl,
                speciesAbundanceUrl,
                alphaDiversityUrl,
                contigsUrl,
                assemblySummaryUrl,
                predictedGenesUrl,
                predictedProteinsUrl,
                functionalAnnotationUrl,
                functionalAbundanceUrl,
                binsUrls,
                binQualityUrl);

        return Pair.of(
                uploadMap,
                stageResult);

    }

}
