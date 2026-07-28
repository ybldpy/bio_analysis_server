package com.xjtlu.bio.analysisPipeline.stageResult;


import java.util.List;

import com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput.MetagenomicsShotgunAnalysisStageOutput;

public class MetagenomicShotgunStageResult implements StageResult{


    /**
     * Kraken2 生成的分类汇总报告。
     */
    private String kraken2Report;

    /**
     * Bracken 生成的物种级丰度表。
     */
    private String speciesAbundance;

    public void setKraken2Report(String kraken2Report) {
        this.kraken2Report = kraken2Report;
    }

    public void setSpeciesAbundance(String speciesAbundance) {
        this.speciesAbundance = speciesAbundance;
    }

    public void setAlphaDiversity(String alphaDiversity) {
        this.alphaDiversity = alphaDiversity;
    }

    public void setContigs(String contigs) {
        this.contigs = contigs;
    }

    public void setAssemblySummary(String assemblySummary) {
        this.assemblySummary = assemblySummary;
    }

    public void setPredictedGenes(String predictedGenes) {
        this.predictedGenes = predictedGenes;
    }

    public void setPredictedProteins(String predictedProteins) {
        this.predictedProteins = predictedProteins;
    }

    public void setFunctionalAnnotation(String functionalAnnotation) {
        this.functionalAnnotation = functionalAnnotation;
    }

    public void setFunctionalAbundance(String functionalAbundance) {
        this.functionalAbundance = functionalAbundance;
    }

    public void setBinsDir(List<String> binsUrls) {
        this.binsUrls = binsUrls;
    }

    public void setBinQuality(String binQuality) {
        this.binQuality = binQuality;
    }

    public String getKraken2Report() {
        return kraken2Report;
    }

    public String getSpeciesAbundance() {
        return speciesAbundance;
    }

    public String getAlphaDiversity() {
        return alphaDiversity;
    }

    public String getContigs() {
        return contigs;
    }

    public String getAssemblySummary() {
        return assemblySummary;
    }

    public String getPredictedGenes() {
        return predictedGenes;
    }

    public String getPredictedProteins() {
        return predictedProteins;
    }

    public String getFunctionalAnnotation() {
        return functionalAnnotation;
    }

    public String getFunctionalAbundance() {
        return functionalAbundance;
    }

    public List<String> getBinsUrls() {
        return binsUrls;
    }

    public String getBinQuality() {
        return binQuality;
    }

    /**
     * 基于物种丰度计算的单样本 Alpha 多样性结果。
     */
    private String alphaDiversity;

    /**
     * 宏基因组组装得到的 contigs。
     */
    private String contigs;

    /**
     * contig 数量、总长度、N50 等组装统计。
     */
    private String assemblySummary;

    /**
     * 预测基因的核酸 FASTA。
     */
    private String predictedGenes;

    /**
     * 预测蛋白的氨基酸 FASTA。
     */
    private String predictedProteins;

    public MetagenomicShotgunStageResult(String kraken2Report, String speciesAbundance, String alphaDiversity,
            String contigs, String assemblySummary, String predictedGenes, String predictedProteins,
            String functionalAnnotation, String functionalAbundance, List<String> binsUrls, String binQuality) {
        this.kraken2Report = kraken2Report;
        this.speciesAbundance = speciesAbundance;
        this.alphaDiversity = alphaDiversity;
        this.contigs = contigs;
        this.assemblySummary = assemblySummary;
        this.predictedGenes = predictedGenes;
        this.predictedProteins = predictedProteins;
        this.functionalAnnotation = functionalAnnotation;
        this.functionalAbundance = functionalAbundance;
        this.binsUrls = binsUrls;
        this.binQuality = binQuality;
    }

    /**
     * 基因或蛋白功能注释结果。
     */
    private String functionalAnnotation;

    /**
     * 功能丰度结果；未实现功能定量时可以先不加。
     */
    private String functionalAbundance;

    /**
     * 分箱得到的 MAG 文件目录。
     */
    private List<String> binsUrls;

    /**
     * MAG 完整度、污染度等质量结果。
     */
    private String binQuality;

    



}
