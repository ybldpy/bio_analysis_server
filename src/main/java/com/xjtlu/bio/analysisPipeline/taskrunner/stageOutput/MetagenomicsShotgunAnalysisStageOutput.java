package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class MetagenomicsShotgunAnalysisStageOutput implements StageOutput {



    /**
     * Kraken2 生成的分类汇总报告。
     */
    private final Path kraken2ReportPath;

    /**
     * Bracken 生成的物种级丰度表。
     */
    private final Path speciesAbundancePath;

    /**
     * 基于物种丰度计算的单样本 Alpha 多样性结果。
     */
    private final Path alphaDiversityPath;

    /**
     * 宏基因组组装得到的 contigs。
     */
    private final Path contigsPath;

    /**
     * contig 数量、总长度、N50 等组装统计。
     */
    private final Path assemblySummaryPath;

    /**
     * 预测基因的核酸 FASTA。
     */
    private final Path predictedGenesPath;

    /**
     * 预测蛋白的氨基酸 FASTA。
     */
    private final Path predictedProteinsPath;

    /**
     * 基因或蛋白功能注释结果。
     */
    private final Path functionalAnnotationPath;

    /**
     * 功能丰度结果；未实现功能定量时可以先不加。
     */
    private final Path functionalAbundancePath;

    /**
     * 分箱得到的 MAG 文件目录。
     */
    private final Path binsDir;

    /**
     * MAG 完整度、污染度等质量结果。
     */
    private final Path binQualityPath;



    






    public Path getContigsPath() {
        return contigsPath;
    }





    public Path getAssemblySummaryPath() {
        return assemblySummaryPath;
    }





    public Path getPredictedGenesPath() {
        return predictedGenesPath;
    }





    public Path getPredictedProteinsPath() {
        return predictedProteinsPath;
    }





    public Path getFunctionalAnnotationPath() {
        return functionalAnnotationPath;
    }





    public Path getFunctionalAbundancePath() {
        return functionalAbundancePath;
    }





    public Path getBinsDir() {
        return binsDir;
    }





    public Path getBinQualityPath() {
        return binQualityPath;
    }





    





    





    





    public MetagenomicsShotgunAnalysisStageOutput(Path kraken2ReportPath, Path speciesAbundancePath,
            Path alphaDiversityPath, Path contigsPath, Path assemblySummaryPath, Path predictedGenesPath,
            Path predictedProteinsPath, Path functionalAnnotationPath, Path functionalAbundancePath, Path binsDir,
            Path binQualityPath) {
        this.kraken2ReportPath = kraken2ReportPath;
        this.speciesAbundancePath = speciesAbundancePath;
        this.alphaDiversityPath = alphaDiversityPath;
        this.contigsPath = contigsPath;
        this.assemblySummaryPath = assemblySummaryPath;
        this.predictedGenesPath = predictedGenesPath;
        this.predictedProteinsPath = predictedProteinsPath;
        this.functionalAnnotationPath = functionalAnnotationPath;
        this.functionalAbundancePath = functionalAbundancePath;
        this.binsDir = binsDir;
        this.binQualityPath = binQualityPath;
    }





    public Path getKraken2ReportPath() {
        return kraken2ReportPath;
    }





    public Path getSpeciesAbundancePath() {
        return speciesAbundancePath;
    }





    public Path getAlphaDiversityPath() {
        return alphaDiversityPath;
    }





    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        return kraken2ReportPath.getParent();
    }

}
