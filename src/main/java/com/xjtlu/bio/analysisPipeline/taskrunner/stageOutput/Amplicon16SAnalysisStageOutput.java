package com.xjtlu.bio.analysisPipeline.taskrunner.stageOutput;

import java.nio.file.Path;

public class Amplicon16SAnalysisStageOutput implements StageOutput {

    /**
     * ASV 丰度计数表。
     */
    private final Path asvTablePath;

    /**
     * ASV 代表序列。
     */
    private final Path representativeSequencesPath;

    /**
     * ASV 分类注释结果。
     */
    private final Path taxonomyPath;

    /**
     * 各分类层级的相对丰度结果。
     */
    private final Path relativeAbundancePath;

    /**
 *      属水平丰度统计结果，包含原始计数和相对丰度。
    */
    private final Path genusAbundancePath;

    /**
     * Alpha 多样性结果。
     */
    private final Path alphaDiversityPath;

    /**
     * Beta 多样性距离矩阵。
     * 单样本时可以为 null。
     */
    private final Path betaDiversityPath;

    public Path getGenusAbundancePath() {
        return genusAbundancePath;
    }




    /**
     * 分析汇总结果。
     */
    private final Path summaryPath;

    public Amplicon16SAnalysisStageOutput(Path asvTablePath, Path representativeSequencesPath,
            Path taxonomyPath, Path relativeAbundancePath, Path genusAbundancePath, Path alphaDiversityPath, Path betaDiversityPath,
            Path summaryPath) {
        this.asvTablePath = asvTablePath;
        this.representativeSequencesPath = representativeSequencesPath;
        this.taxonomyPath = taxonomyPath;
        this.relativeAbundancePath = relativeAbundancePath;
        this.alphaDiversityPath = alphaDiversityPath;
        this.betaDiversityPath = betaDiversityPath;
        this.summaryPath = summaryPath;
        this.genusAbundancePath = genusAbundancePath;
    }




    public Path getAsvTablePath() {
        return asvTablePath;
    }




    public Path getRepresentativeSequencesPath() {
        return representativeSequencesPath;
    }




    public Path getTaxonomyPath() {
        return taxonomyPath;
    }




    public Path getRelativeAbundancePath() {
        return relativeAbundancePath;
    }




    public Path getAlphaDiversityPath() {
        return alphaDiversityPath;
    }




    public Path getBetaDiversityPath() {
        return betaDiversityPath;
    }




    public Path getSummaryPath() {
        return summaryPath;
    }




    @Override
    public Path getParentPath() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getParentPath'");
    }

}
