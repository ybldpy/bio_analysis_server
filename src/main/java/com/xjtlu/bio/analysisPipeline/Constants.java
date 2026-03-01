package com.xjtlu.bio.analysisPipeline;

import java.util.Map;

public class Constants {

    public static class StageStatus {

        public static final int PIPELINE_STAGE_STATUS_PENDING = 0;
        public static final int PIPELINE_STAGE_STATUS_QUEUING = 1;
        public static final int PIPELINE_STAGE_STATUS_RUNNING = 2;
        public static final int PIPELINE_STAGE_STATUS_FAIL = 3;
        public static final int PIPELINE_STAGE_STATUS_FINISHED = 4;
        public static final int PIPELINE_STAGE_STATUS_ACTION_REQUIRED = 5;
        public static final int PIPELINE_STAGE_STATUS_NOT_APPLICABLE = 6;

    }

    public static class StageType {

        public static final int PIPELINE_STAGE_QC = 0; // 质控 fastp
        public static final int PIPELINE_STAGE_TAXONOMY = 10; // 物种鉴定 Kraken2/Mash

        // 比对 / 组装
        public static final int PIPELINE_STAGE_MAPPING = 20; // 有参比对 minimap2/bwa
        public static final int PIPELINE_STAGE_MAPPING_NO_REFSEQ = 21;
        

        public static final int PIPELINE_STAGE_ASSEMBLY = 30; // 无参拼装 SPAdes/Flye
        public static final int PIPELINE_STAGE_ASSEMBLY_POLISH = 31; // 抛光 Pilon/Racon/Medaka

        // 变异 / 一致性 / 深度（病毒常用）
        public static final int PIPELINE_STAGE_VARIANT_CALL = 40; // 变异调用 bcftools/snippy
        public static final int PIPELINE_STAGE_CONSENSUS = 41; // 一致性序列 bcftools consensus
        public static final int PIPELINE_STAGE_DEPTH_COVERAGE = 42; // 覆盖度/深度图 mosdepth

        // SNP & 溯源
        public static final int PIPELINE_STAGE_SNP_SINGLE = 70; // 单样本对近邻参考的SNP
        public static final int PIPELINE_STAGE_SNP_CORE = 71; // 多样本核心SNP/建树

        // 病原学特征（细菌模块）
        public static final int PIPELINE_STAGE_AMR = 60; // 耐药基因 AMRFinder/ResFinder
        public static final int PIPELINE_STAGE_VIRULENCE = 61; // 毒力因子 VFDB/abricate
        public static final int PIPELINE_STAGE_MLST = 62; // MLST 分型
        public static final int PIPELINE_STAGE_CGMLST = 63; // cgMLST chewBBACA
        public static final int PIPELINE_STAGE_SEROTYPE = 64; // 血清型（ECTyper/SeqSero2/Kaptive等）

        public static final int PIPELINE_STAGE_READ_LENGTH_DETECT = 80;

        // 物种鉴定
        public static final String PIPELINE_STAGE_NAME_TAXONOMY = "物种鉴定 (Taxonomy)";

        // 比对 / 组装相关
        public static final String PIPELINE_STAGE_NAME_ASSEMBLY_POLISH = "组装抛光 (Polishing)";
        public static final String PIPELINE_STAGE_NAME_CONSENSUS = "一致性序列 (Consensus)";
        public static final String PIPELINE_STAGE_NAME_DEPTH_COVERAGE = "深度分布图 (Depth / Coverage)";

        // 功能注释
        public static final String PIPELINE_STAGE_NAME_FUNC_ANNOTATION = "功能注释 (Functional annotation)";

        // 细菌病原学特征
        public static final String PIPELINE_STAGE_NAME_AMR = "耐药基因分析 (AMR)";
        public static final String PIPELINE_STAGE_NAME_VIRULENCE = "毒力因子分析 (Virulence)";
        public static final String PIPELINE_STAGE_NAME_MLST = "MLST 分型";
        public static final String PIPELINE_STAGE_NAME_CGMLST = "cgMLST 分型";
        public static final String PIPELINE_STAGE_NAME_SEROTYPE = "血清型预测 (Serotyping)";

        // SNP / 溯源
        public static final String PIPELINE_STAGE_NAME_SNP_SINGLE = "单样本 SNP 分析";
        public static final String PIPELINE_STAGE_NAME_SNP_CORE = "核心 SNP 分析 / 建树";

        public static final String PIPELINE_STAGE_NAME_QC = "质控 (QC)";
        public static final String PIPELINE_STAGE_NAME_ASSEMBLY = "组装 (Assembly)";
        public static final String PIPELINE_STAGE_NAME_MAPPING = "有参比对 (Mapping)";
        public static final String PIPELINE_STAGE_NAME_VARIANT = "变异检测 (Variant calling)";
        

        public static final Map<Integer, String> STAGE_NAME_MAP = Map.ofEntries(
                Map.entry(PIPELINE_STAGE_QC, PIPELINE_STAGE_NAME_QC),
                Map.entry(PIPELINE_STAGE_ASSEMBLY, PIPELINE_STAGE_NAME_ASSEMBLY),
                Map.entry(PIPELINE_STAGE_MAPPING, PIPELINE_STAGE_NAME_MAPPING),
                Map.entry(PIPELINE_STAGE_VARIANT_CALL, PIPELINE_STAGE_NAME_VARIANT),
                Map.entry(PIPELINE_STAGE_CONSENSUS, PIPELINE_STAGE_NAME_CONSENSUS),
                Map.entry(PIPELINE_STAGE_TAXONOMY, PIPELINE_STAGE_NAME_TAXONOMY),
                Map.entry(PIPELINE_STAGE_MLST, PIPELINE_STAGE_NAME_MLST),
                Map.entry(PIPELINE_STAGE_AMR, PIPELINE_STAGE_NAME_AMR),
                Map.entry(PIPELINE_STAGE_SEROTYPE, PIPELINE_STAGE_NAME_SEROTYPE),
                Map.entry(PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_NAME_VIRULENCE)
            );

    }

    private Constants(){

    }

}
