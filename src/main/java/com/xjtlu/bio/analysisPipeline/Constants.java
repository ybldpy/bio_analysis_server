package com.xjtlu.bio.analysisPipeline;

import java.util.Locale;
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

    public static class PipelineType {

        public static final int PIPELINE_VIRUS = 100;
        public static final int PIPELINE_VIRUS_COVID = 101;
        public static final int PIPELINE_REGULAR_BACTERIA = 200;

        public static final int PIPELINE_SNP_ANALYSIS = 300;
        public static final int PIPELINE_SNP_SUB_ANALYSIS = 301;
        public static final int PIPELINE_SNP_ANALYSIS_MERGE = 302;

    }

    public static class SequencingPlatform {

        private SequencingPlatform() {
        }

        public static final int UNKNOWN = 0;

        // ---------- short-read platform family: 1000 ~ 1999 ----------
        public static final int ILLUMINA = 1001;
        public static final int BGI_MGI_DNBSEQ = 1002;
        public static final int ION_TORRENT = 1003;
        public static final int SOLID = 1004;
        public static final int ROCHE_454 = 1005;
        public static final int ELEMENT = 1006;
        public static final int ULTIMA = 1007;
        public static final int SINGULAR = 1008;
        public static final int GENAPSYS = 1009;
        public static final int COMPLETE_GENOMICS = 1010;
        public static final int HELICOS = 1011;

        // ---------- long-read platform family: 2000 ~ 2999 ----------
        public static final int ONT = 2001;
        public static final int PACBIO = 2002;

        public static boolean isShortReadPlatform(int platformCode) {
            return platformCode >= 1000 && platformCode < 2000;
        }

        public static boolean isLongReadPlatform(int platformCode) {
            return platformCode >= 2000 && platformCode < 3000;
        }

        public static boolean isKnown(int platformCode) {
            return platformCode != UNKNOWN;
        }

        public static String toName(int platformCode) {
            return switch (platformCode) {
                case ILLUMINA -> "ILLUMINA";
                case BGI_MGI_DNBSEQ -> "BGI_MGI_DNBSEQ";
                case ION_TORRENT -> "ION_TORRENT";
                case SOLID -> "SOLID";
                case ROCHE_454 -> "ROCHE_454";
                case ELEMENT -> "ELEMENT";
                case ULTIMA -> "ULTIMA";
                case SINGULAR -> "SINGULAR";
                case GENAPSYS -> "GENAPSYS";
                case COMPLETE_GENOMICS -> "COMPLETE_GENOMICS";
                case HELICOS -> "HELICOS";
                case ONT -> "ONT";
                case PACBIO -> "PACBIO";
                default -> "UNKNOWN";
            };
        }

        public static int fromString(String rawValue) {
            if (rawValue == null || rawValue.isBlank()) {
                return UNKNOWN;
            }

            String s = normalize(rawValue);

            // ---------- long-read first ----------
            if (containsAny(s,
                    "ONT",
                    "NANOPORE",
                    "OXFORDNANOPORE",
                    "GRIDION",
                    "GRIDIONX5",
                    "MINION",
                    "PROMETHION",
                    "FLONGLE")) {
                return ONT;
            }

            if (containsAny(s,
                    "PACBIO",
                    "SMRT",
                    "SEQUEL",
                    "SEQUELII",
                    "SEQUEL2",
                    "SEQUELIIE",
                    "REVIO",
                    "RSII")) {
                return PACBIO;
            }

            // ---------- short-read ----------
            if (containsAny(s,
                    "ILLUMINA",
                    "HISEQ",
                    "NOVASEQ",
                    "NEXTSEQ",
                    "MISEQ",
                    "MINISEQ",
                    "ISEQ")) {
                return ILLUMINA;
            }

            if (containsAny(s,
                    "BGI",
                    "MGI",
                    "DNBSEQ",
                    "COOLMPS",
                    "T7",
                    "T10",
                    "T1",
                    "G99",
                    "G400",
                    "G50",
                    "E25")) {
                return BGI_MGI_DNBSEQ;
            }

            if (containsAny(s,
                    "IONTORRENT",
                    "GENESTUDIOS5",
                    "IONS5",
                    "PGM",
                    "PROTON")) {
                return ION_TORRENT;
            }

            if (containsAny(s, "SOLID")) {
                return SOLID;
            }

            if (containsAny(s,
                    "ROCHE454",
                    "454",
                    "GSFLX")) {
                return ROCHE_454;
            }

            if (containsAny(s,
                    "ELEMENT",
                    "AVITI")) {
                return ELEMENT;
            }

            if (containsAny(s,
                    "ULTIMA",
                    "UG100")) {
                return ULTIMA;
            }

            if (containsAny(s,
                    "SINGULAR",
                    "G4")) {
                return SINGULAR;
            }

            if (containsAny(s,
                    "GENAPSYS")) {
                return GENAPSYS;
            }

            if (containsAny(s,
                    "COMPLETEGENOMICS")) {
                return COMPLETE_GENOMICS;
            }

            if (containsAny(s,
                    "HELICOS")) {
                return HELICOS;
            }

            return UNKNOWN;
        }

        private static String normalize(String rawValue) {
            return rawValue.trim()
                    .toUpperCase(Locale.ROOT)
                    .replace(" ", "")
                    .replace("-", "")
                    .replace("_", "");
        }

        private static boolean containsAny(String value, String... candidates) {
            for (String candidate : candidates) {
                if (value.contains(candidate)) {
                    return true;
                }
            }
            return false;
        }
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

        // 病毒 FASTA sequence-based 分析
        public static final int PIPELINE_STAGE_REFERENCE_COMPARISON = 43; // input FASTA vs reference FASTA，输出 PAF + difference TSV

        // SNP & 溯源
        public static final int PIPELINE_STAGE_SNP_SINGLE = 70; // 单样本对近邻参考的SNP
        public static final int PIPELINE_STAGE_SNP_CORE = 71; // 多样本核心SNP/建树
        public static final int PIPELINE_STAGE_SNP_ANNOTATION = 72; // SNP注释
        public static final int PIPELINE_STAGE_SNP_MERGE_RESULT = 73;

        // 病原学特征（细菌模块）
        public static final int PIPELINE_STAGE_AMR = 60; // 耐药基因 AMRFinder/ResFinder
        public static final int PIPELINE_STAGE_VIRULENCE = 61; // 毒力因子 VFDB/abricate
        public static final int PIPELINE_STAGE_MLST = 62; // MLST 分型
        public static final int PIPELINE_STAGE_CGMLST = 63; // cgMLST chewBBACA
        public static final int PIPELINE_STAGE_SEROTYPE = 64; // 血清型（ECTyper/SeqSero2/Kaptive等）

        public static final int PIPELINE_STAGE_READ_INSPECT = 80;

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

        public static final String PIPELINE_STAGE_NAME_READ_INSPECT = "预处理";

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
                Map.entry(PIPELINE_STAGE_VIRULENCE, PIPELINE_STAGE_NAME_VIRULENCE),
                Map.entry(PIPELINE_STAGE_READ_INSPECT, PIPELINE_STAGE_NAME_READ_INSPECT));
    }

    private Constants() {

    }

}
