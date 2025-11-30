package com.xjtlu.bio.service;

import java.io.File;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.javassist.tools.framedump;
import org.bouncycastle.jcajce.provider.asymmetric.ec.GMSignatureSpi.sha256WithSM2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.common.StageRunResult;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioPipelineStageExample;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.entity.BioSampleExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.taskrunner.PipelineStageTaskDispatcher;
import com.xjtlu.bio.utils.ParameterUtil;


import jakarta.annotation.Resource;



class BioPipelineStagesBuilder {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static Map<String, Object> substractStageParams(String stageName, Map<String, Object> pipelineStageParams) {
        Object obj = pipelineStageParams.get(stageName);
        if (obj == null) {
            return new HashMap<>();
        }
        return (Map) obj;
    }


    public static List<BioPipelineStage> buildBacteriaStages(){
        //todo
        return null;
    }



    public static List<BioPipelineStage> buildVirusStages(long pid, int pipelineType, BioSample bioSample, Map<String,Object> pipelineStageParams) throws JsonProcessingException{

        ArrayList<BioPipelineStage> stages = new ArrayList<>(8);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

    
        String refSeqKey = "refSeq";
        String refSeq = ParameterUtil.getStrFromMap(refSeqKey, pipelineStageParams);



        int index = 0;
        BioPipelineStage qc = new BioPipelineStage();
        

        Map<String,String> qcInputMap = new HashMap<>();


        qcInputMap.put("r1", qcInputRead1);
        qcInputMap.put("r2", qcInputRead2);
        String qcInputMapStr = null;
        qc.setStageIndex(index);
        qc.setStageName("质控(QC)");
        qc.setStageType(PipelineService.PIPELINE_STAGE_QC);
        qc.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        qc.setPipelineId(pid);
        qc.setInputUrl(qcInputMapStr);
        stages.add(qc);

        index++;

        BioPipelineStage assembly = new BioPipelineStage();

        Map<String,Object> assemblyParams = substractStageParams("assembly", pipelineStageParams);
        String assemlyParamsStr = assemblyParams == null? null: objectMapper.writeValueAsString(assemblyParams);
        assembly.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        assembly.setPipelineId(pid);
        assembly.setStageIndex(index);
        assembly.setParameters(assemlyParamsStr);

    
        if (StringUtils.isBlank(refSeq)) {
            assembly.setStageType(PipelineService.PIPELINE_STAGE_ASSEMBLY);
            assembly.setStageName("组装");
        } else {
            assembly.setStageType(PipelineService.PIPELINE_STAGE_MAPPING);
            assemblyParams.put("refSeq", refSeq);
            assembly.setParameters(objectMapper.writeValueAsString(assemlyParamsStr));
            assembly.setStageName("参考");
        }

        stages.add(assembly);
        index++;

        if (refSeq == null && pipelineType != PipelineService.PIPELINE_VIRUS_COVID) {
            return stages;
        }



        Map<String,Object> varientStageParams = substractStageParams("varient", pipelineStageParams);
        varientStageParams.put("refSeq", refSeq);
        BioPipelineStage varient = new BioPipelineStage();
        varient.setStageName(PipelineService.PIPELINE_STAGE_NAME_VARIANT);
        varient.setPipelineId(pid);
        varient.setStageIndex(index);
        
        varient.setParameters(objectMapper.writeValueAsString(varientStageParams));
        varient.setStageType(PipelineService.PIPELINE_STAGE_VARIANT_CALL);
        varient.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);

        stages.add(varient);

        index++;

        BioPipelineStage consensus = new BioPipelineStage();
        Map<String,Object> consesusParams = substractStageParams("consensus", pipelineStageParams);
        consesusParams.put("refSeq", refSeq);
        consensus.setStageName("生成一致性序列");
        consensus.setPipelineId(pid);
        consensus.setStageIndex(index);
        consensus.setParameters(objectMapper.writeValueAsString(consesusParams));
        consensus.setStageType(PipelineService.PIPELINE_STAGE_VARIANT_CALL);
        consensus.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(consensus);

        if (pipelineType != PipelineService.PIPELINE_VIRUS_COVID) {
            return stages;
        }


        Map<String,Object> snpParams = substractStageParams("snp", pipelineStageParams);
        snpParams.put("refSeq", refSeq);
        BioPipelineStage snp = new BioPipelineStage();
        snp.setStageName("SNP注释");
        snp.setPipelineId(pid);
        snp.setStageIndex(index);
        snp.setParameters(objectMapper.writeValueAsString(snp));
        snp.setStageType(PipelineService.PIPELINE_STAGE_SNP_CORE);
        snp.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);

        index++;

        BioPipelineStage depthConverage = new BioPipelineStage();
        Map<String,Object> depthParams = substractStageParams("depth", pipelineStageParams);
        depthParams.put("refSeq",refSeq);
        depthConverage.setStageName("深度分布图");
        depthConverage.setPipelineId(pid);
        depthConverage.setStageIndex(index);
        depthConverage.setParameters(objectMapper.writeValueAsString(depthParams));
        depthConverage.setStageType(PipelineService.PIPELINE_STAGE_DEPTH_COVERAGE);
        depthConverage.setStatus(PipelineService.PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);
        index++;
        return stages;


    }

}

@Service
public class PipelineService {

    @Resource
    private BioAnalysisPipelineMapper analysisPipelineMapper;

    @Resource
    private BioPipelineStageMapper bioPipelineStageMapper;
    @Resource
    private BioAnalysisStageMapperExtension bioAnalysisStageMapperExtension;

    @Resource
    private BioSampleMapper bioSampleMapper;

    @Resource
    private BioAnalysisPipelineMapper bioAnalysisPipelineMapper;

    @Resource
    private SampleService sampleService;

    @Resource
    private MinioService minioService;

    @Resource
    private PipelineStageTaskDispatcher pipelineStageTaskDispatcher;

    @Value("${analysisPipeline.stage.baseOutputPath}")
    private String stagesOutputBasePath;

    private Set<Integer> bioAnalysisPipelineLockSet = ConcurrentHashMap.newKeySet();

    private ObjectMapper jsonMapper = new ObjectMapper();

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;

    public static final int PIPELINE_STAGE_STATUS_PENDING = 0;
    public static final int PIPELINE_STAGE_STATUS_QUEUING = 1;
    public static final int PIPELINE_STAGE_STATUS_RUNNING = 2;
    public static final int PIPELINE_STAGE_STATUS_FAIL = 3;
    public static final int PIPELINE_STAGE_STATUS_FINISHED = 4;

    public static final String PIPELINE_REFSEQ_ACCESSION_KEY = "refSeq";

    public static final String PIPELINE_STAGE_NAME_QC = "质控 (QC)";
    public static final String PIPELINE_STAGE_NAME_ASSEMBLY = "组装 (Assembly)";
    public static final String PIPELINE_STAGE_NAME_MAPPING = "有参比对 (Mapping)";
    public static final String PIPELINE_STAGE_NAME_VARIANT = "变异检测 (Variant calling)";

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

    public static final String PIPELINE_STAGE_QC_INPUT_R1 = "r1";
    public static final String PIPELINE_STAGE_QC_INPUT_R2 = "r2";

    public static final String PIPELINE_STAGE_QC_OUTPUT_R1 = "trimmed_r1";
    public static final String PIPELINE_STAGE_QC_OUTPUT_R2 = "trimmed_r2";
    public static final String PIPELINE_STAGE_QC_OUTPUI_JSON = "qc_json";
    public static final String PIPELINE_STAGE_QC_OUTPUT_HTML = "qc_html";

    public static final String PIPELINE_STAGE_MAPPING_OUTPUT_BAM_KEY = "bam";
    public static final String PIPELINE_STAGE_MAPPING_OUTPUT_BAM_INDEX_KEY = "bamIndex";

    public static final String PIPELINE_STAGE_ASSEMBLY_OUTPUT_CONTIGS_KEY = "contigs";
    public static final String PIPELINE_STAGE_ASSEMBLY_OUTPUT_SCAFFOLDS_KEY = "scaffold";

    public static final String PIPELINE_STAGE_VARIENT_OUTPUT_VCF_GZ = "vcf.gz";
    public static final String PIPELINE_STAGE_VARIENT_OUTPUT_VCF_TBI = "vcf.tbi";

    public static final String PIPELINE_STAGE_CONSENSUS_OUTPUT_CONSENSUSFA = "consensus";

    public static final int PIPELINE_STAGE_QC = 0; // 质控 fastp
    public static final int PIPELINE_STAGE_TAXONOMY = 10; // 物种鉴定 Kraken2/Mash

    // 比对 / 组装
    public static final int PIPELINE_STAGE_MAPPING = 20; // 有参比对 minimap2/bwa
    public static final int PIPELINE_STAGE_ASSEMBLY = 30; // 无参拼装 SPAdes/Flye
    public static final int PIPELINE_STAGE_ASSEMBLY_POLISH = 31; // 抛光 Pilon/Racon/Medaka

    // 变异 / 一致性 / 深度（病毒常用）
    public static final int PIPELINE_STAGE_VARIANT_CALL = 40; // 变异调用 bcftools/snippy
    public static final int PIPELINE_STAGE_CONSENSUS = 41; // 一致性序列 bcftools consensus
    public static final int PIPELINE_STAGE_DEPTH_COVERAGE = 42; // 覆盖度/深度图 mosdepth

    // 功能注释（可选通用）
    public static final int PIPELINE_STAGE_FUNC_ANNOTATION = 50; // Prokka/Bakta/eggNOG

    // 病原学特征（细菌模块）
    public static final int PIPELINE_STAGE_AMR = 60; // 耐药基因 AMRFinder/ResFinder
    public static final int PIPELINE_STAGE_VIRULENCE = 61; // 毒力因子 VFDB/abricate
    public static final int PIPELINE_STAGE_MLST = 62; // MLST 分型
    public static final int PIPELINE_STAGE_CGMLST = 63; // cgMLST chewBBACA
    public static final int PIPELINE_STAGE_SEROTYPE = 64; // 血清型（ECTyper/SeqSero2/Kaptive等）

    // SNP & 溯源
    public static final int PIPELINE_STAGE_SNP_SINGLE = 70; // 单样本对近邻参考的SNP
    public static final int PIPELINE_STAGE_SNP_CORE = 71; // 多样本核心SNP/建树

    private boolean isLegalPipelineType(int pipelineType) {
        return pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID
                || pipelineType == PIPELINE_VIRUS_BACKTERIA;
    }

    
    // 0: success
    //-1: error
    @Transactional(rollbackFor = Exception.class)
    public int updateRunning(long stageId){
        try{
            return this.bioAnalysisStageMapperExtension.updateStatusTo(stageId, PipelineService.PIPELINE_STAGE_STATUS_QUEUING, PipelineService.PIPELINE_STAGE_STATUS_RUNNING) == 1? 0:-1;
        }catch(Exception e){
            return -1;
        }

    }


    private int mapSampleTypeToPipelineType(int sampleType) {
        if (sampleType == SampleService.SAMPLE_TYPE_VIRUS) {
            return PIPELINE_VIRUS;
        }
        if (sampleType == SampleService.SAMPLE_TYPE_BACTERIA) {
            return PIPELINE_VIRUS_BACKTERIA;
        }
        return PIPELINE_VIRUS_COVID;
    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Long> createPipeline(BioSample bioSample,
            Map<String, Object> pipelineStageParams) {

        BioAnalysisPipeline bioAnalysisPipeline = new BioAnalysisPipeline();
        bioAnalysisPipeline.setPipelineType(mapSampleTypeToPipelineType(bioSample.getSampleType()));
        bioAnalysisPipeline.setSampleId(bioSample.getSid());
        int insertRes = this.bioAnalysisPipelineMapper.insert(bioAnalysisPipeline);
        if (insertRes < 1) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        List<BioPipelineStage> stages = this.buildPipelineStages(bioSample, bioAnalysisPipeline, pipelineStageParams);
        if (stages == null || stages.isEmpty()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        insertRes = this.bioAnalysisStageMapperExtension.batchInsert(stages);
        if (insertRes != stages.size()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<Long>(Result.INTERNAL_FAIL, -1l, "创建分析流水线错误");
        }

        return new Result<Long>(Result.SUCCESS, bioAnalysisPipeline.getPipelineId(), null);
    }

    private List<BioPipelineStage> buildPipelineStages(BioSample bioSample, BioAnalysisPipeline bioAnalysisPipeline,
            Map<String, Object> pipelineParams) {

        if (bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS || bioAnalysisPipeline.getPipelineType() == PIPELINE_VIRUS_COVID) {
            try {
                List<BioPipelineStage> stages = BioPipelineStagesBuilder.buildVirusStages(bioAnalysisPipeline.getPipelineId(), bioAnalysisPipeline.getPipelineType(), bioSample, pipelineParams);
                return stages;
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                return null;
            }
        }else {
            return null;
        }

    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Boolean> pipelineStart(long runSampleId) {


        
        BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
        bioPipelineStageExample.createCriteria().andPipelineIdEqualTo(pid).andStageIndexEqualTo(0);
        List<BioPipelineStage> stages = this.bioPipelineStageMapper.selectByExample(bioPipelineStageExample);
        if (stages == null || stages.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线");
        }
        BioPipelineStage firstStage = stages.get(0);
        BioPipelineStage updatedFirstStage = new BioPipelineStage();
        updatedFirstStage.setStatus(PIPELINE_STAGE_STATUS_QUEUING);
        updatedFirstStage.setStageId(firstStage.getStageId());
        int updateRes = this.bioPipelineStageMapper.updateByPrimaryKeySelective(updatedFirstStage);
        if(updateRes < 1){
            return new Result<Boolean>(Result.INTERNAL_FAIL, false, "流水线启动失败");
        }

        firstStage.setStatus(PIPELINE_STAGE_STATUS_RUNNING);

        boolean addRes = this.pipelineStageTaskDispatcher.addTask(firstStage);
        

        return new Result<Boolean>(Result.SUCCESS, true, null);
        
    }

    @Async
    @Transactional(rollbackFor = Exception.class)
    public void pipelineStageDone(StageRunResult stageRunResult) {
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        if (!stageRunResult.isSuccess()) {
            bioPipelineStage.setStatus(PIPELINE_STAGE_STATUS_FAIL);
            BioPipelineStageExample bioPipelineStageExample = new BioPipelineStageExample();
            int res = this.bioPipelineStageMapper.updateByPrimaryKey(bioPipelineStage);
            // assume success here;
            return;
        }

        if (bioPipelineStage.getStageType() == PIPELINE_STAGE_QC) {
            handleQcStage(stageRunResult);
        }
    }




    // params[0]: local path
    // params[1]: object name
    private boolean batchUploadObjectsFromLocal(Map<String, String> params) {
        return false;
    }

    private void handleQcStage(StageRunResult stageRunResult) {

        Map<String, String> outputPathMap = stageRunResult.getOutputPath();
        BioPipelineStage bioPipelineStage = stageRunResult.getStage();
        String qcR1Path = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUT_R1);
        String qcR2Path = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUT_R2);
        boolean hasR2 = qcR2Path != null;

        String qcJsonPath = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUI_JSON);
        String qcHTMLPath = outputPathMap.get(PIPELINE_STAGE_QC_OUTPUT_HTML);

        String format = "%s/%d/%d/%s";
        String r1OutputPath = String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                bioPipelineStage.getStageIndex(), qcR1Path.substring(qcR1Path.lastIndexOf("/") + 1));
        String r2OutputPath = !hasR2 ? null
                : String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                        bioPipelineStage.getStageIndex(), qcR2Path.substring(qcR2Path.lastIndexOf("/") + 1));
        String jsonOutputPath = String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                bioPipelineStage.getStageIndex(), "qc.json");
        String htmlOutputPath = String.format(format, this.stagesOutputBasePath, bioPipelineStage.getStageId(),
                bioPipelineStage.getStageIndex(), "qc.html");

        Map<String, String> params = new HashMap<>();
        params.put(qcR1Path, r1OutputPath);
        if (hasR2) {
            params.put(qcR2Path, r2OutputPath);
        }
        params.put(qcJsonPath, jsonOutputPath);
        params.put(qcHTMLPath, htmlOutputPath);

        boolean uploadSuccess = this.batchUploadObjectsFromLocal(params);
        if (!uploadSuccess) {
            // todo
        } else {
            outputPathMap.clear();
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R1, r1OutputPath);
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_R2, r2OutputPath);
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUI_JSON, jsonOutputPath);
            outputPathMap.put(PIPELINE_STAGE_QC_OUTPUT_HTML, htmlOutputPath);
            try {
                String outputPathMapJson = this.jsonMapper.writeValueAsString(outputPathMap);
                bioPipelineStage.setStatus(PIPELINE_STAGE_STATUS_FINISHED);
                bioPipelineStage.setOutputUrl(outputPathMapJson);
                this.bioPipelineStageMapper.updateByPrimaryKey(bioPipelineStage);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

    }

}
