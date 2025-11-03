package com.xjtlu.bio.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioAnalysisPipeline;
import com.xjtlu.bio.entity.BioPipelineStage;
import com.xjtlu.bio.entity.BioSample;
import com.xjtlu.bio.entity.BioSampleExample;
import com.xjtlu.bio.mapper.BioAnalysisPipelineMapper;
import com.xjtlu.bio.mapper.BioAnalysisStageMapperExtension;
import com.xjtlu.bio.mapper.BioPipelineStageMapper;
import com.xjtlu.bio.mapper.BioSampleMapper;
import com.xjtlu.bio.utils.ParameterUtil;

import jakarta.annotation.Resource;

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

    private Set<Integer> bioAnalysisPipelineLockSet = ConcurrentHashMap.newKeySet();

    private ObjectMapper jsonMapper = new ObjectMapper();

    public static final int PIPELINE_VIRUS = 0;
    public static final int PIPELINE_VIRUS_COVID = 1;
    public static final int PIPELINE_VIRUS_BACKTERIA = 2;


    public static final int PIPELINE_STAGE_STATUS_PENDING = 0;
    public static final int PIPELINE_STAGE_STATUS_RUNNING = 1;
    public static final int PIPELINE_STAGE_STATUS_FAIL = 2;
    public static final int PIPELINE_STAGE_STATUS_FINISHED = 3;


    public static final String PIPELINE_STAGE_QC_INPUT_R1 = "r1";
    public static final String PIPELINE_STAGE_QC_INPUT_R2 = "r2";

    public static final String PIPELINE_STAGE_QC_OUTPUT_R1 = "trimmed_r1";
    public static final String PIPELINE_STAGE_QC_OUTPUT_R2 = "trimmed_r2";
    public static final String PIPELINE_STAGE_QC_OUTPUI_JSON = "qc_json";
    public static final String PIPELINE_STAGE_QC_OUTPUT_HTML = "qc_html";


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

    private Result<BioSample> createVirusPipeline(int type, boolean sampleIsPair, String sampleName,
            long sampleProjectId, int sampleType, Map<String, Object> pipelineStageParams) {

        BioAnalysisPipeline bAnalysisPipeline = new BioAnalysisPipeline();
        bAnalysisPipeline.setPipelineType(type);
        int res = this.bioAnalysisPipelineMapper.insertSelective(bAnalysisPipeline);
        if (res <= 0) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "内部错误");
        }

        Result<BioSample> createSampleResult = sampleService.createSample(sampleIsPair, sampleName, sampleProjectId,
                bAnalysisPipeline.getPipelineId(), sampleType);

        int status = createSampleResult.getStatus();
        if (status != Result.SUCCESS) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return createSampleResult;
        }

        long sid = createSampleResult.getData().getSid();

        List<BioPipelineStage> stages = null;
        try {
            stages = createVirusPipelineStages(bAnalysisPipeline.getPipelineId(),
                    createSampleResult.getData(), type, pipelineStageParams);
        } catch (JsonProcessingException e) {
            // TODO Auto-generated catch block
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "内部错误");
        }

        res = bioAnalysisStageMapperExtension.batchInsert(stages);
        if (res != stages.size()) {
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new Result<BioSample>(Result.INTERNAL_FAIL, null, "内部错误");
        }

        return createSampleResult;
    }

    private List<BioPipelineStage> createVirusPipelineStages(long pid, BioSample bioSample, int type,
            Map<String, Object> pipelineStageParams) throws JsonProcessingException {
        ArrayList<BioPipelineStage> stages = new ArrayList<>(8);
        String qcInputRead1 = bioSample.getRead1Url();
        String qcInputRead2 = bioSample.getRead2Url();

        Map<String, Object> stageParams = new HashMap<>();
        String refSeqKey = "refSeq";
        String refSeq = ParameterUtil.getStrFromMap(refSeqKey, pipelineStageParams);
        stageParams.put(refSeqKey, refSeq);


        int index = 0;
        BioPipelineStage qc = new BioPipelineStage();
        qc.setStageIndex(index);
        qc.setStageName("质控(QC)");
        qc.setStageType(PIPELINE_STAGE_QC);
        qc.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        qc.setPipelineId(pid);
        stages.add(qc);

        index++;

        BioPipelineStage assembly = new BioPipelineStage();
        assembly.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        assembly.setPipelineId(pid);
        assembly.setStageIndex(index);



        String jsonedParams = jsonMapper.writeValueAsString(stageParams);
        if (StringUtils.isBlank(refSeq)) {
            assembly.setStageType(PIPELINE_STAGE_ASSEMBLY);
            assembly.setStageName("组装");
        }else {
            assembly.setStageType(PIPELINE_STAGE_MAPPING);
            
            assembly.setParameters(jsonedParams);
            assembly.setStageName("参考");
        }

        stages.add(assembly);
        index++;

        if (refSeq == null && type != PIPELINE_VIRUS_COVID) {
            return stages;
        }

        BioPipelineStage varient = new BioPipelineStage();
        varient.setStageName("变异检测");
        varient.setPipelineId(pid);
        varient.setStageIndex(index);
        varient.setParameters(jsonedParams);
        varient.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        varient.setStatus(PIPELINE_STAGE_STATUS_PENDING);

        stages.add(varient);

        index++;


        BioPipelineStage consensus = new BioPipelineStage();
        consensus.setStageName("生成一致性序列");
        consensus.setPipelineId(pid);
        consensus.setStageIndex(index);
        consensus.setParameters(jsonedParams);
        consensus.setStageType(PIPELINE_STAGE_VARIANT_CALL);
        consensus.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stages.add(consensus);


        if (type != PIPELINE_VIRUS_COVID) {
            return stages;
        }

        BioPipelineStage snp = new BioPipelineStage();
        snp.setStageName("SNP注释");
        snp.setPipelineId(pid);
        snp.setStageIndex(index);
        snp.setParameters(jsonedParams);
        snp.setStageType(PIPELINE_STAGE_SNP_CORE);
        snp.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);

        index++;

        BioPipelineStage depthConverage = new BioPipelineStage();
        depthConverage.setStageName("深度分布图");
        depthConverage.setPipelineId(pid);
        depthConverage.setStageIndex(index);
        depthConverage.setParameters(jsonedParams);
        depthConverage.setStageType(PIPELINE_STAGE_DEPTH_COVERAGE);
        depthConverage.setStatus(PIPELINE_STAGE_STATUS_PENDING);
        stages.add(snp);

        index++;
        

        return stages;

    }

    @Transactional(rollbackFor = Exception.class)
    public Result<Long> createPipeline(int pipelineType, boolean sampleIsPair, int sampleName, int sampleProjectId,
            int sampleType, Map<String, Object> pipelineStageParams, Map<String, Object> parameters) {

        if (pipelineType == PIPELINE_VIRUS || pipelineType == PIPELINE_VIRUS_COVID) {
            return this.createVirusPipeline(pipelineType, sampleIsPair, sampleName, sampleProjectId, sampleType,
                    parameters);
        }
        return null;
    }

    private static List<BioPipelineStage> buildPipelineStages(int pipelineType) {

    }

    @Transactional
    public Result<Boolean> pipelineStart(int pid, int sid) {
        if (!this.bioAnalysisPipelineLockSet.add(pid)) {
            return new Result<Boolean>(Result.DUPLICATE_OPERATION, false, "流水线不能重复启动");
        }
        BioAnalysisPipeline bioAnalysisPipeline = this.bioAnalysisPipelineMapper.selectByPrimaryKey(pid);
        if (bioAnalysisPipeline == null) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到分析流水线");
        }

        BioSampleExample bioSampleExample = new BioSampleExample();
        bioSampleExample.createCriteria().andSidEqualTo(sid).andPipelineIdEqualTo(pid);
        List<BioSample> bioSamples = this.bioSampleMapper.selectByExample(bioSampleExample);
        if (bioSamples == null || bioSamples.isEmpty()) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "未找到流水线对应样本");
        }
        if (bioSamples.size() > 1) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "找到多条流水线对应样本");
        }

        BioSample bioSample = bioSamples.get(0);
        if (bioSample.getRead1Url() == null || (bioSample.getIsPair() && bioSample.getRead2Url() == null)) {
            return new Result<Boolean>(Result.BUSINESS_FAIL, false, "样本数据未完全上传");
        }

        List<BioPipelineStage> stages = buildPipelineStages(bioAnalysisPipeline.getPipelineType());

    }

    public void pipelineStageDone(int pid) {

    }

}
